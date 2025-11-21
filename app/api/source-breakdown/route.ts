import { NextResponse } from 'next/server';

// In-memory cache
let cachedBreakdown: {
  data: Array<{ name: string; value: number }>;
  lastUpdated: number;
} | null = null;

const CACHE_TTL = 60 * 60 * 1000; // 1 hour

const HF_DATASET_API = 'https://datasets-server.huggingface.co/rows';
const BATCH_SIZE = 100; // Process 100 rows at a time
const MAX_BATCHES = 50; // Limit to 5000 rows total to avoid overwhelming the server

async function* fetchDatasetStream() {
  let offset = 0;
  let batchCount = 0;
  
  while (batchCount < MAX_BATCHES) {
    const url = `${HF_DATASET_API}?dataset=shachardon%2FShareLM&config=default&split=train&offset=${offset}&length=${BATCH_SIZE}`;
    
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000); // 15 second timeout per batch

      const response = await fetch(url, {
        headers: {
          'Accept': 'application/json',
        },
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        if (response.status === 422) {
          // No more data available
          break;
        }
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data = await response.json();
      
      if (!data.rows || data.rows.length === 0) {
        // No more rows
        break;
      }

      yield data.rows;
      
      offset += BATCH_SIZE;
      batchCount++;
      
      // Small delay between requests to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 200));
    } catch (error) {
      if (error instanceof Error && error.name === 'AbortError') {
        console.warn(`Batch timeout at offset ${offset}, stopping stream`);
        break;
      }
      console.error('Error fetching batch:', error);
      throw error;
    }
  }
}

export async function GET() {
  try {
    // Check cache first
    if (cachedBreakdown && Date.now() - cachedBreakdown.lastUpdated < CACHE_TTL) {
      return NextResponse.json({
        data: cachedBreakdown.data,
        cached: true,
      });
    }

    const sourceCounts: Record<string, number> = {};
    let processedCount = 0;

    // Stream and process data in batches
    for await (const batch of fetchDatasetStream()) {
      for (const row of batch) {
        processedCount++;
        
        // Get row data - handle different formats
        let rowData: any = {};
        if (row.row) {
          rowData = row.row;
        } else if (typeof row === 'object') {
          rowData = row;
        }
        
        const source = rowData.source || 'unknown';
        sourceCounts[source] = (sourceCounts[source] || 0) + 1;
      }
    }

    // Convert to array format for chart
    const data = Object.entries(sourceCounts)
      .map(([name, value]) => ({ name, value }))
      .sort((a, b) => b.value - a.value); // Sort by value descending

    // Update cache
    cachedBreakdown = {
      data,
      lastUpdated: Date.now(),
    };

    return NextResponse.json({
      data,
      processedCount,
      cached: false,
    });
  } catch (error) {
    console.error('Error processing source breakdown:', error);
    return NextResponse.json(
      { error: 'Failed to process dataset', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
