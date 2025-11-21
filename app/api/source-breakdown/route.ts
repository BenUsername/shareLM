import { NextResponse } from 'next/server';

// In-memory cache
let cachedBreakdown: {
  data: Array<{ name: string; value: number }>;
  lastUpdated: number;
} | null = null;

const CACHE_TTL = 60 * 60 * 1000; // 1 hour

const HF_DATASET_API = 'https://datasets-server.huggingface.co/parquet';

async function fetchDatasetSample(maxRows: number = 50000) {
  const url = `${HF_DATASET_API}?dataset=shachardon%2FShareLM&config=default&split=train&offset=0&length=${maxRows}`;
  
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 25000); // 25 second timeout

    const response = await fetch(url, {
      headers: {
        'Accept': 'application/json',
      },
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data;
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error('Request timeout - dataset is too large to process in time limit');
    }
    console.error('Error fetching from HF API:', error);
    throw error;
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

    // Fetch dataset sample - start with smaller sample
    let datasetData;
    let maxRows = 10000;
    
    try {
      datasetData = await fetchDatasetSample(maxRows);
    } catch (error) {
      // If timeout, try with even smaller sample
      if (error instanceof Error && error.message.includes('timeout')) {
        maxRows = 5000;
        datasetData = await fetchDatasetSample(maxRows);
      } else {
        throw error;
      }
    }
    
    const sourceCounts: Record<string, number> = {};
    
    // Handle different response formats
    let rows: any[] = [];
    if (datasetData.rows) {
      rows = datasetData.rows;
    } else if (Array.isArray(datasetData)) {
      rows = datasetData;
    } else if (datasetData.data) {
      rows = datasetData.data;
    }
    
    let processedCount = 0;

    for (const row of rows) {
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
