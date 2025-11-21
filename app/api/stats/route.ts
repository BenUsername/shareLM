import { NextResponse } from 'next/server';

// In-memory cache (will be reset on serverless function restart)
let cachedStats: {
  sourceBreakdown: Record<string, number>;
  timeSeries: Record<string, number>;
  lastUpdated: number;
} | null = null;

const CACHE_TTL = 60 * 60 * 1000; // 1 hour in milliseconds

// Hugging Face API endpoint for dataset
const HF_DATASET_API = 'https://datasets-server.huggingface.co/rows';

async function fetchDatasetSample(maxRows: number = 500) {
  // Use Hugging Face Datasets Server API to get a sample
  // API limits length to 100 rows per request, so we fetch in batches
  const MAX_BATCH_SIZE = 100;
  const batches = Math.min(Math.ceil(maxRows / MAX_BATCH_SIZE), 10); // Limit to 10 batches (1000 rows max)
  const allRows: any[] = [];
  
  try {
    for (let i = 0; i < batches; i++) {
      const offset = i * MAX_BATCH_SIZE;
      const length = Math.min(MAX_BATCH_SIZE, maxRows - offset);
      
      if (length <= 0) break;
      
      const url = `${HF_DATASET_API}?dataset=shachardon%2FShareLM&config=default&split=train&offset=${offset}&length=${length}`;
      
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
        const errorText = await response.text();
        throw new Error(`HTTP error! status: ${response.status}, message: ${errorText}`);
      }

      const data = await response.json();
      if (data.rows && Array.isArray(data.rows)) {
        allRows.push(...data.rows);
      }
      
      // Small delay between requests to avoid rate limiting
      if (i < batches - 1) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
    
    return { rows: allRows };
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
    if (cachedStats && Date.now() - cachedStats.lastUpdated < CACHE_TTL) {
      return NextResponse.json({
        sourceBreakdown: cachedStats.sourceBreakdown,
        timeSeries: cachedStats.timeSeries,
        cached: true,
      });
    }

    // Fetch dataset sample from Hugging Face API
    // Start with smaller sample to avoid timeout and API limits
    let datasetData;
    let maxRows = 500; // Will fetch 5 batches of 100 rows
    
    try {
      datasetData = await fetchDatasetSample(maxRows);
    } catch (error) {
      // If error, try with even smaller sample
      if (error instanceof Error) {
        if (error.message.includes('timeout') || error.message.includes('422')) {
          maxRows = 200; // 2 batches of 100 rows
          datasetData = await fetchDatasetSample(maxRows);
        } else {
          throw error;
        }
      } else {
        throw error;
      }
    }
    
    const sourceBreakdown: Record<string, number> = {};
    const timeSeries: Record<string, number> = {};

    // Process the data - handle different response formats
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
      
      // Aggregate by source
      const source = rowData.source || 'unknown';
      sourceBreakdown[source] = (sourceBreakdown[source] || 0) + 1;

      // Aggregate by timestamp (group by date)
      if (rowData.timestamp) {
        try {
          const date = new Date(rowData.timestamp);
          if (!isNaN(date.getTime())) {
            const dateKey = date.toISOString().split('T')[0]; // YYYY-MM-DD
            timeSeries[dateKey] = (timeSeries[dateKey] || 0) + 1;
          }
        } catch (e) {
          // Skip invalid timestamps
        }
      }
    }

    // Sort time series by date
    const sortedTimeSeries: Record<string, number> = {};
    Object.keys(timeSeries)
      .sort()
      .forEach((key) => {
        sortedTimeSeries[key] = timeSeries[key];
      });

    // Update cache
    cachedStats = {
      sourceBreakdown,
      timeSeries: sortedTimeSeries,
      lastUpdated: Date.now(),
    };

    return NextResponse.json({
      sourceBreakdown,
      timeSeries: sortedTimeSeries,
      processedCount,
      cached: false,
      sampleSize: maxRows,
      note: processedCount < maxRows ? 'Full sample processed' : `Processed ${processedCount} rows (limited to avoid timeout)`,
    });
  } catch (error) {
    console.error('Error processing dataset:', error);
    return NextResponse.json(
      { error: 'Failed to process dataset', details: error instanceof Error ? error.message : 'Unknown error' },
      { status: 500 }
    );
  }
}
