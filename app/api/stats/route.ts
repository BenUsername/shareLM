import { NextResponse } from 'next/server';

// In-memory cache (will be reset on serverless function restart)
let cachedStats: {
  sourceBreakdown: Record<string, number>;
  timeSeries: Record<string, number>;
  lastUpdated: number;
} | null = null;

const CACHE_TTL = 60 * 60 * 1000; // 1 hour in milliseconds

// Hugging Face API endpoint for dataset
const HF_DATASET_API = 'https://datasets-server.huggingface.co/parquet';

async function fetchDatasetSample(maxRows: number = 50000) {
  // Use Hugging Face Datasets Server API to get a sample
  // For large datasets, we'll process a sample to avoid timeout
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
    if (cachedStats && Date.now() - cachedStats.lastUpdated < CACHE_TTL) {
      return NextResponse.json({
        sourceBreakdown: cachedStats.sourceBreakdown,
        timeSeries: cachedStats.timeSeries,
        cached: true,
      });
    }

    // Fetch dataset sample from Hugging Face API
    // Start with smaller sample to avoid timeout
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
