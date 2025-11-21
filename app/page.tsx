'use client';

import { useEffect, useState } from 'react';
import { Charts } from '@/components/Charts';
import styles from './page.module.css';

interface SourceData {
  name: string;
  value: number;
}

interface TimeSeriesData {
  date: string;
  count: number;
}

export default function Home() {
  const [sourceData, setSourceData] = useState<SourceData[]>([]);
  const [timeSeriesData, setTimeSeriesData] = useState<TimeSeriesData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchData() {
      try {
        setLoading(true);
        setError(null);

        // Fetch stats from API
        const response = await fetch('/api/stats');
        
        if (!response.ok) {
          throw new Error(`Failed to fetch data: ${response.statusText}`);
        }

        const data = await response.json();

        if (data.error) {
          throw new Error(data.error);
        }

        // Format source breakdown data
        const sourceBreakdown = Object.entries(data.sourceBreakdown || {}).map(([name, value]) => ({
          name,
          value: value as number,
        })).sort((a, b) => b.value - a.value);

        // Format time series data
        const timeSeries = Object.entries(data.timeSeries || {}).map(([date, count]) => ({
          date,
          count: count as number,
        })).sort((a, b) => a.date.localeCompare(b.date));

        setSourceData(sourceBreakdown);
        setTimeSeriesData(timeSeries);
      } catch (err) {
        console.error('Error fetching data:', err);
        setError(err instanceof Error ? err.message : 'An unknown error occurred');
      } finally {
        setLoading(false);
      }
    }

    fetchData();
  }, []);

  return (
    <main className={styles.mainContainer}>
      <div className={styles.header}>
        <h1>ShareLM Dataset Analysis</h1>
        <p className={styles.subtitle}>Analyzing conversations from the ShareLM Hugging Face dataset</p>
      </div>

      {error && (
        <div className={styles.errorBanner}>
          <p>Error: {error}</p>
          <button onClick={() => window.location.reload()}>Retry</button>
        </div>
      )}

      <Charts 
        sourceData={sourceData} 
        timeSeriesData={timeSeriesData} 
        loading={loading} 
      />

      {!loading && !error && (
        <div className={styles.statsInfo}>
          <p>Total sources: {sourceData.length}</p>
          <p>Total time points: {timeSeriesData.length}</p>
          <p>Total conversations analyzed: {sourceData.reduce((sum, item) => sum + item.value, 0).toLocaleString()}</p>
        </div>
      )}
    </main>
  );
}
