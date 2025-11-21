'use client';

import { useEffect, useState } from 'react';
import { SourceDoughnutChart } from '@/components/Charts';
import styles from './page.module.css';

interface SourceData {
  name: string;
  value: number;
}

export default function Home() {
  const [sourceData, setSourceData] = useState<SourceData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [processedCount, setProcessedCount] = useState<number>(0);

  useEffect(() => {
    async function fetchData() {
      try {
        setLoading(true);
        setError(null);

        // Fetch source breakdown from API
        const response = await fetch('/api/source-breakdown');
        
        if (!response.ok) {
          throw new Error(`Failed to fetch data: ${response.statusText}`);
        }

        const data = await response.json();

        if (data.error) {
          throw new Error(data.error);
        }

        // Set source breakdown data
        if (data.data && Array.isArray(data.data)) {
          setSourceData(data.data);
        }

        if (data.processedCount) {
          setProcessedCount(data.processedCount);
        }
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
        <p className={styles.subtitle}>Source breakdown from the ShareLM Hugging Face dataset</p>
      </div>

      {error && (
        <div className={styles.errorBanner}>
          <p>Error: {error}</p>
          <button onClick={() => window.location.reload()}>Retry</button>
        </div>
      )}

      <SourceDoughnutChart data={sourceData} loading={loading} />

      {!loading && !error && (
        <div className={styles.statsInfo}>
          <p>Total sources: {sourceData.length}</p>
          <p>Total conversations analyzed: {processedCount.toLocaleString()}</p>
          {processedCount > 0 && (
            <p className={styles.note}>
              Processed {processedCount.toLocaleString()} rows using streaming import
            </p>
          )}
        </div>
      )}
    </main>
  );
}
