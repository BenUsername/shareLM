'use client';

import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip, Legend } from 'recharts';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, ResponsiveContainer as LineResponsiveContainer } from 'recharts';
import styles from './Charts.module.css';

interface SourceData {
  name: string;
  value: number;
}

interface ChartsProps {
  sourceData: SourceData[];
  timeSeriesData: Array<{ date: string; count: number }>;
  loading: boolean;
}

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884d8', '#82ca9d', '#ffc658', '#ff7300'];

export function SourceDoughnutChart({ data, loading }: { data: SourceData[]; loading: boolean }) {
  if (loading) {
    return (
      <div className={styles.chartContainer}>
        <div className={styles.loadingState}>Loading source breakdown...</div>
      </div>
    );
  }

  if (!data || data.length === 0) {
    return (
      <div className={styles.chartContainer}>
        <div className={styles.errorState}>No data available</div>
      </div>
    );
  }

  return (
    <div className={styles.chartContainer}>
      <h2>Source Breakdown</h2>
      <ResponsiveContainer width="100%" height={400}>
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            labelLine={false}
            label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`}
            outerRadius={120}
            innerRadius={60}
            fill="#8884d8"
            dataKey="value"
          >
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
            ))}
          </Pie>
          <Tooltip />
          <Legend />
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}

export function TimeSeriesChart({ data, loading }: { data: Array<{ date: string; count: number }>; loading: boolean }) {
  if (loading) {
    return (
      <div className={styles.chartContainer}>
        <div className={styles.loadingState}>Loading time series data...</div>
      </div>
    );
  }

  if (!data || data.length === 0) {
    return (
      <div className={styles.chartContainer}>
        <div className={styles.errorState}>No data available</div>
      </div>
    );
  }

  // Format data for chart (sample if too many points)
  const chartData = data.length > 100 
    ? data.filter((_, i) => i % Math.ceil(data.length / 100) === 0)
    : data;

  return (
    <div className={styles.chartContainer}>
      <h2>Total Count Over Time</h2>
      <LineResponsiveContainer width="100%" height={400}>
        <LineChart data={chartData}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis 
            dataKey="date" 
            angle={-45}
            textAnchor="end"
            height={100}
            interval="preserveStartEnd"
          />
          <YAxis />
          <Tooltip />
          <Line type="monotone" dataKey="count" stroke="#8884d8" strokeWidth={2} dot={{ r: 3 }} />
        </LineChart>
      </LineResponsiveContainer>
    </div>
  );
}

export function Charts({ sourceData, timeSeriesData, loading }: ChartsProps) {
  return (
    <div className={styles.chartsWrapper}>
      <SourceDoughnutChart data={sourceData} loading={loading} />
      <TimeSeriesChart data={timeSeriesData} loading={loading} />
    </div>
  );
}
