# ShareLM Dataset Analysis Dashboard

A Next.js application for analyzing the ShareLM Hugging Face dataset (12GB) with interactive visualizations. The app uses serverless functions to process the dataset and displays:

- **Doughnut chart**: Breakdown of conversations by source
- **Time series chart**: Total count of conversations over time

## Features

- Streams and processes large datasets efficiently using Hugging Face Datasets Server API
- In-memory caching to avoid reprocessing on every request
- Responsive dashboard with beautiful charts using Recharts
- Error handling and timeout management for large dataset processing
- Deployed on Vercel serverless functions

## Setup

1. Install dependencies:
```bash
npm install
```

2. Run the development server:
```bash
npm run dev
```

3. Open [http://localhost:3000](http://localhost:3000) in your browser

## Deployment to Vercel

1. Push your code to GitHub
2. Import the project in Vercel
3. Deploy (no environment variables needed for basic functionality)

The app will automatically:
- Cache aggregated statistics to avoid reprocessing
- Handle timeouts gracefully by processing smaller samples
- Display loading states and error messages

## Technical Details

- **Framework**: Next.js 14 with TypeScript
- **Charts**: Recharts
- **Data Source**: Hugging Face Datasets Server API
- **Caching**: In-memory cache (1 hour TTL)
- **Timeout Management**: 25-second timeout with fallback to smaller samples

## Notes

- Due to Vercel's serverless function timeout limits (10s free, 50s pro), the app processes a sample of the dataset (10,000-50,000 rows) rather than the full 3.5M rows
- Results are cached for 1 hour to improve performance
- The dataset is accessed via Hugging Face's Datasets Server API

