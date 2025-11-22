---
title: ShareLM Dataset Analysis
emoji: 📊
colorFrom: purple
colorTo: pink
sdk: gradio
pinned: false
---

# ShareLM Dataset Analysis

Interactive dashboard for analyzing the ShareLM Hugging Face dataset with visualizations of:
- Source breakdown (pie chart)
- Time series of conversations over time

The app fetches data from the ShareLM dataset and displays interactive charts using Plotly.

## MongoDB Migration

To load the Hugging Face dataset into MongoDB for building custom dashboards:

1. **Set up MongoDB connection** (create a `.env` file):
   ```bash
   # For MongoDB Atlas (free tier)
   MONGODB_URI=mongodb+srv://<username>:<password>@<cluster>.mongodb.net/
   
   # For local MongoDB
   MONGODB_URI=mongodb://localhost:27017/
   
   MONGODB_DATABASE=sharelm
   MONGODB_COLLECTION=conversations
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the migration script**:
   ```bash
   python migrate_to_mongodb.py
   ```

   Options:
   - `--max-files N`: Limit to first N parquet files (useful for testing)
   - `--batch-size N`: Set batch size for inserts (default: 1000)

   Example:
   ```bash
   python migrate_to_mongodb.py --max-files 5 --batch-size 500
   ```

4. **Access your data in MongoDB**:
   - The script creates indexes on `source`, `timestamp`, `date`, `created_at` for fast queries
   - All documents include an `_imported_at` field with the import timestamp
   - You can now build dashboards using MongoDB's free tier tools or connect any dashboard tool to your MongoDB collection
