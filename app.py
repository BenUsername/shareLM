import gradio as gr
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, date
import requests
from collections import defaultdict
import duckdb
from huggingface_hub import HfFileSystem

HF_DATASET_API = 'https://datasets-server.huggingface.co/rows'
DATASET_NAME = 'shachardon/ShareLM'
HF_PARQUET_PATH = "hf://datasets/shachardon/ShareLM@~parquet/**/**/*.parquet"

# Initialize Hugging Face filesystem
fs = HfFileSystem()

def get_parquet_urls():
    """Get actual HTTP URLs for parquet files from Hugging Face"""
    try:
        # Use HfFileSystem to get parquet file URLs
        # Try multiple path patterns
        parquet_files = []
        patterns = [
            "datasets/shachardon/ShareLM/*/train/*.parquet",
            "datasets/shachardon/ShareLM/**/*.parquet",
            "datasets/shachardon/ShareLM/**/train/*.parquet"
        ]
        
        for pattern in patterns:
            try:
                files = fs.glob(pattern)
                if files:
                    parquet_files = files
                    break
            except Exception as e:
                print(f"Error with pattern {pattern}: {e}")
                continue
        
        if not parquet_files:
            raise Exception("No parquet files found with any pattern")
        
        # Convert to HTTP URLs
        urls = []
        for file_path in parquet_files[:10]:  # Limit to first 10 files for performance
            try:
                url = fs.url(file_path)
                if url and url.startswith('http'):
                    urls.append(url)
            except Exception as e:
                print(f"Error converting {file_path} to URL: {e}")
                continue
        
        if urls:
            return urls
        else:
            raise Exception("No valid parquet URLs found")
    except Exception as e:
        print(f"Error getting parquet URLs: {e}")
        raise

def fetch_dataset_with_duckdb(max_rows=None, min_date=None, max_date=None, selected_sources=None):
    """Fetch data from HuggingFace dataset using DuckDB with HTTP URLs"""
    try:
        con = duckdb.connect()
        
        # Install and load httpfs extension for remote file access
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        
        # Get parquet file URLs
        parquet_urls = get_parquet_urls()
        if not parquet_urls:
            raise Exception("No parquet files found")
        
        # Build UNION query for multiple parquet files
        # Escape single quotes in URLs to prevent SQL injection
        table_queries = []
        for url in parquet_urls:
            # Escape single quotes by doubling them (SQL standard)
            escaped_url = url.replace("'", "''")
            table_queries.append(f"SELECT * FROM read_parquet('{escaped_url}')")
        
        base_query = " UNION ALL ".join(table_queries)
        
        # Build the query with filters
        query = f"""
        SELECT *
        FROM ({base_query}) AS data
        WHERE 1=1
        """
        
        # Note: Date filtering will be done in Python after we see the actual column names
        # This avoids SQL errors if the timestamp column has a different name
        
        # Add source filter if provided
        if selected_sources and len(selected_sources) > 0:
            # Escape single quotes to prevent SQL injection
            escaped_sources = [s.replace("'", "''") for s in selected_sources]
            sources_str = "', '".join(escaped_sources)
            query += f" AND source IN ('{sources_str}')"
        
        # Add limit if specified
        if max_rows:
            query += f" LIMIT {max_rows}"
        
        # Execute query and fetch results
        result = con.execute(query).fetchdf()
        con.close()
        
        # Convert DataFrame to list of dicts (matching old format)
        rows = []
        for _, row in result.iterrows():
            rows.append({'row': row.to_dict()})
        
        return rows
    except Exception as e:
        print(f"Error fetching data with DuckDB: {e}")
        raise

def fetch_dataset_sample(max_rows=500):
    """Fetch a sample from the Hugging Face dataset using DuckDB"""
    return fetch_dataset_with_duckdb(max_rows=max_rows)

def process_data(max_rows, min_date, max_date, selected_sources):
    """Process dataset and return charts with filters using DuckDB"""
    try:
        # Fetch data (source filter is done in SQL, date filter in Python)
        rows = fetch_dataset_with_duckdb(max_rows=max_rows, min_date=None, max_date=None, selected_sources=selected_sources)
        
        if not rows:
            return None, None, "No data fetched. Please try again."
        
        # Parse date filters
        min_date_obj = None
        max_date_obj = None
        if min_date:
            try:
                min_date_obj = datetime.strptime(min_date, '%Y-%m-%d').date()
            except:
                pass
        if max_date:
            try:
                max_date_obj = datetime.strptime(max_date, '%Y-%m-%d').date()
            except:
                pass
        
        source_counts = defaultdict(int)
        time_series = defaultdict(int)
        filtered_count = 0
        
        # Find the timestamp field name by checking first row
        timestamp_field = None
        if rows:
            first_row_data = rows[0].get('row', rows[0]) if isinstance(rows[0], dict) else rows[0]
            # Debug: print available fields
            print(f"Available fields in first row: {list(first_row_data.keys())}")
            timestamp_fields = ['timestamp', 'date', 'created_at', 'time', 'created', 'ts', 'datetime']
            for field in timestamp_fields:
                if field in first_row_data:
                    timestamp_field = field
                    print(f"Found timestamp field: {field}")
                    break
            if not timestamp_field:
                # Try to find any field that looks like a date
                for key in first_row_data.keys():
                    if 'time' in key.lower() or 'date' in key.lower() or 'created' in key.lower():
                        timestamp_field = key
                        print(f"Found potential timestamp field: {key}")
                        break
        
        for row in rows:
            row_data = row.get('row', row) if isinstance(row, dict) else row
            
            # Filter by source (already done in SQL, but double-check)
            source = row_data.get('source', 'unknown')
            if selected_sources and len(selected_sources) > 0 and source not in selected_sources:
                continue
            
            # Filter by date in Python
            row_date = None
            if timestamp_field and timestamp_field in row_data and row_data[timestamp_field] is not None:
                try:
                    timestamp_val = str(row_data[timestamp_field])
                    # Try different date formats
                    if 'T' in timestamp_val:
                        # ISO format
                        dt = datetime.fromisoformat(timestamp_val.replace('Z', '+00:00').split('.')[0])
                    elif ' ' in timestamp_val:
                        # Datetime string
                        dt = datetime.strptime(timestamp_val.split('.')[0], '%Y-%m-%d %H:%M:%S')
                    else:
                        # Date only format
                        dt = datetime.strptime(timestamp_val, '%Y-%m-%d')
                    
                    row_date = dt.date()
                    
                    # Apply date filters
                    if min_date_obj and row_date < min_date_obj:
                        continue
                    if max_date_obj and row_date > max_date_obj:
                        continue
                except Exception as e:
                    pass
            
            filtered_count += 1
            
            # Count by source
            source_counts[source] += 1
            
            # Count by date
            if row_date:
                date_key = row_date.strftime('%Y-%m-%d')
                time_series[date_key] += 1
        
        # Create source breakdown pie chart
        if source_counts:
            sources = list(source_counts.keys())
            values = list(source_counts.values())
            
            fig_pie = go.Figure(data=[go.Pie(
                labels=sources,
                values=values,
                hole=0.4,
                textinfo='label+percent',
                textposition='outside'
            )])
            fig_pie.update_layout(
                title="Source Breakdown",
                height=500,
                showlegend=True
            )
        else:
            fig_pie = None
        
        # Create time series chart
        if time_series:
            sorted_dates = sorted(time_series.keys())
            counts = [time_series[date] for date in sorted_dates]
            
            fig_line = go.Figure()
            fig_line.add_trace(go.Scatter(
                x=sorted_dates,
                y=counts,
                mode='lines+markers',
                name='Conversations',
                line=dict(width=2)
            ))
            fig_line.update_layout(
                title="Total Count Over Time",
                xaxis_title="Date",
                yaxis_title="Count",
                height=500,
                hovermode='x unified'
            )
        else:
            fig_line = None
        
        total = sum(source_counts.values())
        info = f"Fetched {len(rows)} rows\nAfter filters: {filtered_count} rows\nTotal conversations: {total:,}\nSources: {len(source_counts)}\nTime points: {len(time_series)}"
        
        return (fig_pie, fig_line, info)
        
    except Exception as e:
        return (None, None, f"Error: {str(e)}")

def get_available_sources(max_rows=1000):
    """Get list of available sources for the filter using DuckDB"""
    try:
        con = duckdb.connect()
        
        # Install and load httpfs extension for remote file access
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")
        
        # Get parquet file URLs
        parquet_urls = get_parquet_urls()
        if not parquet_urls:
            return []
        
        # Build UNION query for multiple parquet files
        # Escape single quotes in URLs to prevent SQL injection
        table_queries = []
        for url in parquet_urls:
            # Escape single quotes by doubling them (SQL standard)
            escaped_url = url.replace("'", "''")
            table_queries.append(f"SELECT DISTINCT source FROM read_parquet('{escaped_url}')")
        
        base_query = " UNION ".join(table_queries)
        
        # Query to get distinct sources
        query = f"""
        SELECT DISTINCT source
        FROM ({base_query}) AS data
        ORDER BY source
        """
        
        result = con.execute(query).fetchdf()
        con.close()
        
        sources = result['source'].tolist() if not result.empty else []
        return sources
    except Exception as e:
        print(f"Error getting sources with DuckDB: {e}")
        return []

def create_interface():
    """Create the Gradio interface"""
    with gr.Blocks(title="ShareLM Dataset Analysis", theme=gr.themes.Soft()) as demo:
        gr.Markdown("# ShareLM Dataset Analysis")
        gr.Markdown("Analyzing conversations from the ShareLM Hugging Face dataset")
        
        with gr.Row():
            with gr.Column(scale=1):
                max_rows = gr.Slider(
                    minimum=100,
                    maximum=10000,
                    value=1000,
                    step=100,
                    label="Number of Rows to Fetch",
                    info="Maximum number of rows to fetch from the dataset"
                )
                
                min_date = gr.Textbox(
                    label="Min Date (YYYY-MM-DD)",
                    placeholder="e.g., 2023-01-01",
                    info="Optional: Filter by minimum date"
                )
                
                max_date = gr.Textbox(
                    label="Max Date (YYYY-MM-DD)",
                    placeholder="e.g., 2023-12-31",
                    info="Optional: Filter by maximum date"
                )
                
                # Initialize with empty, will be populated on first load
                source_filter = gr.CheckboxGroup(
                    label="Filter by Source",
                    choices=[],
                    value=[],
                    info="Select sources to include (leave empty for all). Sources will be populated after first data load."
                )
                
                btn = gr.Button("Load & Analyze Data", variant="primary", size="lg")
            
            with gr.Column(scale=2):
                with gr.Row():
                    with gr.Column():
                        pie_chart = gr.Plot(label="Source Breakdown")
                    with gr.Column():
                        line_chart = gr.Plot(label="Time Series")
                
                info_text = gr.Textbox(label="Statistics", lines=4, interactive=False)
        
        def analyze_with_source_update(max_rows_val, min_date_val, max_date_val, selected_sources_val):
            """Analyze data and update source filter"""
            # Get available sources from the data
            try:
                available_sources = get_available_sources(min(max_rows_val, 1000))
            except Exception as e:
                print(f"Error getting available sources: {e}")
                available_sources = []
            
            # If no sources selected, use all available
            if not selected_sources_val or len(selected_sources_val) == 0:
                selected_sources_val = available_sources
            else:
                # Filter to only include sources that exist
                selected_sources_val = [s for s in selected_sources_val if s in available_sources]
            
            # Analyze data
            results = process_data(max_rows_val, min_date_val, max_date_val, selected_sources_val)
            
            # Return results and updated source filter using gr.update()
            return results[0], results[1], results[2], gr.update(choices=available_sources, value=selected_sources_val)
        
        btn.click(
            fn=analyze_with_source_update,
            inputs=[max_rows, min_date, max_date, source_filter],
            outputs=[pie_chart, line_chart, info_text, source_filter]
        )
        
        # Load data on startup with default values
        def initial_load():
            try:
                sources = get_available_sources(1000)
                results = process_data(1000, None, None, sources if sources else [])
                return results[0], results[1], results[2], gr.update(choices=sources, value=sources if sources else [])
            except Exception as e:
                import traceback
                error_msg = f"Error loading initial data: {str(e)}\n{traceback.format_exc()}"
                return None, None, error_msg, gr.update(choices=[], value=[])
        
        demo.load(
            fn=initial_load,
            outputs=[pie_chart, line_chart, info_text, source_filter]
        )
    
    return demo

if __name__ == "__main__":
    demo = create_interface()
    demo.launch()

