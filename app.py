import gradio as gr
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, date
import requests
from collections import defaultdict
import duckdb

HF_DATASET_API = 'https://datasets-server.huggingface.co/rows'
DATASET_NAME = 'shachardon/ShareLM'
HF_PARQUET_PATH = "hf://datasets/shachardon/ShareLM@~parquet/**/**/*.parquet"

def fetch_dataset_with_duckdb(max_rows=None, min_date=None, max_date=None, selected_sources=None):
    """Fetch data from HuggingFace dataset using DuckDB with hf:// protocol"""
    try:
        con = duckdb.connect()
        
        # Build the query with filters
        query = f"""
        SELECT *
        FROM '{HF_PARQUET_PATH}'
        WHERE 1=1
        """
        
        # Add date filters if provided
        if min_date:
            query += f" AND CAST(timestamp AS DATE) >= CAST('{min_date}' AS DATE)"
        if max_date:
            query += f" AND CAST(timestamp AS DATE) <= CAST('{max_date}' AS DATE)"
        
        # Add source filter if provided
        if selected_sources and len(selected_sources) > 0:
            sources_str = "', '".join(selected_sources)
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
        rows = fetch_dataset_with_duckdb(max_rows=max_rows, min_date=min_date, max_date=max_date, selected_sources=selected_sources)
        
        if not rows:
            return None, None, "No data fetched. Please try again."
        
        # Parse dates if provided
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
        
        # Get all unique sources first
        all_sources = set()
        for row in rows:
            row_data = row.get('row', row) if isinstance(row, dict) else row
            source = row_data.get('source', 'unknown')
            all_sources.add(source)
        
        # If no sources selected, use all sources
        if not selected_sources or len(selected_sources) == 0:
            selected_sources = list(all_sources)
        
        source_counts = defaultdict(int)
        time_series = defaultdict(int)
        filtered_count = 0
        
        for row in rows:
            row_data = row.get('row', row) if isinstance(row, dict) else row
            
            # Filter by source
            source = row_data.get('source', 'unknown')
            if source not in selected_sources:
                continue
            
            # Filter by date
            row_date = None
            if 'timestamp' in row_data:
                try:
                    dt = datetime.fromisoformat(str(row_data['timestamp']).replace('Z', '+00:00'))
                    row_date = dt.date()
                    
                    if min_date_obj and row_date < min_date_obj:
                        continue
                    if max_date_obj and row_date > max_date_obj:
                        continue
                except:
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
        info = f"Fetched {len(rows)} rows\nFiltered to {filtered_count} rows\nTotal conversations: {total:,}\nSources: {len(source_counts)}\nTime points: {len(time_series)}"
        
        return (fig_pie, fig_line, info)
        
    except Exception as e:
        return (None, None, f"Error: {str(e)}")

def get_available_sources(max_rows=1000):
    """Get list of available sources for the filter using DuckDB"""
    try:
        con = duckdb.connect()
        
        # Query to get distinct sources
        query = f"""
        SELECT DISTINCT source
        FROM '{HF_PARQUET_PATH}'
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
                    maximum=5000,
                    value=500,
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
            except:
                available_sources = []
            
            # If no sources selected, use all available
            if not selected_sources_val or len(selected_sources_val) == 0:
                selected_sources_val = available_sources
            else:
                # Filter to only include sources that exist
                selected_sources_val = [s for s in selected_sources_val if s in available_sources]
            
            # Analyze data
            results = process_data(max_rows_val, min_date_val, max_date_val, selected_sources_val)
            
            # Return results and updated source filter
            return results[0], results[1], results[2], gr.CheckboxGroup(choices=available_sources, value=selected_sources_val)
        
        btn.click(
            fn=analyze_with_source_update,
            inputs=[max_rows, min_date, max_date, source_filter],
            outputs=[pie_chart, line_chart, info_text, source_filter]
        )
        
        # Load data on startup with default values
        def initial_load():
            try:
                sources = get_available_sources(1000)
                results = process_data(500, None, None, sources)
                return results[0], results[1], results[2], gr.CheckboxGroup(choices=sources, value=sources)
            except Exception as e:
                return None, None, f"Error loading initial data: {str(e)}", gr.CheckboxGroup(choices=[], value=[])
        
        demo.load(
            fn=initial_load,
            outputs=[pie_chart, line_chart, info_text, source_filter]
        )
    
    return demo

if __name__ == "__main__":
    demo = create_interface()
    demo.launch()

