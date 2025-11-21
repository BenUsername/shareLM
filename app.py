import gradio as gr
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import requests
from collections import defaultdict

HF_DATASET_API = 'https://datasets-server.huggingface.co/rows'
DATASET_NAME = 'shachardon/ShareLM'

def fetch_dataset_sample(max_rows=500):
    """Fetch a sample from the Hugging Face dataset"""
    MAX_BATCH_SIZE = 100
    batches = min((max_rows + MAX_BATCH_SIZE - 1) // MAX_BATCH_SIZE, 10)
    all_rows = []
    
    for i in range(batches):
        offset = i * MAX_BATCH_SIZE
        length = min(MAX_BATCH_SIZE, max_rows - offset)
        
        if length <= 0:
            break
        
        url = f"{HF_DATASET_API}?dataset={DATASET_NAME.replace('/', '%2F')}&config=default&split=train&offset={offset}&length={length}"
        
        try:
            response = requests.get(url, headers={'Accept': 'application/json'}, timeout=25)
            response.raise_for_status()
            data = response.json()
            
            if data.get('rows') and isinstance(data['rows'], list):
                all_rows.extend(data['rows'])
            
            # Small delay to avoid rate limiting
            if i < batches - 1:
                import time
                time.sleep(0.1)
        except Exception as e:
            print(f"Error fetching batch {i}: {e}")
            if i == 0:
                raise
    
    return all_rows

def process_data():
    """Process dataset and return charts"""
    try:
        rows = fetch_dataset_sample(500)
        
        if not rows:
            return None, "No data fetched. Please try again."
        
        source_counts = defaultdict(int)
        time_series = defaultdict(int)
        
        for row in rows:
            row_data = row.get('row', row) if isinstance(row, dict) else row
            
            # Count by source
            source = row_data.get('source', 'unknown')
            source_counts[source] += 1
            
            # Count by date
            if 'timestamp' in row_data:
                try:
                    date = datetime.fromisoformat(str(row_data['timestamp']).replace('Z', '+00:00'))
                    date_key = date.strftime('%Y-%m-%d')
                    time_series[date_key] += 1
                except:
                    pass
        
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
        info = f"Processed {len(rows)} rows\nTotal conversations: {total:,}\nSources: {len(source_counts)}\nTime points: {len(time_series)}"
        
        return (fig_pie, fig_line, info)
        
    except Exception as e:
        return (None, None, f"Error: {str(e)}")

def create_interface():
    """Create the Gradio interface"""
    with gr.Blocks(title="ShareLM Dataset Analysis", theme=gr.themes.Soft()) as demo:
        gr.Markdown("# ShareLM Dataset Analysis")
        gr.Markdown("Analyzing conversations from the ShareLM Hugging Face dataset")
        
        with gr.Row():
            btn = gr.Button("Load & Analyze Data", variant="primary")
        
        with gr.Row():
            with gr.Column():
                pie_chart = gr.Plot(label="Source Breakdown")
            with gr.Column():
                line_chart = gr.Plot(label="Time Series")
        
        info_text = gr.Textbox(label="Statistics", lines=4, interactive=False)
        
        btn.click(
            fn=process_data,
            outputs=[pie_chart, line_chart, info_text]
        )
        
        # Load data on startup
        demo.load(
            fn=process_data,
            outputs=[pie_chart, line_chart, info_text]
        )
    
    return demo

if __name__ == "__main__":
    demo = create_interface()
    demo.launch()

