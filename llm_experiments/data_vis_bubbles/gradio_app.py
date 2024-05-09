import gradio as gr
import plotly.graph_objs as go

# Create your Plotly figure
def create_figure():
    fig = go.Figure(data=[go.Scatter(
        x=[1, 2, 3, 4],
        y=[10, 11, 12, 13],
        mode='markers',
        marker=dict(
            size=[40, 60, 80, 100],
            color=[10, 20, 30, 40]
        ),
        text=["Point 1", "Point 2", "Point 3", "Point 4"]  # Text to show on hover
    )])
    fig.update_layout(clickmode='event+select')
    return fig

# Define what happens when you launch the app
def update_output():
    # Return the figure created by create_figure()
    return create_figure()

# Create the Gradio interface
iface = gr.Interface(
    fn=update_output,
    inputs=[],
    outputs=[gr.Plot()],
    live=True
)

# Run the app
iface.launch()
