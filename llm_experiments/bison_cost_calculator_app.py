import streamlit as st

# Set up the pricing for each model
MODEL_PRICING = {
    'PaLM 2 Text Bison': {'input': 0.0010, 'output': 0.0010},
    'PaLM 2 Chat Bison': {'input': 0.0005, 'output': 0.0005},
    'Embeddings Gecko': {'input': 0.0001, 'output': 0},
    'Code Generation': {'input': 0.0005, 'output': 0.0005},
    'Code Chat': {'input': 0.0005, 'output': 0.0005},
    'Code Completion': {'input': 0.0005, 'output': 0.0005},
}

st.title("Vertex AI Cost Calculator")

# Input for model selection
model = st.selectbox(
    'Select the Vertex AI model',
    list(MODEL_PRICING.keys())
)

# Input for number of characters in input and output
input_chars = st.number_input('Number of characters in input', min_value=0)
output_chars = st.number_input('Number of characters in output', min_value=0)

# Calculate the cost
input_cost = (input_chars / 1000) * MODEL_PRICING[model]['input']
output_cost = (output_chars / 1000) * MODEL_PRICING[model]['output']
total_cost = input_cost + output_cost

# Display the cost
st.subheader("Cost")
st.write(f'Input cost: ${input_cost:.4f}')
st.write(f'Output cost: ${output_cost:.4f}')
st.write(f'Total cost: ${total_cost:.4f}')
