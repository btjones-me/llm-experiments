import numpy as np
import pandas as pd
import json

from llm_experiments.utils import here


DATA_PATH = here() / 'data'


# Load the CSV data into a pandas DataFrame
df = pd.read_csv(DATA_PATH / 'messages.csv')
df.columns = ['datetime', 'sender', 'message']

# Reorder the DataFrame based on date in ascending order
df = df.sort_values('datetime')

# Identify the sender name
sender_name = "Sender"  # replace with the actual name

# Initialize an empty list to hold the new rows
new_rows = []

# Initialize variables to hold the current grouped message and its sender
current_message = str(df.iloc[0]['message'] if df.iloc[0]['message'] is not np.nan else '')
current_sender = df.iloc[0]['sender']

# Loop through the DataFrame starting from the second row
for i in range(1, len(df)):
    # If the sender is the same as the current sender, append the message to the current message
    if df.iloc[i]['sender'] == current_sender:
        current_message += ' ' + str(df.iloc[i]['message'] if df.iloc[i]['message'] is not np.nan else '')
    else:
        # If the sender is different, add the current row to the new_rows list
        new_rows.append({
            'datetime': df.iloc[i - 1]['datetime'],
            'sender': current_sender,
            'message': current_message
        })
        # Then start a new current message with the new sender
        current_message = str(df.iloc[i]['message'] if df.iloc[i]['message'] is not np.nan else '')
        current_sender = df.iloc[i]['sender']

# Add the last current message to the new_rows list
new_rows.append({
    'datetime': df.iloc[-1]['datetime'],
    'sender': current_sender,
    'message': current_message
})

# Create a new DataFrame from the list of new rows
grouped_df = pd.DataFrame(new_rows)

# Initialize an empty list to hold our formatted data
data = []

# Loop through the grouped DataFrame with a step of 1
for i in range(0, len(grouped_df) - 1):
    # Only process if the current message is from the sender and the next one is from the recipient
    if grouped_df.iloc[i]['sender'] == sender_name and grouped_df.iloc[i+1]['sender'] != sender_name:
        # Add the sender and recipient messages to the data list in the required format
        data.append({
            'input_text': grouped_df.iloc[i]['message'],
            'context': 'This is a conversation between two individuals in a committed relationship. The sender (the boyfriend) sends a message and the recipient (the girlfriend) responds. Sometimes the sender sends multiple messages in a row.',
            'output_text': grouped_df.iloc[i + 1]['message'],
        })

# Convert the list into JSON Lines format
with open(DATA_PATH / 'messages.jsonl', 'w', encoding='utf-8') as f:
    for entry in data:
        json.dump(entry, f, ensure_ascii=False)
        f.write('\n')

# Load messages.jsonl file and read into pandas df
df_jsonl = pd.read_json(DATA_PATH / 'messages.jsonl', lines=True)
