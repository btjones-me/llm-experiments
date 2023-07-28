import json
import re
import pandas as pd


def clean_data(df):
    """
    Sorts the DataFrame by 'conversation_id' and 'created_at', replaces non-finite 'conversation_id' values with 0,
    converts the 'conversation_id' column to integer type, and filters out rows where the 'sender' is the same
    as the 'prev_sender'.
    """
    df = df.sort_values(['conversation_id', 'created_at'])
    df['conversation_id'].fillna(0, inplace=True)
    df['conversation_id'] = df['conversation_id'].astype(int)
    df['prev_sender'] = df.groupby('conversation_id')['sender'].shift()
    df = df[df['sender'] != df['prev_sender']]
    df = df.sort_values('conversation_id')
    return df

def is_consecutive(s):
    """Checks if a sequence is consecutive"""
    return (s == range(s.min(), s.max() + 1)).all()

def clean_conversation_ids(df):
    """
    Checks whether the 'id' within each conversation (grouped by 'conversation_id') are consecutive.
    Drops the rows where 'id' is not consecutive and prints the number of rows that were dropped.
    """
    consecutive_ids = df.groupby('conversation_id')['id'].apply(is_consecutive)
    non_consecutive_ids = consecutive_ids[~consecutive_ids].index
    rows_to_drop = df[df['conversation_id'].isin(non_consecutive_ids)].shape[0]
    print(f'Number of rows to be dropped: {rows_to_drop} ({(rows_to_drop / df.shape[0]) * 100:.2f}%)')
    df = df[~df['conversation_id'].isin(non_consecutive_ids)]
    return df

def convert_to_jsonl(df_greater_than_2):
    """
    Converts the DataFrame into a JSONL format where each JSON object represents a conversation.
    """
    df_sorted = df_greater_than_2.sort_values(['conversation_id', 'id'], ascending=[True, False])
    df_sorted[['sender', 'content']] = df_sorted[['sender', 'content']].fillna('')
    grouped = df_sorted.groupby('conversation_id')[['sender', 'content']].apply(
        lambda x: '||'.join(x['sender'] + ": " + x['content']))
    output = []
    for conversation_id, conversation in zip(grouped.index, grouped.values):
        messages = conversation.split('||')[::-1]
        if messages[0].startswith('Agent:'):
            continue
        current_conversation = []
        for message in messages:
            current_conversation.append(message)
            if message.startswith('Agent:'):
                record = {
                    'input_text': ' '.join(current_conversation[:-1]) + ' Agent:',
                    'output_text': current_conversation[-1].split(": ", 1)[1]
                }
                output.append(record)
    jsonl_output = '\n'.join(json.dumps(record) for record in output)
    print(jsonl_output[:500])
    return jsonl_output

def strip_unwanted_text(jsonl_output):
    """
    Removes URLs and contents within angle brackets from the JSONL string.
    """
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    pattern_within_angle_brackets = r'<~~.*?~~>'
    jsonl_output1 = re.sub(url_pattern, '', jsonl_output)
    jsonl_output2 = re.sub(pattern_within_angle_brackets, '', jsonl_output1)
    return jsonl_output2


if __name__ == '__main__':
    # Load and process the data
    df = pd.read_csv('/Users/benjaminjones/Downloads/bquxjob_5fb08910_189997c7634.csv')

    # Drop rows with non-finite 'id' values and convert the 'id' column to integer
    df = df[df['id'].notna()]
    df['id'] = df['id'].astype(int)

    df = clean_data(df)
    df = clean_conversation_ids(df)

    series_greater_than_2 = df.value_counts('conversation_id')[df.value_counts('conversation_id') > 1]
    df_greater_than_2_counts = series_greater_than_2.reset_index()
    df_greater_than_2_counts.columns = ['conversation_id', 'count']

    df_greater_than_2 = df.merge(df_greater_than_2_counts, how='right', on='conversation_id')

    jsonl_output = convert_to_jsonl(df_greater_than_2)
    jsonl_output_cleaned = strip_unwanted_text(jsonl_output)

    print(jsonl_output_cleaned[:500])
