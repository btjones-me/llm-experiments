import json
import re

import pandas as pd




def clean_data(df):
    # Sort the DataFrame by 'conversation_id' and 'created_at'
    df = df.sort_values(['conversation_id', 'created_at'])

    # Replace non-finite 'conversation_id' values with 0
    df['conversation_id'].fillna(0, inplace=True)

    # Convert the 'conversation_id' column to integer type
    df['conversation_id'] = df['conversation_id'].astype(int)

    # Create a new column 'prev_sender' that contains the 'sender' value of the previous row within each group
    df['prev_sender'] = df.groupby('conversation_id')['sender'].shift()

    # Filter the DataFrame to keep only the rows where 'sender' is not equal to 'prev_sender'
    df = df[df['sender'] != df['prev_sender']]

    df = df.sort_values('conversation_id')

    return df

def is_consecutive(s):
    # Define a function to check if a sequence is consecutive
    return (s == range(s.min(), s.max() + 1)).all()
def clean_conversation_ids(df):

    # Group the DataFrame by 'conversation_id' and check if 'id' is consecutive in each group
    consecutive_ids = df.groupby('conversation_id')['id'].apply(is_consecutive)

    # Get the 'conversation_id' values where 'id' is not consecutive
    non_consecutive_ids = consecutive_ids[~consecutive_ids].index

    # Calculate the number of rows to be dropped
    rows_to_drop = df[df['conversation_id'].isin(non_consecutive_ids)].shape[0]

    # Print the number of rows to be dropped and the percentage
    print(f'Number of rows to be dropped: {rows_to_drop} ({(rows_to_drop / df.shape[0]) * 100:.2f}%)')

    # Remove the rows where 'conversation_id' is in 'non_consecutive_ids'
    df = df[~df['conversation_id'].isin(non_consecutive_ids)]

    return df


def convert_to_jsonl(df_greater_than_2):
    # Rerun the above cell with 'Agent: ' appended to the end of each 'input_text'

    # 1. Sort the DataFrame by 'conversation_id' and 'id' in descending order
    df_sorted = df_greater_than_2.sort_values(['conversation_id', 'id'], ascending=[True, False])

    # Replace NaN in 'sender' and 'content' columns with empty strings
    df_sorted[['sender', 'content']] = df_sorted[['sender', 'content']].fillna('')

    # 2. Group the DataFrame by 'conversation_id' and concatenate the 'content' strings within each group
    grouped = df_sorted.groupby('conversation_id')[['sender', 'content']].apply(
        lambda x: '||'.join(x['sender'] + ": " + x['content']))

    # Initialize an empty list to store the output
    output = []

    # Iterate over each group
    for conversation_id, conversation in zip(grouped.index, grouped.values):
        # Split the conversation into messages and reverse the order
        messages = conversation.split('||')[::-1]
        # Skip if the first message is from an agent
        if messages[0].startswith('Agent:'):
            continue

        # Initialize an empty list to store the current conversation
        current_conversation = []
        for message in messages:
            # Add the message to the current conversation
            current_conversation.append(message)
            if message.startswith('Agent:'):
                # If the current message is from an agent, add a record to the output
                record = {
                    'input_text': ' '.join(current_conversation[:-1]) + ' Agent:',
                    # All messages before the last one and 'Agent:'
                    'output_text': current_conversation[-1].split(": ", 1)[1]  # The last message, without the sender
                }
                output.append(record)

    # Serialize the list of dictionaries into the jsonl format
    jsonl_output = '\n'.join(json.dumps(record) for record in output)

    # Print the first 500 characters of the output to check
    print(jsonl_output[:500])

    return jsonl_output


def test_is_consequtive(df=None):
    # Test the function
    if df is None:  # noqa
        df = pd.DataFrame({
            'conversation_id': [1, 1, 1, 2, 2, 3, 3, 3],
            'id': [1, 2, 4, 1, 2, 1, 2, 3],  # The 'id' for 'conversation_id' 1 is not consecutive
            'sender': ['Customer', 'Agent', 'Customer', 'Agent', 'Customer', 'Agent', 'Customer', 'Agent']
        })

    df_cleaned_test = clean_conversation_ids(df)
    assert df_cleaned_test['conversation_id'].nunique() == 2  # Only 'conversation_id' 2 and 3 should remain
    assert df_cleaned_test.shape[0] == 5  # Only five rows should remain
    assert (df_cleaned_test.groupby('conversation_id')['id'].apply(
        is_consecutive) == True).all()  # All 'id' sequences should be consecutive


# The test function
def test_clean_data(df=None):
    # Load the cleaned DataFrame
    if df is None:  # noqa
        df = pd.read_csv('data/output.csv')

    df = clean_data(df)
    df = df.sort_values('conversation_id')
    # Test that the sender alternates within each conversation_id
    assert all(df['sender'] != df['prev_sender'])



if __name__ == '__main__':
    df = pd.read_csv('/Users/benjaminjones/Downloads/bquxjob_5fb08910_189997c7634.csv')
    df = clean_data(df)
    test_clean_data(df)

    df = clean_conversation_ids(df)
    test_is_consequtive(df)

    df_greater_than_2 = df.merge(df.value_counts('conversation_id')[df.value_counts('conversation_id') > 1],
                                 how='right', left_on='conversation_id', right_index=True)

    jsonl_output = convert_to_jsonl(df_greater_than_2)

    # strip urls
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    pattern_within_angle_brackets = r'<~~.*?~~>'
    jsonl_output1 = re.sub(url_pattern, '', jsonl_output)
    jsonl_output2 = re.sub(pattern_within_angle_brackets, '', jsonl_output1)

