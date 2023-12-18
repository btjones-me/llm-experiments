import json
from datetime import datetime
import pandas as pd

from llm_experiments.utils import here

# Example DataFrame
df_calls = pd.read_csv(here() / "data/insights/outs/calls.csv")


# Function to convert datetime to microseconds since Unix epoch
def to_microseconds(timestamp):
    return int(datetime.timestamp(pd.to_datetime(timestamp)) * 1e6)


# Initialize a list to hold all conversation data
all_conversations = []

# Group by interaction_id
grouped = df_calls.groupby("interaction_id")
i = 0
for interaction_id, group in grouped:
    # Initialize conversation data structure for each interaction_id
    conversation_data = {
        "conversation_info": {
            "categories": [
                {
                    "filename": group["filename"].iloc[0],
                    "interaction_id": interaction_id,
                }
            ]
        },
        "entries": [],
    }
    # initialise the timestamp variable to increment each message (because we just get the same timestamp back
    # from talkdesk for every message in an interaction
    if group["interaction_started"].nunique() != 1:
        raise ValueError(
            "Expected all timestamps to be the same, because we're incrementing by 5 seconds"
        )
    timestamp = to_microseconds(group["interaction_started"].iloc[0])

    for index, row in group.iterrows():
        # set the user id based on the message participant
        user_id = (
            11111111  # redacted
            if row["message_participant"] == "CUSTOMER"
            else 99999999  # 99999999 is the designated user id for the agent
        )
        timestamp += 5000000
        entry = {
            "text": row["message_text"],
            "role": row["message_participant"],
            "user_id": user_id,
            "start_timestamp_usec": timestamp,
        }
        conversation_data["entries"].append(entry)

    # Add the conversation to the list
    all_conversations.append(conversation_data)

    i += 1
    if i > 3:
        break

# Convert list of conversations to JSON string
json_data = json.dumps(all_conversations, indent=2)

# Write JSON data to a file
with open(here() / "data/insights/outs" / "conversation_data_4.json", "w") as file:
    file.write(json_data)
