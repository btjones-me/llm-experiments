""" Process Talkdesk conversations from individual zip files and join them with the interaction ids supplied
by Hannah in the Talkdesk Export file.
Outputs a CSV file with the following columns:
id,time,call_id,contact_id,interaction_id,enquiry_id,message_id,message_text,message_participant,message_agent_name,interaction_started,filename,_merge,intent_value,sentiment_label
"""

import io
import os
import zipfile

import pandas as pd
from loguru import logger

from llm_experiments.utils import here

# get the file that contains the ids that we're looking for in the transcripts
IDS_PATH = (
    here() / "data/insights/Talkdesk Export '2023-11-08' - results-20231108-090222.csv"
)
df_ids = pd.read_csv(IDS_PATH)

# get the folder that contains the transcripts
TRANSCRIPTION_FOLDER = here() / "data/insights/transcriptions"


# Function to unzip a file into memory
def unzip_to_memory(zip_file_path):
    data_dict = {}  # Dictionary to hold the content of the zip files

    # Open the zip file
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        # Iterate over each file in the zip file
        for file in zip_ref.namelist():
            with zip_ref.open(file) as f:
                # Read the file content into memory
                data_dict[file] = io.BytesIO(f.read())

    return data_dict


# Directory where your zip files are located
zip_files_dir = TRANSCRIPTION_FOLDER

# Now, all_data contains the unzipped content of all your zip files
all_dfs = []

# Iterate over each file in the zip file, check which are in the ids file and join them
# List all the files in the directory
for file_name in os.listdir(zip_files_dir):
    if file_name.endswith(".zip"):
        zip_file_path = os.path.join(zip_files_dir, file_name)
        # Unzip the file and store the content
        for filename, dataio in unzip_to_memory(zip_file_path).items():
            logger.info(f"{filename=}")
            df = pd.read_csv(dataio)
            df["filename"] = filename
            # df_crossover = df_ids.merge(
            #     df,
            #     left_on="interaction_id",
            #     right_on="interaction_id",
            #     how="left",
            #     indicator=True,
            # )
            # logger.info(f"found {len(df_crossover)} crossover rows")
            # all_dfs.append(df_crossover)
            all_dfs.append(df)


# Step 0: Combine all dfs into one
df_all = pd.concat(all_dfs)
print(df_all["interaction_id"].drop_duplicates().shape)

# Step 1: Extract interaction_ids that are in 'both'
both_ids = df_all[df_all["_merge"] == "both"]["interaction_id"].unique()

# Step 2: Find interaction_ids in df_ids that are not in both_ids
missing_ids = df_ids[~df_ids["interaction_id"].isin(both_ids)]

# Step 3: Save missing_ids to csv
missing_ids.to_csv(here() / "data/insights/outs/missing_ids.csv")

# Step 4: Save df_all to csv
df_calls = df_all[df_all["_merge"] == "both"]
df_calls.to_csv(here() / "data/insights/outs/all_calls.csv")


#### Analysis / Bug fixing
# Visualise these dfs to see if results are correct
# count unique rows in df_all
# df_all.groupby("interaction_id").count().shape
# df_all
# # order by time
# df_ids.sort_values("time", ascending=False)
# df_all.sort_values("time", ascending=False).time
# a = (
#     df_all[["interaction_id", "_merge"]]
#     .drop_duplicates(["interaction_id", "_merge"])
#     .groupby("_merge")
#     .count()
# )
# # length of interaction ids matched
# b = df_all.groupby("interaction_id").count()
# b = b[b["message_id"] > 0].shape
#
# c = df_all[df_all["_merge"] == "right_only"].groupby("interaction_id").count()

## analysis to test if the dates are as expected
# earliest_date = None
# latest_date = None
# df_rows = pd.DataFrame()
# # Iterate over each file in the zip file and find earliest and latest date
# for file_name in os.listdir(zip_files_dir):
#     if file_name.endswith(".zip"):
#         zip_file_path = os.path.join(zip_files_dir, file_name)
#         # Unzip the file and store the content
#         for filename, dataio in unzip_to_memory(zip_file_path).items():
#             logger.info(f"{filename=}")
#             df = pd.read_csv(dataio)
#             df["filename"] = filename
#             # find earliest and latest date, overwrite variable if later
#             if earliest_date is None:
#                 earliest_date = df["interaction_started"].min()
#             elif df["interaction_started"].min() < earliest_date:
#                 earliest_date = df["interaction_started"].min()
#             if latest_date is None:
#                 latest_date = df["interaction_started"].max()
#             elif df["interaction_started"].max() > latest_date:
#                 latest_date = df["interaction_started"].max()
#
#             # add row to new dataframe that has the filename and the number of rows
#             pd.concat(
#                 [
#                     df_rows,
#                     pd.DataFrame(
#                         [[filename, df.groupby("interaction_id").count().shape[0]]],
#                         columns=["filename", "rows"],
#                     ),
#                 ],
#                 ignore_index=True,
#             )
#
# earliest_date
# latest_date
# df_rows.rows.sum()
