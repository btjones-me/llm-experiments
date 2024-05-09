import hashlib
import json
import os
import subprocess
from datetime import datetime
import requests

from loguru import logger
import pandas as pd
from google.cloud import storage
from pathlib import Path
from llm_experiments.utils import here


class ConversationDataTransformer:
    def __init__(
        self, input_csv_path, output_folder, gcs_bucket_name, gcs_directory_path
    ):
        self.input_csv_path = Path(input_csv_path)
        self.output_folder = Path(output_folder)
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_directory_path = gcs_directory_path

        # Initialize GCS client and bucket here because you don't want to do it every method call
        self.gcs_client = storage.Client()
        self.bucket = self.gcs_client.get_bucket(gcs_bucket_name)

        # Ensure output folder exists
        if not self.output_folder.exists():
            self.output_folder.mkdir(parents=True)

    @staticmethod
    def to_microseconds(timestamp):
        # Function to convert datetime to microseconds since Unix epoch
        return int(datetime.timestamp(pd.to_datetime(timestamp)) * 1e6)

    @staticmethod
    def string_to_int_id(input_string):
        # Function to convert a string to an integer id
        # Use SHA-256 hash function
        hash_object = hashlib.md5(input_string.encode())
        # Get hexadecimal digest
        hex_dig = hash_object.hexdigest()
        # Convert hexadecimal to integer
        int_id = int(hex_dig, 16)  # 40 digit unique integer
        return int(
            str(int_id)[:8]
        )  # reduce to 8 digit integer and hope for no hash collisions lol

    def transform_data(self, group: pd.DataFrame, interaction_id: str):
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
        # from talkdesk for every message in an interaction)
        timestamp = self.to_microseconds(group["interaction_started"].iloc[0])

        for _, row in group.iterrows():
            # set the user id based on the message participant
            user_id = (
                row["enquiry_id"]
                if row["message_participant"] == "CUSTOMER"
                else self.string_to_int_id(
                    row["message_agent_name"]
                )  # CCAI needs integers for IDs not strings
            )
            timestamp += 5000000  # increment by 5 seconds because all timestamps are the same in the data from Talkdesk
            entry = {
                "text": row["message_text"],
                "role": row["message_participant"],
                "user_id": user_id,
                "start_timestamp_usec": timestamp,
            }
            conversation_data["entries"].append(entry)

        return conversation_data

    def save_to_file(self, interaction_id, data):
        file_name = f"conversation_{interaction_id}.json"
        file_path = self.output_folder / file_name
        with open(file_path, "w") as file:
            json.dump(data, file, indent=2)
        return file_path

    def upload_to_gcs(self, upload_file_path, gcs_file_name):
        blob = self.bucket.blob(str(gcs_file_name))
        blob.upload_from_filename(str(upload_file_path))

    def process_conversations(self, save_to_gcs_flag=False):
        df_calls = pd.read_csv(self.input_csv_path)
        grouped = df_calls.groupby("interaction_id")
        for interaction_id, group in grouped:
            conversation_data = self.transform_data(group, interaction_id)

            # save to local by default
            output_file_path = self.save_to_file(interaction_id, conversation_data)
            if save_to_gcs_flag:
                self.upload_to_gcs(
                    output_file_path,
                    Path(self.gcs_directory_path) / Path(output_file_path).name,
                )


class IngestToCCAI:
    """Class to ingest all files in a GCS directory to CCAI. Needs authentication with a bearer token."""

    def __init__(self, gcs_bucket_name, gcs_directory_path):
        self.gcs_bucket_name = gcs_bucket_name
        self.gcs_directory_path = gcs_directory_path

        self.bearer_token = None
        self.successful_conversations = []
        self.failed_conversations = []

    def _set_bearer_token(self, bearer_token):
        self.bearer_token = bearer_token

    def _set_bearer_token_from_cli_input(self):
        """Set the bearer token based on user input from the command line interface."""
        self.bearer_token = input("Please enter your bearer token: ").strip()

    def _set_bearer_token_from_service_account_file(self, gcp_credentials_json):
        """Get the bearer token from a service account json file for authentication to the CCAI endpoint."""
        from google.oauth2 import service_account
        import google.auth.transport.requests

        # Expand the user's home directory (~ part in the path if applicable)
        expanded_path = os.path.expanduser(gcp_credentials_json)

        # Load the service account credentials from the file
        credentials = service_account.Credentials.from_service_account_file(
            expanded_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        # Obtain the bearer token
        request = google.auth.transport.requests.Request()
        credentials.refresh(request)
        self.bearer_token = credentials.token

    def _set_bearer_token_from_env_var(self):
        import dotenv

        dotenv.load_dotenv()
        self.bearer_token = os.getenv("GCLOUD_BEARER_TOKEN")

    def _set_bearer_token_from_user_default(self):
        # Run the gcloud command and capture the output
        token = subprocess.check_output(
            ["gcloud", "auth", "application-default", "print-access-token"], text=True
        ).strip()
        self.bearer_token = token

    def _make_request(self, bucket_uri: str):
        """Make request to the CCAI endpoint to take a GCS URI and ingest to Insights."""

        # The URL for the API endpoint
        endpoint = "https://contactcenterinsights.googleapis.com/v1/projects/motorway-genai/locations/us-central1/conversations:ingest"

        # The bearer token for authentication
        bearer_token = self.bearer_token
        # Headers for the request
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json; charset=utf-8",
        }

        # The data payload for the POST request
        data = {
            "gcsSource": {"bucketUri": bucket_uri, "bucketObjectType": "TRANSCRIPT"},
            "transcriptObjectConfig": {"medium": "PHONE_CALL"},
            "conversationConfig": {},
        }

        # Make the POST request
        response = requests.post(endpoint, headers=headers, data=json.dumps(data))

        # Check if the request was successful
        if response.status_code == 200:
            logger.info("Success!")
            self.successful_conversations.append(bucket_uri)
        else:
            logger.error("An error occurred:" + response.text)
            self.failed_conversations.append(bucket_uri)

    def ingest_all_files_to_ccai(self):
        """Ingest all files in a GCS directory to CCAI."""
        # Get the list of files in the GCS directory
        bucket = storage.Client().get_bucket(self.gcs_bucket_name)

        # Get the blob for the folder only
        folder_blob = bucket.blob(self.gcs_directory_path)
        all_blobs = list(
            bucket.list_blobs(prefix=self.gcs_directory_path)
        )  # this includes the folder and the files

        # find number of items in list that end in .json
        all_json_blobs = [blob for blob in all_blobs if blob.name.endswith(".json")]
        num_files = len(all_json_blobs)
        logger.info(f"Found {num_files} files to ingest.")

        logger.info(f"Ingesting blobs to CCAI...")
        # Make the request to CCAI in bulk for all items in the blob
        gcs_uri = f"gs://{self.gcs_bucket_name}/{folder_blob.name}"
        self._make_request(gcs_uri)

    def ingest_one_by_one_to_ccai(self):
        # Not used but exists for completeness. Untested.

        # Get the list of files in the GCS directory
        bucket = storage.Client().get_bucket(self.gcs_bucket_name)

        # Get the blob for the folder only
        folder_blob = bucket.blob(self.gcs_directory_path)
        all_blobs = list(
            bucket.list_blobs(prefix=self.gcs_directory_path)
        )  # this includes the folder and the files

        # find number of items in list that end in .json
        all_json_blobs = [blob for blob in all_blobs if blob.name.endswith(".json")]
        num_files = len(all_json_blobs)
        logger.info(f"Found {num_files} files to ingest.")

        i = 0  # add break clause for testing
        # Iterate through the list of files
        for blob in all_json_blobs:
            # Get the GCS URI for the file
            gcs_uri = f"gs://{self.gcs_bucket_name}/{blob.name}"
            logger.info(f"Ingesting {gcs_uri} to CCAI...")
            # Make the request to CCAI
            self._make_request(gcs_uri)
            logger.info(f"Ingested {gcs_uri} to CCAI.")

            i += 1
            if i == 5:
                break

        logger.info(
            f"Ingestion complete. "
            f"Num successful completions: {len(self.successful_conversations)}. "
            f"Num failed completions: {len(self.failed_conversations)}."
        )
        logger.debug(f"Failed completions: {self.failed_conversations}")


if __name__ == "__main__":
    output_folder = here() / "data/insights/outs" / "conversations"
    input_csv_path = here() / "data/insights/outs/all_calls.csv"
    gcs_bucket_name = "gen-ai-test-playground"
    gcs_directory_path = "ccai-insights-json/all_conversations"

    # Process conversations and upload to GCS
    # Note: shell running this file must be authenticated to GCP first. Run `gcloud auth application-default login` in terminal
    transformer = ConversationDataTransformer(
        input_csv_path,
        output_folder,
        gcs_bucket_name,
        gcs_directory_path,
    )
    transformer.process_conversations(save_to_gcs_flag=True)

    class ConversationDataTransformer:
        def __init__(
            self, input_csv_path, output_folder, gcs_bucket_name, gcs_directory_path
        ):
            self.input_csv_path = Path(input_csv_path)
            self.output_folder = Path(output_folder)
            self.gcs_bucket_name = gcs_bucket_name
            self.gcs_directory_path = gcs_directory_path

            # Initialize GCS client and bucket here because you don't want to do it every method call
            self.gcs_client = storage.Client()
            self.bucket = self.gcs_client.get_bucket(gcs_bucket_name)

            # Ensure output folder exists
            if not self.output_folder.exists():
                self.output_folder.mkdir(parents=True)

        @staticmethod
        def to_microseconds(timestamp):
            # Function to convert datetime to microseconds since Unix epoch
            return int(datetime.timestamp(pd.to_datetime(timestamp)) * 1e6)

        @staticmethod
        def string_to_int_id(input_string, log_to_file=False, file_path=None):
            # Function to convert a string to an integer id
            # Use SHA-256 hash function
            hash_object = hashlib.md5(input_string.encode())
            # Get hexadecimal digest
            hex_dig = hash_object.hexdigest()
            # Convert hexadecimal to integer
            int_id = int(hex_dig, 16)  # 40 digit unique integer

            # Reduce to 8 digit integer, hope for no hash collisions lol
            short_int_id = int(str(int_id)[:8])

            if log_to_file:
                if not file_path:
                    file_path = here() / "logs" / "string_to_int_id_log.txt"
                    # use pathlib to check if parent folder exists and create if not
                    file_path.parent.mkdir(parents=True, exist_ok=True)

                with open(file_path, "a+") as file:
                    file.seek(0)  # Go to the start of the file
                    existing_hashes = file.read()
                    # Check if the hash exists in the file
                    if hex_dig not in existing_hashes:
                        # Log the new hash
                        file.write(f"{input_string}: {hex_dig}\n")

            return short_int_id

        def transform_data(self, group: pd.DataFrame, interaction_id: str):
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
            # from talkdesk for every message in an interaction)
            timestamp = self.to_microseconds(group["interaction_started"].iloc[0])

            for _, row in group.iterrows():
                # set the user id based on the message participant.
                # not all customers have enquiry ids. If not, hash their interaction id
                user_id = (
                    row["enquiry_id"]
                    if row["message_participant"] == "CUSTOMER"
                    else self.string_to_int_id(
                        row["message_agent_name"]
                    )  # CCAI needs integers for IDs not strings
                )
                timestamp += 5000000  # increment by 5 seconds because all timestamps are the same in the data from Talkdesk
                entry = {
                    "text": row["message_text"],
                    "role": row["message_participant"],
                    "user_id": user_id,
                    "start_timestamp_usec": timestamp,
                }
                conversation_data["entries"].append(entry)

            return conversation_data

        def save_to_file(self, interaction_id, data):
            file_name = f"conversation_{interaction_id}.json"
            file_path = self.output_folder / file_name
            with open(file_path, "w") as file:
                json.dump(data, file, indent=2)
            return file_path

        def upload_to_gcs(self, upload_file_path, gcs_file_name):
            blob = self.bucket.blob(str(gcs_file_name))
            blob.upload_from_filename(str(upload_file_path))

        def process_conversations(self, save_to_gcs_flag=False):
            df_calls = pd.read_csv(self.input_csv_path)
            grouped = df_calls.groupby("interaction_id")
            for interaction_id, group in grouped:
                conversation_data = self.transform_data(group, interaction_id)

                # save to local by default
                output_file_path = self.save_to_file(interaction_id, conversation_data)
                if save_to_gcs_flag:
                    self.upload_to_gcs(
                        output_file_path,
                        Path(self.gcs_directory_path) / Path(output_file_path).name,
                    )

    class IngestToCCAI:
        """Class to ingest all files in a GCS directory to CCAI. Needs authentication with a bearer token."""

        def __init__(self, gcs_bucket_name, gcs_directory_path):
            self.gcs_bucket_name = gcs_bucket_name
            self.gcs_directory_path = gcs_directory_path

            self.bearer_token = None
            self.successful_conversations = []
            self.failed_conversations = []

        def _set_bearer_token(self, bearer_token):
            self.bearer_token = bearer_token

        def _set_bearer_token_from_cli_input(self):
            """Set the bearer token based on user input from the command line interface."""
            self.bearer_token = input("Please enter your bearer token: ").strip()

        def _set_bearer_token_from_service_account_file(self, gcp_credentials_json):
            """Get the bearer token from a service account json file for authentication to the CCAI endpoint."""
            from google.oauth2 import service_account
            import google.auth.transport.requests

            # Expand the user's home directory (~ part in the path if applicable)
            expanded_path = os.path.expanduser(gcp_credentials_json)

            # Load the service account credentials from the file
            credentials = service_account.Credentials.from_service_account_file(
                expanded_path,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )

            # Obtain the bearer token
            request = google.auth.transport.requests.Request()
            credentials.refresh(request)
            self.bearer_token = credentials.token

        def _set_bearer_token_from_env_var(self):
            import dotenv

            dotenv.load_dotenv()
            self.bearer_token = os.getenv("GCLOUD_BEARER_TOKEN")

        def _set_bearer_token_from_user_default(self):
            # Run the gcloud command and capture the output
            token = subprocess.check_output(
                ["gcloud", "auth", "application-default", "print-access-token"],
                text=True,
            ).strip()
            self.bearer_token = token

        def _make_request(self, bucket_uri: str):
            """Make request to the CCAI endpoint to take a GCS URI and ingest to Insights."""

            # The URL for the API endpoint
            endpoint = "https://contactcenterinsights.googleapis.com/v1/projects/motorway-genai/locations/us-central1/conversations:ingest"

            # The bearer token for authentication
            bearer_token = self.bearer_token
            # Headers for the request
            headers = {
                "Authorization": f"Bearer {bearer_token}",
                "Content-Type": "application/json; charset=utf-8",
            }

            # The data payload for the POST request
            data = {
                "gcsSource": {
                    "bucketUri": bucket_uri,
                    "bucketObjectType": "TRANSCRIPT",
                },
                "transcriptObjectConfig": {"medium": "PHONE_CALL"},
                "conversationConfig": {},
            }

            # Make the POST request
            response = requests.post(endpoint, headers=headers, data=json.dumps(data))

            # Check if the request was successful
            if response.status_code == 200:
                logger.info("Success!")
                self.successful_conversations.append(bucket_uri)
            else:
                logger.error("An error occurred:" + response.text)
                self.failed_conversations.append(bucket_uri)

        def ingest_all_files_to_ccai(self):
            """Ingest all files in a GCS directory to CCAI."""
            # Get the list of files in the GCS directory
            bucket = storage.Client().get_bucket(self.gcs_bucket_name)

            # Get the blob for the folder only
            folder_blob = bucket.blob(self.gcs_directory_path)
            all_blobs = list(
                bucket.list_blobs(prefix=self.gcs_directory_path)
            )  # this includes the folder and the files

            # find number of items in list that end in .json
            all_json_blobs = [blob for blob in all_blobs if blob.name.endswith(".json")]
            num_files = len(all_json_blobs)
            logger.info(f"Found {num_files} files to ingest.")

            logger.info(f"Ingesting blobs to CCAI...")
            # Make the request to CCAI in bulk for all items in the blob
            gcs_uri = f"gs://{self.gcs_bucket_name}/{folder_blob.name}"
            self._make_request(gcs_uri)

        def ingest_one_by_one_to_ccai(self):
            # Not used but exists for completeness. Untested.

            # Get the list of files in the GCS directory
            bucket = storage.Client().get_bucket(self.gcs_bucket_name)

            # Get the blob for the folder only
            folder_blob = bucket.blob(self.gcs_directory_path)
            all_blobs = list(
                bucket.list_blobs(prefix=self.gcs_directory_path)
            )  # this includes the folder and the files

            # find number of items in list that end in .json
            all_json_blobs = [blob for blob in all_blobs if blob.name.endswith(".json")]
            num_files = len(all_json_blobs)
            logger.info(f"Found {num_files} files to ingest.")

            i = 0  # add break clause for testing
            # Iterate through the list of files
            for blob in all_json_blobs:
                # Get the GCS URI for the file
                gcs_uri = f"gs://{self.gcs_bucket_name}/{blob.name}"
                logger.info(f"Ingesting {gcs_uri} to CCAI...")
                # Make the request to CCAI
                self._make_request(gcs_uri)
                logger.info(f"Ingested {gcs_uri} to CCAI.")

                # i += 1
                # if i == 5:
                #     break

            logger.info(
                f"Ingestion complete. "
                f"Num successful completions: {len(self.successful_conversations)}. "
                f"Num failed completions: {len(self.failed_conversations)}."
            )
            logger.debug(f"Failed completions: {self.failed_conversations}")

    if __name__ == "__main__":
        output_folder = here() / "data/insights/outs" / "conversations"
        input_csv_path = here() / "data/insights/outs/all_calls.csv"
        gcs_bucket_name = "gen-ai-test-playground"
        gcs_directory_path = "ccai-insights-json/all_conversations"

        # Process conversations and upload to GCS
        # Note: shell running this file must be authenticated to GCP first. Run `gcloud auth application-default login` in terminal
        transformer = ConversationDataTransformer(
            input_csv_path,
            output_folder,
            gcs_bucket_name,
            gcs_directory_path,
        )
        transformer.process_conversations(save_to_gcs_flag=True)

        # Ingest all conversations to CCAI
        # This one handles authentication because it requires particular permissions, just pick your method
        # TODO: handle auth in the ConversationDataTransformer class too
        ingestor = IngestToCCAI(gcs_bucket_name, gcs_directory_path)
        ingestor._set_bearer_token_from_env_var()
        ingestor.ingest_all_files_to_ccai()

    # Ingest all conversations to CCAI
    # This one handles authentication because it requires particular permissions, just pick your method
    # TODO: handle auth in the ConversationDataTransformer class too
    ingestor = IngestToCCAI(gcs_bucket_name, gcs_directory_path)
    ingestor._set_bearer_token_from_env_var()
    ingestor.ingest_all_files_to_ccai()
