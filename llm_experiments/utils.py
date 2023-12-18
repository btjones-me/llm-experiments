import os
from pathlib import Path

from google.cloud import storage


def here() -> Path:
    """Function to get the path of the project root directory.
    Works irrespective of cwd.
    """
    import llm_experiments

    return Path(llm_experiments.__file__).parents[1]


import subprocess


def run_command(command_template: str, **kwargs):
    """
    Runs a shell command with placeholders substituted by kwargs and returns the result.

    Args:
    command_template (str): The shell command template with placeholders in curly braces.
    **kwargs: Arbitrary keyword arguments that correspond to the placeholders.

    Returns:
    A tuple containing (stdout, stderr) of the command execution.

    Example usage:
    stdout, stderr = run_command("gsutil cp {source} {destination}", source="/local/path/to/file", destination="gs://bucket/path")
    """
    # Remove leading '!' if present
    if command_template.startswith("!"):
        command_template = command_template[1:]

    # Format the command template with the provided keyword arguments
    command = command_template.format(**kwargs)

    # Execute the formatted command and capture the output
    process = subprocess.run(
        command,
        shell=True,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    # Return stdout and stderr
    return process.stdout, process.stderr


def upload_to_gcs(local_file_path, gcs_uri):
    """
    Uploads a file to Google Cloud Storage.

    Args:
    local_file_path (str): The path to the local file to be uploaded.
    gcs_uri (str): The GCS URI where the file will be uploaded, in the format 'gs://bucket_name/path/to/object'.
    """
    # Initialize a storage client
    storage_client = storage.Client()

    # Extract the bucket name and object name from the GCS URI
    bucket_name, object_name = gcs_uri.replace("gs://", "").split("/", 1)

    # Get the bucket object
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object
    blob = bucket.blob(object_name)

    # Upload the file
    blob.upload_from_filename(local_file_path)

    print(f"File {local_file_path} uploaded to {gcs_uri}.")

    # Example usage
    # Replace 'path/to/local/file.wav' with your local file path and 'gs://your_bucket_name/your_file.wav' with your GCS URI
    # upload_to_gcs('path/to/local/file.wav', 'gs://your_bucket_name/your_file.wav')
