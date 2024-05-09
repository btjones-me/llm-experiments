import os


def _set_bearer_token_from_service_account_file(gcp_credentials_json):
    """Get the bearer token from a service account json file for authentication to the CCAI endpoint.

    Args:
        gcp_credentials_json (str): The path to the service account JSON file.

    Returns:
        str: The bearer token for authentication.

    """
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
    bearer_token = credentials.token

    return bearer_token