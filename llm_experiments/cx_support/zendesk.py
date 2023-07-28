import pandas as pd
import requests

# Replace these with your own values
subdomain = 'motorway'
api_token = 'QzCkN3oLpnMspySENGTA7OWXQTxxycVP9vROsfK1'
email = 'bjones@motorway.co.uk/token'
agent_id = '7816092261148'

url = f"https://{subdomain}.zendesk.com/api/v2/search.json?query=type:ticket assignee:{agent_id}"

headers = {
    "Content-Type": "application/json",
}

response = requests.get(
    url,
    headers=headers,
    auth=(email, api_token)
)

tickets = response.json()
# Normalizing the JSON data into a pandas DataFrame
# tickets_df = pd.json_normalize(tickets['results'], 'comments', ['id', 'subject', 'description'])
import pandas as pd

comments_data = []

# Loop through each ticket
for ticket in tickets['results']:
    ticket_id = ticket['id']

    # Make a GET request to the Ticket Comments API for the current ticket
    comments_url = f"https://{subdomain}.zendesk.com/api/v2/tickets/{ticket_id}/comments.json"
    comments_response = requests.get(
        comments_url,
        headers=headers,
        auth=(email, api_token)
    )

    # Add the ticket's data and comments to the list
    comments = comments_response.json()
    for i, comment in enumerate(comments['comments']):
        comment_data = {
            'ticket_id': ticket_id,
            'comment_id': comment['id'],
            'comment_body': comment['body'],
            'comment_author_id': comment['author_id'],
            'comment_created_at': comment['created_at'],
            'comment_order': i + 1  # Comment order (1 for the first comment, 2 for the second comment, etc.)
        }
        comments_data.append(comment_data)

# Convert the list of comments data to a DataFrame
comments_df = pd.DataFrame(comments_data)

