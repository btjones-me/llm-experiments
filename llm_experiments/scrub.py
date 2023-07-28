import pandas as pd
import scrubadub
import re

# Load the data
df = pd.read_csv('/Users/benjaminjones/Downloads/bquxjob_5fb08910_189997c7634.csv')

# Initialize scrubber
scrubber = scrubadub.Scrubber()

# Define regex patterns for sensitive data
email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
phone_pattern = r'\b(\+\d{1,2}\s?)?1?\-?\.?\s?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b'
postcode_pattern = r'\b([A-Za-z]{1,2}[0-9]{1,2}[A-Za-z]?\s?[0-9][A-Za-z]{2}|GIR\s?0AA)\b'  # UK postcode pattern

# Define a function to apply the scrubber and regex to a whole DataFrame
def scrub_df(data):
    def clean_text(text):
        text = str(text)  # Convert to string
        text = scrubber.clean(text)
        text = re.sub(email_pattern, "[email]", text)
        text = re.sub(phone_pattern, "[phone]", text)
        text = re.sub(postcode_pattern, "[postcode]", text)
        return text

    return data.applymap(clean_text)

# Scrub the data
df_scrubbed = scrub_df(df)


df_scrubbed['content'] = df_scrubbed['content'].str[:20]


# Save the scrubbed data to a new CSV file
df_scrubbed.to_csv('scrubbed_output.csv', index=False)
