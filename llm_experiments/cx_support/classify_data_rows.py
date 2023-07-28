import os
import time
import vertexai
from vertexai.language_models import TextGenerationModel
import pandas as pd

from llm_experiments.utils import here

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(here() / 'motorway-genai-ccebd34bd403.json')

class TextGenerator:
    def __init__(self, project, location, model_name, rate_limit):
        self.project = project
        self.location = location
        self.model_name = model_name
        self.rate_limit = rate_limit

        vertexai.init(project=self.project, location=self.location)
        self.model = TextGenerationModel.from_pretrained(self.model_name)

        self.parameters = {
            "temperature": 0.2,
            "max_output_tokens": 256,
            "top_p": 0.8,
            "top_k": 40
        }

    def generate_text(self, prompt):
        response = self.model.predict(prompt, **self.parameters)
        return response.text

    def generate_text_for_dataframe(self, df):
        results = []
        for index, row in df.iterrows():
            prompt = row['prompt']
            generated_text = self.generate_text(prompt)
            results.append(generated_text)

            if index % self.rate_limit == 0 and index != 0:
                time.sleep(1)

        return results


def make_prompt(example):
    return f"""You will classify messages between customer service agents for a company called Motorway, and their customers, and internal communications. Given the following pieces of text, classify them as either 'Customer Related - From Customer', 'Customer Related - From Agent', or 'Internal'. 'Customer Related - From Customer' indicates the message is from a customer to an agent. 'Customer Related - From Agent' indicates the message is from the agent to the customer. 'Internal' refers to messages such as notes, subject lines, and other internal communications from Agent to Agent that do not involve the customer.

    Examples:

    1: "Hey motorway, my car purchase didnt go ahead. Please could we canx in dash? Thanks!"
    - Classification: Customer Related - From Customer

    2: AD13CAD - 7129055 - BMW M4 Competition M xDrive Auto - Cancellation
    - Classification: Internal

    3: Historical canx macro sent
    - Classification: Internal

    4: "Hi, We have been informed by the dealer that the sale of your vehicle has been cancelled as they are unable to wait over 2 weeks to collect the vehicle. We can relist the vehicle for you once you return from your holiday. Please let us know how you wish to proceed. If you have any questions, please reply to this email or call us on 0203 988 3388. We are available from Monday-Friday 9 am - 5.30 pm, Saturday 9 am - 5 pm and on bank holidays 10 am - 5 pm. You can read more about how our service works by visiting our help centre. Kind regards,
    - Classification: Customer Related - From Agent

    Note: Make your classifications based on the content and context of the message, not on the specific format or wording. If a message is primarily about an internal matter but happens to mention a customer, classify it as 'Internal'. If a message is primarily addressing a customer or a customer's needs, classify it as 'Customer Related'.

    The real message will be separated from the prompt with the delimiter: ####

    Respond only with the classification: "Customer Related - From Customer", "Internal", or "Customer Related - From Agent"

    Message to classify:

    ####

    {example}

    ####

    Classification:
    """


##


# df = pd.read_csv('prompts.csv')

generator = TextGenerator(
    project="motorway-genai",
    location="us-central1",
    model_name="text-bison@001",
    rate_limit=100
)

##
df = pd.read_csv('data/bq-results-20230728-135043-1690552255754.csv')

df = df.head(10)
df['prompt'] = df['body'].apply(make_prompt)
# filter out where the df column channel is = side_conversation
df = df[df['channel'] != 'side_conversation']


print('calling llm')
results = generator.generate_text_for_dataframe(df)

