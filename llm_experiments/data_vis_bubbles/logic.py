import ast

from loguru import logger
import pandas as pd
import bigframes.pandas as bpd
import bigframes.ml.llm as llm

from llm_experiments.utils import here


# Set BigQuery DataFrames options
bpd.options.bigquery.project = "motorway-genai"
bpd.options.bigquery.location = "europe-west2"

# # Create a DataFrame from a BigQuery table
# query_or_table = "motorway-dl.llm_customers_comments.nps_comments_classified"
# df_bq = bpd.read_gbq(query_or_table)
# # Sample
# df = df_bq.head(30).to_pandas()


def generate_sentiment_score(series, temperature):
    df = pd.DataFrame(series, columns=["comment"])
    model = llm.PaLM2TextGenerator(model_name="text-bison")
    prefix_prompt = f"""
    Classify whether or not this feedback is: 1: very negative, 2: slightly negative, 3: neutral, 4: slightly positive, or 5: very positive.
    Respond with only the corresponding number.    
    Example: "The service was quick and efficient, but it was a bit boring.    
    Response: 2    
    Example:    

    """
    suffix_prompt = f"\n    Response: "
    df["prompt"] = prefix_prompt + df["comment"] + suffix_prompt

    df_pred = model.predict(bpd.read_pandas(df[["prompt"]]), 
                            max_output_tokens=40,
                            temperature=temperature,
                            )
    
    categories = {
        "1": 'very negative',
        "2": 'slightly negative',
        "3": 'neutral',
        "4": 'slightly positive',
        "5": 'very positive'
    }

    df_pred["sentiment"] = df_pred["ml_generate_text_llm_result"].str.strip().str.get(0).map(categories)

    return df_pred


def get_example_category_taxonomy(series, num_comments=200, num_categories=15):
    # Concatenate the top 'num_comments' comments into a single string
    logger.debug(f"series: {series}")
    concatenated_comments = " ".join(series.head(num_comments).tolist())
    
    # Set up the model and the prompt
    model = llm.PaLM2TextGenerator(model_name="text-bison")
    prompt = f"The following will be a text dump of comments. " \
             f"List the {num_categories} most common categories as a valid comma separated list " \
             f"in valid python syntax, using \" as the speech mark, where each category identified is its own element. " \
             f"Only return the list, and nothing else." \
             f"Comments:      {concatenated_comments}"
    
    # Make a prediction using the model
    df_pred = model.predict(bpd.read_pandas(pd.DataFrame([prompt], columns=["prompt"])), 
                               max_output_tokens=512,
                               temperature=0.3)
    
    # Return the predicted sentiment categories as a Series
    return df_pred


def categorise_comments(series, categories: list):
    df = pd.DataFrame(series, columns=["comment"])
    # Prepare the DataFrame to include prompts for each comment
    prefix_prompt = f"Classify this feedback into between minimum one and maximum three of the following categories: {categories}. " \
            f"Return your result as a valid comma separated list with maximum number of elements as 3 " \
            f"using valid python syntax, using \" as the speech mark, where each category identified is its own element. " \
            f"Only return the list, and nothing else. If the categories available are not applicable, use \"Other\". " \
            f"Feedback:     `n"
    suffix_prompt = f"\n    Response: "
    df['prompt'] = prefix_prompt + df['comment'] + suffix_prompt
    
    # Set up the model
    model = llm.PaLM2TextGenerator(model_name="text-bison")
    
    # Make a batch prediction using the model
    df_preds = model.predict(bpd.read_pandas(df[['prompt']]), 
                                max_output_tokens=256,
                                temperature=0.3)
    df_preds = df_preds.to_pandas()
    

    def validate_categories(returned_str, valid_categories):
        try:
            # Safely parse the string as a list
            categories_list = ast.literal_eval(returned_str)
            
            # Check each category and replace invalid ones
            return [category if category in valid_categories else f"~: {category}" for category in categories_list]
        except Exception as e:
            # Handle parsing errors or other issues
            return [str(e)]

    # Add the sentiment predictions to the DataFrame
    df_preds['listed_categories'] = df_preds['ml_generate_text_llm_result'].apply(lambda x: validate_categories(x, categories))
    
    return df_preds

import pandas as pd


def preprocess_categories_for_plotting(df):
    """
    Preprocesses a DataFrame to expand the categories from lists into individual rows,
    remove unvalidated categories, and calculate occurrences and average sentiment.

    Parameters:
    - df (pd.DataFrame): The original DataFrame containing the 'categories' and 'sentiment_value' columns.

    Returns:
    - pd.DataFrame: A DataFrame with each category, its occurrences, and average sentiment score.
    """

    df_copy = df.copy()

    # # Convert string representation of lists into actual lists if necessary
    # df['categories'] = df['categories'].apply(lambda x: x if isinstance(x, list) else literal_eval(x))

    # Convert sentiment values to numeric, coercing errors to NaN
    df_copy['sentiment_value'] = pd.to_numeric(df['sentiment_value'], errors='coerce')
    logger.debug(f"df_copy: {df_copy}")

    # Explode the DataFrame such that each category has its own row
    df_exploded = df_copy.explode('categories')

    # Filter out any rows where the category was not validated
    # df_exploded = df_exploded[~df_exploded['categories'].str.contains('not_validated')]

    # Group by category to calculate count and average sentiment
    category_stats = df_exploded.groupby('categories').agg(
        occurrences=('categories', 'size'),
        average_sentiment=('sentiment_value', 'mean')
    ).reset_index()

    return category_stats


# predict(
#     X: typing.Union[bigframes.dataframe.DataFrame, bigframes.series.Series],
#     *,
#     temperature: float = 0.9,
#     max_output_tokens: int = 8192,
#     top_k: int = 40,
#     top_p: float = 1.0
# )



