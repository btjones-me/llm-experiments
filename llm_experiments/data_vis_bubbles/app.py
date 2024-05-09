import streamlit as st
from streamlit_plotly_events import plotly_events
import plotly.express as px

# # Create a Plotly figure
# fig = px.scatter(
#     x=[1, 2, 3, 4],
#     y=[10, 11, 12, 13],
#     size=[30, 40, 50, 60],
#     color=["red", "green", "blue", "purple"],
#     hover_name=["First", "Second", "Third", "Fourth"]
# )

# # Use the plotly_events function to capture click events on the figure
# selected_points = plotly_events(fig)

# # Do something with the selected points (display the results for instance)
# if selected_points:
#     for point in selected_points:
#         st.write(f"You clicked on: {point}")


import streamlit as st
import pandas as pd
import bigframes.pandas as bpd
import ast
from llm_experiments.data_vis_bubbles.logic import generate_sentiment_score, get_example_category_taxonomy, categorise_comments, preprocess_categories_for_plotting


# Create a DataFrame from a BigQuery table
query_or_table = "motorway-dl.llm_customers_comments.nps_comments_classified"
df_bq = bpd.read_gbq(query_or_table)
# Sample
df = df_bq.head(30).to_pandas()



# Set the page config to wide mode for better DataFrame display
st.set_page_config(layout="wide")

# Display the head of the DataFrame
st.write("### Gemini for Sentiment Analysis")
st.dataframe(df)

# User selects the column to analyze
column_to_analyze = st.selectbox("Select the column you'd like to analyse:", df.columns)

# Button to start analysis
if st.button("Get categories ✨"):
    # Display a placeholder while processing
    with st.spinner('Calculating the top occurring categories...'):
        # Assume the selected column contains comments
        comments = df[column_to_analyze]

        # Get example categories from the model
        predicted_categories = get_example_category_taxonomy(comments)
        
        # Evaluate and display categories
        try:
            initial_categories = ast.literal_eval(predicted_categories.iloc[0]['ml_generate_text_llm_result'])
        except Exception as e:
            st.error(f"Error parsing categories: {e}")
            initial_categories = []

    # Allow the user to modify the list of categories
    categories = st.multiselect("Review and modify the categories:", options=initial_categories, default=initial_categories)

    if categories:
        # if st.button("Analyse Content ✨"):

        with st.spinner('Categorizing comments and generating sentiment scores...'):

            # Categorize comments using the finalized categories
            categorized_comments = categorise_comments(comments, categories)
            df['categories'] = categorized_comments['listed_categories']

            # Generate sentiment scores
            sentiment_scores = generate_sentiment_score(comments, temperature=0.3)
            df['sentiment'] = sentiment_scores['sentiment']
            df['sentiment_value'] = sentiment_scores['ml_generate_text_llm_result']

            # Display the updated DataFrame
            st.write("### Updated DataFrame with Categories and Sentiment Scores")
            df_filtered = df[[column_to_analyze, 'categories', 'sentiment', 'sentiment_value']]
            st.dataframe(df_filtered)
        
        category_stats = preprocess_categories_for_plotting(df)

        st.data_editor(category_stats)

        # Create the bubble chart using Plotly Express
        fig = px.scatter(
            category_stats,
            x='categories', 
            y='average_sentiment',
            size='occurrences', 
            color='average_sentiment',
            hover_name='categories',
            size_max=60
        )

        # Customize the layout
        fig.update_layout(
            title='Category Sentiment Bubble Chart',
            xaxis_title='Categories',
            yaxis_title='Average Sentiment Score'
        )

        # Show the figure
        # fig.show()

        # Use the plotly_events function to capture click events on the figure
        selected_points = plotly_events(fig)

        # # Do something with the selected points (display the results for instance)
        # if selected_points:
        #     st.write(f"Selected Points: {selected_points}")



else:
    st.write("Click the button to analyze the data.")
