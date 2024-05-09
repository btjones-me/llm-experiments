
import pytest
import pandas as pd 

from llm_experiments.data_vis_bubbles.logic import preprocess_categories_for_plotting


def test_preprocess_categories_for_plotting():
    # Sample data for testing
    test_data = {
        'categories': [['Excellent', 'Efficient'], ['Heard', 'not_validated: Other'], ['Excellent']],
        'sentiment_value': [5, 1, 5]
    }
    test_df = pd.DataFrame(test_data)

    # Expected result
    expected_data = {
        'categories': ['Excellent', 'Efficient'],
        'occurrences': [2, 1],
        'average_sentiment': [5.0, 5.0]
    }
    expected_df = pd.DataFrame(expected_data)

    # Run the preprocessing function
    result_df = preprocess_categories_for_plotting(test_df)

    # Validate the results
    pd.testing.assert_frame_equal(result_df, expected_df, check_dtype=False)

# Execute the test
pytest.main()
