"""Tests for `llm_experiments` package."""

import pytest
from assertpy import assert_that

from llm_experiments.deep_learning_ai.experiments.classification_01 import main


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    assert_that(main()).is_equal_to("Hello, World!")
