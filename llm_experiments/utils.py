from pathlib import Path


def here() -> Path:
    """Function to get the path of the project root directory.
    Works irrespective of cwd.
    """
    import llm_experiments

    return Path(llm_experiments.__file__).parents[1]
