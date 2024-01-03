from kfp.dsl import component


@component(base_image="python:3.10")
def merge_dicts(dict1: dict, dict2: dict) -> dict:
    """Merge two dicts.

    Args:
        dict1 (dict): First dict.
        dict2 (dict): Second dict.

    Returns:
        dict: Merged dict.
    """
    return dict1 | dict2
