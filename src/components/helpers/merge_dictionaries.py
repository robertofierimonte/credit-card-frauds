from kfp.dsl import component

from src.components.dependencies import PYTHON


@component(base_image=PYTHON)
def merge_dicts(dict1: dict, dict2: dict) -> dict:
    """Merge two dicts.

    Args:
        dict1 (dict): First dict.
        dict2 (dict): Second dict.

    Returns:
        dict: Merged dict.
    """
    return dict1 | dict2
