from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
from sklearn.base import ClassifierMixin
from sklearn.metrics import PrecisionRecallDisplay


def plot_precision_recall_curve(
    model: ClassifierMixin,
    model_name: str,
    X: np.ndarray,
    y: np.ndarray,
    save_path: Optional[str] = None,
) -> plt.Figure:
    """Plot and export the Precision Recall curve for an estimator.

    Args:
        model (ClassifierMixin): Model used to generate the plot
        model_name (str): Name of the model, used to name the output file
        X (np.ndarray): Model inputs
        y (np.ndarray): Ground-truth values for the target variable
        save_path ():

    Returns:
        Figure: the PRC plot
    """
    fig, ax = plt.subplots(1, 1, figsize=(20, 20))

    _ = PrecisionRecallDisplay.from_estimator(
        model,
        X=X,
        y=y,
        ax=ax,
        name=model_name,
    )
    ax.set_title(f"Precision-Recall Curve - Model {model_name}.")
    if save_path is not None:
        plt.savefig(save_path)
    plt.close(fig)
    return fig
