import os

# Required images for components + Vertex training

PYTHON = "python:3.9"
PIPELINE_IMAGE_NAME = os.getenv("IMAGE_NAME")

# Required packages and versions for components (ensure that these are in sync with pyproject.toml)

# Google SDK specific
GOOGLE_CLOUD_BIGQUERY = "google-cloud-bigquery==3.11.1"
GOOGLE_CLOUD_STORAGE = "google-cloud-storage==2.9.0"
GOOGLE_CLOUD_AIPLATFORM = "google-cloud-aiplatform==1.26.0"

# Miscellaneous
MATPLOTLIB = "matplotlib==3.5.1"
LOGURU = "loguru==0.7.0"
PYARROW = "pyarrow==6.0.1"
REQUESTS = "requests==2.30.0"

# Data science
PANDAS = "pandas==1.4.3"
NUMPY = "numpy==1.22.0"
SCIKIT_LEARN = "scikit-learn==1.1.2"
JOBLIB = "joblib==1.2.0"

# Tensorflow data validation
TFDV = "tensorflow-data-validation==1.13.0"
