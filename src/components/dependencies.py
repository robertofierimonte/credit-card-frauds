import os

# Required images for components + Vertex training

PYTHON = "python:3.9"
CICD_TRIGGER_IMAGE_NAME = f"{os.getenv('ARTIFACT_REGISTRY_REPO')}/bitbucket-cicd"
PIPELINE_IMAGE_NAME = os.getenv("IMAGE_NAME")

# Required packages and versions for components (ensure that these are in sync with pyproject.toml)

# Google SDK specific
GOOGLE_CLOUD_BIGQUERY = "google-cloud-bigquery==3.11.0"
GOOGLE_CLOUD_STORAGE = "google-cloud-storage==2.9.0"
GOOGLE_CLOUD_AIPLATFORM = "google-cloud-aiplatform==1.26.0"

# Miscellaneous
LOGURU = "loguru==0.7.0"
PYARROW = "pyarrow==12.0.0"
REQUESTS = "requests==2.30.0"

# Data science
PANDAS = "pandas==1.4.3"
NUMPY = "numpy==1.23.0"
SCIKIT_LEARN = "scikit-learn==1.1.2"
JOBLIB = "joblib==1.2.0"
