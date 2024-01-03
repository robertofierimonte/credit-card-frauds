import os

# Required image for components + Vertex training
PIPELINE_IMAGE_NAME = os.getenv("IMAGE_NAME")

# Extra packages
MATPLOTLIB = "matplotlib==3.5.1"
TFDV = "tensorflow-data-validation==1.13.0"
