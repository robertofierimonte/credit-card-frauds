<div id="top"></div>

<!-- PROJECT LOGO -->
<br>
<div align="center">
  <a href="https://bitbucket.org/robertofierimonte/credit-card-frauds/">
    <img src="docs/images/logo.png" alt="Logo" height="350">
  </a>

<h2 align="center">MLOps for Credit Card Frauds Detection</h2>
  <p>End-to-end training and serving pipelines to deploy and manage credit card fraud detection models in GCP using Vertex AI.<br>
  Author: <a href="mailto:roberto.fierimonte@gmail.com"><b>Roberto Fierimonte</b></a></p>
</div>

<!-- TABLE OF CONTENTS -->
 <h3 align="center">Table of Contents</h3>
 <ol>
 <li>
  <a href="#about-the-project">About the Project</a>
 </li>
 <li>
  <a href="#getting-started">Getting Started</a>
  <ul>
   <li><a href="#prerequisites">Prerequisites</a></li>
   <li><a href="#repo-structure">Repo structure</a></li>
   <li><a href="#gcp-setup">GCP setup</a></li>
   <li><a href="#bitbucket-cicd-setup">Bitbucket CICD setup</a></li>
   <li><a href="#local-setup">Local setup</a></li>
   <li><a href="#running-the-code">Running the code</a></li>
  </ul>
 </li>
 <li>
  <a href="#classification-example">Classification Example</a>
 </li>
</ol>

<!-- ABOUT THE PROJECT -->
## About the Project

The goal of this project is to implement training and serving pipelines for credit card fraud detection models in GCP.

The project aims at showcasing technical skills and understanding of:
  - Data handling, storage, and processing.
  - Model training, versioning, and deployment.
  - Model monitoring.
  - Cloud platforms.

<p align="right">(<a href="#top">back to top</a>)</p>

## Getting Started

### Prerequisites
- [Python](https://www.python.org/) (at least version `3.9`)
- [Pyenv](https://github.com/pyenv/pyenv) (the `PYENV_ROOT` environment variable must be set up)
- [Poetry](https://python-poetry.org/) (we are using version `1.6.1`)
- [Docker](https://www.docker.com/) (we are using version `24.0.6`)
- [Terraform](https://www.terraform.io/) (we are using version `1.5.7`)
- [Google Cloud CLI SDK](https://cloud.google.com/sdk/docs/install)
- `make`
- `base64`

Accounts:
- A [Kaggle](https://www.kaggle.com/) account and relative API credentials
- A [GCP](https://cloud.google.com/) account and relative API credentials

### Repo structure

The repository is structured as follows:

```
credit-card-frauds
├── containers
├── data
├── docs
├── model
├── notebooks
├── scripts
├── src
|    ├── base
|    ├── components
|    ├── pipelines
|    ├── serving_api
|    ├── trigger
|    └── utils
└── tests
```

- The `containers` folder
- The `data` folder is empty and it will be populated with the dataset after completing the local setup
- The `docs` folder contains the images and additional documentation about the project
- The `notebooks` folder contains the notebooks provided for exploratory analysis on the data, as well as the training and testing of the models
- The `src` folder contains the source code for the ML
     * The `base` subfolder contains the code provided used for the exploratory analysis and the modelling
     * The `components` subfolder contains the code for the pipeline's components
     * The `pipelines` subfolder contains the code for the training pipeline
     * The `serving_api` subfolder contains the code to deploy the model on a Vertex AI endpoint or on a local endpoint and to use it to make predictions
     * The `scripts` folder contains the python entry-point scripts used to interact with the code
     * The `utils` subfolder contains the code to support other functionalities (e.g. manage environment variables and patch jupyter notebooks)
- The `tests` folder contains the unit tests for the source code

### GCP setup

### Bitbucket CICD setup

### Local setup

In the repository, execute:

1. Install the required version of poetry: `pip install --upgrade pip --quiet && pip install poetry==1.6.1 --quiet`
2. Download the data, set up the virtual environment, install the packages, and configure the Jupyter kernel: `make setup` (don't worry about an error message related to some dependencies not being installed, that will be fixed by installing the dependencies during the setup)
3. Create 2 configuration files (`.env`, and `kaggle.json`) by copying the examples provided
4. Populate the configuration files with the necessary environment variables
5. See all the options provided in the Makefile: `make help`

### Running the code

I recommend to use the `make` recipes provided to interact with the code, as they abstract some of the complexity, and automatically source environment variables into the scripts. You can list them and see their usage with `make help`.

1. Check that the code is free from (most) pesky bugs: `make unit-tests`
2. Upload the newly downloaded data to GCP Bigquery: `make upload-data`
3. ...
4.  Enjoy!

<p align="right">(<a href="#top">back to top</a>)</p>
