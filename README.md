<div id="top"></div>

<!-- PROJECT LOGO -->
<br>
<div align="center">
  <a href="https://github.com/robertofierimonte/credit-card-frauds/">
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
  - Handling, storing, processing, and versioning data.
  - Training, versioning, and deployment of ML models.
  - Monitoring and automated retraining of ML models.
  - General proficiency in Google Cloud Platform.

<p align="right">(<a href="#top">back to top</a>)</p>

## Getting Started

### Prerequisites
- [Python](https://www.python.org/) (at least version `3.9`)
- [Pyenv](https://github.com/pyenv/pyenv) (the `PYENV_ROOT` environment variable must be set up)
- [Poetry](https://python-poetry.org/) (we are using version `1.6.1`)
- [Docker](https://www.docker.com/) (we are using version `24.0.6`)
- [Terraform](https://www.terraform.io/) (we are using version `1.5.7`)
- [Google Cloud CLI SDK](https://cloud.google.com/sdk/docs/install)
- [GNU make](https://www.gnu.org/software/make/) (included by default in Linux and Mac machines, must be installed on Windows)

**Accounts:**
- A [Kaggle](https://www.kaggle.com/) account and relative API credentials
- A [GCP](https://cloud.google.com/) account with Vertex AI APIs enabled and relative Service Accounts. For more info about the GCP configuration please refer to the [GCP README](./docs/GCP.md).

### Repo structure

The repository is structured as follows:

```
credit-card-frauds
├── .github
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

- The `.github` folder contains the Github Actions CI/CD workflows
- The `containers` folder contains the Dockerfile for the base image
- The `data` folder is empty and it will be populated with the dataset after completing the local setup
- The `docs` folder contains the images and additional documentation about the project
- The `notebooks` folder contains the notebooks provided for exploratory analysis on the data, as well as the training and testing of the models
- The `scripts` folder contains the scripts for downloading the raw data from Kaggle and uploading it to Bigquery. For more info run: `poetry run python scripts/upload_data.py --help`
- The `src` folder contains the source code for the ML model
     * The `base` subfolder contains the code provided used for the exploratory analysis and the modelling
     * The `components` subfolder contains the code for the pipeline's components
     * The `pipelines` subfolder contains the code for the training pipeline
     * The `serving_api` subfolder contains the code to deploy the model on a Vertex AI endpoint or on a local endpoint and to use it to make predictions
     * The `trigger` subfolder contains the code to trigger the Vertex AI pipelines
     * The `utils` subfolder contains the code to support other functionalities (e.g. manage environment variables and patch jupyter notebooks)
- The `tests` folder contains the unit tests for the source code

### GCP setup

Please refer to the [GCP README](./docs/GCP.md).

### Local setup

In the root directory of the repository, execute:

1. Install the required version of poetry: `pip install --upgrade pip --quiet && pip install poetry==1.6.1 --quiet`
2. Download and install the required verion of python: `pyenv install $(sh cat .python-version)` (accept if requested)
3. Set up the virtual environment, install the packages, and configure the Jupyter kernel: `make setup` (don't worry about an error message related to some dependencies not being installed, that will be fixed by installing the dependencies during the setup)
4. Create 2 configuration files (`.env`, and `kaggle.json`) by copying the examples provided: `cp .env.example .env && cp kaggle.json.example kaggle.json`
5. Populate the `.env` and `kaggle.json` configuration files with the necessary environment variables
6. Download the data: `make download-data`
7. See all the options provided in the Makefile: `make help`

**LightGBM not installing on Mac M1/M2:**
- If the `make setup` command fails with the error `Exception: An error has occurred while building lightgbm library file` when installing dependencies on Macbooks with M1/M2 processors, you can fix it by using a more recent version of `libomp`: `brew install libomp && export LDFLAGS="-L/opt/homebrew/opt/libomp/lib" CPPFLAGS="-I/opt/homebrew/opt/libomp/include" && make setup`. Don't forget to restart the terminal afterwards to reset the env variables to their original values.

### Running the code

I recommend to use the `make` recipes provided to interact with the code, as they abstract some of the complexity, and automatically source environment variables into the scripts. You can list them and see their usage with `make help`.

1. Check that the code is free from (most) pesky bugs: `make unit-tests`
2. Upload the newly downloaded data to GCP Bigquery: `make upload-data`
3. ...
4.  Enjoy!

<p align="right">(<a href="#top">back to top</a>)</p>
