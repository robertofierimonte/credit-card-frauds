#!make
.envvars: set-current-env-vars
set-current-env-vars:
	@poetry run python -m src.utils.environment

-include set-current-env-vars
-include .env
data-version ?= ""
export

help: ## Display this help screen
	@grep -h -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

pre-commit: ## Run the pre-commit over the entire repo
	@poetry run pre-commit run --all-files

pre-commit-light: ## Run the pre-commit over the entire repo skipping the notebooks stripout hook
	@export SKIP=nbstripout && $(MAKE) pre-commit

download-data: ## Download the dataset inside the `data` folder for local usage (called automatically from inside `make setup`)
	@poetry run python -m scripts.download_data && \
		mv data/credit-card-transactions/* data/ && \
		rm -rf data/credit-card-transactions/

setup: ## Install all the required Python dependencies, download the data, and create a jupyter kernel for the project
	@poetry env use ${PYENV_ROOT}/versions/$(shell cat .python-version)/bin/python && \
		poetry install && \
		$(MAKE) download-data && \
		poetry run python -m ipykernel install --user --name="${REPO_NAME}-venv"

unit-tests: ## Runs unit tests for the source code
	@poetry run python -m coverage run -m xmlrunner discover -b tests/base --output-file unit-tests.xml && \
		poetry run python -m coverage report -m

upload-data: ## Upload the data from the local folder to Bigquery and create a schema where to save the table. Optionally specify data-version={data_version}
	@poetry run python -m scripts.upload_data --data-version ${data-version}

build-image: ## Build the Docker image locally
	@docker build  \
		-f ./containers/base/Dockerfile \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		--cache-from ${IMAGE_NAME} \
		--tag ${IMAGE_NAME} \
		--build-arg PYTHON_VERSION="$(shell cat .python-version)" \
		--build-arg POETRY_VERSION="1.6.1" \
		.

push-image: ## Push the Docker image to the container registry. Must specify image=<base|bitbucket-cicd>
	@$(MAKE) build-image && \
		gcloud auth activate-service-account --key-file=${GOOGLE_APPLICATION_CREDENTIALS} --verbosity error && \
		gcloud auth configure-docker ${VERTEX_LOCATION}-docker.pkg.dev --verbosity error && \
		docker push ${IMAGE_NAME}

compile: ## Compile the pipeline. Must specify pipeline=<training|prediction>
	@poetry run python -m src.pipelines.${pipeline}.pipeline

run: ## Run the pipeline. Must specify pipeline=<training|prediction>. Optionally specify data-version={data_version}
	@$(MAKE) compile && \
		poetry run python -m src.trigger.main --payload=./src/pipelines/${pipeline}/payloads/${PAYLOAD} --data-version=${data-version}

run-server-local: ## Run the REST API server
	@$(MAKE) build-image && \
		docker run -it --rm --env-file=.env -p 8080:8080 -v ./model:/tmp/model --entrypoint=gunicorn \
		${IMAGE_NAME} src.serving_api.app:app --config=./src/serving_api/config.py

test-api-health: ## Check that the API is healthy
	@curl -X GET http://localhost:8080/health

test-api-predict: ## Send a prediction request to the API. Must specify a file name with payload=<payload>
	@curl -X POST -H 'accept: application/json' -H 'Content-Type: application/json' -d @${payload} http://localhost:8080/predict
