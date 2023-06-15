#!make
.envvars: set-current-env-vars
set-current-env-vars:
	@poetry run python -m src.utils.environment

-include set-current-env-vars
-include .env
enable-caching ?= ""
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
	@poetry install && \
		$(MAKE) download-data && \
		poetry run python -m ipykernel install --user --name="credit-card-frauds-venv"

upload-data: ## Upload the data from the local folder to Azure and create a data asset. Optionally specify data-version={data_version}
	@poetry run python -m scripts.upload_data --data-version ${data-version}

build-image: ## Build the Docker image locally. Must specify image=<base|bitbucket-cicd>
	@if [ ${image} = "base" ]; then \
		docker build --tag ${IMAGE_NAME} -f ./containers/${image}/Dockerfile .; \
	elif [ ${image} = "bitbucket-cicd" ]; then \
		docker build --tag ${ARTIFACT_REGISTRY_REPO}/${image} -f ./containers/${image}/Dockerfile \
		--build-arg BITBUCKET_WORKSPACE=${BITBUCKET_WORKSPACE} \
		--build-arg BITBUCKET_REPO_SLUG=${BITBUCKET_REPO_SLUG} \
		--build-arg BITBUCKET_CICD_TOKEN=${BITBUCKET_CICD_TOKEN} .; \
	else \
		echo "Image name unknown"; \
	fi

push-image: ## Push the Docker image to the container registry. Must specify image=<base|bitbucket-cicd>
	@ $(MAKE) build-image && \
	gcloud auth activate-service-account --key-file=${GOOGLE_APPLICATION_CREDENTIALS} && \
	gcloud auth configure-docker ${VERTEX_LOCATION}-docker.pkg.dev && \
	if [ ${image} = "base" ]; then \
		docker push ${IMAGE_NAME}; \
	elif [ ${image} = "bitbucket-cicd" ]; then \
		docker push ${ARTIFACT_REGISTRY_REPO}/${image}; \
	else \
		echo "Image name unknown"; \
	fi

compile: ## Compile the pipeline. Must specify pipeline=<training|prediction>
	@ poetry run python -m src.pipelines.${pipeline}.pipeline

run: ## Run the pipeline. Must specify pipeline=<training|prediction>. Optionally specify enable-caching=<true|false> and data-version={data_version}
	@ $(MAKE) compile && \
		poetry run python -m src.trigger.main --payload=./src/pipelines/${pipeline}/payloads/${pipeline}.json --enable-caching=${enable-caching} --data-version=${data-version}
