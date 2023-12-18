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
	@poetry env use $(shell cat .python-version) && \
		poetry install --without beam --sync && \
		$(MAKE) download-data && \
		poetry run python -m ipykernel install --user --name="credit-card-frauds-venv"

unit-tests: ## Runs unit tests for pipeline components
	@poetry run python -m pytest tests/base --junitxml=unit-base.xml

unit-components-tests: ## Runs unit tests for base source code and pipeline components
	@poetry run python -m pytest tests/components --junitxml=unit-components.xml

trigger-tests: ## Runs unit tests for the pipeline trigger code
	@unset GOOGLE_APPLICATION_CREDENTIALS VERTEX_TRIGGER_MODE && \
    poetry run python -m pytest tests/trigger --junitxml=trigger.xml

e2e-tests: ## Compile pipeline, trigger pipeline and perform end-to-end (E2E) pipeline tests. Must specify pipeline=<training|prediction>
	@$(MAKE) compile && \
	poetry run python -m pytest tests/pipelines/$(pipeline) --junitxml=$(pipeline).xml

upload-data: ## Upload the data from the local folder to Bigquery and create a schema where to save the table. Optionally specify data-version={data_version}
	@poetry run python -m scripts.upload_data --data-version ${data-version}

build-image: ## Build the Docker image locally
	@docker build  \
		-f ./containers/Dockerfile \
		--build-arg BUILDKIT_INLINE_CACHE=1 \
		--cache-from ${IMAGE_NAME} \
		--tag ${IMAGE_NAME} \
		--build-arg PYTHON_VERSION="$(shell cat .python-version)" \
		--build-arg POETRY_VERSION="1.6.1" \
		.

push-image: ## Push the Docker image to the container registry. Must specify image=<base|bitbucket-cicd>
	@$(MAKE) build-image && \
		gcloud auth activate-service-account --key-file=${GOOGLE_APPLICATION_CREDENTIALS} --quiet --verbosity error && \
		gcloud auth configure-docker ${VERTEX_LOCATION}-docker.pkg.dev --quiet --verbosity error && \
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
	@curl -X POST -H 'accept: application/json' -H 'Content-Type: application/json' -d @${payload-file} http://localhost:8080/predict

tf-init-validate: ## Runs terraform init and validate
	@export GOOGLE_APPLICATION_CREDENTIALS=${TF_GOOGLE_APPLICATION_CREDENTIALS} && \
    cd terraform && \
    rm -rf .terraform && \
    terraform init -backend-config="prefix=${VERTEX_PROJECT_ID}-triggers" -backend-config="bucket=${VERTEX_PROJECT_ID}-tfstates" && \
    terraform validate

tf-plan: ## Runs terraform plan
	@$(MAKE) tf-init-validate && \
    echo \
    { \
      \"cloud_run_config\": { \
        \"image\": \"${PIPELINE_IMAGE_NAME}\", \
        \"command\": [\"gunicorn\"], \
        \"args\": [\"src.trigger.app:app\", \"--config=./src/trigger/config.py\"], \
        \"container_port\": \"8080\", \
        \"env_vars\": { \
          \"VERTEX_LOCATION\": \"${VERTEX_LOCATION}\", \
          \"VERTEX_PROJECT_ID\": \"${VERTEX_PROJECT_ID}\", \
          \"VERTEX_SA_EMAIL\": \"${VERTEX_SA_EMAIL}\", \
          \"PIPELINE_TAG\": \"${PIPELINE_TAG}\", \
          \"VERTEX_PIPELINE_FILES_GCS_PATH\": \"${VERTEX_PIPELINE_FILES_GCS_PATH}\", \
          \"VERTEX_PIPELINE_ROOT\": \"${VERTEX_PIPELINE_ROOT}\", \
          \"TEMPLATE_BASE_PATH\": \"${VERTEX_PIPELINE_FILES_GCS_PATH}\" \
        }, \
        \"service_account\": \"terraform-deploy-sa@${VERTEX_PROJECT_ID}.iam.gserviceaccount.com\", \
        \"vpc_connector\": \"cloud-function-connector\" \
      } \
    } > terraform/cloud_run_config.json && \
    cd terraform && \
    terraform plan \
      -out=output.tfplan \
      -var-file=project_configuration/${VERTEX_PROJECT_ID}/variables.auto.tfvars \
      -var-file=cloud_run_config.json

tf-apply: ## Runs terraform apply
	@$(MAKE) tf-plan && \
    cd terraform && \
    terraform apply -input=false output.tfplan

tf-destroy: ## Runs terraform destroy
	@export GOOGLE_APPLICATION_CREDENTIALS=${TF_GOOGLE_APPLICATION_CREDENTIALS} && \
    cd terraform && \
    terraform destroy -auto-approve -input=false \
    -var-file=project_configuration/${VERTEX_PROJECT_ID}/variables.auto.tfvars

release: ## Create a new model release. Must specify version={release_version}
	@echo 'AAA'
