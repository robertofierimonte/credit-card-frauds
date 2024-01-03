import argparse
import base64
import datetime
import json
import os
from unittest import mock

import pytest
from cloudevents.http import CloudEvent

from src.trigger.main import cf_handler, convert_payload, get_env


def test_cf_handler():
    data = {"key1": "val1", "key2": "val2"}
    encoded_data = str(base64.b64encode(json.dumps(data).encode("utf-8")), "utf-8")
    payload = {
        "attributes": {"template_path": "gs://my-bucket/my-template-path.json"},
        "data": encoded_data,
    }
    attributes = {
        "type": "google.cloud.pubsub.topic.v1.messagePublished",
        "source": "//pubsub.googleapis.com/",
    }
    event = CloudEvent(attributes=attributes, data={"message": payload})

    with mock.patch(
        "src.trigger.main.trigger_pipeline_from_payload"
    ) as mock_trigger_pipeline_from_payload:
        cf_handler(event)

        mock_trigger_pipeline_from_payload.assert_called_with(
            {
                "attributes": {"template_path": "gs://my-bucket/my-template-path.json"},
                "data": data,
            }
        )


def test_trigger_pipeline():
    project_id = "my-test-project"
    location = "europe-west2"
    template_path = "gs://my-bucket/pipeline.json"
    parameter_values = {"key1": "val1", "key2": "val2"}
    pipeline_root = "gs://my-bucket/pipeline_root"
    service_account = "my_service_account@my-test-project.iam.gserviceaccount.com"
    display_name = "pipeline-execution"
    enable_caching = True

    with mock.patch("src.trigger.main.aiplatform") as mock_aiplatform:
        from src.trigger.main import trigger_pipeline

        pl = trigger_pipeline(
            project_id=project_id,
            location=location,
            template_path=template_path,
            parameter_values=parameter_values,
            pipeline_root=pipeline_root,
            service_account=service_account,
            enable_caching=enable_caching,
        )

        mock_aiplatform.init.assert_called_with(project=project_id, location=location)

        mock_aiplatform.PipelineJob.assert_called_with(
            display_name=display_name,
            enable_caching=enable_caching,
            template_path=template_path,
            parameter_values=parameter_values,
            pipeline_root=pipeline_root,
        )

        pl.submit.assert_called_with(service_account=service_account)


def test_trigger_pipeline_run():
    project_id = "my-test-project"
    location = "europe-west2"
    template_path = "gs://my-bucket/pipeline.json"
    parameter_values = {"key1": "val1", "key2": "val2"}
    pipeline_root = "gs://my-bucket/pipeline_root"
    service_account = "my_service_account@my-test-project.iam.gserviceaccount.com"
    display_name = "pipeline-execution"
    enable_caching = True
    mode = "run"

    with mock.patch("src.trigger.main.aiplatform") as mock_aiplatform, mock.patch(
        "src.trigger.main.wait_pipeline_until_complete"
    ) as mock_utils:
        from src.trigger.main import trigger_pipeline

        pl = trigger_pipeline(
            project_id=project_id,
            location=location,
            template_path=template_path,
            parameter_values=parameter_values,
            pipeline_root=pipeline_root,
            service_account=service_account,
            enable_caching=enable_caching,
            mode=mode,
        )

        mock_aiplatform.init.assert_called_with(project=project_id, location=location)

        mock_aiplatform.PipelineJob.assert_called_with(
            display_name=display_name,
            enable_caching=enable_caching,
            template_path=template_path,
            parameter_values=parameter_values,
            pipeline_root=pipeline_root,
        )

        pl.run.assert_called_with(service_account=service_account)

        mock_utils.assert_called_with(job=pl)


@pytest.fixture
def patch_datetime_now(monkeypatch):
    """Fixture to patch datetime.now() to a fixed date.

    Date used is 2021-10-21 00:00:00 (UTC).
    """
    FAKE_TIME = datetime.datetime(2021, 10, 21, 0, 0, 0)

    class MyDatetime:
        def now(self):
            return FAKE_TIME

    monkeypatch.setattr(datetime, "datetime", MyDatetime)


@pytest.mark.usefixtures("patch_datetime_now")
@pytest.mark.parametrize(
    "env_vars,test_input,expected",
    [
        # enable_caching
        (
            {},
            {"attributes": {"template_path": "pipeline.json"}},
            {
                "attributes": {
                    "template_path": "pipeline.json",
                    "enable_caching": None,
                },
                "data": {"email_notification_recipients": [""]},
            },
        ),
        # enable_caching true
        (
            {},
            {
                "attributes": {
                    "template_path": "pipeline.json",
                    "enable_caching": "true",
                }
            },
            {
                "attributes": {
                    "template_path": "pipeline.json",
                    "enable_caching": True,
                },
                "data": {"email_notification_recipients": [""]},
            },
        ),
        # template_path NOT overridden
        (
            {"MONITORING_EMAIL_ADDRESS": "email1"},
            {
                "attributes": {
                    "template_path": "pipeline.json",
                }
            },
            {
                "attributes": {
                    "template_path": "pipeline.json",
                    "enable_caching": None,
                },
                "data": {"email_notification_recipients": ["email1"]},
            },
        ),
        # template_path overridden
        (
            {"TEMPLATE_BASE_PATH": "gs://my-original-path"},
            {
                "attributes": {
                    "template_path": "pipeline.json",
                }
            },
            {
                "attributes": {
                    "template_path": "gs://my-original-path/pipeline.json",
                    "enable_caching": None,
                },
                "data": {"email_notification_recipients": [""]},
            },
        ),
        # model_file present and overridden
        (
            {
                "MODEL_FILE_PATH": "gs://my-overridden-path",
                "MONITORING_EMAIL_ADDRESS": "email1,email2",
            },
            {
                "attributes": {
                    "template_path": "pipeline.json",
                },
                "data": {
                    "model_file": "gs://my-original-path",
                },
            },
            {
                "attributes": {
                    "template_path": "pipeline.json",
                    "enable_caching": None,
                },
                "data": {
                    "model_file": "gs://my-overridden-path",
                    "email_notification_recipients": ["email1", "email2"],
                },
            },
        ),
        # model_file not present and overridden
        (
            {"MODEL_FILE_PATH": "gs://my-overridden-path"},
            {
                "attributes": {
                    "template_path": "pipeline.json",
                }
            },
            {
                "attributes": {
                    "template_path": "pipeline.json",
                    "enable_caching": None,
                },
                "data": {"email_notification_recipients": [""]},
            },
        ),
    ],
)
def test_convert_payload(env_vars, test_input, expected):
    with mock.patch.dict(os.environ, env_vars, clear=True):
        assert convert_payload(test_input) == expected


@pytest.mark.parametrize(
    "env_vars,expected",
    [
        (
            {
                "VERTEX_PROJECT_ID": "my-project-id",
                "VERTEX_LOCATION": "europe-west2",
                "VERTEX_PIPELINE_ROOT": "gs://my-pipeline-root/folder",
                "VERTEX_SA_EMAIL": "my-sa@my-project-id.iam.gserviceaccount.com",
                "VERTEX_TRIGGER_MODE": "run",
            },
            {
                "project_id": "my-project-id",
                "location": "europe-west2",
                "pipeline_root": "gs://my-pipeline-root/folder",
                "service_account": "my-sa@my-project-id.iam.gserviceaccount.com",
                "mode": "run",
            },
        ),
        (
            {
                "VERTEX_PROJECT_ID": "my-project-id",
                "VERTEX_LOCATION": "europe-west2",
                "VERTEX_PIPELINE_ROOT": "gs://my-pipeline-root/folder",
                "VERTEX_SA_EMAIL": "my-sa@my-project-id.iam.gserviceaccount.com",
                "VERTEX_TRIGGER_MODE": "",
            },
            {
                "project_id": "my-project-id",
                "location": "europe-west2",
                "pipeline_root": "gs://my-pipeline-root/folder",
                "service_account": "my-sa@my-project-id.iam.gserviceaccount.com",
                "mode": None,
            },
        ),
    ],
)
def test_get_env(env_vars, expected, monkeypatch):
    for k, v in env_vars.items():
        monkeypatch.setenv(k, v)

    env = get_env()

    assert env == expected


def test_sandbox_run():
    dummy_payload = {"attributes": {"template_path": "pipeline.json"}}
    dummy_payload_str = json.dumps(dummy_payload)

    with mock.patch("src.trigger.main.get_args") as mock_get_args, mock.patch(
        "builtins.open", mock.mock_open(read_data=dummy_payload_str)
    ) as mock_file, mock.patch(
        "src.trigger.main.trigger_pipeline_from_payload"
    ) as mock_trigger_pipeline_from_payload:
        # fix return value of get_args()
        mock_get_args.return_value = argparse.Namespace(payload="payload.json")

        from src.trigger.main import sandbox_run

        sandbox_run()

        mock_get_args.assert_called_once()
        mock_file.assert_called_once_with("payload.json")
        mock_trigger_pipeline_from_payload.assert_called_once_with(dummy_payload)
