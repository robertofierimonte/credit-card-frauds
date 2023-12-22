import os
import re
from pathlib import Path
from typing import Tuple

import dotenv
from loguru import logger


def _clean_git_info(*args: str) -> tuple[str, ...]:
    """Sanitise the git arguments passed as inputs.

    Returns:
        tuple[str, ...]: Inputs lower-cased and stripped of _:/. characters
    """
    return (re.sub(r"[_.:\/]", "-", a).lower() for a in args)


def get_current_git_info() -> Tuple[bool, str, str, str, str, str]:
    """Return git branch, sha, and running environment of the repository.

    Returns:
        bool: Whether the code is executing in a CI/CD pipeline (True) or \
            in a local environment (False)
        str: Current CI/CD deployment environment
        str: Name of the git repository
        str: Current git branch
        str: Current git tag
        str: Short SHA (7 digits) of the current git commit
    """
    if os.environ.get("CI") and os.environ.get("BITBUCKET_BUILD_NUMBER"):
        # Pick up git info if running from Bitbucket CI
        env = os.environ.get("BITBUCKET_DEPLOYMENT_ENVIRONMENT", "prod")
        sha = os.environ.get("BITBUCKET_COMMIT", "no-sha")[:7]
        branch = os.environ.get("BITBUCKET_BRANCH", "no-branch")
        tag = os.environ.get("BITBUCKET_TAG", "no-tag")
        repo_name = os.environ.get("BITBUCKET_REPO_SLUG")
        is_cicd = True
        logger.info(
            f"Running on Bitbucket with sha={sha}, branch={branch}, tag={tag}, env={env}."
        )
    elif os.environ.get("CI") and os.environ.get("GITLAB_CI"):
        # Pick up git info if running from Gitlab CI
        env = os.environ.get("CI_ENVIRONMENT_SLUG", "prod")
        sha = os.environ.get("CI_COMMIT_SHORT_SHA", "no-sha")
        branch = os.environ.get("CI_COMMIT_BRANCH", "no-branch")
        tag = os.environ.get("CI_COMMIT_TAG", "no-tag")
        repo_name = os.environ.get("CI_PROJECT_NAME")
        is_cicd = True
        logger.info(
            f"Running on Gitlab with sha={sha}, branch={branch}, tag={tag}, env={env}."
        )
    elif os.environ.get("CI") and os.environ.get("GITHUB_ACTIONS"):
        # Pick up git info if running from Github Actions CI
        sha = os.environ.get("GITHUB_SHA", "no-sha")[:7]
        ref_type = os.environ.get("GITHUB_REF_TYPE")
        if ref_type == "branch":
            branch = os.environ.get("GITHUB_REF_NAME")
            tag = "no-tag"
            env = "dev"
        elif ref_type == "tag":
            branch = "no-branch"
            tag = os.environ.get("GITHUB_REF_NAME")
            env = "prod"
        repo_name = os.environ.get("GITHUB_REPOSITORY").split("/")[-1]
        is_cicd = True
        logger.info(
            f"Running on Github with sha={sha}, branch={branch}, tag={tag}, env={env}."
        )
    else:
        # Pick up git info from local folder
        import git

        repo = git.Repo(search_parent_directories=True)
        env = "dev"
        sha = repo.git.rev_parse(repo.head, short=7)
        branch = str(repo.active_branch)
        tag = "no-tag"
        repo_name = repo.remotes.origin.url.split(".git")[0].split("/")[-1]
        is_cicd = False
        logger.info(
            f"Running locally with sha={sha}, branch={branch}, tag={tag}, env={env}."
        )
    return is_cicd, env, repo_name, branch, tag, sha


def set_env_variables(env_var_path: os.PathLike = ".env") -> None:
    """Configure the environment variables based on the current git info.

    Args:
        env_var_path (os.PathLike, optional): Path of the .env file. \
            Defaults to ".env".
    """
    is_cicd, env, repo_name, git_branch, git_tag, commit_sha = get_current_git_info()
    repo_name, git_branch, git_tag, commit_sha = _clean_git_info(
        repo_name, git_branch, git_tag, commit_sha
    )
    if is_cicd:
        current_env_vars = os.environ
    else:
        current_env_vars = dotenv.dotenv_values(env_var_path)

    docker_repo = current_env_vars["DOCKER_REPO"]
    project_id = current_env_vars["VERTEX_PROJECT_ID"]
    project_location = current_env_vars["VERTEX_LOCATION"]

    gcs_root = f"gs://{project_id}/{repo_name}"

    if git_tag != "no-tag":
        pipeline_tag = git_tag
        image_tag = git_tag
        pipeline_root = f"{gcs_root}-pipeline-root/{git_tag}"
        pipeline_files_gcs_path = f"{gcs_root}/{git_tag}"
    else:
        pipeline_tag = f"{git_branch}-{commit_sha}"
        image_tag = git_branch
        pipeline_root = f"{gcs_root}-pipeline-root/{git_branch}/{commit_sha}"
        pipeline_files_gcs_path = f"{gcs_root}/{git_branch}/{commit_sha}"

    image_name = f"{project_location}-docker.pkg.dev/{project_id}/{docker_repo}"
    image_name = f"{image_name}/{repo_name}:{image_tag}"

    dotenv.set_key(
        env_var_path, "VERTEX_PIPELINE_ROOT", pipeline_root, quote_mode="never"
    )
    dotenv.set_key(
        env_var_path,
        "VERTEX_PIPELINE_FILES_GCS_PATH",
        pipeline_files_gcs_path,
        quote_mode="never",
    )
    dotenv.set_key(env_var_path, "ENVIRONMENT", env, quote_mode="never")
    dotenv.set_key(env_var_path, "PAYLOAD", f"{env}.json", quote_mode="never")
    dotenv.set_key(env_var_path, "IMAGE_NAME", image_name, quote_mode="never")
    dotenv.set_key(env_var_path, "PIPELINE_TAG", pipeline_tag, quote_mode="never")
    dotenv.set_key(env_var_path, "IS_CICD", str(is_cicd), quote_mode="never")
    dotenv.set_key(env_var_path, "REPO_NAME", repo_name, quote_mode="never")
    dotenv.set_key(env_var_path, "CURRENT_COMMIT", commit_sha, quote_mode="never")
    dotenv.set_key(env_var_path, "CURRENT_TAG", git_tag, quote_mode="never")
    dotenv.set_key(env_var_path, "CURRENT_BRANCH", git_branch, quote_mode="never")


if __name__ == "__main__":
    if not Path(".env").is_file():
        with open(".env", "w") as f:
            pass
    set_env_variables(".env")
