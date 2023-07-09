import os
from pathlib import Path
from typing import Tuple

import dotenv
from loguru import logger


def get_current_git_info() -> Tuple[str, str, str, bool]:
    """Return git branch, sha, and running environment of the repository.

    Returns:
        str: Current branch
        str: Current tag
        str: Short SHA (7 digits) of the current commit
        bool: Whether the code is executing on a local environment (True) or \
            in a CI/CD pipeline (False)
    """
    if os.environ.get("CI"):
        # pick up sha and branch name if running from Bitbucket CI
        sha = os.environ["BITBUCKET_COMMIT"][:7]
        branch = os.environ.get("BITBUCKET_BRANCH", "master").replace("/", "-")
        tag = os.environ.get("BITBUCKET_TAG", "no_tag")
        is_local_env = False
        logger.info(f"Running on Bitbucket with sha={sha}, branch={branch}.")
    else:
        import git

        repo = git.Repo(search_parent_directories=True)
        sha = repo.git.rev_parse(repo.head, short=7)
        branch = str(repo.active_branch).replace("/", "-")
        tag = "no_tag"
        is_local_env = True
        logger.info(f"Running locally with sha={sha}, branch={branch}.")
    return branch, tag, sha, is_local_env


def set_env_variables(env_var_path: os.PathLike = ".env") -> None:
    """Configure the environment variables based on the current git info.

    Args:
        env_var_path (os.PathLike, optional): Path of the .env file. \
            Defaults to ".env".
    """
    git_branch, git_tag, commit_sha, is_local_env = get_current_git_info()
    if is_local_env:
        current_env_vars = dotenv.dotenv_values(env_var_path)
    else:
        current_env_vars = os.environ

    development_stage = current_env_vars["DEVELOPMENT_STAGE"]

    base_image_name = current_env_vars["BASE_IMAGE_NAME"]
    container_repo = current_env_vars["ARTIFACT_REGISTRY_REPO"]

    if git_branch == "master":
        image_name = f"{container_repo}/{base_image_name}:latest"
    else:
        image_name = f"{container_repo}/{base_image_name}:{git_branch}"

    dotenv.set_key(env_var_path, "IMAGE_NAME", image_name, quote_mode="never")
    dotenv.set_key(env_var_path, "PIPELINE_TAG", development_stage, quote_mode="never")
    dotenv.set_key(env_var_path, "CURRENT_COMMIT", commit_sha, quote_mode="never")
    dotenv.set_key(env_var_path, "CURRENT_TAG", git_tag, quote_mode="never")
    dotenv.set_key(env_var_path, "CURRENT_BRANCH", git_branch, quote_mode="never")
    dotenv.set_key(env_var_path, "IS_LOCAL_ENV", str(is_local_env), quote_mode="never")


if __name__ == "__main__":
    if not Path(".env").is_file():
        with open(".env", "w") as f:
            pass
    logger.info("Setting up environment variables...")
    set_env_variables(".env")
    logger.info("Done.")
