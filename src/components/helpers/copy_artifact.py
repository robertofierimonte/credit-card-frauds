from typing import Optional

from kfp.dsl import Artifact, Input, Output, component

from src.components.dependencies import PIPELINE_IMAGE_NAME


@component(base_image=PIPELINE_IMAGE_NAME)
def copy_artifact(
    source_artifact: Input[Artifact],
    dest_artifact: Output[Artifact],
    dest_uri: Optional[str] = None,
    dest_uri_suffix: Optional[str] = None,
) -> None:
    """Copy an Artifact from one location to another.

    Args:
        source_artifact (Input[Artifact]): Source Artifact.
        dest_artifact (Output[Artifact]): Destination Artifact.
        dest_uri (Optional[str], optional): If provided, set destination URI of
            copied Artifact. Defaults to None.
        dest_uri_suffix (Optional[str], optional): If provided, append a suffix
            string to the destination URI. Defaults to None.
    """
    import shutil
    from pathlib import Path

    from loguru import logger

    if dest_uri is not None:
        if dest_uri_suffix is not None:
            dest_artifact.uri = dest_uri + "_" + dest_uri_suffix
        else:
            dest_artifact.uri = dest_uri

    # Ensure that the destination's parent folder(s) exist
    Path(dest_artifact.path).parent.mkdir(parents=True, exist_ok=True)

    # Copy the artifact
    try:
        shutil.copy(source_artifact.path, dest_artifact.path)
        logger.info(f"Copied {source_artifact.path} to {dest_artifact.path}.")
    except OSError as e:
        logger.error(e)
        raise e
