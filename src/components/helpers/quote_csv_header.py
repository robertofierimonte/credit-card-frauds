from pathlib import Path
from typing import Optional

from kfp.v2.dsl import Dataset, Input, Output, component

from src.components.dependencies import LOGURU, PYTHON


@component(
    base_image=PYTHON,
    packages_to_install=[LOGURU],
    output_component_file=str(Path(__file__).with_suffix(".yaml")),
)
def quote_csv_header(
    input_dataset: Input[Dataset],
    output_dataset: Output[Dataset],
    file_pattern: Optional[str] = None,
) -> None:
    """Adjust header in CSV datasets to include quotes for each column name.

    Args:
        input_dataset (Input[Dataset]): Input Dataset in CSV format.
        output_dataset (Output[Dataset]): Output Dataset with the adjusted header.
        file_pattern (Optional[str], optional): Optional file pattern (e.g.
            "files-*.csv") which assumes that `input_dataset` is a folder
            containing files. Each file in the folder will be processed and
            saved with the same name in the folder `output_dataset`. If not
            provided both `input_dataset` and `output_dataset` are assumed to
            be a single file. Defaults to None.
    """
    import shutil
    from pathlib import Path

    from loguru import logger

    def quote_cols_in_header(
        header: str, quote: str = '"', delimiter: str = ","
    ) -> str:
        quoted_cols = [quote + col + quote for col in header.split(delimiter)]
        return delimiter.join(quoted_cols)

    def process_file(input_path: Path, output_path: Path) -> None:
        logger.info()
        with open(input_path, "r") as in_f:
            # Read first time and strip newline
            header = in_f.readline().rstrip("\n")
            logger.info(f"Found header: {header}.")

            # Change header
            new_header = quote_cols_in_header(header)
            logger.info(f"Adjusted header to: {new_header}.")

            logger.info(f"Writing output file to {output_path}.")
            with open(output_path, "w") as out_f:
                # Write updated header
                out_f.write(new_header + "\n")
                # Write remaining input file content
                shutil.copyfileobj(in_f, out_f)

    input_path = Path(input_dataset.path)
    output_path = Path(output_dataset.path)

    if file_pattern:
        output_path.mkdir(exist_ok=True, parents=True)
        for input_file in input_path.glob(file_pattern):
            process_file(input_file, output_path / input_file.name)
    else:
        process_file(input_path, output_path)
