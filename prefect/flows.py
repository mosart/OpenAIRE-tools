from __future__ import annotations

import os
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

import duckdb
import requests
from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner


def _sanitize_paths_for_sql(paths: Iterable[Path]) -> str:
    parts = []
    for p in paths:
        s = str(p)
        s = s.replace("'", "''")
        parts.append(f"'{s}'")
    return ",".join(parts)


@task(retries=3, retry_delay_seconds=10)
def download_file(url: str, save_path: Path, chunk_size: int = 1024 * 1024) -> Path:
    logger = get_run_logger()
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)

    if save_path.exists() and save_path.stat().st_size > 0:
        logger.info(f"File already exists: {save_path}")
        return save_path

    logger.info(f"Downloading {url} -> {save_path}")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(save_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
    logger.info(f"Downloaded: {save_path} ({save_path.stat().st_size} bytes)")
    return save_path


@task
def extract_tar(tar_path: Path, extract_to: Path) -> Path:
    logger = get_run_logger()
    tar_path = Path(tar_path)
    extract_to = Path(extract_to)
    extract_to.mkdir(parents=True, exist_ok=True)

    logger.info(f"Extracting {tar_path} -> {extract_to}")
    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(path=extract_to)
    logger.info(f"Extracted to: {extract_to}")
    return extract_to


@task
def find_latest_subdirectory(root: Path) -> Optional[Path]:
    root = Path(root)
    subdirs = [p for p in root.iterdir() if p.is_dir()]
    if not subdirs:
        return None
    return max(subdirs, key=lambda p: p.stat().st_mtime)


@task
def choose_json_root(extracted_root: Path) -> Path:
    """Return the directory containing the JSON files.

    If exactly one subdirectory exists, descend into it; otherwise use the root.
    """
    root = Path(extracted_root)
    subs = [p for p in root.iterdir() if p.is_dir()]
    if len(subs) == 1:
        return subs[0]
    return root


@task
def list_json_gz_files(folder: Path) -> List[Path]:
    folder = Path(folder)
    return sorted(folder.glob("*.json.gz"))


@task
def transform_json_to_parquet(json_gz_files: List[Path], output_parquet_path: Path) -> Path:
    logger = get_run_logger()
    if not json_gz_files:
        raise ValueError("No .json.gz files found to transform")

    out = Path(output_parquet_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    files_sql = _sanitize_paths_for_sql(json_gz_files)
    sql = (
        "COPY (\n"
        "    SELECT * FROM read_json_auto([" + files_sql + "], \n"
        "        compression='gzip', union_by_name=true, ignore_errors=true)\n"
        ") TO '" + str(out).replace("'", "''") + "' (FORMAT 'parquet');"
    )

    logger.info(f"Writing Parquet: {out}")
    con = duckdb.connect()
    try:
        con.execute(sql)
    finally:
        con.close()
    logger.info(f"Parquet written: {out}")
    return out


@flow(name="subgraph-etl", task_runner=ConcurrentTaskRunner())
def process_subgraph_data(
    url: str = "https://zenodo.org/records/14887484/files/aurora.tar?download=1",
    base_folder: Path | str = Path("./data"),
) -> dict:
    logger = get_run_logger()
    base_folder = Path(base_folder)

    timestamp = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = url.split("/")[-1].split("?")[0]

    input_folder = base_folder / "01_input"
    extracted_folder = base_folder / "02_extracted" / f"{file_name}_{timestamp}"
    transformed_folder = base_folder / "03_transformed"

    download_path = input_folder / file_name

    # Download and extract
    downloaded = download_file(url, download_path)
    extracted_root = extract_tar(downloaded, extracted_folder)

    # Identify where JSON files live
    candidate_root = choose_json_root(extracted_root)
    latest_subdir = find_latest_subdirectory(candidate_root)
    json_root = latest_subdir if latest_subdir else candidate_root

    # Transform to Parquet
    json_files = list_json_gz_files(json_root)
    parquet_path = transformed_folder / f"{file_name}_{timestamp}.parquet"
    parquet_file = transform_json_to_parquet(json_files, parquet_path)

    logger.info("ETL finished")
    return {
        "download": str(downloaded),
        "extracted": str(extracted_root),
        "json_root": str(json_root),
        "json_count": len(json_files),
        "parquet": str(parquet_file),
    }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run the Subgraph ETL Prefect flow")
    parser.add_argument("--url", type=str, default="https://zenodo.org/records/14887484/files/aurora.tar?download=1")
    parser.add_argument("--base-folder", type=str, default="./data")
    args = parser.parse_args()

    result = process_subgraph_data(url=args.url, base_folder=args.base_folder)
    print(result)

