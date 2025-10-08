from __future__ import annotations

import os
import tarfile
from datetime import datetime
import time
import json
from pathlib import Path
import sys
from typing import Iterable, List, Optional

import duckdb
import requests
import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.task_runners import ConcurrentTaskRunner


def _sanitize_paths_for_sql(paths: Iterable[Path]) -> str:
    parts = []
    for p in paths:
        s = str(p)
        s = s.replace("'", "''")
        parts.append(f"'{s}'")
    return ",".join(parts)


@task(name="download-file", retries=3, retry_delay_seconds=10, task_run_name="download {save_path}")
def download_file(url: str, save_path: Path, chunk_size: int = 1024 * 1024) -> Path:
    logger = get_run_logger()
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)

    def _validate_tar(path: Path) -> None:
        try:
            with tarfile.open(path, "r:*") as tar:
                # Force reading headers to detect truncation
                tar.getmembers()
        except tarfile.ReadError as e:
            raise ValueError(f"Downloaded file is not a valid tar archive or is truncated: {e}")

    # If a file already exists, verify it is a valid tar; if not, re-download
    if save_path.exists() and save_path.stat().st_size > 0:
        try:
            _validate_tar(save_path)
            logger.info(f"File already exists and validated: {save_path}")
            return save_path
        except Exception as e:
            logger.warning(f"Existing file invalid, re-downloading: {e}")
            try:
                save_path.unlink()
            except Exception:
                pass

    logger.info(f"Downloading {url} -> {save_path}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        expected = None
        try:
            expected = int(r.headers.get("Content-Length") or 0) or None
        except Exception:
            expected = None
        downloaded = 0
        tmp_path = save_path.with_suffix(save_path.suffix + ".part")
        with open(tmp_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
        tmp_path.replace(save_path)

    # Validate size if known
    if expected is not None and downloaded != expected:
        try:
            save_path.unlink()
        finally:
            raise ValueError(f"Download size mismatch: got {downloaded} bytes, expected {expected}")

    # Validate tar structure to prevent extraction errors later
    _validate_tar(save_path)
    logger.info(f"Downloaded and validated: {save_path} ({save_path.stat().st_size} bytes)")
    return save_path


@task(name="extract-tar", task_run_name="extract {tar_path}", retries=2, retry_delay_seconds=10)
def extract_tar(tar_path: Path, extract_to: Path) -> Path:
    logger = get_run_logger()
    tar_path = Path(tar_path)
    extract_to = Path(extract_to)
    extract_to.mkdir(parents=True, exist_ok=True)

    logger.info(f"Extracting {tar_path} -> {extract_to}")
    try:
        with tarfile.open(tar_path, "r:*") as tar:
            tar.extractall(path=extract_to)
    except tarfile.ReadError as e:
        logger.error(f"Extraction failed due to tar corruption: {e}")
        raise
    logger.info(f"Extracted to: {extract_to}")
    return extract_to


@task(name="find-latest-subdir", task_run_name="latest-of {root}")
def find_latest_subdirectory(root: Path) -> Optional[Path]:
    root = Path(root)
    subdirs = [p for p in root.iterdir() if p.is_dir()]
    if not subdirs:
        return None
    return max(subdirs, key=lambda p: p.stat().st_mtime)


@task(name="choose-json-root", task_run_name="json-root {extracted_root}")
def choose_json_root(extracted_root: Path) -> Path:
    """Return the directory containing the JSON files.

    If exactly one subdirectory exists, descend into it; otherwise use the root.
    """
    root = Path(extracted_root)
    subs = [p for p in root.iterdir() if p.is_dir()]
    if len(subs) == 1:
        return subs[0]
    return root


@task(name="list-json-gz", task_run_name="list-json {folder}")
def list_json_gz_files(folder: Path) -> List[Path]:
    folder = Path(folder)
    return sorted(folder.glob("*.json.gz"))


@task(name="transform-json-to-parquet", task_run_name="to-parquet {output_parquet_path}")
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


@flow(name="subgraph-etl", task_runner=ConcurrentTaskRunner(), flow_run_name="etl-{dataset}")
def process_subgraph_data(
    url: str = "https://zenodo.org/records/14887484/files/aurora.tar?download=1",
    base_folder: Path | str = Path("./data"),
    dataset: str | None = None,
) -> dict:
    logger = get_run_logger()
    base_folder = Path(base_folder)

    timestamp = datetime.utcnow().strftime("%Y-%m-%d")
    file_name = url.split("/")[-1].split("?")[0]
    # Auto-infer dataset name from URL if not provided
    if not dataset or not str(dataset).strip():
        dataset = Path(file_name).stem

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


# -----------------------------
# Orchestrator: ETL then parallel enrichments
# -----------------------------

@flow(name="etl-and-enrich", task_runner=ConcurrentTaskRunner(), flow_run_name="orchestrator-{dataset}")
def orchestrate_etl_and_enrichments(
    # ETL
    url: str = "https://zenodo.org/records/14887484/files/aurora.tar?download=1",
    base_folder: Path | str = Path("./data"),
    input_parquet: Path | str | None = None,
    dataset: str | None = None,
    # Toggles
    enable_altmetric: bool = True,
    enable_overton: bool = True,
    enable_sdg: bool = True,
    enable_genderize: bool = True,
    # Altmetric params
    record_id_column: str = "record_id",
    id_priority: list[str] | None = None,
    altmetric_api_key: str | None = None,
    altmetric_limit: int | None = None,
    altmetric_rate_per_minute: float = 60.0,
    altmetric_big_pause_every: int | None = None,
    altmetric_big_pause_seconds: float = 0.0,
    # Overton params
    doi_column: str = "doi",
    overton_api_key: str | None = None,
    overton_limit: int | None = None,
    overton_rate_per_minute: float = 30.0,
    overton_big_pause_every: int | None = None,
    overton_big_pause_seconds: float = 0.0,
    # SDG params
    abstract_column: str = "abstract",
    model: str = "aurora-sdg",
    min_tokens: int = 100,
    sdg_limit: int | None = None,
    sdg_rate_per_minute: float = 60.0,
    sdg_big_pause_every: int | None = None,
    sdg_big_pause_seconds: float = 0.0,
    # Genderize params
    first_name_column: str = "first_name",
    genderize_api_key: str | None = None,
    genderize_limit: int | None = None,
    genderize_rate_per_minute: float = 100.0,
    genderize_big_pause_every: int | None = None,
    genderize_big_pause_seconds: float = 0.0,
):
    logger = get_run_logger()
    base_folder = Path(base_folder)
    tasks_mod = sys.modules[__name__]

    # Determine dataset name (prefer explicit, else derive from input or URL)
    if not dataset or not str(dataset).strip():
        if input_parquet:
            dataset = Path(str(input_parquet)).stem
        else:
            _fname = url.split("/")[-1].split("?")[0]
            dataset = Path(_fname).stem

    # Determine input parquet
    if input_parquet:
        parquet_path = Path(input_parquet)
        logger.info(f"Using provided parquet: {parquet_path}")
    else:
        etl = process_subgraph_data(url=url, base_folder=base_folder, dataset=dataset)
        parquet_path = Path(etl["parquet"])  # type: ignore[index]
        logger.info(f"ETL parquet: {parquet_path}")

    # Prepare output folder and names
    folder_name = parquet_path.stem
    processing_folder = base_folder / "04_processed" / folder_name
    processing_folder.mkdir(parents=True, exist_ok=True)

    futures = {}
    # Altmetric
    if enable_altmetric:
        altmetric_out = processing_folder / f"{folder_name}-altmetric.parquet"
        futures["altmetric"] = tasks_mod.altmetric_enrich.submit(
            input_parquet=parquet_path,
            output_parquet=altmetric_out,
            record_id_column=record_id_column,
            id_priority=id_priority or ["doi", "pmid", "pmcid", "arxiv_id"],
            api_key=altmetric_api_key,
            limit=altmetric_limit,
            rate_per_minute=altmetric_rate_per_minute,
            big_pause_every=altmetric_big_pause_every,
            big_pause_seconds=altmetric_big_pause_seconds,
        )

    # Overton
    if enable_overton:
        overton_out = processing_folder / f"{folder_name}-overton.parquet"
        futures["overton"] = tasks_mod.overton_enrich.submit(
            input_parquet=parquet_path,
            output_parquet=overton_out,
            record_id_column=record_id_column,
            doi_column=doi_column,
            api_key=overton_api_key,
            limit=overton_limit,
            rate_per_minute=overton_rate_per_minute,
            big_pause_every=overton_big_pause_every,
            big_pause_seconds=overton_big_pause_seconds,
        )

    # SDG
    if enable_sdg:
        sdg_out = processing_folder / f"{folder_name}-sdg-{model}.parquet"
        futures["sdg"] = tasks_mod.aurora_sdg_classify.submit(
            input_parquet=parquet_path,
            output_parquet=sdg_out,
            record_id_column=record_id_column,
            abstract_column=abstract_column,
            model=model,
            min_tokens=min_tokens,
            limit=sdg_limit,
            rate_per_minute=sdg_rate_per_minute,
            big_pause_every=sdg_big_pause_every,
            big_pause_seconds=sdg_big_pause_seconds,
        )

    # Genderize
    if enable_genderize:
        genderize_out = processing_folder / f"{folder_name}-genderize.parquet"
        futures["genderize"] = tasks_mod.genderize_enrich.submit(
            input_parquet=parquet_path,
            output_parquet=genderize_out,
            record_id_column=record_id_column,
            first_name_column=first_name_column,
            api_key=genderize_api_key,
            limit=genderize_limit,
            rate_per_minute=genderize_rate_per_minute,
            big_pause_every=genderize_big_pause_every,
            big_pause_seconds=genderize_big_pause_seconds,
        )

    # Gather results
    results: dict[str, str] = {"parquet": str(parquet_path)}
    for k, fut in futures.items():
        try:
            path = fut.result()  # type: ignore[assignment]
            results[k] = str(path)
        except Exception as e:
            logger.error(f"{k} failed: {e}")
            results[k] = "FAILED"

    logger.info(f"Orchestration finished: {results}")
    return results


if False:  # placeholder to maintain patch context
    pass


# -----------------------------
# Enrichment helper utilities
# -----------------------------

def _sleep_for_rate(i: int, rate_per_minute: float, big_pause_every: int | None = None, big_pause_sec: float = 0.0):
    if rate_per_minute and rate_per_minute > 0:
        time.sleep(max(0.0, 60.0 / rate_per_minute))
    if big_pause_every and big_pause_every > 0 and i > 0 and i % big_pause_every == 0 and big_pause_sec > 0:
        time.sleep(big_pause_sec)


def _safe_get(d: dict, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _validate_columns_present(df: pd.DataFrame, required: list[str] | None = None) -> None:
    required = required or []
    missing = [c for c in required if c not in df.columns]
    if missing:
        msg = (
            "Input parquet is missing required columns: "
            + ", ".join(missing)
            + ". Available columns: "
            + ", ".join(map(str, df.columns))
        )
        logger = get_run_logger()
        logger.error(msg)
        raise ValueError(msg)


def _validate_any_identifier(df: pd.DataFrame, candidates: list[str]) -> list[str]:
    available = [c for c in candidates if c in df.columns]
    if not available:
        msg = (
            "Input parquet contains none of the expected identifier columns: "
            + ", ".join(candidates)
        )
        logger = get_run_logger()
        logger.error(msg)
        raise ValueError(msg)
    return available


# -----------------------------
# Altmetric enrichment
# -----------------------------

@task(name="altmetric-enrich", retries=2, retry_delay_seconds=10, task_run_name="altmetric {output_parquet}")
def altmetric_enrich(
    input_parquet: Path | str,
    output_parquet: Path | str,
    record_id_column: str = "record_id",
    id_priority: list[str] | None = None,
    api_key: str | None = None,
    limit: int | None = None,
    rate_per_minute: float = 60.0,
    big_pause_every: int | None = None,
    big_pause_seconds: float = 0.0,
) -> Path:
    logger = get_run_logger()
    id_priority = id_priority or ["doi", "pmid", "pmcid", "arxiv_id"]
    df = pd.read_parquet(input_parquet)

    # Validate presence of record id and at least one identifier column
    _validate_columns_present(df, [record_id_column])
    available_ids = _validate_any_identifier(df, id_priority)

    # Only keep rows that have at least one of the available id columns
    has_any = df[available_ids].notna().any(axis=1)
    df_ids = df[has_any].copy()
    if limit:
        df_ids = df_ids.head(limit)
    logger.info(f"Altmetric: processing {len(df_ids)} rows from {input_parquet}")

    rows = []
    session = requests.Session()
    for i, row in enumerate(df_ids.itertuples(index=False)):
        rid = getattr(row, record_id_column) if hasattr(row, record_id_column) else None

        used_scheme = None
        used_value = None
        for scheme in available_ids:
            val = getattr(row, scheme)
            if pd.notna(val) and str(val).strip():
                used_scheme = scheme
                used_value = str(val).strip()
                break
        if not used_scheme:
            continue

        endpoint = {
            "doi": "doi",
            "pmid": "pmid",
            "pmcid": "pmcid",
            "arxiv_id": "arxiv",
        }[used_scheme]
        url = f"https://api.altmetric.com/v1/{endpoint}/{used_value}"
        if api_key:
            # Some endpoints support `?key=`
            url = url + f"?key={api_key}"

        try:
            resp = session.get(url, timeout=30)
            status = resp.status_code
            payload = None
            try:
                payload = resp.json()
            except Exception:
                payload = {"text": resp.text[:1000]}
            rows.append({
                "record_id": rid,
                "scheme": used_scheme,
                "identifier": used_value,
                "status": status,
                "payload": json.dumps(payload),
            })
        except Exception as e:
            rows.append({
                "record_id": rid,
                "scheme": used_scheme,
                "identifier": used_value,
                "status": None,
                "error": str(e),
            })

        _sleep_for_rate(i, rate_per_minute, big_pause_every, big_pause_seconds)

    out = Path(output_parquet)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(out, index=False)
    logger.info(f"Altmetric results -> {out}")
    return out


@flow(name="altmetric-enrichment", flow_run_name="altmetric-{dataset}")
def altmetric_enrichment_flow(
    input_parquet: Path | str,
    output_parquet: Path | str,
    record_id_column: str = "record_id",
    id_priority: list[str] | None = None,
    api_key: str | None = None,
    limit: int | None = None,
    rate_per_minute: float = 60.0,
    big_pause_every: int | None = None,
    big_pause_seconds: float = 0.0,
    dataset: str | None = None,
):
    # Infer dataset from parquet path if not provided
    if not dataset or not str(dataset).strip():
        dataset = Path(str(input_parquet)).stem
    return altmetric_enrich(
        input_parquet,
        output_parquet,
        record_id_column,
        id_priority,
        api_key,
        limit,
        rate_per_minute,
        big_pause_every,
        big_pause_seconds,
    )


# -----------------------------
# Overton enrichment
# -----------------------------

@task(name="overton-enrich", retries=2, retry_delay_seconds=10, task_run_name="overton {output_parquet}")
def overton_enrich(
    input_parquet: Path | str,
    output_parquet: Path | str,
    record_id_column: str = "record_id",
    doi_column: str = "doi",
    api_key: str | None = None,
    limit: int | None = None,
    rate_per_minute: float = 30.0,
    big_pause_every: int | None = None,
    big_pause_seconds: float = 0.0,
) -> Path:
    logger = get_run_logger()
    df = pd.read_parquet(input_parquet)
    _validate_columns_present(df, [record_id_column, doi_column])

    df_dois = df[df[doi_column].notna()].copy()
    if limit:
        df_dois = df_dois.head(limit)
    logger.info(f"Overton: processing {len(df_dois)} DOIs from {input_parquet}")

    base_url = "https://app.overton.io/documents.php"
    rows = []
    session = requests.Session()
    for i, row in enumerate(df_dois.itertuples(index=False)):
        rid = getattr(row, record_id_column) if hasattr(row, record_id_column) else None
        doi = str(getattr(row, doi_column)).strip()
        params = {"doi": doi}
        headers = {}
        if api_key:
            # Some deployments accept key as query or header; try query first
            params["api_key"] = api_key
            headers["Authorization"] = f"Bearer {api_key}"
        try:
            resp = session.get(base_url, params=params, headers=headers, timeout=60)
            status = resp.status_code
            payload = None
            try:
                payload = resp.json()
            except Exception:
                payload = {"text": resp.text[:2000]}
            rows.append({
                "record_id": rid,
                "doi": doi,
                "status": status,
                "payload": json.dumps(payload),
            })
        except Exception as e:
            rows.append({
                "record_id": rid,
                "doi": doi,
                "status": None,
                "error": str(e),
            })

        _sleep_for_rate(i, rate_per_minute, big_pause_every, big_pause_seconds)

    out = Path(output_parquet)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(out, index=False)
    logger.info(f"Overton results -> {out}")
    return out


@flow(name="overton-enrichment", flow_run_name="overton-{dataset}")
def overton_enrichment_flow(
    input_parquet: Path | str,
    output_parquet: Path | str,
    record_id_column: str = "record_id",
    doi_column: str = "doi",
    api_key: str | None = None,
    limit: int | None = None,
    rate_per_minute: float = 30.0,
    big_pause_every: int | None = None,
    big_pause_seconds: float = 0.0,
    dataset: str | None = None,
):
    if not dataset or not str(dataset).strip():
        dataset = Path(str(input_parquet)).stem
    return overton_enrich(
        input_parquet,
        output_parquet,
        record_id_column,
        doi_column,
        api_key,
        limit,
        rate_per_minute,
        big_pause_every,
        big_pause_seconds,
    )


# -----------------------------
# Aurora SDG classification
# -----------------------------

@task(name="aurora-sdg-classify", retries=2, retry_delay_seconds=10, task_run_name="sdg {model} {output_parquet}")
def aurora_sdg_classify(
    input_parquet: Path | str,
    output_parquet: Path | str,
    record_id_column: str = "record_id",
    abstract_column: str = "abstract",
    model: str = "aurora-sdg",
    min_tokens: int = 100,
    limit: int | None = None,
    rate_per_minute: float = 60.0,
    big_pause_every: int | None = None,
    big_pause_seconds: float = 0.0,
) -> Path:
    logger = get_run_logger()
    df = pd.read_parquet(input_parquet)
    _validate_columns_present(df, [record_id_column, abstract_column])
    df_in = df[df[abstract_column].notna()].copy()

    # crude token filter by whitespace split
    df_in = df_in[df_in[abstract_column].apply(lambda x: len(str(x).split()) >= min_tokens)]
    if limit:
        df_in = df_in.head(limit)
    logger.info(f"Aurora SDG: processing {len(df_in)} abstracts from {input_parquet}")

    base_url = f"https://aurora-sdg.labs.vu.nl/classifier/classify/{model}"
    rows = []
    session = requests.Session()
    for i, row in enumerate(df_in.itertuples(index=False)):
        rid = getattr(row, record_id_column) if hasattr(row, record_id_column) else None
        text = str(getattr(row, abstract_column))
        try:
            resp = session.post(base_url, json={"text": text}, timeout=60)
            status = resp.status_code
            payload = None
            try:
                payload = resp.json()
            except Exception:
                payload = {"text": resp.text[:2000]}

            sdgs = _safe_get(payload, "sdgs", default=[])
            rows.append({
                "record_id": rid,
                "sdgs": json.dumps(sdgs),
                "status": status,
            })
        except Exception as e:
            rows.append({
                "record_id": rid,
                "sdgs": json.dumps([]),
                "status": None,
                "error": str(e),
            })

        _sleep_for_rate(i, rate_per_minute, big_pause_every, big_pause_seconds)

    # Build DataFrame and compute top_predicted_sdgs using 90th percentile threshold across scores
    out_df = pd.DataFrame(rows)
    # Expand scores list to compute threshold
    scores = []
    for v in out_df["sdgs"].fillna("[]"):
        try:
            for item in json.loads(v):
                score = item.get("prediction_score") or item.get("score") or 0.0
                if isinstance(score, (int, float)):
                    scores.append(float(score))
        except Exception:
            continue
    if scores:
        thresh = pd.Series(scores).quantile(0.9)
    else:
        thresh = 0.0

    def _top_sdgs(v: str) -> list[str]:
        try:
            items = json.loads(v)
        except Exception:
            return []
        out = []
        for item in items or []:
            code = item.get("sdg", {}).get("code") or item.get("goal_code")
            score = item.get("prediction_score") or item.get("score") or 0.0
            if code and isinstance(score, (int, float)) and float(score) >= max(0.1, float(thresh)):
                out.append(str(code))
        return out

    out_df["top_predicted_sdgs"] = out_df["sdgs"].apply(_top_sdgs)

    out = Path(output_parquet)
    out.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out, index=False)
    logger.info(f"Aurora SDG results -> {out}")
    return out


@flow(name="aurora-sdg-enrichment", flow_run_name="sdg-{model}-{dataset}")
def aurora_sdg_enrichment_flow(
    input_parquet: Path | str,
    output_parquet: Path | str,
    record_id_column: str = "record_id",
    abstract_column: str = "abstract",
    model: str = "aurora-sdg",
    min_tokens: int = 100,
    limit: int | None = None,
    rate_per_minute: float = 60.0,
    big_pause_every: int | None = None,
    big_pause_seconds: float = 0.0,
    dataset: str | None = None,
):
    if not dataset or not str(dataset).strip():
        dataset = Path(str(input_parquet)).stem
    return aurora_sdg_classify(
        input_parquet,
        output_parquet,
        record_id_column,
        abstract_column,
        model,
        min_tokens,
        limit,
        rate_per_minute,
        big_pause_every,
        big_pause_seconds,
    )


# -----------------------------
# Genderize enrichment
# -----------------------------

@task(name="genderize-enrich", retries=2, retry_delay_seconds=10, task_run_name="genderize {output_parquet}")
def genderize_enrich(
    input_parquet: Path | str,
    output_parquet: Path | str,
    record_id_column: str = "record_id",
    first_name_column: str = "first_name",
    api_key: str | None = None,
    limit: int | None = None,
    rate_per_minute: float = 100.0,
    big_pause_every: int | None = None,
    big_pause_seconds: float = 0.0,
) -> Path:
    logger = get_run_logger()
    df = pd.read_parquet(input_parquet)
    _validate_columns_present(df, [record_id_column, first_name_column])
    df_in = df[df[first_name_column].notna()].copy()
    if limit:
        df_in = df_in.head(limit)
    logger.info(f"Genderize: processing {len(df_in)} names from {input_parquet}")

    base_url = "https://api.genderize.io"
    rows = []
    session = requests.Session()
    for i, row in enumerate(df_in.itertuples(index=False)):
        rid = getattr(row, record_id_column) if hasattr(row, record_id_column) else None
        name = str(getattr(row, first_name_column)).strip()
        params = {"name": name}
        if api_key:
            params["apikey"] = api_key
        try:
            resp = session.get(base_url, params=params, timeout=30)
            status = resp.status_code
            payload = None
            try:
                payload = resp.json()
            except Exception:
                payload = {"text": resp.text[:1000]}
            rows.append({
                "record_id": rid,
                "first_name": name,
                "status": status,
                "payload": json.dumps(payload),
            })
        except Exception as e:
            rows.append({
                "record_id": rid,
                "first_name": name,
                "status": None,
                "error": str(e),
            })

        _sleep_for_rate(i, rate_per_minute, big_pause_every, big_pause_seconds)

    out = Path(output_parquet)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(out, index=False)
    logger.info(f"Genderize results -> {out}")
    return out


@flow(name="genderize-enrichment", flow_run_name="genderize-{dataset}")
def genderize_enrichment_flow(
    input_parquet: Path | str,
    output_parquet: Path | str,
    record_id_column: str = "record_id",
    first_name_column: str = "first_name",
    api_key: str | None = None,
    limit: int | None = None,
    rate_per_minute: float = 100.0,
    big_pause_every: int | None = None,
    big_pause_seconds: float = 0.0,
    dataset: str | None = None,
):
    if not dataset or not str(dataset).strip():
        dataset = Path(str(input_parquet)).stem
    return genderize_enrich(
        input_parquet,
        output_parquet,
        record_id_column,
        first_name_column,
        api_key,
        limit,
        rate_per_minute,
        big_pause_every,
        big_pause_seconds,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Prefect flows from this repo")
    parser.add_argument(
        "--flow",
        choices=["etl", "orchestrator", "altmetric", "overton", "sdg", "genderize"],
        default="etl",
        help="Select which flow to run",
    )

    # ETL params
    parser.add_argument("--url", type=str, default="https://zenodo.org/records/14887484/files/aurora.tar?download=1")
    parser.add_argument("--base-folder", type=str, default="./data")
    parser.add_argument("--dataset", type=str, default=None)

    # Common enrichment params
    parser.add_argument("--input-parquet", type=str, default="")
    parser.add_argument("--output-parquet", type=str, default="")
    parser.add_argument("--record-id-column", type=str, default="record_id")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--rate-per-minute", type=float, default=None)
    parser.add_argument("--big-pause-every", type=int, default=None)
    parser.add_argument("--big-pause-seconds", type=float, default=0.0)

    # Altmetric
    parser.add_argument("--id-priority", type=str, default="doi,pmid,pmcid,arxiv_id")
    parser.add_argument("--altmetric-api-key", type=str, default=None)

    # Overton
    parser.add_argument("--doi-column", type=str, default="doi")
    parser.add_argument("--overton-api-key", type=str, default=None)

    # SDG
    parser.add_argument("--abstract-column", type=str, default="abstract")
    parser.add_argument("--model", type=str, default="aurora-sdg")
    parser.add_argument("--min-tokens", type=int, default=100)

    # Genderize
    parser.add_argument("--first-name-column", type=str, default="first_name")
    parser.add_argument("--genderize-api-key", type=str, default=None)

    args = parser.parse_args()

    # Derive dataset early for run name clarity
    dataset_val = (args.dataset or "").strip()
    if not dataset_val:
        if args.input_parquet:
            dataset_val = Path(args.input_parquet).stem
        elif args.url:
            fname = args.url.split("/")[-1].split("?")[0]
            dataset_val = Path(fname).stem
        else:
            dataset_val = "dataset"

    if args.flow == "etl":
        result = process_subgraph_data(url=args.url, base_folder=args.base_folder, dataset=dataset_val)
        print(result)
    elif args.flow == "orchestrator":
        id_priority = [s.strip() for s in (args.id_priority or "").split(",") if s.strip()]
        result = orchestrate_etl_and_enrichments(
            url=args.url,
            base_folder=args.base_folder,
            input_parquet=(args.input_parquet or None),
            dataset=dataset_val,
            enable_altmetric=True,
            enable_overton=True,
            enable_sdg=True,
            enable_genderize=True,
            record_id_column=args.record_id_column,
            id_priority=id_priority or ["doi","pmid","pmcid","arxiv_id"],
            altmetric_api_key=args.altmetric_api_key,
            altmetric_limit=args.limit,
            altmetric_rate_per_minute=args.rate_per_minute or 60.0,
            altmetric_big_pause_every=args.big_pause_every,
            altmetric_big_pause_seconds=args.big_pause_seconds,
            doi_column=args.doi_column,
            overton_api_key=args.overton_api_key,
            overton_limit=args.limit,
            overton_rate_per_minute=args.rate_per_minute or 30.0,
            overton_big_pause_every=args.big_pause_every,
            overton_big_pause_seconds=args.big_pause_seconds,
            abstract_column=args.abstract_column,
            model=args.model,
            min_tokens=args.min_tokens,
            sdg_limit=args.limit,
            sdg_rate_per_minute=args.rate_per_minute or 60.0,
            sdg_big_pause_every=args.big_pause_every,
            sdg_big_pause_seconds=args.big_pause_seconds,
            first_name_column=args.first_name_column,
            genderize_api_key=args.genderize_api_key,
            genderize_limit=args.limit,
            genderize_rate_per_minute=args.rate_per_minute or 100.0,
            genderize_big_pause_every=args.big_pause_every,
            genderize_big_pause_seconds=args.big_pause_seconds,
        )
        print(result)
    elif args.flow == "altmetric":
        id_priority = [s.strip() for s in (args.id_priority or "").split(",") if s.strip()]
        altmetric_enrichment_flow(
            input_parquet=args.input_parquet,
            output_parquet=args.output_parquet,
            record_id_column=args.record_id_column,
            id_priority=id_priority,
            api_key=args.altmetric_api_key,
            limit=args.limit,
            rate_per_minute=args.rate_per_minute or 60.0,
            big_pause_every=args.big_pause_every,
            big_pause_seconds=args.big_pause_seconds,
            dataset=dataset_val,
        )
    elif args.flow == "overton":
        overton_enrichment_flow(
            input_parquet=args.input_parquet,
            output_parquet=args.output_parquet,
            record_id_column=args.record_id_column,
            doi_column=args.doi_column,
            api_key=args.overton_api_key,
            limit=args.limit,
            rate_per_minute=args.rate_per_minute or 30.0,
            big_pause_every=args.big_pause_every,
            big_pause_seconds=args.big_pause_seconds,
            dataset=dataset_val,
        )
    elif args.flow == "sdg":
        aurora_sdg_enrichment_flow(
            input_parquet=args.input_parquet,
            output_parquet=args.output_parquet,
            record_id_column=args.record_id_column,
            abstract_column=args.abstract_column,
            model=args.model,
            min_tokens=args.min_tokens,
            limit=args.limit,
            rate_per_minute=args.rate_per_minute or 60.0,
            big_pause_every=args.big_pause_every,
            big_pause_seconds=args.big_pause_seconds,
            dataset=dataset_val,
        )
    elif args.flow == "genderize":
        genderize_enrichment_flow(
            input_parquet=args.input_parquet,
            output_parquet=args.output_parquet,
            record_id_column=args.record_id_column,
            first_name_column=args.first_name_column,
            api_key=args.genderize_api_key,
            limit=args.limit,
            rate_per_minute=args.rate_per_minute or 100.0,
            big_pause_every=args.big_pause_every,
            big_pause_seconds=args.big_pause_seconds,
            dataset=dataset_val,
        )
