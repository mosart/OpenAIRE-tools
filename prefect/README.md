# Prefect v3 ETL Flows

This folder contains Prefect v3 tasks and a flow that orchestrate the bulk subgraph processing from the notebook into a monitored, restartable pipeline.

What it does:
- Downloads a Zenodo subgraph tarball.
- Extracts the archive to a dated run folder.
- Reads all `.json.gz` files via DuckDB and writes a single Parquet file.

Enrichment flows included:
- Altmetric enrichment: fetches Altmetric details for DOIs/PMIDs/PMCIDs/ArXiv IDs
- Overton enrichment: fetches policy mentions for DOIs (API key usually required)
- Aurora SDG classification: classifies abstracts into SDGs via Aurora SDG API
- Genderize enrichment: infers gender probability from first names


## 1) Clone and set up

```bash
git clone https://github.com/<your-org-or-user>/OpenAIRE-tools.git
cd OpenAIRE-tools

# (Recommended) create a virtualenv
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install project + Prefect deps
pip install -r requirements.txt
pip install -r prefect/requirements.txt
```


## 2) Start the Prefect Server (open source UI)

```bash
prefect server start
```

- The local UI opens at http://127.0.0.1:4200
- Keep this process running while you execute flows/workers.


## 3) Run the flow locally (no deployment)

You can execute the flow directly for quick iteration:

```bash
python prefect/flows.py \
  --url "https://zenodo.org/records/14887484/files/aurora.tar?download=1" \
  --base-folder ./data
```

Outputs:
- Downloaded tar in `./data/01_input/`
- Extracted content in `./data/02_extracted/<file>_<YYYY-MM-DD>/`
- Parquet in `./data/03_transformed/<file>_<YYYY-MM-DD>.parquet`


## 4) Run enrichment flows locally

Altmetric (choose identifier priority and throttle):
```bash
python prefect/flows.py --flow altmetric \
  --input-parquet data/03_transformed/aurora.tar_2025-03-20.parquet \
  --output-parquet data/04_processed/altmetric.parquet \
  --record-id-column record_id \
  --id-priority doi,pmid,pmcid,arxiv_id \
  --altmetric-api-key YOUR_KEY_OR_EMPTY \
  --limit 1000 \
  --rate-per-minute 60 \
  --big-pause-every 500 \
  --big-pause-seconds 30
```

Overton (usually requires API key):
```bash
python prefect/flows.py --flow overton \
  --input-parquet data/03_transformed/aurora.tar_2025-03-20.parquet \
  --output-parquet data/04_processed/overton.parquet \
  --record-id-column record_id \
  --doi-column doi \
  --overton-api-key YOUR_OVERTON_API_KEY \
  --limit 1000 \
  --rate-per-minute 30
```

Aurora SDG classification:
```bash
python prefect/flows.py --flow sdg \
  --input-parquet data/03_transformed/aurora.tar_2025-03-20.parquet \
  --output-parquet data/04_processed/sdg_aurora.parquet \
  --record-id-column record_id \
  --abstract-column abstract \
  --model aurora-sdg \
  --min-tokens 100 \
  --limit 500 \
  --rate-per-minute 60
```

Genderize:
```bash
python prefect/flows.py --flow genderize \
  --input-parquet data/03_transformed/aurora.tar_2025-03-20.parquet \
  --output-parquet data/04_processed/genderize.parquet \
  --record-id-column record_id \
  --first-name-column first_name \
  --genderize-api-key YOUR_KEY_OR_EMPTY \
  --limit 2000 \
  --rate-per-minute 100
```


## 5) Deploy to Prefect (managed runs in the UI)

First, create a work pool and start a worker:

```bash
# Create a local process work pool (one-time)
prefect work-pool create -t process default-pool

# In a new terminal, start a worker attached to that pool
prefect worker start -p default-pool
```

Then, build and apply a deployment for the flow:

```bash
prefect deployment build prefect/flows.py:process_subgraph_data \
  -n subgraph-etl \
  -p default-pool \
  -q default \
  --param url="https://zenodo.org/records/14887484/files/aurora.tar?download=1" \
  --param base_folder="./data" \
  -o prefect_deploy.yaml

prefect deployment apply prefect_deploy.yaml
```

Kick off a run from CLI (or use the UI):

```bash
prefect deployment run 'subgraph-etl/process_subgraph_data' \
  --param url="https://zenodo.org/records/14887484/files/aurora.tar?download=1" \
  --param base_folder="./data"

# Optionally build deployments for enrichment flows too, e.g.:
prefect deployment build prefect/flows.py:altmetric_enrichment_flow \
  -n altmetric \
  -p default-pool \
  -q default \
  --param record_id_column=record_id \
  --param id_priority='["doi","pmid","pmcid","arxiv_id"]' \
  --param api_key=null \
  --param limit=null \
  --param rate_per_minute=60 \
  --param big_pause_every=500 \
  --param big_pause_seconds=30 \
  -o altmetric_deploy.yaml
prefect deployment apply altmetric_deploy.yaml

prefect deployment build prefect/flows.py:overton_enrichment_flow \
  -n overton \
  -p default-pool \
  -q default \
  --param record_id_column=record_id \
  --param doi_column=doi \
  --param api_key=null \
  --param limit=null \
  --param rate_per_minute=30 \
  -o overton_deploy.yaml
prefect deployment apply overton_deploy.yaml

prefect deployment build prefect/flows.py:aurora_sdg_enrichment_flow \
  -n sdg \
  -p default-pool \
  -q default \
  --param record_id_column=record_id \
  --param abstract_column=abstract \
  --param model=aurora-sdg \
  --param min_tokens=100 \
  --param limit=null \
  --param rate_per_minute=60 \
  -o sdg_deploy.yaml
prefect deployment apply sdg_deploy.yaml

prefect deployment build prefect/flows.py:genderize_enrichment_flow \
  -n genderize \
  -p default-pool \
  -q default \
  --param record_id_column=record_id \
  --param first_name_column=first_name \
  --param api_key=null \
  --param limit=null \
  --param rate_per_minute=100 \
  -o genderize_deploy.yaml
prefect deployment apply genderize_deploy.yaml
```

Notes:
- Some Prefect CLIs also support `prefect deploy` which builds+applies in one step. If available, you can try:
  ```bash
  prefect deploy prefect/flows.py:process_subgraph_data -n subgraph-etl -p default-pool -q default
  ```


## 6) UI monitoring

- View flow and task runs, logs, and retries in the UI at http://127.0.0.1:4200
- You can set schedules, edit parameters, and trigger runs from the UI.


## 7) Configuration & parameters

- Flow: `process_subgraph_data(url: str, base_folder: str)`
- Defaults mirror the notebook’s example. Override `--url` to target another subgraph tarball.
- The flow uses Prefect retries on download and a concurrent task runner for responsiveness.

Enrichment flow parameters (common highlights):
- `input_parquet`, `output_parquet`: IO paths.
- `record_id_column`: your unique record id column name.
- `limit`: cap items for test runs.
- `rate_per_minute`, `big_pause_every`, `big_pause_seconds`: basic rate limiting knobs.

Altmetric-specific:
- `id_priority`: order of identifier preference among `doi`, `pmid`, `pmcid`, `arxiv_id`.
- `api_key`: optional; if provided appended as `?key=`.

Overton-specific:
- `doi_column`: name of DOI column.
- `api_key`: typically required; passed via query and Authorization header.

Aurora SDG-specific:
- `abstract_column`: column with abstract text.
- `model`: Aurora model name (e.g. `aurora-sdg`).
- `min_tokens`: lower bound on abstract length.

Genderize-specific:
- `first_name_column`: column with given names.
- `api_key`: optional key to raise rate limits.


## 8) Troubleshooting


## 9) Full Orchestrator: ETL then parallel enrichments

Run locally:
```bash
python prefect/flows.py --flow orchestrator \
  --url "https://zenodo.org/records/14887484/files/aurora.tar?download=1" \
  --base-folder ./data \
  --record-id-column record_id \
  --id-priority doi,pmid,pmcid,arxiv_id \
  --doi-column doi \
  --abstract-column abstract \
  --model aurora-sdg \
  --first-name-column first_name \
  --limit 1000 \
  --rate-per-minute 60
```

Create a deployment and start from the UI:
```bash
prefect deployment build prefect/flows.py:orchestrate_etl_and_enrichments \
  -n etl-and-enrich \
  -p default-pool \
  -q default \
  --param url="https://zenodo.org/records/14887484/files/aurora.tar?download=1" \
  --param base_folder="./data" \
  --param enable_altmetric=true \
  --param enable_overton=true \
  --param enable_sdg=true \
  --param enable_genderize=true \
  --param record_id_column=record_id \
  --param id_priority='["doi","pmid","pmcid","arxiv_id"]' \
  --param altmetric_api_key=null \
  --param altmetric_limit=null \
  --param altmetric_rate_per_minute=60 \
  --param altmetric_big_pause_every=500 \
  --param altmetric_big_pause_seconds=30 \
  --param doi_column=doi \
  --param overton_api_key=null \
  --param overton_limit=null \
  --param overton_rate_per_minute=30 \
  --param abstract_column=abstract \
  --param model=aurora-sdg \
  --param min_tokens=100 \
  --param sdg_limit=null \
  --param sdg_rate_per_minute=60 \
  --param first_name_column=first_name \
  --param genderize_api_key=null \
  --param genderize_limit=null \
  --param genderize_rate_per_minute=100 \
  -o orchestrator_deploy.yaml

prefect deployment apply orchestrator_deploy.yaml
```

Then open the Prefect UI (http://127.0.0.1:4200), go to Deployments → `etl-and-enrich/orchestrate_etl_and_enrichments`, click Run, and adjust parameters (API keys, rate limits, etc.) before starting the run. The enrichment steps run in parallel after the transform finishes.


## 10) Schema mapping (adjust column names)

If your Parquet schema uses different column names, pass parameters to map them:

- Orchestrator example (custom columns):
  ```bash
  python prefect/flows.py --flow orchestrator \
    --input-parquet data/03_transformed/mydata.parquet \
    --record-id-column rec_id \
    --doi-column doi_norm \
    --abstract-column abstract_text \
    --first-name-column given_name
  ```

- Altmetric only (custom id columns and priority):
  ```bash
  python prefect/flows.py --flow altmetric \
    --input-parquet data/03_transformed/mydata.parquet \
    --output-parquet data/04_processed/altmetric.parquet \
    --record-id-column rec_id \
    --id-priority doi_norm,pmid,pmcid,arxiv
  ```

- Overton only (custom DOI column):
  ```bash
  python prefect/flows.py --flow overton \
    --input-parquet data/03_transformed/mydata.parquet \
    --output-parquet data/04_processed/overton.parquet \
    --record-id-column rec_id \
    --doi-column doi_norm
  ```

- Aurora SDG only (custom abstract column):
  ```bash
  python prefect/flows.py --flow sdg \
    --input-parquet data/03_transformed/mydata.parquet \
    --output-parquet data/04_processed/sdg_aurora.parquet \
    --record-id-column rec_id \
    --abstract-column abstract_text
  ```

- Genderize only (custom first name column):
  ```bash
  python prefect/flows.py --flow genderize \
    --input-parquet data/03_transformed/mydata.parquet \
    --output-parquet data/04_processed/genderize.parquet \
    --record-id-column rec_id \
    --first-name-column given_name
  ```

In the Prefect UI
- When starting a Deployment run, click Parameters and set:
  - `record_id_column`, `doi_column`, `abstract_column`, `first_name_column` as needed.
  - Also set API keys and rate limits under the same Parameters panel.


## 11) Ready-made deployment YAMLs

Pre-baked deployment specs with default parameters live in `prefect/deployments/`. Apply them directly:

```bash
# Ensure a work pool exists and a worker is running
prefect work-pool create -t process default-pool || true
prefect worker start -p default-pool  # run in another terminal

# Apply deployments
prefect deployment apply prefect/deployments/etl.yaml
prefect deployment apply prefect/deployments/orchestrator.yaml
prefect deployment apply prefect/deployments/altmetric.yaml
prefect deployment apply prefect/deployments/overton.yaml
prefect deployment apply prefect/deployments/sdg.yaml
prefect deployment apply prefect/deployments/genderize.yaml
```

Edit any YAML to change defaults (paths, columns, rates), then re-apply. You can also override parameters per run in the UI.


- Network: ensure the URL is reachable; the download task retries on transient errors.
- Disk space: large subgraphs require several GB free for extraction and Parquet.
- Permissions: run from a location where the process can create `./data/**` folders.
- DuckDB: make sure `duckdb` is installed (already in the project `requirements.txt`).
