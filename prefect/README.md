# Prefect v3 ETL Flows

This folder contains Prefect v3 tasks and a flow that orchestrate the bulk subgraph processing from the notebook into a monitored, restartable pipeline.

What it does:
- Downloads a Zenodo subgraph tarball.
- Extracts the archive to a dated run folder.
- Reads all `.json.gz` files via DuckDB and writes a single Parquet file.


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


## 4) Deploy to Prefect (managed runs in the UI)

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
  -o prefect_deploy.yaml

prefect deployment apply prefect_deploy.yaml
```

Kick off a run from CLI (or use the UI):

```bash
prefect deployment run 'subgraph-etl/process_subgraph_data' \
  --param url="https://zenodo.org/records/14887484/files/aurora.tar?download=1" \
  --param base_folder="./data"
```

Notes:
- Some Prefect CLIs also support `prefect deploy` which builds+applies in one step. If available, you can try:
  ```bash
  prefect deploy prefect/flows.py:process_subgraph_data -n subgraph-etl -p default-pool -q default
  ```


## 5) UI monitoring

- View flow and task runs, logs, and retries in the UI at http://127.0.0.1:4200
- You can set schedules, edit parameters, and trigger runs from the UI.


## 6) Configuration & parameters

- Flow: `process_subgraph_data(url: str, base_folder: str)`
- Defaults mirror the notebook’s example. Override `--url` to target another subgraph tarball.
- The flow uses Prefect retries on download and a concurrent task runner for responsiveness.


## 7) Troubleshooting

- Network: ensure the URL is reachable; the download task retries on transient errors.
- Disk space: large subgraphs require several GB free for extraction and Parquet.
- Permissions: run from a location where the process can create `./data/**` folders.
- DuckDB: make sure `duckdb` is installed (already in the project `requirements.txt`).

