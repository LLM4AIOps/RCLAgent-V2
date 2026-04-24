# RCLAgent

> **Paper**: *Towards In-Depth Root Cause Localization for Microservices with Multi-Agent Recursion-of-Thought*

![RCLAgent Workflow](assets/RCLAgent_workflow.png)

RCLAgent is an in-depth root cause localization (RCL) framework for microservice systems. It decomposes the diagnostic process along the trace graph topology, assigning each span to a Dedicated Agent with bounded context. Agents reason in parallel following the recursive trace structure, propagate structured evidence upward, and synthesize a final ranked list of root cause candidates via a Global Evidence Graph and a Root-Level Diagnosis Report.

---

## Table of Contents

1. [Requirements](#1-requirements)
2. [Quick Start (Sample Data)](#2-quick-start-sample-data)
3. [Datasets](#3-datasets)
4. [Data Preprocessing](#4-data-preprocessing)
5. [Running Experiments](#5-running-experiments)
6. [Evaluation](#6-evaluation)
7. [Reproducing Paper Results](#7-reproducing-paper-results)
8. [Configuration Reference](#8-configuration-reference)
9. [Project Structure](#9-project-structure)

---

## 1. Requirements

- **Python**: 3.10 or higher
- **OS**: Linux (tested on CentOS 8 / Ubuntu 22.04); macOS should work; Windows is untested.

Install dependencies:

```bash
pip install -r requirements.txt
```

**LLM API**: RCLAgent calls any OpenAI-compatible chat completion endpoint. The default configuration targets Qwen-3.6-Plus via Alibaba Cloud DashScope. You must provide your own API key.

---

## 2. Quick Start (Sample Data)

The repository includes a minimal sample dataset under `sample_data/` so you can verify the pipeline end-to-end without downloading any external data.

### Step 1 — Set your LLM API key

```bash
export LLM_API_KEY="<your-api-key>"
# Optional overrides (defaults: DashScope / qwen-plus):
# export LLM_API_URL="https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
# export LLM_MODEL="qwen-plus"
```

### Step 2 — Start the Tool Server

```bash
python3 tool_server.py
```

You should see `* Running on http://127.0.0.1:5000`.

### Step 3 — Run the Coordinator (separate terminal)

```bash
python3 coordinator.py
```

The coordinator reads `sample_data/error_traces.txt`, runs the multi-agent analysis, and writes per-trace result files to `sample_data/result/`.

### Step 4 — Evaluate

```bash
python3 evaluate.py sample_data result
```

The reference result shipped in `sample_data/result/` corresponds to MRR = 1.0 (ground truth `recommendationservice` is ranked first). Because LLM outputs are stochastic, a fresh run may differ slightly — the ground truth should still appear within the top few candidates. Evaluating against the shipped reference:

```
R@1  : 1.0000  (1/1)
R@3  : 1.0000  (1/1)
R@5  : 1.0000  (1/1)
R@10 : 1.0000  (1/1)
MRR  : 1.0000
```

---

## 3. Datasets

The paper evaluates on three public datasets. None are shipped with the repo (they are large); download and place them under `data/`, then point `DATA_ROOT` at each subset.

### 3.1 AIOPS 2022

HipsterShop microservice application. The paper uses 6 subsets:

| Paper Label | Directory | Error Traces | Fault Cases |
|-------------|-----------|--------------|-------------|
| **A** | `2022-03-20-cloudbed2` | 222 | 33 |
| **B** | `2022-03-20-cloudbed3` | 834 | 31 |
| **Γ** | `2022-03-21-cloudbed1` | 958 | 43 |
| **Δ** | `2022-03-21-cloudbed2` | 880 | 55 |
| **E** | `2022-03-21-cloudbed3` | 453 | 51 |
| **Z** | `2022-03-24-cloudbed3` | 789 | 50 |

**Download**: <https://zenodo.org/records/19176851>
(the original [NetManAIOps challenge repository](https://github.com/NetManAIOps/AIOps-Challenge-2022-Data) is no longer accessible, so we have re-hosted the data on Zenodo.)

Expected raw layout per subset:

```
data/2022-03-20-cloudbed2/
  metric/
    container/   *.csv   (timestamp, cmdb_id, kpi_name, value)
    node/        *.csv
    service/     *.csv   (timestamp, service, rr, sr, mrt, count)
    istio/       *.csv   (optional)
    jvm/         *.csv   (optional)
  log/all/       *.csv   (timestamp, service_name, log_message)
  trace/all/     trace_jaeger-span.csv
  groundtruth.csv
```

### 3.2 Augmented-TrainTicket (Nezha)

TrainTicket microservice application. The paper uses the **2023-01-30** subset only.

**Download**: [Nezha GitHub](https://github.com/IntelligentDDS/Nezha) — the relevant raw data lives under `rca_data/2023-01-30/` (contains `trace/`, `log/`, `metric/`, `traceid/`, and `2023-01-30-fault_list.json`).

### 3.3 RCAEval RE2-OB

Online Boutique (HipsterShop) microservice application with 90 fault injection cases: 5 services × 6 fault types (cpu, delay, disk, loss, mem, socket) × 3 runs.

**Download**: [RCAEval GitHub](https://github.com/phamquiluan/RCAEval).

Expected layout:

```
datasets/RE2-OB/
  checkoutservice_cpu/
    1/   traces.csv  logs.csv  metrics.csv  inject_time.txt
    2/   ...
    3/   ...
  checkoutservice_delay/
  ...  (90 cases total)
```

---

## 4. Data Preprocessing

All datasets must be converted into the canonical RCLAgent format before running experiments.

### 4.1 AIOPS 2022

```bash
# For each of the 6 subsets:
python3 preprocess/preprocessing_metrics.py  data/2022-03-20-cloudbed2
python3 preprocess/preprocessing_logs.py     data/2022-03-20-cloudbed2
python3 preprocess/preprocessing_traces.py   data/2022-03-20-cloudbed2
python3 preprocess/preprocess_groundtruth.py data/2022-03-20-cloudbed2
```

### 4.2 Augmented-TrainTicket (Nezha-30)

```bash
python3 preprocess/preprocess_nezha.py \
    --input  datasets/Nezha/rca_data/2023-01-30 \
    --output data/nezha-2023-01-30
```

### 4.3 RCAEval RE2-OB

```bash
python3 preprocess/preprocess_re2ob.py \
    --input  datasets/RE2-OB \
    --output data/re2ob
```

### Canonical Output Layout

After preprocessing, every dataset directory should contain:

```
data/<subset>/
  trace/all/trace_jaeger-span.csv   # all spans
  metric/all/metrics.csv            # merged KPI metrics (long format)
  metric/node_service_map.pkl       # node → service mapping
  metric/service_node_map.pkl       # service → node mapping
  log/all/logs.csv                  # filtered logs (non-INFO/DEBUG)
  groundtruth.csv                   # fault injection labels
  error_traces.txt                  # root spans flagged as anomalous
```

---

## 5. Running Experiments

Every experiment follows the same two-process pattern: a long-running **Tool Server** that serves preprocessed data, and a **Coordinator** that drives the multi-agent reasoning.

### Run a single subset

```bash
# Terminal 1 — tool server
DATA_ROOT=data/2022-03-20-cloudbed2 TOOL_SERVER_PORT=5001 DATASET_TYPE=aiops2022 \
    python3 tool_server.py

# Terminal 2 — coordinator (same env vars)
DATA_ROOT=data/2022-03-20-cloudbed2 TOOL_SERVER_PORT=5001 DATASET_TYPE=aiops2022 \
    python3 coordinator.py
```

### Nezha / RE2-OB

Switch `DATA_ROOT` and `DATASET_TYPE` accordingly:

```bash
# Augmented-TrainTicket (Nezha-30) — deeper trace graphs, cap depth at 10
DATA_ROOT=data/nezha-2023-01-30 TOOL_SERVER_PORT=5002 DATASET_TYPE=nezha \
MAX_TRACE_DEPTH=10 MAX_AGENT_PARALLEL=16 \
    python3 tool_server.py &

DATA_ROOT=data/nezha-2023-01-30 TOOL_SERVER_PORT=5002 DATASET_TYPE=nezha \
MAX_TRACE_DEPTH=10 MAX_AGENT_PARALLEL=16 \
    python3 coordinator.py
```

```bash
# RCAEval RE2-OB
DATA_ROOT=data/re2ob TOOL_SERVER_PORT=5003 DATASET_TYPE=re2ob \
    python3 tool_server.py &

DATA_ROOT=data/re2ob TOOL_SERVER_PORT=5003 DATASET_TYPE=re2ob \
    python3 coordinator.py
```

### Running multiple subsets concurrently

For the six AIOPS 2022 subsets, launch one tool-server + coordinator pair per subset, each on its own port (5001–5006), in the background. Example:

```bash
for pair in \
    "2022-03-20-cloudbed2:5001" "2022-03-20-cloudbed3:5002" \
    "2022-03-21-cloudbed1:5003" "2022-03-21-cloudbed2:5004" \
    "2022-03-21-cloudbed3:5005" "2022-03-24-cloudbed3:5006"; do
  SUBSET=${pair%:*}; PORT=${pair#*:}
  DATA_ROOT=data/$SUBSET TOOL_SERVER_PORT=$PORT DATASET_TYPE=aiops2022 \
      nohup python3 tool_server.py > tool_${SUBSET}.log 2>&1 &
  sleep 10  # wait for server to come up
  DATA_ROOT=data/$SUBSET TOOL_SERVER_PORT=$PORT DATASET_TYPE=aiops2022 \
      nohup python3 -u coordinator.py > coord_${SUBSET}.log 2>&1 &
done
```

### Resumability

The coordinator is **idempotent** — it skips traces that already have a result file. Interrupted runs can be resumed by simply re-invoking the same command.

---

## 6. Evaluation

```bash
# Single subset
python3 evaluate.py data/2022-03-20-cloudbed2 result

# Aggregate report across all three benchmarks (matches paper numbers)
bash final_eval.sh
```

`evaluate.py` uses **per-fault** evaluation: for each groundtruth fault-injection case, the best rank across all error traces belonging to that case is taken. This matches the standard RCL evaluation protocol (one fault = one evaluation unit).

Metrics reported:

| Metric | Definition |
|--------|------------|
| R@1 / R@3 / R@5 / R@10 | Fraction of faults whose true root cause appears in top-k |
| MRR | Mean Reciprocal Rank over all fault cases |

---

## 7. Reproducing Paper Results

Numbers below are obtained with the default **Qwen-3.6-Plus** backbone on all three benchmarks.

### 7.1 AIOPS 2022

Per-subset MRR:

| Backbone | A | B | Γ | Δ | E | Z |
|----------|---|---|---|---|---|---|
| Qwen-3.6-Plus | 66.67 | 81.39 | 67.97 | 66.78 | 82.22 | 73.67 |

Weighted average over all 66 covered faults:

| R@1 | R@3 | R@5 | R@10 | MRR |
|-----|-----|-----|------|-----|
| 65.15 | 78.79 | 86.36 | 95.45 | **73.24** |

### 7.2 Augmented-TrainTicket (Nezha-30, 17 faults)

| R@1 | R@3 | R@5 | R@10 | MRR |
|-----|-----|-----|------|-----|
| 82.35 | 88.24 | 94.12 | 94.12 | **86.47** |

### 7.3 RCAEval RE2-OB (90 faults)

| R@1 | R@3 | R@5 | R@10 | MRR |
|-----|-----|-----|------|-----|
| 56.67 | 80.00 | 86.67 | 100.00 | **71.03** |

### 7.4 End-to-End Reproduction

```bash
# 1. Preprocess all datasets (see Section 4)

# 2. Set LLM credentials
export LLM_API_KEY="<your-api-key>"
export LLM_API_URL="https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"
export LLM_MODEL="qwen-plus"

# 3. Run each dataset (see Section 5)

# 4. Print paper-style comparison report
bash final_eval.sh
```

> **Rate-limit note**: Running 6 AIOPS subsets in parallel with `MAX_AGENT_PARALLEL=32` each can issue up to ~200 concurrent LLM calls. Reduce `MAX_AGENT_PARALLEL` or limit concurrent subsets if you hit 429 errors. Interrupted runs resume cleanly thanks to the coordinator's idempotent design.

---

## 8. Configuration Reference

All settings are read from environment variables (falling back to defaults in `config.py`).

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_API_URL` | DashScope endpoint | OpenAI-compatible chat completion URL |
| `LLM_API_KEY` | *(required)* | API key for the LLM provider |
| `LLM_MODEL` | `qwen-plus` | Model name passed in the API request |
| `LLM_FORCE_STREAM` | `false` | Force SSE streaming (required for some endpoints) |
| `DATA_ROOT` | `sample_data` | Root directory of the dataset to analyze |
| `RESULT_SUB_DIR` | `result` | Subdirectory under `DATA_ROOT` for output files |
| `TOOL_SERVER_HOST` | `127.0.0.1` | Tool Server bind address |
| `TOOL_SERVER_PORT` | `5000` | Tool Server port |
| `DATASET_TYPE` | `aiops2022` | One of `aiops2022`, `nezha`, `re2ob` |
| `MAX_AGENT_PARALLEL` | `32` | Max concurrent Dedicated Agents |
| `MAX_TOOL_TURNS` | `5` | Max tool-call rounds per agent |
| `MAX_TRACE_DEPTH` | `20` | Max trace graph depth (use 10 for Nezha) |
| `DURATION_THRESHOLD_US` | `10000000` | Span duration threshold (µs) for error-trace selection |

---

## 9. Project Structure

```
RCLAgent-V2/
├── README.md              # This file
├── LICENSE                # MIT license
├── requirements.txt       # Python dependencies
├── config.py              # Centralized configuration (env-var overrides)
│
├── coordinator.py         # Multi-agent RoT coordinator (main entry)
├── tool_server.py         # Flask HTTP server serving trace/metric/log APIs
├── llm.py                 # OpenAI-compatible LLM client with retry logic
├── tools_gpt.py           # Tool schema definitions
├── evaluate.py            # R@k / MRR evaluation
├── evaluate_baro.py       # BARO baseline evaluation (optional)
├── final_eval.sh          # Aggregated report across all three benchmarks
│
├── preprocess/
│   ├── preprocessing_metrics.py    # Merge raw metric CSVs
│   ├── preprocessing_traces.py     # Extract error root spans
│   ├── preprocessing_logs.py       # Merge raw log CSVs
│   ├── preprocess_groundtruth.py   # Process groundtruth CSV
│   ├── preprocess_nezha.py         # Nezha raw → canonical format
│   └── preprocess_re2ob.py         # RE2-OB raw → canonical format
│
├── sample_data/           # Minimal 1-fault dataset for quick verification
│   ├── error_traces.txt
│   ├── groundtruth.csv
│   ├── trace/all/trace_jaeger-span.csv
│   ├── log/all/logs.csv
│   ├── metric/all/metrics.csv
│   └── result/            # Reference output
│
└── assets/
    └── RCLAgent_workflow.png
```

### Tool Server API

| Endpoint | Parameters | Description |
|----------|------------|-------------|
| `GET /search_span` | `span_id` | Fetch metadata for a single span |
| `GET /search_traces` | `parent_span_id` | Fetch all child spans |
| `GET /search_logs` | `service_name`, `timestamp` | Error/warning logs ±60s around timestamp |
| `GET /search_fluctuating_metrics` | `service_name`, `timestamp` | KPIs with 3-sigma anomaly spike in ±10min window |

---

## Citation

If you use RCLAgent in your research, please cite our paper (BibTeX to be added upon publication).

## License

Released under the MIT License. See `LICENSE` for details.
