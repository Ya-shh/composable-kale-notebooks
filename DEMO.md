# Composable Kale Notebooks Guide

Compose several independent notebooks into one Kubeflow pipeline, where each
notebook becomes its own nested sub-pipeline (sub-DAG), with execution order and
data passing inferred automatically. Everything is driven by the `kale` CLI.

The three notebooks in `notebooks/` are a small end-to-end example: `notebook_a`
builds a dataset, `notebook_b` fits a model from it, `notebook_c` predicts from
the model.

---

## 1. Install

Requires Python 3.11+.

```
pip install -e .
```

## 2. Compile (no cluster needed)

```
cd notebooks
kale --workflow notebook_a.ipynb notebook_b.ipynb notebook_c.ipynb --pipeline_name notebook-sequence
```

Output:

```
Composed workflow (sub-pipelines), inferred order: notebook_a -> notebook_b -> notebook_c
dsl_script_path:      .kale/notebook-sequence.workflow.py
pipeline_package_path: .kale/notebook-sequence.pipeline.yaml
```

The order is inferred from the data: `notebook_b` uses `dataset` (defined by
`notebook_a`) and `notebook_c` uses `model` (defined by `notebook_b`).

## 3. Inspect the compiled structure

Each notebook compiles to a sub-DAG inside one pipeline:

```
python -c "import yaml; ir=next(yaml.safe_load_all(open('.kale/notebook-sequence.pipeline.yaml'))); print('root tasks:', list(ir['root']['dag']['tasks'])); [print(' ', c, '->', 'SUB-DAG' if 'dag' in ir['components'][c] else 'leaf') for c in sorted(ir['components'])]"
```

Root has three tasks (`notebook-a/b/c`); `comp-notebook-a/b/c` are sub-DAGs; the
inner per-step components are leaves.

## 4. Run on a Kubeflow cluster

From **inside a Kubeflow Notebook server** (the endpoint and service-account
auth resolve automatically):

```
kale --workflow notebook_a.ipynb notebook_b.ipynb notebook_c.ipynb --pipeline_name notebook-sequence --run_pipeline
```

From **outside the cluster**, pass the endpoint and configure auth in
`~/.config/kale/kfp_server_config.json`. Set `auth_type` to match your
deployment: `kubernetes_service_account_token`, `existing_bearer_token` (token
in the `KF_PIPELINES_TOKEN` env var), or `dex` for a Dex-protected cluster
(session cookie in the `KF_PIPELINES_COOKIES` env var):

```
kale --workflow ... --kfp_host https://<your-kfp-endpoint>/pipeline --run_pipeline
```

The example notebooks set a `base_image` in their metadata. If that image is not
present on your cluster, override it with `--docker_image <your-image>`.

The command prints a `Run details:` URL.

## 5. View it in the dashboard

Open the `Run details:` URL in the Kubeflow Pipelines UI. The graph shows three
nodes, `notebook-a`, `notebook-b`, and `notebook-c`, each a sub-DAG. Click a node
to expand its inner steps, then click a **leaf** step (for example `summary`).
Click the leaf steps, not the `notebook-a/b/c` wrapper nodes, for their details.

Kale runs each step's code in an embedded Jupyter kernel, so the printed output
is captured into the step's **HTML report artifact**, not the Logs tab. Open that
artifact (for `summary` it is `summary_html_report`, under the step's
Input/Output view) to see the captured output: the names, the model, the
prediction, the predicted label, and the sentence. For the example data the final
step reports a prediction of 30.

## 6. Change a notebook, re-run

The notebook is the source of truth. Edit a step cell, save, and re-run section
2 or 4; the compiler reads the notebooks fresh each time.

> Put new code inside a cell **tagged as a step** (`step:<name>`). A variable
> defined in an untagged cell before the first step is not part of any step, so
> it will not be passed between notebooks.

For example, change `notebook_a`'s data from `y = 3x` to `y = 4x` and the
predicted value moves from 30 to 40.

## 7. CLI reference (`--workflow`)

| Flag | Effect |
|---|---|
| `--workflow nb1 nb2 ...` | Notebooks to compose (replaces `--nb`). |
| `--run_pipeline` | Upload and create a run. |
| `--upload_pipeline` | Upload without running. |
| `--pipeline_name` | Name of the composed pipeline. |
| `--experiment_name` | KFP experiment name. |
| `--pipeline_description` | Description shown in the KFP UI. |
| `--kfp_host` | KFP endpoint (override; default is in Kale's config). |
| `--docker_image` | Base image (override; default is each notebook's metadata). |

`--workflow` accepts the same flags as `--nb`. Relevant env vars:
`KF_PIPELINES_ENDPOINT`, `KF_PIPELINES_UI_ENDPOINT`, `KF_PIPELINES_TOKEN`.

## 8. What this adds on top of Kale

| File | Role |
|---|---|
| `kale/processors/workflow.py` | Composer: infers cross-notebook data flow, wires each notebook as a sub-pipeline. |
| `kale/templates/workflow_template.jinja2` | Renders the sub-pipelines and the top-level pipeline. |
| `kale/cli.py` (`--workflow`) | CLI entry point. |
| `kale/tests/unit_tests/test_workflow.py` | Tests. |

It reuses Kale's existing engine (parsing, dependency detection, marshalling,
component generation). The single-notebook path (`kale --nb`) is unchanged.

## 9. Tests

```
pip install pytest testfixtures
pytest kale/tests/unit_tests/test_workflow.py -q
```
