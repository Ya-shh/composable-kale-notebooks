# Composable Kale Notebooks

Compose several independent notebooks into **one** Kubeflow pipeline, where each
notebook becomes its own nested sub-pipeline (sub-DAG), with execution order and
data passing inferred automatically. This extends Kale with a single new CLI
command, `kale --workflow`, reusing Kale's existing engine (parsing, dependency
detection, marshalling, compilation). The single-notebook path (`kale --nb`) is
unchanged.

## Quick start

Requires Python 3.11+.

```
pip install -e .
cd notebooks
kale --workflow notebook_a.ipynb notebook_b.ipynb notebook_c.ipynb --pipeline_name notebook-sequence
```

This composes the three example notebooks into one pipeline (inferred order
`notebook_a -> notebook_b -> notebook_c`) and writes the KFP DSL and IR to
`.kale/`. Add `--run_pipeline` to submit it to a Kubeflow cluster.

**See [DEMO.md](DEMO.md) for the full guide:** running on a cluster, inspecting
the sub-DAG structure, the edit-and-re-run loop, and the CLI reference.

**See [working.md](working.md) for a step-by-step walkthrough of the example:**
what each notebook does, the data put into them, how it flows through the
sub-DAGs, and how `notebook_c` produces the prediction, the label, and the
sentence.

## What this adds on top of Kale

| File | Role |
|---|---|
| `kale/processors/workflow.py` | Composer: infers cross-notebook data flow, wires each notebook as a sub-pipeline. |
| `kale/templates/workflow_template.jinja2` | Renders the sub-pipelines and the top-level pipeline. |
| `kale/cli.py` (`--workflow`) | CLI entry point. |
| `kale/tests/unit_tests/test_workflow.py` | Tests. |

## Tests

```
pip install pytest testfixtures
pytest kale/tests/unit_tests/test_workflow.py -q
```
