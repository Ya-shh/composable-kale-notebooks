# Copyright 2026 The Kubeflow Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for multi-notebook composition (kale.processors.workflow)."""

import nbformat as nbf

from kale.processors.workflow import (
    _topo_sort,
    _type_for,
    compose_notebooks_as_subpipelines,
)


def _write_nb(path, name, cells):
    nb = nbf.v4.new_notebook()
    nb.metadata["kubeflow_notebook"] = {
        "pipeline_name": name,
        "experiment_name": "test",
        "volumes": [],
    }
    for tags, src in cells:
        cell = nbf.v4.new_code_cell(source=src)
        cell.metadata["tags"] = tags
        nb.cells.append(cell)
    nbf.write(nb, str(path))


def test_topo_sort_orders_by_dependency():
    """Order is derived from edges, not input order."""
    assert _topo_sort(["c", "a", "b"], [("a", "b"), ("b", "c")]) == ["a", "b", "c"]


def test_topo_sort_detects_cycle():
    """A cyclic dependency graph is rejected."""
    import pytest

    with pytest.raises(ValueError):
        _topo_sort(["a", "b"], [("a", "b"), ("b", "a")])


def test_type_for_name_heuristic():
    """Artifact type matches the compiler's name heuristic."""
    assert _type_for("model") == "Model"
    assert _type_for("my_model") == "Model"
    assert _type_for("dataset") == "Dataset"
    assert _type_for("anything_else") == "Dataset"


def test_compose_infers_order_and_builds_subpipelines(tmp_path, monkeypatch):
    """Order is inferred from shared variable names, and each notebook becomes a
    wired sub-pipeline, regardless of the order the notebooks are passed in."""
    a = tmp_path / "notebook_a.ipynb"
    b = tmp_path / "notebook_b.ipynb"
    c = tmp_path / "notebook_c.ipynb"
    _write_nb(a, "notebook-a", [(["step:gen"], "dataset = [1, 2, 3]")])
    _write_nb(b, "notebook-b", [(["step:fit"], "model = sum(dataset)")])
    _write_nb(c, "notebook-c", [(["step:pred"], "prediction = model + 1")])

    monkeypatch.chdir(tmp_path)
    # Pass them out of order on purpose: order must be inferred, not positional.
    dsl_path, order = compose_notebooks_as_subpipelines(
        [str(c), str(a), str(b)], pipeline_name="seq"
    )

    assert order == ["notebook_a", "notebook_b", "notebook_c"]

    dsl = open(dsl_path).read()
    # One sub-pipeline per notebook, with the inferred typed boundary I/O.
    assert "def notebook_a_pipeline(" in dsl
    assert "def notebook_b_pipeline(" in dsl
    assert "def notebook_c_pipeline(" in dsl
    assert "dataset_input_artifact: Input[Dataset]" in dsl
    assert "model_input_artifact: Input[Model]" in dsl
    # Top-level pipeline wires each producer's output to the next consumer.
    assert "def auto_generated_pipeline(" in dsl
    assert "dataset_input_artifact=notebook_a_task.output" in dsl
    assert "model_input_artifact=notebook_b_task.output" in dsl


def test_compose_handles_multiple_outputs_from_one_notebook(tmp_path, monkeypatch):
    """A notebook producing >1 boundary output returns a NamedTuple, and the
    consumer references each output by name."""
    a = tmp_path / "notebook_a.ipynb"
    b = tmp_path / "notebook_b.ipynb"
    _write_nb(a, "notebook-a", [(["step:gen"], "dataset = [1, 2, 3]\nmodel = {'w': 2}")])
    _write_nb(b, "notebook-b", [(["step:use"], "prediction = sum(dataset) * model['w']")])

    monkeypatch.chdir(tmp_path)
    dsl_path, order = compose_notebooks_as_subpipelines(
        [str(a), str(b)], pipeline_name="multi"
    )
    assert order == ["notebook_a", "notebook_b"]

    dsl = open(dsl_path).read()
    # producer returns a NamedTuple of both outputs
    assert "-> NamedTuple('Outputs'" in dsl
    assert "return NamedTuple('Outputs'" in dsl
    # consumer references each output by name (not the ambiguous `.output`)
    assert 'notebook_a_task.outputs["dataset"]' in dsl
    assert 'notebook_a_task.outputs["model"]' in dsl


def test_untagged_cell_is_not_treated_as_a_boundary_variable(tmp_path, monkeypatch):
    """A variable defined in an untagged cell is not part of any step, so it
    must not be injected as a step output (which crashed compilation before)."""
    a = tmp_path / "notebook_a.ipynb"
    b = tmp_path / "notebook_b.ipynb"
    # `scratch` lives in an untagged cell; only `dataset` is a real step output.
    nb = nbf.v4.new_notebook()
    nb.metadata["kubeflow_notebook"] = {"pipeline_name": "notebook-a", "volumes": []}
    c0 = nbf.v4.new_code_cell(source='scratch = "x"')  # no tags
    c1 = nbf.v4.new_code_cell(source="dataset = [1, 2, 3]")
    c1.metadata["tags"] = ["step:gen"]
    nb.cells = [c0, c1]
    nbf.write(nb, str(a))
    _write_nb(b, "notebook-b", [(["step:use"], "total = sum(dataset)")])

    monkeypatch.chdir(tmp_path)
    dsl_path, _ = compose_notebooks_as_subpipelines([str(a), str(b)], pipeline_name="ut")
    dsl = open(dsl_path).read()
    # `scratch` must never be saved/loaded as a pipeline artifact
    assert "scratch" not in dsl
    assert "dataset_output_artifact" in dsl  # the real step output is still wired
