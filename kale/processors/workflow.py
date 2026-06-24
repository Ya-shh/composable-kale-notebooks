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
"""Compose multiple notebooks into a single Kubeflow pipeline.

Each notebook is compiled into its own KFP sub-pipeline by reusing Kale's
per-notebook processing and component generation. The sub-pipelines are wired
together by matching the variables one notebook defines to those another
consumes, the same name-based detection Kale uses between cells, one level up.
The single-notebook path is untouched.
"""

import ast
import os

from kale.common import flakeutils


def _defined_names(code):
    """Names assigned at the top level of a code block."""
    names = set()
    for node in ast.walk(ast.parse(code)):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return names


def _topo_sort(nodes, edges):
    """Kahn topological sort. edges are (producer, consumer); raises on a cycle."""
    indeg = dict.fromkeys(nodes, 0)
    adj = {n: [] for n in nodes}
    for producer, consumer in edges:
        adj[producer].append(consumer)
        indeg[consumer] += 1
    queue = sorted(n for n in nodes if indeg[n] == 0)
    order = []
    while queue:
        node = queue.pop(0)
        order.append(node)
        for child in sorted(adj[node]):
            indeg[child] -= 1
            if indeg[child] == 0:
                queue.append(child)
    if len(order) != len(nodes):
        raise ValueError("Cycle detected in the notebook dependency graph.")
    return order


def _notebook_name(path):
    base = os.path.splitext(os.path.basename(path))[0]
    return base.replace("-", "_").lower()


def _type_for(var_name):
    """KFP artifact type, matching Kale's compiler name heuristic."""
    return "Model" if "model" in var_name else "Dataset"


def _defining_step(steps, var):
    """Last step (in order) whose code defines ``var``."""
    found = None
    for step in steps:
        if var in _defined_names("\n".join(step.source)):
            found = step
    return found or steps[-1]


def _using_step(steps, var):
    """First step whose code references ``var`` without defining it."""
    for step in steps:
        if var in flakeutils.pyflakes_report("\n".join(step.source)):
            return step
    return steps[0]


def _producer_step(steps, var, consumer):
    """Upstream step that lists ``var`` among its outputs."""
    producer = None
    for step in steps:
        if step.name == consumer.name:
            break
        if var in step.outs:
            producer = step
    return producer or consumer


def compose_notebooks_as_subpipelines(
    notebook_paths,
    pipeline_name="kale-workflow",
    experiment_name="Kale-Workflow-Experiment",
    base_image="",
    pipeline_description=None,
):
    """Compose notebooks into one pipeline, each notebook a KFP sub-pipeline.

    Runs Kale's NotebookProcessor and Compiler per notebook so each notebook's
    internal steps form a nested sub-DAG, then wires the sub-pipelines together
    by matching the data each notebook produces and consumes. Returns
    ``(dsl_script_path, order)``.
    """
    import autopep8

    from kale.compiler import Compiler
    from kale.processors import NotebookProcessor

    overrides = {"experiment_name": experiment_name}
    if base_image:
        overrides["base_image"] = base_image

    notebooks = {}
    for path in notebook_paths:
        name = _notebook_name(path)
        proc = NotebookProcessor(path, {**overrides, "pipeline_name": name.replace("_", "-")})
        pipeline = proc.run()
        # Only code that runs as a step counts. Untagged cells never become a
        # step, so scanning raw cells would invent variables no step produces.
        step_code = "\n".join("\n".join(s.source) for s in pipeline.steps)
        notebooks[name] = {
            "pipeline": pipeline,
            "imports_and_functions": proc.get_imports_and_functions(),
            "defined": _defined_names(step_code),
            "needed": flakeutils.pyflakes_report(step_code),
            "display": name.replace("_", "-"),
        }

    producer = {}
    for name, nb in notebooks.items():
        for var in nb["defined"]:
            producer.setdefault(var, name)

    boundary_needs = {name: [] for name in notebooks}
    boundary_provides = {name: [] for name in notebooks}
    edges = []
    for name, nb in notebooks.items():
        for var in sorted(nb["needed"]):
            src = producer.get(var)
            if src and src != name:
                boundary_needs[name].append(var)
                if var not in boundary_provides[src]:
                    boundary_provides[src].append(var)
                edges.append((src, name))

    order = _topo_sort(list(notebooks), edges)

    # Mark each boundary variable on its producing/consuming step so Kale's
    # component generator exposes it as an input/output artifact.
    produces_var = {}
    for name, nb in notebooks.items():
        steps = list(nb["pipeline"].steps)
        for var in boundary_provides[name]:
            step = _defining_step(steps, var)
            if var not in step.outs:
                step.outs.append(var)
            produces_var[(name, var)] = step.name
        for var in boundary_needs[name]:
            step = _using_step(steps, var)
            if var not in step.ins:
                step.ins.append(var)

    components = []
    subpipelines = []
    for name in order:
        nb = notebooks[name]
        steps = list(nb["pipeline"].steps)
        compiler = Compiler(nb["pipeline"], nb["imports_and_functions"])

        params = [
            {"name": f"{v}_input_artifact", "type": _type_for(v)}
            for v in sorted(boundary_needs[name])
        ]
        outputs = [
            {
                "var": v,
                "type": _type_for(v),
                "ref": f'{produces_var[(name, v)]}_task.outputs["{v}_output_artifact"]',
            }
            for v in sorted(boundary_provides[name])
        ]

        tasks = []
        for step in steps:
            # Capture the wiring first: generate_lightweight_component mutates
            # step.source.
            inputs, after = [], []
            for var in sorted(step.ins):
                if var in boundary_needs[name]:
                    ref = f"{var}_input_artifact"
                else:
                    anc = _producer_step(steps, var, step)
                    ref = f'{anc.name}_task.outputs["{var}_output_artifact"]'
                    after.append(f"{anc.name}_task")
                inputs.append({"arg": f"{var}_input_artifact", "ref": ref})
            tasks.append(
                {
                    "task_var": f"{step.name}_task",
                    "fn": f"{step.name}_step",
                    "inputs": inputs,
                    "after": sorted(set(after)),
                    "display": step.name,
                }
            )
            components.append(compiler.generate_lightweight_component(step))

        subpipelines.append(
            {
                "name": name,
                "display": nb["display"],
                "params": params,
                "outputs": outputs,
                "tasks": tasks,
            }
        )

    toplevel_calls = []
    for name in order:
        inputs, after = [], []
        for var in sorted(boundary_needs[name]):
            src = producer[var]
            # One boundary output is reachable as `.output`; several become a
            # NamedTuple addressed by field name.
            if len(boundary_provides[src]) > 1:
                ref = f'{src}_task.outputs["{var}"]'
            else:
                ref = f"{src}_task.output"
            inputs.append({"arg": f"{var}_input_artifact", "ref": ref})
            after.append(f"{src}_task")
        toplevel_calls.append(
            {
                "task_var": f"{name}_task",
                "fn": f"{name}_pipeline",
                "inputs": inputs,
                "after": sorted(set(after)),
                "display": notebooks[name]["display"],
            }
        )

    env = Compiler(notebooks[order[0]]["pipeline"], "")._get_templating_env()
    dsl = autopep8.fix_code(
        env.get_template("workflow_template.jinja2").render(
            components=components,
            subpipelines=subpipelines,
            toplevel_calls=toplevel_calls,
            pipeline_name=pipeline_name,
            pipeline_description=(
                pipeline_description or "Composed sub-pipelines: " + " -> ".join(order)
            ),
        )
    )

    out_dir = os.path.join(os.getcwd(), ".kale")
    os.makedirs(out_dir, exist_ok=True)
    dsl_path = os.path.abspath(os.path.join(out_dir, f"{pipeline_name}.workflow.py"))
    with open(dsl_path, "w") as f:
        f.write(dsl)
    return dsl_path, order
