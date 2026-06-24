# How the Example Works, Step by Step

Three independent notebooks are composed into one Kubeflow pipeline with
`kale --workflow`. Each notebook becomes its own sub-pipeline (a sub-DAG), and
Kale infers both the run order and the data passed between them from the
variable names they share. The example carries a tiny dataset through a linear
regression and ends with a labeled prediction and a sentence.

## The three notebooks and their data

**notebook_a (the data).** Declares the inputs:

- `names = ["Eder", "Stefano", "Yash", "GSoC"]`, four labels.
- `raw_points = [(1, 3), (2, 6), (3, 9), (4, 12)]`, four points that follow `y = 3x`.
- `dataset = [{"x": x, "y": y} ...]`, the points as records.
- `weights = [1, 2, 3, 4]`, one weight per point.

**notebook_b (the model).** Consumes A's data and fits a model:

- prints the `names` it received.
- `fit`: least-squares linear regression over `dataset`, giving `slope = 3` and `intercept = 0`.
- `model = {"slope": 3.0, "intercept": 0.0}`.
- `weight_total = sum(weights) = 10`.

**notebook_c (the prediction).** Consumes B's `model` and `weight_total` and A's `names`:

- `predict`: `prediction = slope * 10 + intercept = 30`.
- `report`: prints the prediction.
- `combine`: `adjusted_prediction = prediction + weight_total = 40`.
- `summary`: turns the prediction into a name and a sentence (below).

## What happens through the sub-DAGs

Running:

```
kale --workflow notebook_a.ipynb notebook_b.ipynb notebook_c.ipynb --pipeline_name notebook-sequence
```

compiles each notebook into a nested sub-pipeline and wires them into one
top-level pipeline. The order A, B, C is not given by you; it is inferred from
the shared variables:

- `dataset`, `names`, `weights` are produced in A and used later, so A runs first.
- `model`, `weight_total` are produced in B and used in C, so B runs before C.

Each shared variable crosses a notebook boundary as a typed KFP artifact:

- A to B: `dataset`, `names`, `weights`
- B to C: `model`, `weight_total`
- A to C: `names` (used directly in C as well)

Inside each sub-DAG, that notebook's own steps run in their data-dependency
order (for example `generate` before `assemble`, `fit` before `package`).

## How notebook C predicts a value and a name

C's `summary` step receives the `prediction` (from B's model) and the `names`
(from A), and produces two outputs:

- **A value mapped to a name.** `predicted_label = names[int(prediction) % len(names)]`.
  With `prediction = 30` and four names, `30 % 4 = 2`, so
  `predicted_label = names[2] = "Yash"`. This turns the regression's number into
  one of the four labels.
- **A sentence.** `sentence = f"{names[0]} and {names[1]} are working with {names[2]} on {names[3]}."`,
  which reads "Eder and Stefano are working with Yash on kale-GSoC'2026."

So C goes from a raw number (30) to a labeled prediction ("Yash") and a readable
sentence, using both the model output and the names that came from notebook A.

## How Kale ties it together

- Every cell tagged `step:<name>` becomes a step; the cell tags also declare
  order (`prev:`), and the boundary data is found from the code itself.
- Kale parses each notebook, builds each one's internal step DAG, and the
  composer detects which variables are produced in one notebook and consumed in
  another.
- For each boundary variable it marshals the value (serialize on the producer
  step, load on the consumer step) and passes it as a typed artifact from one
  sub-pipeline to the next.
- The result is a single KFP pipeline in which `notebook_a`, `notebook_b`, and
  `notebook_c` appear as three sub-DAGs with data flowing A to B to C, all
  compiled and run through the `kale` CLI.
