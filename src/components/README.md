# KFP Components

In this folder are the components for the training and prediction [pipelines](../pipelines/README.md).

## Add a new Python function-based component

KPF components are created as python functions that do not have any code declared outside of the function definition. So package importing and variable creation should all be done inside the python function itself. In addition, a component decorator wraps up the function into a component (`ContainerOp` object).

One can specify a name, brief description, base image, extra packages needed, and more in the component decorator. The following example shows how to create a KFP component that computes the mean of two numbers. In this component we will use `python:3.9` as our base image, and we will install `numpy` as an extra package to import it and use it within the component's code. When the number of extra packages to be installed and used increases or when it's necessary to use a large custom code base, it is a good practice to use a custom image with all the necessary dependencies included.

```python
from kfp.v2.dsl import component

@component(
  base_image="python:3.9",
  packages_to_install=["numpy"],
)
def average(a: float, b:float) -> float:
  """Computes the average of two numbers.
  """
  import numpy as np

  arr = np.array([a, b])
  return np.mean(arr)
```

## Components I/O

There are two categories of components' inputs and outputs: parameters, and artifacts. Parameters represent values are usually passed to components in forms of python's
