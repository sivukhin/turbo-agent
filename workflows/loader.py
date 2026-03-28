"""Load @workflow functions from Python files."""

import importlib.util
import sys
from pathlib import Path


def load_workflows_from_file(file_path: str) -> dict:
    """Import a Python file and return all @workflow decorated functions.

    Returns dict of {name: workflow_wrapper}.
    """
    path = Path(file_path).resolve()
    module_name = path.stem

    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    return collect_workflows(module)


def collect_workflows(module) -> dict:
    """Scan a module for @workflow decorated functions.

    Detects them by checking for the `.create` and `.resume` attributes
    that the @workflow decorator adds.
    """
    workflows = {}
    for name in dir(module):
        obj = getattr(module, name)
        if callable(obj) and hasattr(obj, 'create') and hasattr(obj, 'resume'):
            workflows[name] = obj
    return workflows
