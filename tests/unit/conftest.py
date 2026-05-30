"""
Unit-test conftest — loaded by pytest before any test file is collected.

Why this file exists
--------------------
``mlflow`` (any version >= 2.13) has a complex chain of transitive imports
that break in certain environments:

  * Python 3.12 runners without ``setuptools``:
    ``mlflow.utils.requirements_utils`` -> ``import pkg_resources`` -> FAIL
  * Environments where ``opentelemetry`` packages are mismatched:
    ``mlflow.tracing`` -> ``opentelemetry.sdk.trace`` ->
    ``opentelemetry.semconv.attributes`` -> FAIL

Because the import chain fires at *collection time* -- before any
``monkeypatch`` fixture can run -- the only reliable fix is to inject a
``MagicMock`` for ``mlflow`` into ``sys.modules`` here, where conftest
is executed first.

The unit tests in ``test_training.py`` already patch every mlflow method
they call (``set_tracking_uri``, ``set_experiment``, ``start_run``,
``log_params``, ``log_metrics``, ``mlflow.sklearn.log_model``). When
``mlflow`` is already a ``MagicMock`` in ``sys.modules``, those
``monkeypatch.setattr`` calls simply overwrite attributes on the mock --
which is exactly what the tests need.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock


def _mock_mlflow_if_unimportable() -> None:
    """Replace mlflow with a MagicMock when it cannot be imported cleanly."""
    if "mlflow" in sys.modules:
        # Already imported (or already mocked) -- nothing to do.
        return

    try:
        import mlflow  # noqa: F401  -- try the real package first
    except (ImportError, ModuleNotFoundError):
        pass
    else:
        return  # real mlflow imported successfully; tests can use it as-is

    # Real mlflow failed to import -> inject stubs so test_training.py works.
    mlflow_mock = MagicMock(name="mlflow")
    mlflow_mock.__version__ = "2.13.0"

    mlflow_sklearn_mock = MagicMock(name="mlflow.sklearn")
    mlflow_mock.sklearn = mlflow_sklearn_mock

    sys.modules["mlflow"] = mlflow_mock
    sys.modules["mlflow.sklearn"] = mlflow_sklearn_mock
    # Register common sub-modules that mlflow's own code may reference
    for sub in (
        "mlflow.tracking",
        "mlflow.models",
        "mlflow.entities",
        "mlflow.store",
        "mlflow.utils",
    ):
        sys.modules.setdefault(sub, MagicMock(name=sub))


_mock_mlflow_if_unimportable()
