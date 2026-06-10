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
import warnings
from unittest.mock import MagicMock


def _mock_mlflow_if_unimportable() -> None:
    """Replace mlflow with a MagicMock when it cannot be imported cleanly.

    mlflow 2.13+ defines ``PromptModelConfig(BaseModel)`` with a ``model_name``
    field, which triggers a Pydantic ``UserWarning`` about the protected
    ``model_`` namespace.  When pytest is invoked with ``-W error::UserWarning``
    that warning is promoted to an exception *before* the later
    ``ModuleNotFoundError`` (missing ``opentelemetry.semconv.attributes``) fires.
    We install a targeted ``ignore`` filter for the duration of the import probe
    so the collection-time error disappears without affecting any other
    ``UserWarning`` checks in the test suite.
    """
    if "mlflow" in sys.modules:
        # Already imported (or already mocked) -- nothing to do.
        return

    try:
        # Suppress pydantic's protected-namespace UserWarning emitted by
        # mlflow.entities.model_registry.prompt_version.PromptModelConfig
        # before the real import error (opentelemetry missing) surfaces.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message=r"Field .* has conflict with protected namespace",
                category=UserWarning,
                module=r"pydantic.*",
            )
            import mlflow
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
