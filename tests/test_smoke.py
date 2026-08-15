"""Smoke test: the pipeline package must import cleanly."""


def test_import_pipeline():
    import pipeline

    assert pipeline is not None
    assert hasattr(pipeline, "__version__")
