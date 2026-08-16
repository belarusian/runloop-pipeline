# TICKET-38: `Pipeline` constructor lacks an optional `schema` parameter to pin ingestion types

## Title
`Pipeline.__init__` accepts `source`, `transforms`, `encoding`, and
`sample_size`, but no `schema` parameter. The pipeline always infers column
types at ingestion time. The `schema` parameter on `to_csv`/`stream_to_csv`
controls **output column ordering** only — it does not affect ingestion type
coercion.

## Evidence
- `pipeline/pipeline.py:78-90` — `__init__` signature:
