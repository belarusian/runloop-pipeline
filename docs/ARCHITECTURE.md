# Architecture

## The phase model

The pipeline is a sequence of **phases**. Each phase is a pure function over
records: it takes records in, produces records out, and never mutates its
input. This makes phases composable and independently testable.
