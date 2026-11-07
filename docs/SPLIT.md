# How this repository was split

This repository, `retail-data-pipeline`, is one of three public sibling repositories split from a single earlier monorepo, `retail-data-platform`:

| Repository | Owns |
|---|---|
| **`retail-data-pipeline`** (this one) | `pipeline/` and `data/`: synthetic data generation, incremental/idempotent load into DuckDB (Snowflake backend untested), tests, offline demo |
| `retail-data-platform-ops` | CI, Dockerfile, AWS Terraform (offline-validated only), local monitoring/alerting (`retail_ops/`, `platform/`) |
| `retail-ai-support-app` | SYNTHETIC support-ticket triage (`ai_app/`) with a fake/offline provider and evaluation harness |

## Rules of the split

- **No shared runtime package.** Each repo installs and runs on its own.
- The two siblings **vendor a copy of `pipeline/` and `data/`** so their demos run standalone. This repo is the source of truth for those directories. The copies **must not drift silently**: when this repo changes `pipeline/` or `data/`, re-sync the siblings deliberately and re-run their checks.
- Showcase videos are stored in each repo at `docs/showcase.mp4` (silent, offline-rendered).
- This product was built in the earlier monorepo; the videos and the split were done afterwards.
- Verification of the split itself is in [`../EVIDENCE.md`](../EVIDENCE.md).
