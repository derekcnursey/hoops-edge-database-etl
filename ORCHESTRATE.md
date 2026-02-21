# Multi-Agent Orchestration Plan (Sequential — Claude Max 5x)

## Overview
Run 4 specialized agents one at a time across separate Claude Code sessions. Each session dispatches a single subagent via the Task tool, reviews its output, then commits. Order matters — later agents build on earlier work.

## Execution Order

| Session | Agent | Why This Order | Est. Tokens |
|---------|-------|---------------|-------------|
| 1 | Data Engineer | Foundation — everything depends on complete, clean data | ~50-70K |
| 2 | Analytics Engineer | Builds gold layer on top of validated silver data | ~50-70K |
| 3 | Test Engineer | Tests all code that now exists from sessions 1-2 | ~40-60K |
| 4 | Infra Engineer | Production hardening — do last, least dependencies | ~40-60K |
| 5 | Integration | Lead reviews everything, updates docs, final commit | ~15-25K |

## Session Instructions

### Session 1: Data Engineer
Copy and paste `prompts/session_1_data_engineer.txt` into Claude Code.

**What it does:**
- Audits data completeness across all silver tables (2010-2026)
- Fixes plays backfill script edge cases
- Builds reusable gap-fill framework (`src/cbbd_etl/gap_fill.py`)
- Validates partition consistency

**When done, commit:**
```bash
git add -A && git commit -m "feat: data completeness audit, gap-fill framework, plays backfill fixes"
```

### Session 2: Analytics Engineer
Copy and paste `prompts/session_2_analytics_engineer.txt` into Claude Code.

**What it does:**
- Designs gold layer schema (documented in `docs/gold_layer_design.md`)
- Implements gold transforms in `src/cbbd_etl/gold/`
- Wires gold builds into orchestrator
- Creates Athena views for common queries

**When done, commit:**
```bash
git add -A && git commit -m "feat: gold layer transforms, Athena views, analytics schema"
```

### Session 3: Test Engineer
Copy and paste `prompts/session_3_test_engineer.txt` into Claude Code.

**What it does:**
- Creates test infrastructure (conftest, fixtures, moto mocks)
- Writes unit tests for all core modules
- Writes integration tests for pipeline flows
- Builds data quality check framework

**When done, commit:**
```bash
git add -A && git commit -m "feat: test suite, data quality checks, fixtures"
```

### Session 4: Infrastructure Engineer
Copy and paste `prompts/session_4_infra_engineer.txt` into Claude Code.

**What it does:**
- Creates Dockerfile + docker-compose for local testing
- Adds ECS Fargate Terraform (cluster, task def, ECR)
- Adds EventBridge scheduling for daily/weekly runs
- Adds CloudWatch monitoring + alerting
- Creates GitHub Actions CI/CD workflows
- Hardens IAM policies

**When done, commit:**
```bash
git add -A && git commit -m "feat: ECS deployment, CI/CD, monitoring, IAM hardening"
```

### Session 5: Final Integration
Copy and paste `prompts/session_5_integration.txt` into Claude Code.

**What it does:**
- Reviews all changes for consistency
- Resolves any conflicts or gaps
- Updates Makefile with all new targets
- Updates README.md with full documentation
- Runs tests to verify everything works
- Writes buildout summary

**When done, commit:**
```bash
git add -A && git commit -m "docs: final integration, README, buildout summary"
```

## Tips
- **Between sessions**: Do a quick `git diff --stat` to see what changed
- **If a session runs long**: Claude Code will show progress — let it finish before the next session
- **If something breaks**: You can re-run a single agent by pasting its prompt again
- **To skip an agent**: Just move to the next session prompt
- **To run a subset of tasks**: Edit the agent's `.md` file to comment out tasks before launching
