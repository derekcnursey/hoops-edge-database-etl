# Multi-Agent Orchestration Setup — Quick Start

## What's Included

```
CLAUDE.md                              # Project context → drop into repo root
ORCHESTRATE.md                         # Full plan with execution order
.claude/agents/
  data-engineer.md                     # Agent 1: backfill gaps, gap-fill framework
  analytics-engineer.md                # Agent 2: gold layer, Athena views
  infra-engineer.md                    # Agent 3: ECS, CI/CD, monitoring
  test-engineer.md                     # Agent 4: tests, data quality checks
prompts/
  session_1_data_engineer.txt          # Paste into Claude Code → Session 1
  session_2_analytics_engineer.txt     # Paste into Claude Code → Session 2
  session_3_test_engineer.txt          # Paste into Claude Code → Session 3
  session_4_infra_engineer.txt         # Paste into Claude Code → Session 4
  session_5_integration.txt            # Paste into Claude Code → Session 5
```

## How to Use

### 1. Copy files into your repo
```bash
cp CLAUDE.md /path/to/cbbd_etl/
cp ORCHESTRATE.md /path/to/cbbd_etl/
cp -r .claude /path/to/cbbd_etl/
cp -r prompts /path/to/cbbd_etl/
```

If you already have a `CLAUDE.md`, merge the contents.

### 2. Run sessions one at a time
```bash
cd /path/to/cbbd_etl
claude
# Paste contents of prompts/session_1_data_engineer.txt
# Wait for completion
# Commit changes
# Start new Claude Code session
# Paste contents of prompts/session_2_analytics_engineer.txt
# Repeat...
```

### 3. Commit between sessions
Each session prompt tells you what to commit. This gives you clean rollback points:
```bash
git add -A && git commit -m "feat: data completeness audit, gap-fill framework"
# start next session...
```

## Execution Order & Why

| # | Agent | Builds On | Creates |
|---|-------|-----------|---------|
| 1 | Data Engineer | — (foundation) | gap_fill.py, data audit, fixed backfill |
| 2 | Analytics Engineer | Clean silver data | gold/ package, Athena views, design docs |
| 3 | Test Engineer | All code from 1-2 | tests/, quality checks, fixtures |
| 4 | Infra Engineer | — (independent) | Dockerfile, Terraform, CI/CD, monitoring |
| 5 | Integration | Everything | Updated README, Makefile, buildout summary |

Data Engineer goes first because everything else depends on having complete, validated data. Infra Engineer goes last because it's independent — you can keep running locally until it's done.

## File Ownership (No Conflicts)

| Agent | Primary Directories |
|-------|-------------------|
| Data Engineer | `src/cbbd_etl/gap_fill.py`, `scripts/`, `reports/` |
| Analytics Engineer | `src/cbbd_etl/gold/`, `docs/`, `infra/athena_views/` |
| Test Engineer | `tests/`, `src/cbbd_etl/quality/` |
| Infra Engineer | `infra/`, `Dockerfile`, `.github/` |

## Tips

- **If a session errors out**: Start a fresh Claude Code session and re-paste the same prompt. The agent will pick up from the committed state.
- **To skip an agent**: Just move to the next session prompt. They're designed to be resilient to missing earlier work.
- **To run a single task from an agent**: Edit the `.claude/agents/*.md` file to keep only the tasks you want before launching the session.
- **Time per session**: Expect ~20-40 minutes each depending on codebase size and task complexity.
