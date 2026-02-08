# Clean Skills - JobMinglr Skill Analyzer & Sync Pipeline

AI-powered pipeline that analyzes, cleans, categorizes, and syncs professional skills in the JobMinglr production database using Google Gemini 2.5 Flash.

## Overview

This tool processes raw skill entries from the production MongoDB, uses an LLM to determine validity, category, naming corrections, aliases, and similar tools — then syncs the results back to production.

### Pipeline Flow

```
Production DB (skills) --> clean.py (AI analysis) --> Local DB (process_skills) --> sync_skills.py --> Production DB (updated)
```

## Scripts

### `clean.py` — Skill Analyzer

Fetches skills from production, sends each to Google Gemini for structured analysis, and stores results in a local MongoDB collection.

**What it does per skill:**

| Field | Description |
|---|---|
| `is_skill` | Whether the entry is a valid professional skill |
| `is_job_role` | Whether it's a job title (e.g., Software Engineer) |
| `need_name_change` | Whether the name has typos or redundant prefixes |
| `updated_name` | Corrected name (if applicable) |
| `category` | `TECHNICAL` or `NON_TECHNICAL` |
| `similar_tools_and_skills` | 3-5 alternative/competing tools |
| `aliases` | 2-4 abbreviations or alternate names |

**Usage:**

```bash
# Process all unprocessed skills
python clean.py all

# Process a specific number of skills
python clean.py 100
```

- Resumes from where it left off (skips already-processed skills).
- Runs 100 concurrent LLM requests per batch.
- Logs output to `cleanup_skills_log.txt` and failures to `failed_skills_log.txt`.

---

### `sync_skills.py` — Production Sync

Reads analyzed results from local DB and applies changes to production.

**Operations:**

| Operation | Action |
|---|---|
| **DELETE** | Soft-deletes invalid skills (`isDeleted: true`) |
| **UPDATE** | Applies name corrections, aliases, and category |
| **CREATE** | Inserts new skills discovered via `similar_tools_and_skills` |

**Usage:**

```bash
# Dry run — preview all operations without modifying production
python sync_skills.py --dry-run

# Execute all operations (delete + update + create)
python sync_skills.py

# Execute only specific operations
python sync_skills.py --action=delete
python sync_skills.py --action=update
python sync_skills.py --action=create

# Combine dry run with action filter
python sync_skills.py --dry-run --action=update
```

Requires manual confirmation (`yes`) before modifying production (unless `--dry-run`).

## Tech Stack

- **Python 3.10+** with `asyncio`
- **MongoDB** — Motor (async driver)
- **Google Gemini 2.5 Flash** — via LangChain + Vertex AI credentials
- **Pydantic** — structured LLM output validation
- **LangChain** — LLM orchestration

## Prerequisites

1. **Python 3.10+**
2. **MongoDB** running locally on `localhost:27017`
3. **Google Cloud service account** credentials file (`cred.json`) in the project root
4. Access to the JobMinglr production MongoDB

## Installation

```bash
pip install motor pydantic langchain langchain-google-genai langchain-ollama langchain-nvidia-ai-endpoints google-auth
```

## Configuration

Key constants are defined at the top of each script:

| Constant | Default | Description |
|---|---|---|
| `BATCH_CONCURRENCY` | `100` | Concurrent LLM requests per batch |
| `LLM_CONCURRENCY` | `500` | Max concurrent LLM calls |
| `LOCAL_URI` | `mongodb://localhost:27017` | Local MongoDB connection |
| `LOCAL_DB` | `jobminglr` | Local database name |
| `LOCAL_COLLECTION` | `process_skills` | Local collection for results |

## Project Structure

```
clean_skills/
├── clean.py                # Skill analyzer (AI processing)
├── sync_skills.py          # Production sync script
├── cred.json               # Google Cloud service account (gitignored)
├── failed_skills_log.txt   # Skills that failed LLM parsing
├── sync_skills_log.txt     # Sync operation logs
├── .gitignore
└── README.md
```

## Logs

- **`cleanup_skills_log.txt`** — Full processing log from `clean.py`
- **`failed_skills_log.txt`** — Skills that failed LLM analysis with error details and raw responses
- **`sync_skills_log.txt`** — Sync operation log from `sync_skills.py`
