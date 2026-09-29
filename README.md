# Web content → Volume (Genie) → Vector Search

Two notebooks that turn public web pages into AI-ready content on Databricks.

| Step | Notebook | What it does | Result |
|---|---|---|---|
| 1 | `01_ingest_web_to_volume` | Visits the websites you list, keeps only the readable main content of each page, and saves every page as a clean Markdown (`.md`) file. | A Unity Catalog **Volume** of `.md` files. Attach it to a **Genie Agent** as a source. |
| 2 | `02_index_volume_to_vector_search` | Splits the pages into small passages (chunks) with a Lakeflow Spark Declarative Pipeline, then builds a **Vector Search** index. | Tables `documents` and `document_chunks`, plus a Vector Search index for custom AI agents. |

The steps are **independent**. Run step 1 whenever you want fresh pages, and run step 2 whenever you want the search index to catch up. If you only use Genie, you never need step 2.

```
Websites ──► Step 1 (Python crawler) ──► Volume: web_content/*.md ──► Genie Agent (Sources → Volume)
                                                     │
                                                     └──► Step 2 (SDP pipeline) ──► document_chunks ──► Vector Search index
```

## Project layout

```
notebooks/
  01_ingest_web_to_volume.ipynb           Step 1: open this and click Run all
  02_index_volume_to_vector_search.ipynb  Step 2: open this and click Run all
src/web_content_kit/                      The code the notebooks use
  crawler.py                              Crawling, cleaning and saving pages (step 1)
  documents.py                            The page file format (front matter + Markdown)
  chunking.py                             How pages are split into chunks (step 2)
  indexing.py                             Pipeline and Vector Search management (step 2)
  settings.py, logs.py                    Widget validation and log formatting
pipelines/chunk_documents.py              The Spark Declarative Pipeline created by step 2
tests/                                    Unit tests for the code in src/
pyproject.toml                            Package definition and dependencies
databricks.yml, resources/                OPTIONAL bundle that runs both steps as a scheduled job
```

**Keep the folders together in the workspace.** Each notebook installs the project from the folder one level up (`%uv pip install ".."`), and step 2 points its pipeline at `pipelines/`. To get the project into a workspace, import this Git repository as a **Git folder** (Workspace → Create → Git folder). Alternatively, upload the whole folder.

## Before you start

* A workspace with **Serverless** notebooks enabled.
* Permission to create a schema and volumes in the catalog (default `main`). If the schema already exists, you need `USE SCHEMA`, `CREATE VOLUME` and `CREATE TABLE` on it.
* For step 2, permission to create a **pipeline** and **Vector Search** endpoints and indexes.
* The sites you crawl must be reachable from Databricks serverless (public internet).

## Step 1: collect pages

1. Open `notebooks/01_ingest_web_to_volume`.
2. Top right, click **Connect → Serverless**. Under **Environment**, check that the version is **6**; the notebook is saved with it.
3. Fill in the widgets at the top:
   * **Seed URLs**: one or more start pages separated by commas. A `sitemap.xml` address loads every page it lists.
   * **Max pages** / **Follow links: depth**: how much to collect, and how many links deep to follow (recursively). Start small (for example 50 pages, depth 1), check the result, then grow. To crawl a whole section of a site, use a larger depth together with an **Include URL pattern**, e.g. depth 3, 1000 pages, include `/help/`.
   * **Include URL pattern** (optional): only follow links containing this text, for example `/help/`.
4. Click **Run all**.
5. Read the summary at the bottom: pages saved, pages skipped or failed, and a preview of one saved page.

Where things go:

| Location | Contents |
|---|---|
| `/Volumes/<catalog>/<schema>/web_content/<website>/<page>.md` | **Only** clean pages, one per URL. File names come from the address, e.g. `docs.databricks.com/aws-en-ai-search-ai-search.md`. |
| `/Volumes/<catalog>/<schema>/web_content_staging/runs/<run id>/` | `manifest.jsonl` (every URL and what happened to it), `failures.jsonl`, and optional raw HTML. |

Each page file starts with a small header naming its source:

```markdown
---
title: "Query an AI Search index | Databricks on AWS"
source_url: "https://docs.databricks.com/aws/en/ai-search/query-ai-search"
crawled_at: "2026-09-29T16:15:52+00:00"
---

# Query an AI Search index | Databricks on AWS

Source: https://docs.databricks.com/aws/en/ai-search/query-ai-search
...
```

Re-running is safe. A page collected again **overwrites** its own file, so the Volume never holds duplicates.

### Use the Volume in a Genie Agent

1. Open your Genie Agent → **Sources** → **Add** → choose the **`web_content`** volume. Don't choose the staging volume.
2. Write a clear **Description**, for example: *"Help-center pages from example.com, one Markdown file per page. Each file starts with its source URL."*
3. Turn on **content search** (recommended). It supports up to 10,000 files per Volume.
4. Ask questions in **Agent mode**.

Users of the Genie Agent need `READ VOLUME` on the volume. See [Analyze files in volumes with a Genie Agent](https://docs.databricks.com/aws/en/genie-agents/volumes).

## Step 2: build the Vector Search index (optional)

1. Open `notebooks/02_index_volume_to_vector_search` and connect to **Serverless** (environment 6).
2. Check that **Catalog / Schema / Content volume** match step 1.
3. Click **Run all**. The first run creates the Vector Search endpoint and index, which takes about 10–20 minutes. Later runs only process new or changed pages.
4. The last cell runs the **Test question** against the index and shows the top matches.

What gets created in your schema:

| Object | Description |
|---|---|
| Pipeline `web_content_indexing` | Serverless pipeline that reads the `.md` files incrementally (Auto Loader) and chunks them. |
| Table `documents` | One row per page file read. |
| Table `document_chunks` | One row per chunk: `chunk_id`, `chunk_text`, `title`, `section`, `source_url`, … Change Data Feed is on. |
| Index `web_content_chunks_index` | Delta Sync index on `document_chunks.chunk_text` (embeddings: `databricks-gte-large-en`). |

**How chunking works.** Each page is split at its headings, and long sections are split further at paragraphs, then sentences. Chunks are about 512 tokens with 64 tokens of overlap. Every chunk starts with the page title and section heading, so it still makes sense when an agent retrieves it on its own.

**Full refresh.** Set **Full refresh = true** after you delete pages from the Volume or change the chunk size. It rebuilds the chunk tables from scratch. Otherwise, chunks from deleted pages, or leftover chunks from pages that got shorter, stay in the index.

## Optional: run on a schedule with a bundle

If you use Databricks Asset Bundles (DABs), `databricks.yml` defines a job that runs step 1, then step 2. It's weekly and **paused** by default.

```bash
databricks bundle deploy -t dev          # add -p <profile> to pick a workspace
databricks bundle run web_content_job -t dev
```

Change seeds, limits and names in the `variables:` section of `databricks.yml`. You don't need the bundle to use the notebooks.

## Troubleshooting

| Symptom | What to do |
|---|---|
| Many pages **failed** with `HTTP 403` / `429` | The website is blocking or rate-limiting. Increase **Delay per website**, lower **Parallel downloads**, or ask the site owner to allow the crawler. |
| Pages **too_short** | The page has little text, or it builds its content with JavaScript. This crawler reads static HTML only. |
| Pages **skipped: blocked by robots.txt** | The site asks crawlers not to visit those pages. Set **Respect robots.txt = false** only if you have permission. |
| Step 2: *No .md pages found* | Run step 1 first, or check that the catalog, schema and volume widgets match. |
| Step 2: pipeline failed | Open the pipeline link shown in the log to see the error. |
| Index stays "syncing" on the first run | Endpoint and index creation takes a while the first time. The notebook waits for up to an hour. |

## For developers

```bash
uv sync                      # installs the package plus test tools
uv run pytest                # unit tests for src/
uv run ruff check . && uv run ruff format --check .
```

The notebooks are thin drivers. They read widgets, then call functions in `src/web_content_kit`. The step 2 pipeline installs the package from the project folder, so `pipelines/chunk_documents.py` and the notebooks use the same chunking code.
