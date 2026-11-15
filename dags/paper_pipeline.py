"""Airflow pipeline for turning research PDFs into a searchable study library."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from html import escape
from pathlib import Path
from typing import Any

import pendulum
from airflow.decorators import dag, task


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def project_path(environment_name: str, default: Path) -> Path:
    """Resolve configured relative paths from the project root."""
    configured = Path(os.getenv(environment_name, str(default))).expanduser()
    return configured if configured.is_absolute() else PROJECT_ROOT / configured


PAPERS_DIR = project_path(
    "PAPERS_DIR", PROJECT_ROOT / "ResearchLibrary" / "papers"
)
OUTPUT_DIR = project_path(
    "OUTPUT_DIR", PROJECT_ROOT / "ResearchLibrary" / "output"
)
DATABASE_PATH = project_path("DATABASE_PATH", PROJECT_ROOT / "data" / "papers.db")
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
LOGGER = logging.getLogger(__name__)

# Fixed category list. Anything that is not clearly computer science goes under "Other".
CATEGORIES = [
    "AI",
    "Data Engineering",
    "Software Engineering",
    "Cybersecurity",
    "Systems & Networking",
    "Other",
]


@dag(
    dag_id="paper_pipeline",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5)},
    tags=["research", "study-library"],
)
def paper_pipeline():
    """Discover, extract, enrich, store, publish, and notify for research papers."""

    @task(execution_timeout=pendulum.duration(minutes=2))
    def discover_pdfs() -> list[str]:
        """Find PDFs that are new or changed since their last successful run."""
        directory = PAPERS_DIR.resolve()
        LOGGER.info("Scanning PDF directory: %s", directory)
        directory.mkdir(parents=True, exist_ok=True)
        DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DATABASE_PATH) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_files (
                    filename TEXT PRIMARY KEY,
                    file_size INTEGER NOT NULL,
                    modified_ns INTEGER NOT NULL
                )
                """
            )
            processed = {
                filename: (file_size, modified_ns)
                for filename, file_size, modified_ns in connection.execute(
                    "SELECT filename, file_size, modified_ns FROM processed_files"
                )
            }

        pdf_paths = []
        for path in sorted(directory.iterdir()):
            if not path.is_file() or path.suffix.lower() != ".pdf":
                continue
            stat = path.stat()
            signature = (stat.st_size, stat.st_mtime_ns)
            if processed.get(path.name) != signature:
                pdf_paths.append(str(path))

        LOGGER.info(
            "Discovered %d new or changed PDF files (%d unchanged files skipped)",
            len(pdf_paths),
            len(processed),
        )
        return pdf_paths

    @task.short_circuit
    def has_new_pdfs(pdf_paths: list[str]) -> bool:
        """Stop the run when there are no new or changed PDFs to process."""
        if not pdf_paths:
            LOGGER.info("No new or changed PDFs found; stopping this DAG run.")
            return False
        return True

    @task
    def extract_markdown(pdf_paths: list[str]) -> list[dict[str, Any]]:
        """Convert each discovered PDF into Markdown."""
        import pymupdf4llm

        extracted: list[dict[str, Any]] = []
        for pdf_path in pdf_paths:
            path = Path(pdf_path)
            stat = path.stat()
            extracted.append(
                {
                    "filename": path.name,
                    "markdown": pymupdf4llm.to_markdown(str(path)),
                    "file_size": stat.st_size,
                    "modified_ns": stat.st_mtime_ns,
                }
            )
        return extracted

    @task
    def transform_with_llm(extracted_papers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Ask LM Studio for strictly structured metadata and parse its JSON response."""
        from openai import OpenAI

        client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY", "lm-studio"),
            base_url=os.getenv(
                "OPENAI_BASE_URL", "http://host.docker.internal:1235/v1"
            ),
        )
        model = os.getenv("OPENAI_MODEL", "local-model")
        category_list = ", ".join(f'"{name}"' for name in CATEGORIES)
        prompt_template = """Analyze the research paper below. This library focuses on computer science papers.
Return only a valid JSON object with exactly these keys:
title (string), summary (string), keywords (list of strings), category (string).
The category must be exactly one of: {categories}
Use "Other" if the paper is not about computer science or does not fit the other categories.
Do not include Markdown fences or any other keys.

Research paper Markdown:
{markdown}
"""
        response_format = {
            "type": "json_schema",
            "json_schema": {
                "name": "paper_metadata",
                "schema": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "summary": {"type": "string"},
                        "keywords": {"type": "array", "items": {"type": "string"}},
                        "category": {"type": "string", "enum": CATEGORIES},
                    },
                    "required": ["title", "summary", "keywords", "category"],
                    "additionalProperties": False,
                },
            },
        }

        transformed: list[dict[str, Any]] = []
        for paper in extracted_papers:
            prompt = prompt_template.format(
                categories=category_list, markdown=paper["markdown"]
            )
            parsed: dict[str, Any] | None = None
            last_error: json.JSONDecodeError | None = None
            for _attempt in range(2):
                response = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    response_format=response_format,
                )
                content = response.choices[0].message.content or ""
                try:
                    candidate = json.loads(content)
                    if (
                        not isinstance(candidate, dict)
                        or set(candidate) != {"title", "summary", "keywords", "category"}
                        or not isinstance(candidate["title"], str)
                        or not isinstance(candidate["summary"], str)
                        or not isinstance(candidate["keywords"], list)
                        or not all(isinstance(keyword, str) for keyword in candidate["keywords"])
                        or candidate["category"] not in CATEGORIES
                    ):
                        raise ValueError("LLM response did not match the required JSON schema")
                    parsed = candidate
                    break
                except (json.JSONDecodeError, ValueError) as error:
                    if isinstance(error, json.JSONDecodeError):
                        last_error = error
                    if _attempt == 1:
                        raise ValueError(
                            f"Could not parse metadata for {paper['filename']} after two attempts"
                        ) from (last_error or error)
            assert parsed is not None
            transformed.append(
                {
                    "filename": paper["filename"],
                    "file_size": paper["file_size"],
                    "modified_ns": paper["modified_ns"],
                    **parsed,
                }
            )
        return transformed

    @task
    def load_to_sqlite(metadata: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Create the database and upsert metadata, returning only first-seen papers."""
        DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DATABASE_PATH) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS papers (
                    filename TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    keywords TEXT NOT NULL,
                    category TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_files (
                    filename TEXT PRIMARY KEY,
                    file_size INTEGER NOT NULL,
                    modified_ns INTEGER NOT NULL
                )
                """
            )
            new_papers: list[dict[str, Any]] = []
            for paper in metadata:
                exists = connection.execute(
                    "SELECT 1 FROM papers WHERE filename = ?", (paper["filename"],)
                ).fetchone()
                connection.execute(
                    """
                    INSERT INTO papers (filename, title, summary, keywords, category)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(filename) DO UPDATE SET
                        title = excluded.title,
                        summary = excluded.summary,
                        keywords = excluded.keywords,
                        category = excluded.category
                    """,
                    (
                        paper["filename"],
                        paper["title"],
                        paper["summary"],
                        json.dumps(paper["keywords"], ensure_ascii=False),
                        paper["category"],
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO processed_files (filename, file_size, modified_ns)
                    VALUES (?, ?, ?)
                    ON CONFLICT(filename) DO UPDATE SET
                        file_size = excluded.file_size,
                        modified_ns = excluded.modified_ns
                    """,
                    (paper["filename"], paper["file_size"], paper["modified_ns"]),
                )
                if exists is None:
                    new_papers.append(paper)
            connection.commit()
        return new_papers

    @task
    def generate_html() -> str:
        """Render every stored paper as a static HTML page with search and category filter."""
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(DATABASE_PATH) as connection:
            rows = connection.execute(
                "SELECT filename, title, summary, keywords, category "
                "FROM papers ORDER BY title COLLATE NOCASE"
            ).fetchall()

        cards = []
        for filename, title, summary, keywords_json, category in rows:
            keywords = ", ".join(json.loads(keywords_json))
            search_text = escape(f"{title} {category}", quote=True)
            cards.append(
                f"""<article class="paper" data-search="{search_text}" data-category="{escape(category, quote=True)}">
<h2>{escape(title)}</h2><p class="category">{escape(category)}</p>
<p>{escape(summary)}</p><p><strong>Keywords:</strong> {escape(keywords)}</p>
<small>{escape(filename)}</small></article>"""
            )
        options = "\n".join(
            f'<option value="{escape(name, quote=True)}">{escape(name)}</option>'
            for name in CATEGORIES
        )
        html = (
            """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport"
content="width=device-width, initial-scale=1"><title>Research Study Library</title>
<style>body{font-family:system-ui;max-width:900px;margin:2rem auto;padding:0 1rem}
.controls{display:flex;gap:.75rem;margin:1rem 0}
input,select{padding:.7rem;font-size:1rem}input{flex:1}
.paper{border:1px solid #ddd;border-radius:8px;padding:1rem;margin:1rem 0}
.category{color:#555;font-weight:600}</style>
</head><body><h1>Research Study Library</h1>
<div class="controls">
<input id="search" type="search" placeholder="Search by title or category">
<select id="category-filter"><option value="">All categories</option>
"""
            + options
            + """
</select></div>
<main id="papers">"""
            + "\n".join(cards)
            + """</main>
<script>
const searchBox = document.getElementById("search");
const categoryFilter = document.getElementById("category-filter");
function applyFilters() {
  const query = searchBox.value.toLowerCase();
  const category = categoryFilter.value;
  document.querySelectorAll(".paper").forEach((paper) => {
    const matchesText = paper.dataset.search.toLowerCase().includes(query);
    const matchesCategory = !category || paper.dataset.category === category;
    paper.hidden = !(matchesText && matchesCategory);
  });
}
searchBox.addEventListener("input", applyFilters);
categoryFilter.addEventListener("change", applyFilters);
</script></body></html>"""
        )
        output_path = OUTPUT_DIR / "index.html"
        output_path.write_text(html, encoding="utf-8")
        return str(output_path)

    @task
    def notify_discord(new_papers: list[dict[str, Any]]) -> None:
        """Notify Discord once for each paper newly inserted into the library."""
        if not new_papers or not DISCORD_WEBHOOK_URL:
            return
        import requests

        for paper in new_papers:
            response = requests.post(
                DISCORD_WEBHOOK_URL,
                json={"content": f"New research paper added: {paper['title']}"},
                timeout=15,
            )
            response.raise_for_status()

    pdfs = discover_pdfs()
    new_pdfs_available = has_new_pdfs(pdfs)
    extracted = extract_markdown(pdfs)
    new_pdfs_available >> extracted
    metadata = transform_with_llm(extracted)
    new_papers = load_to_sqlite(metadata)
    html = generate_html()
    new_papers >> html
    notify_discord(new_papers)


paper_pipeline()