# Research Paper Airflow Pipeline

This project runs a lightweight Apache Airflow 2.10.2 pipeline in Docker.
It reads PDFs from `ResearchLibrary/papers/`, sends new or changed PDFs to an
OpenAI-compatible LLM server, stores metadata in SQLite, and generates a
searchable HTML library.

## Setup

Requirements:

- Docker Desktop
- An OpenAI-compatible LLM server

From the project root:

```bash
cp .env.example .env
```

Edit `.env` and set `OPENAI_BASE_URL`, `OPENAI_API_KEY`, and `OPENAI_MODEL`.
Set `DISCORD_WEBHOOK_URL` if Discord notifications are required. Leave it empty
to disable notifications.

Build and start Airflow:

```bash
docker compose up --build -d
```

Open <http://localhost:8080>. Airflow Standalone creates the administrator
account on first startup. Get the generated credentials with:

```bash
docker compose logs airflow
```

Place PDFs in:

```text
ResearchLibrary/papers/
```

The DAG runs every 15 minutes. You can also trigger it manually in Airflow.
Only new or changed PDFs are extracted and sent to the LLM. If nothing
changed, the run stops after discovery.

Generated files:

```text
data/papers.db
ResearchLibrary/output/index.html
```

## LLM server URL

The example uses LM Studio running on the same Mac as Docker Desktop:

```text
http://host.docker.internal:1235/v1
```

`host.docker.internal` means "the computer running Docker." It lets the
container reach an LLM server running directly on the host. The port `1235`
must match the port configured in that user's LLM application.

If the server uses another local port, change `OPENAI_BASE_URL` in `.env`, for
example:

```env
OPENAI_BASE_URL=http://host.docker.internal:8080/v1
```

If the server runs on another machine or a hosted provider, use its reachable
OpenAI-compatible URL instead:

```env
OPENAI_BASE_URL=https://api.example.com/v1
OPENAI_API_KEY=your-api-key
OPENAI_MODEL=your-model-name
```

Do not use `localhost` for a host-based server from inside the container:
inside Docker, `localhost` refers to the Airflow container itself.

## Stop and reset

Stop the container:

```bash
docker compose down
```

Remove generated local Airflow and database state if you want a clean reset:

```bash
rm -rf airflow_home data/*.db
```

Start again:

```bash
docker compose up --build -d
```
