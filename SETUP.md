# NL2Pipeline Setup Guide

## Prerequisites
- Docker Desktop installed and running
- Python installed (for model download)
- 10GB free disk space (for the model)

---

## Step 1 — Download the Model (once only)

```bash
pip install huggingface-hub

huggingface-cli download microsoft/Phi-4-mini-instruct --local-dir ./models/phi4-mini
```

---

## Step 2 — Create your `.env` file

```bash
cp .env.example .env
```

Open `.env` and confirm these two lines:

```
MOCK_LLM=false
BASE_MODEL_ID=/model
```

---

## Step 3 — Start

**Mac:**
```bash
docker-compose up -d
```

**Windows with NVIDIA GPU:**
```powershell
docker-compose -f docker-compose.yaml -f docker-compose.gpu.yaml up -d
```

---

## Step 4 — Wait for the model to load

The backend takes 2–5 minutes on first start to load the model.

Watch the logs:
```bash
docker logs nl2pipeline-backend -f
```

When you see this line it is ready:
```
Model ready
```

---

## Step 5 — Check health

```bash
curl http://localhost:8000/health
```

Expected response:
```json
{"status": "ok"}
```

---

## Step 6 — Send a prompt

```bash
curl -N -X POST http://localhost:8000/generate \
  -H "Content-Type: application/json" \
  -d '{"prompt": "Read GDELT events from Kafka and write each event to Cassandra."}'
```

---

## Stop everything

```bash
docker-compose down
```

---

## Quick reference

| What                        | Command                              |
|-----------------------------|--------------------------------------|
| Start (Mac)                 | `docker-compose up -d`               |
| Start (Windows GPU)         | `docker-compose -f docker-compose.yaml -f docker-compose.gpu.yaml up -d` |
| Watch backend logs          | `docker logs nl2pipeline-backend -f` |
| Health check                | `curl http://localhost:8000/health`  |
| Stop everything             | `docker-compose down`                |

---

## Notes

- The model (`./models/phi4-mini/`) lives on your machine, not inside Docker
- You only download it once — it persists across restarts
- On Mac, inference runs on CPU and takes longer (no GPU support in Docker on Mac)
- To test without the model, set `MOCK_LLM=true` in `.env` — starts instantly
