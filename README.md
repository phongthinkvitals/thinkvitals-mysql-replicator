# ThinkVitals MySQL Replicator

Monorepo for the MySQL binlog replicator backend and a React monitoring UI.

## Structure

- `backend/`: Spring Boot MySQL binlog replicator.
- `frontend/`: React dashboard for health, replication status, pause/resume, and table verification.

## Run Everything

```bash
docker compose up --build
```

Open the UI at http://localhost:5173.

Backend API:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/replication/status
```

If you need fresh MySQL volumes:

```bash
docker compose down -v
docker compose up --build
```

## Local Development

Backend:

```bash
cd backend
mvn spring-boot:run
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

The frontend dev server proxies `/health` and `/replication/*` to `http://localhost:8080`.

## Backend Docs

See [backend/README.md](backend/README.md) and [backend/docs/replication-flow.md](backend/docs/replication-flow.md).
