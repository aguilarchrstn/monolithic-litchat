# Litchat

Anonymous stranger chat (Omegle-style), single-file Node.js/Socket.io monolith.

## Run with Docker Compose (recommended)

```bash
docker compose up -d --build
```

App will be live at **http://localhost:7080** (set via the `.env` file in this folder).

To use a different host port, either edit `.env`, or override it inline:

```bash
LITCHAT_PORT=8080 docker compose up -d --build
```

Stop it:

```bash
docker compose down
```

## Run with plain Docker

```bash
docker build -t litchat .
docker run -d -p 3000:3000 --name litchat litchat
```

## Run without Docker

```bash
npm install
npm start
```

## Files

- `app.js` — the entire server + embedded frontend (single file, no external DB)
- `package.json` / `package-lock.json` — dependencies (express, socket.io)
- `Dockerfile` — production image, runs as non-root, includes a healthcheck
- `docker-compose.yml` — one-command deploy, configurable via `LITCHAT_PORT`
