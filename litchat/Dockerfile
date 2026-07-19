# ---- Litchat: single-file Node.js/Socket.io monolith ----
FROM node:20-alpine

# Create app directory
WORKDIR /usr/src/app

# Run as a non-root user for a smaller security footprint
RUN addgroup -S litchat && adduser -S litchat -G litchat

# Install dependencies first (better layer caching — only re-runs on dep changes)
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

# Copy application source
COPY app.js ./

# Drop privileges
USER litchat

ENV PORT=3000
EXPOSE 3000

# Basic container-level healthcheck against the root route
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
  CMD wget -qO- http://127.0.0.1:3000/ >/dev/null 2>&1 || exit 1

CMD ["node", "app.js"]
