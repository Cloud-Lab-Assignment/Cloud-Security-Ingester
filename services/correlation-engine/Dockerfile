# Build from the workspace root: docker build -f services/correlation-engine/Dockerfile -t correlation-engine:dev .
FROM node:22-alpine

WORKDIR /app
ENV NODE_ENV=production

# --chmod: files must be readable by USER node whatever the host umask was.
COPY --chmod=a+rX package.json package-lock.json ./
COPY --chmod=a+rX packages/common/package.json packages/common/package.json
COPY --chmod=a+rX packages/tpc/package.json packages/tpc/package.json
COPY --chmod=a+rX services/event-store/package.json services/event-store/package.json
COPY --chmod=a+rX services/incident-management/package.json services/incident-management/package.json
COPY --chmod=a+rX services/correlation-engine/package.json services/correlation-engine/package.json
RUN npm ci --omit=dev && npm cache clean --force

COPY --chmod=a+rX packages/ packages/
COPY --chmod=a+rX proto/ proto/
COPY --chmod=a+rX services/correlation-engine/src services/correlation-engine/src

USER node
EXPOSE 8083
CMD ["node", "services/correlation-engine/src/server.js"]
