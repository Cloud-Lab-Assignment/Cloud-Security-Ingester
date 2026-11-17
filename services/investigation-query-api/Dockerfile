# Build from the workspace root: docker build -f services/investigation-query-api/Dockerfile -t investigation-query-api:dev .
FROM node:22-alpine@sha256:1314fb30ec37592d5845e5a160d522c0e680972aa534549940b8d73bde88481a

WORKDIR /app
ENV NODE_ENV=production

COPY --chmod=a+rX package.json package-lock.json ./
COPY --chmod=a+rX packages/common/package.json packages/common/package.json
COPY --chmod=a+rX packages/tpc/package.json packages/tpc/package.json
COPY --chmod=a+rX services/event-store/package.json services/event-store/package.json
COPY --chmod=a+rX services/incident-management/package.json services/incident-management/package.json
COPY --chmod=a+rX services/correlation-engine/package.json services/correlation-engine/package.json
COPY --chmod=a+rX services/investigation-query-api/package.json services/investigation-query-api/package.json
RUN npm ci --omit=dev && npm cache clean --force

COPY --chmod=a+rX packages/ packages/
COPY --chmod=a+rX services/investigation-query-api/src services/investigation-query-api/src

USER node
EXPOSE 8084
CMD ["node", "services/investigation-query-api/src/server.js"]
