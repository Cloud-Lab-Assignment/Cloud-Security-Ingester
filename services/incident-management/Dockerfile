# Build from the workspace root: docker build -f services/incident-management/Dockerfile -t incident-management:dev .
# Pinned by digest (P2-7); refresh: see docs/handoff.md "Pinned images".
FROM node:22-alpine@sha256:1314fb30ec37592d5845e5a160d522c0e680972aa534549940b8d73bde88481a

WORKDIR /app
ENV NODE_ENV=production

# AWS RDS CA bundle, so DATABASE_SSL=true verifies the server certificate.
# The build fails if the download does not match the pinned sha256 (P2-7).
RUN mkdir -p -m 755 /etc/ssl/rds
ADD --chmod=644 --checksum=sha256:90a6f1a7978a1b1b096dca6c57511ca48d3b8cf052f8e704baf358f34ea2f21b https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /etc/ssl/rds/global-bundle.pem

# --chmod: files must be readable by USER node whatever the host umask was.
COPY --chmod=a+rX package.json package-lock.json ./
COPY --chmod=a+rX packages/common/package.json packages/common/package.json
COPY --chmod=a+rX packages/tpc/package.json packages/tpc/package.json
COPY --chmod=a+rX services/event-store/package.json services/event-store/package.json
COPY --chmod=a+rX services/incident-management/package.json services/incident-management/package.json
COPY --chmod=a+rX services/correlation-engine/package.json services/correlation-engine/package.json
COPY --chmod=a+rX services/investigation-query-api/package.json services/investigation-query-api/package.json
RUN npm ci --omit=dev && npm cache clean --force

COPY --chmod=a+rX packages/ packages/
COPY --chmod=a+rX proto/ proto/
COPY --chmod=a+rX services/incident-management/src services/incident-management/src
COPY --chmod=a+rX services/incident-management/migrations services/incident-management/migrations

USER node
# 8082 REST (+ /health /ready /metrics), 50051 gRPC
EXPOSE 8082 50051
CMD ["node", "services/incident-management/src/server.js"]
