# Build from the workspace root: docker build -f services/incident-management/Dockerfile -t incident-management:dev .
FROM node:22-alpine

WORKDIR /app
ENV NODE_ENV=production

# AWS RDS CA bundle, so DATABASE_SSL=true verifies the server certificate.
RUN mkdir -p -m 755 /etc/ssl/rds
ADD --chmod=644 https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem /etc/ssl/rds/global-bundle.pem

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
COPY --chmod=a+rX services/incident-management/src services/incident-management/src
COPY --chmod=a+rX services/incident-management/migrations services/incident-management/migrations

USER node
# 8082 REST (+ /health /ready /metrics), 50051 gRPC
EXPOSE 8082 50051
CMD ["node", "services/incident-management/src/server.js"]
