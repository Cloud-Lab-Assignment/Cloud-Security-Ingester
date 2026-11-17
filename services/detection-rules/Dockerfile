# Build context: this directory.  docker build -t detection-rules:dev services/detection-rules
FROM node:22-alpine@sha256:1314fb30ec37592d5845e5a160d522c0e680972aa534549940b8d73bde88481a
WORKDIR /app
ENV NODE_ENV=production
COPY --chmod=a+rX package.json package-lock.json ./
RUN npm ci --omit=dev && npm cache clean --force
COPY --chmod=a+rX src ./src
USER node
CMD ["node", "src/server.js"]
