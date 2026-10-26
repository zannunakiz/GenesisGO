# GenesisGo production images. Multi-stage and reproducible via pinned ARGs.
#   docker build --target api             # API service
#   docker build --target worker          # Worker service
#   docker build --target sample-consumer # Sample fault-injection consumer
#   docker build --target web             # Next.js demo control panel
#
# Security: Go services run as non-root user genesisgo (uid 1000), minimal
# alpine runtime, no secrets baked into images, no Docker socket access.
ARG GO_VERSION=1.27-alpine
ARG ALPINE_VERSION=3.21
ARG NODE_VERSION=22-alpine

# --- Build stage: compile Go binaries ---------------------------------------
FROM golang:${GO_VERSION} AS genesisgo-builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/api ./cmd/api \
    && CGO_ENABLED=0 go build -o /bin/worker ./cmd/worker \
    && CGO_ENABLED=0 go build -o /bin/sample-consumer ./cmd/sample-consumer

# --- Runtime base: minimal alpine, non-root user ----------------------------
FROM alpine:${ALPINE_VERSION} AS genesisgo-base
RUN addgroup -g 1000 genesisgo \
    && adduser -D -u 1000 -G genesisgo genesisgo \
    && apk add --no-cache ca-certificates wget
WORKDIR /app
COPY --from=genesisgo-builder /bin/ /bin/

# API service
FROM genesisgo-base AS api
USER genesisgo:genesisgo
EXPOSE 8080 9090
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=2s --start-period=15s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["/bin/api"]

# Worker service
FROM genesisgo-base AS worker
USER genesisgo:genesisgo
EXPOSE 8081 9090
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=2s --start-period=15s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8081/healthz || exit 1
ENTRYPOINT ["/bin/worker"]

# Sample consumer service (no HTTP server; supervised by the container)
FROM genesisgo-base AS sample-consumer
USER genesisgo:genesisgo
STOPSIGNAL SIGTERM
ENTRYPOINT ["/bin/sample-consumer"]

# --- Web app: Next.js demo control panel ------------------------------------
FROM node:${NODE_VERSION} AS web-build
WORKDIR /app
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM node:${NODE_VERSION} AS web
ENV NODE_ENV=production PORT=3000
WORKDIR /app
COPY --from=web-build /app/node_modules ./node_modules
COPY --from=web-build /app/.next ./.next
COPY --from=web-build /app/public ./public
USER node:node
EXPOSE 3000
STOPSIGNAL SIGTERM
HEALTHCHECK --interval=10s --timeout=2s --start-period=20s --retries=3 \
    CMD wget -qO- http://127.0.0.1:3000/ || exit 1
CMD ["node_modules/.bin/next", "start"]