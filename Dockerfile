# GenesisGo container image. One build produces the API, worker, and sample
# consumer binaries; Compose selects the target stage per service.
FROM golang:1.27-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/api ./cmd/api \
    && CGO_ENABLED=0 go build -o /bin/worker ./cmd/worker \
    && CGO_ENABLED=0 go build -o /bin/sample-consumer ./cmd/sample-consumer

FROM alpine:3.21 AS base
WORKDIR /app
COPY --from=builder /bin/ /bin/

FROM base AS api
EXPOSE 8080 9090
ENTRYPOINT ["/bin/api"]

FROM base AS worker
ENTRYPOINT ["/bin/worker"]

FROM base AS sample-consumer
ENTRYPOINT ["/bin/sample-consumer"]