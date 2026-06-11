# syntax=docker/dockerfile:1.7
FROM golang:1.26.2-bookworm AS build
WORKDIR /src

# Cache deps before copying full source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" \
    -o /out/ws-worker ./cmd/ws-worker

FROM gcr.io/distroless/base-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/paulschick/kalshiflow-pipeline"
LABEL org.opencontainers.image.description="Kalshi WS worker — Plan 2 slice 2.1a"
COPY --from=build /out/ws-worker /ws-worker
USER nonroot:nonroot
ENTRYPOINT ["/ws-worker"]
