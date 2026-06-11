# syntax=docker/dockerfile:1.7
FROM golang:1.26.2-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" \
    -o /out/parquet-exporter ./cmd/parquet-exporter

FROM gcr.io/distroless/base-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/paulschick/kalshiflow-pipeline"
LABEL org.opencontainers.image.description="Kalshi parquet-exporter — Plan 2.8 BQ→GCS daily export"
COPY --from=build /out/parquet-exporter /parquet-exporter
USER nonroot:nonroot
ENTRYPOINT ["/parquet-exporter"]
