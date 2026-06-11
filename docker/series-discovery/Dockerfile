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
    -o /out/series-discovery ./cmd/series-discovery

FROM gcr.io/distroless/base-debian12:nonroot
LABEL org.opencontainers.image.source="https://github.com/paulschick/kalshiflow-pipeline"
LABEL org.opencontainers.image.description="Kalshi series-discovery — HOL-24 6h catalog refresh"
COPY --from=build /out/series-discovery /series-discovery
USER nonroot:nonroot
ENTRYPOINT ["/series-discovery"]
