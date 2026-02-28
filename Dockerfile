# Build the pdboperator binary
FROM golang:1.26 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Copy dependency manifests first for layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY cmd/ cmd/
COPY api/ api/
COPY internal/ internal/

# Build a statically linked, stripped binary
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o pdboperator cmd/main.go

# Runtime stage — distroless static image for Go binaries
FROM gcr.io/distroless/static:nonroot

LABEL org.opencontainers.image.source="https://github.com/pdb-operator/pdb-operator" \
      org.opencontainers.image.description="Kubernetes operator for automated PodDisruptionBudget management" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /
COPY --from=builder /workspace/pdboperator .
USER 65532:65532

ENTRYPOINT ["/pdboperator"]
