FROM --platform=$BUILDPLATFORM golang:1.24 AS builder

WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -o /workspace/kubevolga ./cmd/operator

FROM --platform=$TARGETPLATFORM gcr.io/distroless/static:nonroot

WORKDIR /
COPY --from=builder /workspace/kubevolga /kubevolga

USER 65532:65532
ENTRYPOINT ["/kubevolga"]
