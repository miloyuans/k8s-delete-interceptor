FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/k8s-delete-interceptor-v2 .

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /out/k8s-delete-interceptor-v2 /k8s-delete-interceptor-v2
USER 65532:65532
ENTRYPOINT ["/k8s-delete-interceptor-v2"]
