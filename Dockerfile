FROM golang:1.21 AS builder

WORKDIR /src

COPY go.mod ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/k8s-delete-interceptor ./main.go

FROM alpine:3.20

RUN apk add --no-cache ca-certificates

USER 65532:65532

COPY --from=builder /out/k8s-delete-interceptor /usr/local/bin/k8s-delete-interceptor

EXPOSE 8443

ENTRYPOINT ["/usr/local/bin/k8s-delete-interceptor"]
