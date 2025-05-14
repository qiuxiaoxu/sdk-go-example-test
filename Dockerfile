FROM golang:1.24.1-alpine AS builder
ENV GOPROXY=https://goproxy.cn,direct
WORKDIR /workspace
COPY ./ /workspace
RUN go mod download
RUN go mod tidy
RUN go build -ldflags "-s -w" -o goapp
# ---- Minimal Runtime Stage ----
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /workspace/goapp .
EXPOSE 8848
ENTRYPOINT ["./goapp"]