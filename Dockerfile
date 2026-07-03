# ============ 构建阶段 ============
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/xml2json ./cmd/server

# ============ 运行阶段 ============
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata
ENV TZ=Asia/Shanghai

COPY --from=builder /app/xml2json /app/xml2json
COPY configs/ /app/configs/

WORKDIR /app
EXPOSE 8080

ENTRYPOINT ["/app/xml2json", "--config", "/app/configs/config.yaml"]
