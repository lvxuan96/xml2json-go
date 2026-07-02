.PHONY: build run test clean vet

# 构建
build:
	go build -ldflags="-s -w -X 'main.Version=0.1.0' -X 'main.BuildTime=$$(date -Iseconds)'" -o bin/xml2json ./cmd/server

# 开发运行
run:
	go run ./cmd/server --config configs/config.yaml

# 代码检查
vet:
	go vet ./...

# 测试
test:
	go test ./... -v -count=1

# 测试覆盖率
test-cover:
	go test ./... -cover -coverprofile=coverage.out
	go tool cover -func=coverage.out

# 基准测试
bench:
	go test -bench=. -benchmem ./internal/converter/

# 清理
clean:
	rm -rf bin/

# 依赖整理
tidy:
	go mod tidy

# 全部检查
check: vet test
	@echo "All checks passed!"
