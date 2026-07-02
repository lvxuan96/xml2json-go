package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"xml2json-go/internal/api"
	"xml2json-go/internal/config"
	"xml2json-go/internal/pipeline"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// 版本信息，构建时注入
var (
	Version   = "0.1.0"
	BuildTime = "unknown"
)

func main() {
	// 解析命令行参数
	configPath := "configs/config.yaml"
	if len(os.Args) > 2 && os.Args[1] == "--config" {
		configPath = os.Args[2]
	}

	// 初始化日志
	logger := initLogger()
	defer logger.Sync()

	logger.Info("xml2json-go starting",
		zap.String("version", Version),
		zap.String("build_time", BuildTime),
	)

	// 加载配置
	cfg, err := config.Load(configPath)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	logger.Info("config loaded",
		zap.String("server", fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)),
		zap.Bool("pipeline_enabled", cfg.Pipeline.Enabled),
	)

	// 创建管道
	p, err := pipeline.New(&cfg.Pipeline, logger)
	if err != nil {
		logger.Fatal("failed to create pipeline", zap.Error(err))
	}

	// 如果配置启用，自动启动管道
	if cfg.Pipeline.Enabled {
		if err := p.Start(); err != nil {
			logger.Error("failed to auto-start pipeline", zap.Error(err))
		}
	}

	// 创建 Gin 路由
	gin.SetMode(gin.ReleaseMode)
	router := api.NewRouter(p, logger)

	// 创建 HTTP 服务器
	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// 启动 HTTP 服务（后台）
	go func() {
		logger.Info("HTTP server listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server error", zap.Error(err))
		}
	}()

	logger.Info("xml2json-go started successfully",
		zap.String("health_url", fmt.Sprintf("http://%s/api/v1/health", srv.Addr)),
		zap.String("api_url", fmt.Sprintf("http://%s/api/v1/pipeline", srv.Addr)),
	)

	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down...")

	// 停止管道
	if p.State() == pipeline.StateRunning {
		if err := p.Stop(); err != nil {
			logger.Error("failed to stop pipeline", zap.Error(err))
		}
	}

	// 关闭 HTTP 服务
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", zap.Error(err))
	}

	logger.Info("xml2json-go stopped")
}

func initLogger() *zap.Logger {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "timestamp"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder

	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig = encoderCfg
	cfg.Encoding = "console"
	cfg.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)

	// 从环境变量读取日志级别
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		var l zapcore.Level
		if err := l.UnmarshalText([]byte(level)); err == nil {
			cfg.Level = zap.NewAtomicLevelAt(l)
		}
	}

	logger, _ := cfg.Build()
	return logger
}
