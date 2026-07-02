package api

import (
	"xml2json-go/internal/api/handler"
	"xml2json-go/internal/api/middleware"
	"xml2json-go/internal/pipeline"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// NewRouter 创建 Gin 路由
func NewRouter(p *pipeline.Pipeline, logger *zap.Logger) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(middleware.Cors())
	r.Use(middleware.Logger(logger))
	r.Use(gin.Recovery())

	// 健康检查
	r.GET("/api/v1/health", handler.Health)

	// 管道 API
	pipelineHandler := handler.NewPipelineHandler(p, logger)
	pipelineGroup := r.Group("/api/v1/pipeline")
	{
		pipelineGroup.GET("", pipelineHandler.GetConfig)
		pipelineGroup.PUT("", pipelineHandler.UpdateConfig)
		pipelineGroup.POST("/start", pipelineHandler.Start)
		pipelineGroup.POST("/stop", pipelineHandler.Stop)
		pipelineGroup.GET("/metrics", pipelineHandler.GetMetrics)
		pipelineGroup.POST("/preview", pipelineHandler.Preview)
	}

	return r
}
