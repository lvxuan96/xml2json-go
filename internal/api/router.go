package api

import (
	"xml2json-go/internal/api/handler"
	"xml2json-go/internal/api/middleware"
	"xml2json-go/internal/pipeline"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// NewRouter 创建 Gin 路由
func NewRouter(mgr *pipeline.Manager, logger *zap.Logger) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(middleware.Cors())
	r.Use(middleware.Logger(logger))
	r.Use(gin.Recovery())

	// 健康检查
	r.GET("/api/v1/health", handler.Health)

	// 管道 API
	h := handler.NewPipelineHandler(mgr, logger)
	pipelines := r.Group("/api/v1/pipelines")
	{
		pipelines.GET("", h.ListPipelines)        // 列出所有管道
		pipelines.POST("", h.CreatePipeline)      // 创建新管道
		pipelines.GET("/:id", h.GetPipeline)      // 获取单个管道
		pipelines.PUT("/:id", h.UpdatePipeline)   // 更新管道配置
		pipelines.DELETE("/:id", h.DeletePipeline) // 删除管道
		pipelines.POST("/:id/start", h.StartPipeline)   // 启动管道
		pipelines.POST("/:id/stop", h.StopPipeline)     // 停止管道
		pipelines.GET("/:id/metrics", h.GetPipelineMetrics) // 管道指标
	}

	// 转换预览（不绑定管道，独立使用）
	r.POST("/api/v1/preview", h.Preview)

	return r
}
