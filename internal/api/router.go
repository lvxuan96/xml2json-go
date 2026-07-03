package api

import (
	"embed"
	"io/fs"
	"net/http"

	"xml2json-go/internal/api/handler"
	"xml2json-go/internal/api/middleware"
	"xml2json-go/internal/pipeline"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// NewRouter 创建 Gin 路由
// webFS: 嵌入的前端静态文件（可为 nil，仅 API 模式）
func NewRouter(mgr *pipeline.Manager, logger *zap.Logger, webFS *embed.FS) *gin.Engine {
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
		pipelines.GET("", h.ListPipelines)
		pipelines.POST("", h.CreatePipeline)
		pipelines.GET("/:id", h.GetPipeline)
		pipelines.PUT("/:id", h.UpdatePipeline)
		pipelines.DELETE("/:id", h.DeletePipeline)
		pipelines.POST("/:id/start", h.StartPipeline)
		pipelines.POST("/:id/stop", h.StopPipeline)
		pipelines.GET("/:id/metrics", h.GetPipelineMetrics)
	}

	// 转换预览
	r.POST("/api/v1/preview", h.Preview)

	// 前端静态文件（从内嵌文件系统提供）
	if webFS != nil {
		serveWeb(r, webFS, logger)
	}

	return r
}

func serveWeb(r *gin.Engine, webFS *embed.FS, logger *zap.Logger) {
	sub, err := fs.Sub(webFS, "web")
	if err != nil {
		logger.Warn("failed to load web filesystem", zap.Error(err))
		return
	}

	// 读取 index.html 内容（常驻内存）
	indexData, indexErr := fs.ReadFile(sub, "index.html")

	// NoRoute: 所有非 API 路径回退到 index.html
	r.NoRoute(func(c *gin.Context) {
		path := c.Request.URL.Path

		// API 路径返回 404 JSON
		if len(path) >= 5 && path[:5] == "/api/" {
			c.JSON(http.StatusNotFound, handler.Response{Code: 404, Message: "not found"})
			return
		}

		// index.html 本身
		if path == "/" || path == "/index.html" {
			if indexErr != nil {
				c.String(http.StatusNotFound, "index page not available")
				return
			}
			c.Data(http.StatusOK, "text/html; charset=utf-8", indexData)
			return
		}

		// 尝试匹配静态资源文件
		filePath := path[1:] // 去掉开头的 /
		if data, err := fs.ReadFile(sub, filePath); err == nil {
			contentType := getContentType(filePath)
			c.Data(http.StatusOK, contentType, data)
			return
		}

		// 所有其他路径回退到 index.html（SPA 模式）
		if indexErr != nil {
			c.String(http.StatusNotFound, "page not found")
			return
		}
		c.Data(http.StatusOK, "text/html; charset=utf-8", indexData)
	})
}

func getContentType(path string) string {
	switch {
	case len(path) > 3 && path[len(path)-3:] == ".js":
		return "application/javascript; charset=utf-8"
	case len(path) > 4 && path[len(path)-4:] == ".css":
		return "text/css; charset=utf-8"
	case len(path) > 5 && path[len(path)-4:] == ".svg":
		return "image/svg+xml"
	case len(path) > 4 && path[len(path)-4:] == ".png":
		return "image/png"
	case len(path) > 4 && path[len(path)-4:] == ".ico":
		return "image/x-icon"
	default:
		return "text/plain; charset=utf-8"
	}
}
