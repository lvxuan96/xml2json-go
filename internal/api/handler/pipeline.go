package handler

import (
	"net/http"

	"xml2json-go/internal/config"
	"xml2json-go/internal/converter"
	"xml2json-go/internal/pipeline"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// PipelineHandler 管道 API 处理器
type PipelineHandler struct {
	pipeline *pipeline.Pipeline
	logger   *zap.Logger
}

// NewPipelineHandler 创建管道处理器
func NewPipelineHandler(p *pipeline.Pipeline, logger *zap.Logger) *PipelineHandler {
	return &PipelineHandler{
		pipeline: p,
		logger:   logger,
	}
}

// Response 统一响应
type Response struct {
	Code      int         `json:"code"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data,omitempty"`
	Error     string      `json:"error,omitempty"`
}

// GetConfig 获取管道配置和状态
func (h *PipelineHandler) GetConfig(c *gin.Context) {
	cfg := h.pipeline.GetConfig()
	metrics := h.pipeline.GetMetrics()

	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "success",
		Data: gin.H{
			"config":  cfg,
			"metrics": metrics,
			"state":   metrics.State,
		},
	})
}

// UpdateConfig 更新管道配置
func (h *PipelineHandler) UpdateConfig(c *gin.Context) {
	var cfg config.PipelineCfg
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, Response{
			Code:    1001,
			Message: "invalid request body",
			Error:   err.Error(),
		})
		return
	}

	if err := h.pipeline.Reload(&cfg); err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    2001,
			Message: "failed to reload pipeline",
			Error:   err.Error(),
		})
		return
	}

	h.logger.Info("pipeline config updated via API")

	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "pipeline config updated",
		Data: gin.H{
			"config": h.pipeline.GetConfig(),
			"state":  h.pipeline.State().String(),
		},
	})
}

// Start 启动管道
func (h *PipelineHandler) Start(c *gin.Context) {
	if err := h.pipeline.Start(); err != nil {
		c.JSON(http.StatusConflict, Response{
			Code:    1003,
			Message: "failed to start pipeline",
			Error:   err.Error(),
		})
		return
	}

	h.logger.Info("pipeline started via API")

	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "pipeline started",
		Data: gin.H{
			"state": h.pipeline.State().String(),
		},
	})
}

// Stop 停止管道
func (h *PipelineHandler) Stop(c *gin.Context) {
	if err := h.pipeline.Stop(); err != nil {
		c.JSON(http.StatusConflict, Response{
			Code:    1003,
			Message: "failed to stop pipeline",
			Error:   err.Error(),
		})
		return
	}

	h.logger.Info("pipeline stopped via API")

	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "pipeline stopped",
		Data: gin.H{
			"state": h.pipeline.State().String(),
		},
	})
}

// GetMetrics 获取管道指标
func (h *PipelineHandler) GetMetrics(c *gin.Context) {
	metrics := h.pipeline.GetMetrics()
	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "success",
		Data:    metrics,
	})
}

// PreviewRequest 预览请求
type PreviewRequest struct {
	XML     string                 `json:"xml"`
	Options *config.TransformConfig `json:"options,omitempty"`
}

// Preview 预览 XML → JSON 转换结果
func (h *PipelineHandler) Preview(c *gin.Context) {
	var req PreviewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, Response{
			Code:    1001,
			Message: "invalid request body",
			Error:   err.Error(),
		})
		return
	}

	if req.XML == "" {
		c.JSON(http.StatusBadRequest, Response{
			Code:    1001,
			Message: "xml field is required",
		})
		return
	}

	// 创建临时转换器（如果有自定义选项）
	var conv *converter.XML2JSON
	if req.Options != nil {
		conv = converter.New(req.Options, h.logger)
	} else {
		cfg := h.pipeline.GetConfig()
		conv = converter.New(&cfg.Transform, h.logger)
	}

	result, err := conv.Preview([]byte(req.XML))
	if err != nil {
		c.JSON(http.StatusBadRequest, Response{
			Code:    1005,
			Message: "xml conversion failed",
			Error:   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "success",
		Data:    result,
	})
}
