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
	mgr    *pipeline.Manager
	logger *zap.Logger
}

// NewPipelineHandler 创建管道处理器
func NewPipelineHandler(mgr *pipeline.Manager, logger *zap.Logger) *PipelineHandler {
	return &PipelineHandler{
		mgr:    mgr,
		logger: logger,
	}
}

// Response 统一响应
type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// ListPipelines 列出所有管道
func (h *PipelineHandler) ListPipelines(c *gin.Context) {
	list := h.mgr.List()
	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "success",
		Data: gin.H{
			"pipelines": list,
			"total":     len(list),
			"running":   h.mgr.RunningCount(),
		},
	})
}

// CreatePipeline 创建管道
func (h *PipelineHandler) CreatePipeline(c *gin.Context) {
	var cfg pipeline.PipelineCfg
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, Response{
			Code:    1001,
			Message: "invalid request body",
			Error:   err.Error(),
		})
		return
	}

	p, err := h.mgr.Create(&cfg)
	if err != nil {
		c.JSON(http.StatusBadRequest, Response{
			Code:    1001,
			Message: "failed to create pipeline",
			Error:   err.Error(),
		})
		return
	}

	h.logger.Info("pipeline created via API", zap.String("id", cfg.ID))

	c.JSON(http.StatusCreated, Response{
		Code:    0,
		Message: "pipeline created",
		Data: gin.H{
			"state": p.State().String(),
		},
	})
}

// GetPipeline 获取单个管道详情
func (h *PipelineHandler) GetPipeline(c *gin.Context) {
	id := c.Param("id")
	status, err := h.mgr.Get(id)
	if err != nil {
		c.JSON(http.StatusNotFound, Response{
			Code:    1002,
			Message: "pipeline not found",
			Error:   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "success",
		Data:    status,
	})
}

// UpdatePipeline 更新管道配置
func (h *PipelineHandler) UpdatePipeline(c *gin.Context) {
	id := c.Param("id")

	var cfg pipeline.PipelineCfg
	if err := c.ShouldBindJSON(&cfg); err != nil {
		c.JSON(http.StatusBadRequest, Response{
			Code:    1001,
			Message: "invalid request body",
			Error:   err.Error(),
		})
		return
	}

	if err := h.mgr.Reload(id, &cfg); err != nil {
		c.JSON(http.StatusInternalServerError, Response{
			Code:    2001,
			Message: "failed to update pipeline",
			Error:   err.Error(),
		})
		return
	}

	h.logger.Info("pipeline config updated via API", zap.String("id", id))

	status, _ := h.mgr.Get(id)
	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "pipeline updated",
		Data:    status,
	})
}

// StartPipeline 启动管道
func (h *PipelineHandler) StartPipeline(c *gin.Context) {
	id := c.Param("id")

	if err := h.mgr.Start(id); err != nil {
		c.JSON(http.StatusConflict, Response{
			Code:    1003,
			Message: "failed to start pipeline",
			Error:   err.Error(),
		})
		return
	}

	h.logger.Info("pipeline started via API", zap.String("id", id))

	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "pipeline started",
		Data: gin.H{
			"id":    id,
			"state": "running",
		},
	})
}

// StopPipeline 停止管道
func (h *PipelineHandler) StopPipeline(c *gin.Context) {
	id := c.Param("id")

	if err := h.mgr.Stop(id); err != nil {
		c.JSON(http.StatusConflict, Response{
			Code:    1003,
			Message: "failed to stop pipeline",
			Error:   err.Error(),
		})
		return
	}

	h.logger.Info("pipeline stopped via API", zap.String("id", id))

	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "pipeline stopped",
		Data: gin.H{
			"id":    id,
			"state": "idle",
		},
	})
}

// DeletePipeline 删除管道
func (h *PipelineHandler) DeletePipeline(c *gin.Context) {
	id := c.Param("id")

	if err := h.mgr.Delete(id); err != nil {
		c.JSON(http.StatusNotFound, Response{
			Code:    1002,
			Message: "failed to delete pipeline",
			Error:   err.Error(),
		})
		return
	}

	h.logger.Info("pipeline deleted via API", zap.String("id", id))

	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "pipeline deleted",
	})
}

// GetPipelineMetrics 获取管道指标
func (h *PipelineHandler) GetPipelineMetrics(c *gin.Context) {
	id := c.Param("id")
	status, err := h.mgr.Get(id)
	if err != nil {
		c.JSON(http.StatusNotFound, Response{
			Code:    1002,
			Message: "pipeline not found",
			Error:   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "success",
		Data:    status.Metrics,
	})
}

// PreviewRequest 预览请求
type PreviewRequest struct {
	XML     string `json:"xml"`
	Options *PreviewOptions `json:"options,omitempty"`
}

// PreviewOptions 预览选项
type PreviewOptions struct {
	AttributePrefix string `json:"attributePrefix"`
	TextKey         string `json:"textKey"`
	NamespaceMode   string `json:"namespaceMode"`
	StripLevels     int    `json:"stripLevels"`
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

	// 创建临时转换器
	opts := &config.TransformConfig{
		AttributePrefix: "@",
		TextKey:         "#text",
		CDataKey:        "#cdata",
		NamespaceMode:   "strip",
		TrimElements:    true,
		SkipComments:    true,
		SkipProcInst:    true,
		StripLevels:     0,
	}
	if req.Options != nil {
		if req.Options.AttributePrefix != "" {
			opts.AttributePrefix = req.Options.AttributePrefix
		}
		if req.Options.TextKey != "" {
			opts.TextKey = req.Options.TextKey
		}
		if req.Options.NamespaceMode != "" {
			opts.NamespaceMode = req.Options.NamespaceMode
		}
		opts.StripLevels = req.Options.StripLevels
	}

	result, err := converter.StandalonePreview([]byte(req.XML), opts)
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
