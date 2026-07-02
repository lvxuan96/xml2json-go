package handler

import (
	"net/http"
	"runtime"
	"time"

	"github.com/gin-gonic/gin"
)

var startTime = time.Now()

// Health 健康检查
func Health(c *gin.Context) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	c.JSON(http.StatusOK, Response{
		Code:    0,
		Message: "ok",
		Data: gin.H{
			"status":    "healthy",
			"uptime":    time.Since(startTime).String(),
			"goVersion": runtime.Version(),
			"goroutines": runtime.NumGoroutine(),
			"memoryMB":  mem.Alloc / 1024 / 1024,
		},
	})
}
