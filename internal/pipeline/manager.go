package pipeline

import (
	"fmt"
	"sync"

	"xml2json-go/internal/config"

	"go.uber.org/zap"
)

// PipelineStatus 管道的完整状态信息
type PipelineStatus struct {
	Config  PipelineCfg `json:"config"`
	Metrics Metrics     `json:"metrics"`
	State   string      `json:"state"`
}

// PipelineCfg 重新导出 config.PipelineCfg（避免 api 层直接依赖 config）
type PipelineCfg = config.PipelineCfg

// Manager 管道管理器，管理多条管道的生命周期
type Manager struct {
	mu         sync.RWMutex
	pipelines  map[string]*Pipeline
	configPath string
	logger     *zap.Logger
}

// NewManager 创建管道管理器
// configPath: 配置文件路径，用于持久化（空字符串表示不持久化）
func NewManager(configPath string, logger *zap.Logger) *Manager {
	return &Manager{
		pipelines:  make(map[string]*Pipeline),
		configPath: configPath,
		logger:     logger,
	}
}

// Create 创建一条新管道（不启动）
func (m *Manager) Create(cfg *PipelineCfg) (*Pipeline, error) {
	m.mu.Lock()

	if cfg.ID == "" {
		m.mu.Unlock()
		return nil, fmt.Errorf("pipeline id is required")
	}
	if _, exists := m.pipelines[cfg.ID]; exists {
		m.mu.Unlock()
		return nil, fmt.Errorf("pipeline '%s' already exists", cfg.ID)
	}

	p, err := New(cfg, m.logger)
	if err != nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("failed to create pipeline '%s': %w", cfg.ID, err)
	}

	m.pipelines[cfg.ID] = p
	m.mu.Unlock() // 必须在 save() 之前释放，save() 内部需要 RLock

	m.logger.Info("pipeline created", zap.String("id", cfg.ID), zap.String("name", cfg.Name))
	m.save()
	return p, nil
}

// Start 启动指定管道
func (m *Manager) Start(id string) error {
	p, err := m.get(id)
	if err != nil {
		return err
	}

	if p.State() == StateRunning {
		return fmt.Errorf("pipeline '%s' is already running", id)
	}

	if err := p.Start(); err != nil {
		return fmt.Errorf("failed to start pipeline '%s': %w", id, err)
	}

	m.logger.Info("pipeline started", zap.String("id", id))
	return nil
}

// Stop 停止指定管道
func (m *Manager) Stop(id string) error {
	p, err := m.get(id)
	if err != nil {
		return err
	}

	if p.State() != StateRunning {
		return fmt.Errorf("pipeline '%s' is not running (state: %s)", id, p.State().String())
	}

	if err := p.Stop(); err != nil {
		return fmt.Errorf("failed to stop pipeline '%s': %w", id, err)
	}

	m.logger.Info("pipeline stopped", zap.String("id", id))
	return nil
}

// Delete 删除管道（先停止再移除）
func (m *Manager) Delete(id string) error {
	p, err := m.get(id)
	if err != nil {
		return err
	}

	// 如果正在运行，先停止
	if p.State() == StateRunning {
		if err := p.Stop(); err != nil {
			m.logger.Warn("failed to stop pipeline during delete", zap.String("id", id), zap.Error(err))
		}
	}

	m.mu.Lock()
	delete(m.pipelines, id)
	m.mu.Unlock()

	m.logger.Info("pipeline deleted", zap.String("id", id))

	m.save()
	return nil
}

// Reload 重载管道配置（运行中的管道会先停后启）
func (m *Manager) Reload(id string, cfg *PipelineCfg) error {
	p, err := m.get(id)
	if err != nil {
		return err
	}

	cfg.ID = id // 强制保持 ID 一致
	if err := p.Reload(cfg); err != nil {
		return fmt.Errorf("failed to reload pipeline '%s': %w", id, err)
	}

	m.logger.Info("pipeline reloaded", zap.String("id", id))

	m.save()
	return nil
}

// Get 获取管道完整状态
func (m *Manager) Get(id string) (*PipelineStatus, error) {
	p, err := m.get(id)
	if err != nil {
		return nil, err
	}

	return &PipelineStatus{
		Config:  *p.GetConfig(),
		Metrics: p.GetMetrics(),
		State:   p.State().String(),
	}, nil
}

// List 列出所有管道
func (m *Manager) List() []*PipelineStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*PipelineStatus, 0, len(m.pipelines))
	for _, p := range m.pipelines {
		result = append(result, &PipelineStatus{
			Config:  *p.GetConfig(),
			Metrics: p.GetMetrics(),
			State:   p.State().String(),
		})
	}
	return result
}

// StopAll 停止所有管道（用于优雅关闭）
func (m *Manager) StopAll() {
	m.mu.RLock()
	ids := make([]string, 0, len(m.pipelines))
	for id := range m.pipelines {
		ids = append(ids, id)
	}
	m.mu.RUnlock()

	for _, id := range ids {
		if err := m.Stop(id); err != nil {
			m.logger.Warn("failed to stop pipeline during shutdown",
				zap.String("id", id),
				zap.Error(err),
			)
		}
	}

	m.logger.Info("all pipelines stopped")
}

// Count 返回管道总数
func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.pipelines)
}

// RunningCount 返回运行中的管道数
func (m *Manager) RunningCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, p := range m.pipelines {
		if p.State() == StateRunning {
			count++
		}
	}
	return count
}

// get 内部获取管道（不加锁，由调用方控制）
func (m *Manager) get(id string) (*Pipeline, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	p, ok := m.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("pipeline '%s' not found", id)
	}
	return p, nil
}

// save 将当前所有管道配置持久化到 YAML 文件
func (m *Manager) save() {
	if m.configPath == "" {
		return
	}

	m.mu.RLock()
	pipes := make([]PipelineCfg, 0, len(m.pipelines))
	for _, p := range m.pipelines {
		pipes = append(pipes, *p.GetConfig())
	}
	m.mu.RUnlock()

	cfg := config.DefaultConfig()
	cfg.Pipelines = pipes

	if err := config.Save(m.configPath, cfg); err != nil {
		m.logger.Error("failed to persist config", zap.Error(err))
	} else {
		m.logger.Info("config persisted", zap.Int("pipeline_count", len(pipes)))
	}
}
