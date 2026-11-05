package biz

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

// App 应用：一个可部署的服务单元
type App struct {
	ID            int64
	Name          string
	Description   string
	RepoURL       string
	Image         string
	Replicas      int32
	ContainerPort int32
	Env           string // dev / staging / prod
	Status        string // draft / deployed / stopped
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// ListAppCondition 列表查询条件
type ListAppCondition struct {
	Offset  int32
	Limit   int32
	Env     string
	Keyword string // 匹配 name
}

// AppRepo 应用仓储接口
type AppRepo interface {
	Save(ctx context.Context, a *App) (*App, error)
	Update(ctx context.Context, a *App) (*App, error)
	Delete(ctx context.Context, id int64) error
	FindByID(ctx context.Context, id int64) (*App, error)
	FindByName(ctx context.Context, name string) (*App, error)
	ListByCondition(ctx context.Context, cond *ListAppCondition) ([]*App, int64, error)
}

// AppDeployStatus 部署实时状态（镜像以 K8s 为准——回滚后 DB 可能滞后）
type AppDeployStatus struct {
	Namespace  string
	Desired    int32
	Updated    int32
	Ready      int32
	Available  int32
	Image      string
	NodePort   int32
	Conditions []DeployCondition
}

// DeployCondition 部署就绪条件
type DeployCondition struct {
	Type    string
	Status  string
	Reason  string
	Message string
}

// AppDeployer 应用部署器接口：把应用发布/停止/扩缩/回滚到 K8s
// （实现见 internal/data/k8s.go）
type AppDeployer interface {
	Deploy(ctx context.Context, a *App) error    // 幂等发布：ensure 命名空间/Deployment/Service + 等待就绪
	TearDown(ctx context.Context, a *App) error  // 删除 K8s 资源（stop 与 delete 共用）
	Scale(ctx context.Context, a *App, replicas int32) error
	Rollback(ctx context.Context, a *App) error
	Status(ctx context.Context, a *App) (*AppDeployStatus, error) // 实时读 K8s
}

// AppUsecase 应用业务逻辑
type AppUsecase struct {
	repo     AppRepo
	deployer AppDeployer
	log      *log.Helper
}

// NewAppUsecase 注入仓储、部署器与日志
func NewAppUsecase(repo AppRepo, deployer AppDeployer, logger log.Logger) *AppUsecase {
	return &AppUsecase{repo: repo, deployer: deployer, log: log.NewHelper(log.With(logger, "module", "usecase/app"))}
}

var validAppEnvs = map[string]bool{"dev": true, "staging": true, "prod": true}
var validAppStatus = map[string]bool{"draft": true, "deployed": true, "stopped": true}

func validateApp(a *App) error {
	if a.Name == "" {
		return withMsg(ErrAppInvalid, "name 不能为空")
	}
	// 应用名用于生成 K8s 资源名，限制为 dns-1123 风格
	if !isValidAppName(a.Name) {
		return withMsg(ErrAppInvalid, "name 只允许小写字母、数字和 '-'，且以字母开头")
	}
	if !validAppEnvs[a.Env] {
		return withMsg(ErrAppInvalid, "env 必须是 dev/staging/prod")
	}
	if a.Status != "" && !validAppStatus[a.Status] {
		return withMsg(ErrAppInvalid, "status 必须是 draft/deployed/stopped")
	}
	if a.Replicas < 0 || a.Replicas > 100 {
		return withMsg(ErrAppInvalid, "replicas 取值范围 0-100")
	}
	if a.ContainerPort < 0 || a.ContainerPort > 65535 {
		return withMsg(ErrAppInvalid, "container_port 取值范围 0-65535")
	}
	return nil
}

func isValidAppName(name string) bool {
	if len(name) == 0 || len(name) > 63 {
		return false
	}
	for i, c := range name {
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9' && i > 0) || c == '-'
		if !ok {
			return false
		}
	}
	return name[0] >= 'a' && name[0] <= 'z'
}

// CreateApp 创建应用
func (uc *AppUsecase) CreateApp(ctx context.Context, a *App) (*App, error) {
	if err := validateApp(a); err != nil {
		return nil, err
	}
	if existed, err := uc.repo.FindByName(ctx, a.Name); err == nil && existed != nil {
		return nil, withMsg(ErrAppNameConflict, "应用名 %s 已存在", a.Name)
	}
	if a.Status == "" {
		a.Status = "draft"
	}
	return uc.repo.Save(ctx, a)
}

// UpdateApp 全量更新应用
func (uc *AppUsecase) UpdateApp(ctx context.Context, a *App) (*App, error) {
	if a.ID <= 0 {
		return nil, withMsg(ErrAppInvalid, "id 不能为空")
	}
	if err := validateApp(a); err != nil {
		return nil, err
	}
	if _, err := uc.repo.FindByID(ctx, a.ID); err != nil {
		return nil, err
	}
	if existed, err := uc.repo.FindByName(ctx, a.Name); err == nil && existed.ID != a.ID {
		return nil, withMsg(ErrAppNameConflict, "应用名 %s 已存在", a.Name)
	}
	return uc.repo.Update(ctx, a)
}

// DeleteApp 删除应用；已部署的先清理 K8s 资源，避免孤儿 Deployment
func (uc *AppUsecase) DeleteApp(ctx context.Context, id int64) error {
	a, err := uc.repo.FindByID(ctx, id)
	if err != nil {
		return err
	}
	if a.Status == "deployed" {
		if err := uc.deployer.TearDown(ctx, a); err != nil {
			return err // 清理失败则中止删除，K8s 资源还在可重试
		}
	}
	return uc.repo.Delete(ctx, id)
}

// GetApp 查询应用详情
func (uc *AppUsecase) GetApp(ctx context.Context, id int64) (*App, error) {
	return uc.repo.FindByID(ctx, id)
}

// ListApp 分页查询
func (uc *AppUsecase) ListApp(ctx context.Context, page, pageSize int64, env, keyword string) ([]*App, int64, error) {
	offset, limit, ok := normalizePage(page, pageSize)
	if !ok {
		return nil, 0, withMsg(ErrAppInvalid, "page_size 不能超过 %d", maxPageSize)
	}
	return uc.repo.ListByCondition(ctx, &ListAppCondition{
		Offset: offset, Limit: limit, Env: env, Keyword: keyword,
	})
}

// PublishApp 发布应用（幂等）：发布成功后状态置为 deployed。
// 等待就绪超时时资源已保留，状态不回写，可重发或查部署状态
func (uc *AppUsecase) PublishApp(ctx context.Context, id int64) (*App, error) {
	a, err := uc.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if a.Image == "" || a.ContainerPort <= 0 {
		return nil, withMsg(ErrAppInvalid, "发布前请先配置镜像和容器端口")
	}
	if err := uc.deployer.Deploy(ctx, a); err != nil {
		return nil, err
	}
	a.Status = "deployed"
	return uc.repo.Update(ctx, a)
}

// StopApp 停止应用：删除 K8s 资源，状态置为 stopped（幂等）
func (uc *AppUsecase) StopApp(ctx context.Context, id int64) (*App, error) {
	a, err := uc.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if a.Status == "stopped" {
		return a, nil
	}
	if a.Status != "deployed" {
		return nil, withMsg(ErrAppNotDeployed, "应用尚未发布，无需停止")
	}
	if err := uc.deployer.TearDown(ctx, a); err != nil {
		return nil, err
	}
	a.Status = "stopped"
	return uc.repo.Update(ctx, a)
}

// ScaleApp 扩缩容，成功后回写副本数
func (uc *AppUsecase) ScaleApp(ctx context.Context, id int64, replicas int32) (*App, error) {
	if replicas < 0 || replicas > 100 {
		return nil, withMsg(ErrAppInvalid, "replicas 取值范围 0-100")
	}
	a, err := uc.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if a.Status != "deployed" {
		return nil, withMsg(ErrAppNotDeployed, "应用尚未发布，无法扩缩容")
	}
	if err := uc.deployer.Scale(ctx, a, replicas); err != nil {
		return nil, err
	}
	a.Replicas = replicas
	return uc.repo.Update(ctx, a)
}

// RollbackApp 回滚到上一个发布版本。
// 不回写 DB 的镜像字段：部署状态接口以集群实况为准
func (uc *AppUsecase) RollbackApp(ctx context.Context, id int64) (*App, error) {
	a, err := uc.repo.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if a.Status != "deployed" {
		return nil, withMsg(ErrAppNotDeployed, "应用尚未发布，无法回滚")
	}
	if err := uc.deployer.Rollback(ctx, a); err != nil {
		return nil, err
	}
	return a, nil
}

// GetDeployStatus 查询部署实时状态（直读 K8s）
func (uc *AppUsecase) GetDeployStatus(ctx context.Context, id int64) (*App, *AppDeployStatus, error) {
	a, err := uc.repo.FindByID(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	st, err := uc.deployer.Status(ctx, a)
	if err != nil {
		return nil, nil, err
	}
	return a, st, nil
}
