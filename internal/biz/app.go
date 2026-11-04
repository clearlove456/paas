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

// AppUsecase 应用业务逻辑
type AppUsecase struct {
	repo AppRepo
	log  *log.Helper
}

// NewAppUsecase 注入仓储与日志
func NewAppUsecase(repo AppRepo, logger log.Logger) *AppUsecase {
	return &AppUsecase{repo: repo, log: log.NewHelper(log.With(logger, "module", "usecase/app"))}
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

// DeleteApp 删除应用
func (uc *AppUsecase) DeleteApp(ctx context.Context, id int64) error {
	if _, err := uc.repo.FindByID(ctx, id); err != nil {
		return err
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
