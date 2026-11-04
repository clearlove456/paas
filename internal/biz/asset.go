package biz

import (
	"context"
	"net"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

// Asset 主机资产（业务对象，与 PO/pb 解耦）
type Asset struct {
	ID          int64
	Hostname    string
	IP          string
	Env         string // dev / staging / prod
	Region      string
	CPU         int32 // 核数
	Memory      int32 // GB
	OS          string
	Status      string // online / offline / maintenance
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ListAssetCondition 列表查询条件
type ListAssetCondition struct {
	Offset  int32
	Limit   int32
	Env     string
	Keyword string // 匹配 hostname / ip
}

// AssetRepo 资产仓储接口，由 data 层实现（依赖倒置）
type AssetRepo interface {
	Save(ctx context.Context, a *Asset) (*Asset, error)
	Update(ctx context.Context, a *Asset) (*Asset, error)
	Delete(ctx context.Context, id int64) error
	FindByID(ctx context.Context, id int64) (*Asset, error)
	FindByHostname(ctx context.Context, hostname string) (*Asset, error)
	ListByCondition(ctx context.Context, cond *ListAssetCondition) ([]*Asset, int64, error)
}

// AssetUsecase 资产业务逻辑
type AssetUsecase struct {
	repo AssetRepo
	log  *log.Helper
}

// NewAssetUsecase 注入仓储与日志
func NewAssetUsecase(repo AssetRepo, logger log.Logger) *AssetUsecase {
	return &AssetUsecase{repo: repo, log: log.NewHelper(log.With(logger, "module", "usecase/asset"))}
}

var validAssetEnvs = map[string]bool{"dev": true, "staging": true, "prod": true}
var validAssetStatus = map[string]bool{"online": true, "offline": true, "maintenance": true}

func validateAsset(a *Asset) error {
	if a.Hostname == "" {
		return withMsg(ErrAssetInvalid, "hostname 不能为空")
	}
	if net.ParseIP(a.IP) == nil {
		return withMsg(ErrAssetInvalid, "ip 格式不合法: %s", a.IP)
	}
	if !validAssetEnvs[a.Env] {
		return withMsg(ErrAssetInvalid, "env 必须是 dev/staging/prod")
	}
	if a.Status != "" && !validAssetStatus[a.Status] {
		return withMsg(ErrAssetInvalid, "status 必须是 online/offline/maintenance")
	}
	if a.CPU <= 0 || a.Memory <= 0 {
		return withMsg(ErrAssetInvalid, "cpu/memory 必须大于 0")
	}
	return nil
}

// CreateAsset 创建资产，主机名不允许重复
func (uc *AssetUsecase) CreateAsset(ctx context.Context, a *Asset) (*Asset, error) {
	if err := validateAsset(a); err != nil {
		return nil, err
	}
	if existed, err := uc.repo.FindByHostname(ctx, a.Hostname); err == nil && existed != nil {
		return nil, withMsg(ErrAssetHostConflict, "主机名 %s 已存在", a.Hostname)
	}
	if a.Status == "" {
		a.Status = "online"
	}
	return uc.repo.Save(ctx, a)
}

// UpdateAsset 全量更新资产
func (uc *AssetUsecase) UpdateAsset(ctx context.Context, a *Asset) (*Asset, error) {
	if a.ID <= 0 {
		return nil, withMsg(ErrAssetInvalid, "id 不能为空")
	}
	if err := validateAsset(a); err != nil {
		return nil, err
	}
	if _, err := uc.repo.FindByID(ctx, a.ID); err != nil {
		return nil, err
	}
	// 主机名改成别人的也不行
	if existed, err := uc.repo.FindByHostname(ctx, a.Hostname); err == nil && existed.ID != a.ID {
		return nil, withMsg(ErrAssetHostConflict, "主机名 %s 已存在", a.Hostname)
	}
	return uc.repo.Update(ctx, a)
}

// DeleteAsset 删除资产（软删除）
func (uc *AssetUsecase) DeleteAsset(ctx context.Context, id int64) error {
	if _, err := uc.repo.FindByID(ctx, id); err != nil {
		return err
	}
	return uc.repo.Delete(ctx, id)
}

// GetAsset 查询资产详情
func (uc *AssetUsecase) GetAsset(ctx context.Context, id int64) (*Asset, error) {
	return uc.repo.FindByID(ctx, id)
}

// ListAsset 分页查询，page 从 1 开始
func (uc *AssetUsecase) ListAsset(ctx context.Context, page, pageSize int64, env, keyword string) ([]*Asset, int64, error) {
	offset, limit, ok := normalizePage(page, pageSize)
	if !ok {
		return nil, 0, withMsg(ErrAssetInvalid, "page_size 不能超过 100")
	}
	return uc.repo.ListByCondition(ctx, &ListAssetCondition{
		Offset: offset, Limit: limit, Env: env, Keyword: keyword,
	})
}
