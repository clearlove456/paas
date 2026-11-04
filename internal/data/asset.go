package data

import (
	"context"
	"errors"
	"time"

	"github.com/clearlove456/paas/internal/biz"
	"github.com/clearlove456/paas/internal/data/model"
	"github.com/clearlove456/paas/pkg/cachex"

	"gorm.io/gen/field"
	"gorm.io/gorm"
)

// AssetCacheCfg 资产缓存策略：5min + 2min 抖动防雪崩，空值缓存 60s 防穿透
var AssetCacheCfg = cachex.Config{
	Prefix:  "paas:asset:",
	TTL:     5 * time.Minute,
	Jitter:  2 * time.Minute,
	NullTTL: 60 * time.Second,
}

type assetRepo struct {
	data *Data
}

// NewAssetRepo 实现 biz.AssetRepo 接口
func NewAssetRepo(d *Data) biz.AssetRepo {
	return &assetRepo{data: d}
}

func assetPO(a *biz.Asset) *model.Asset {
	return &model.Asset{
		ID:          a.ID,
		Hostname:    a.Hostname,
		IP:          a.IP,
		Env:         a.Env,
		Region:      a.Region,
		CPU:         a.CPU,
		Memory:      a.Memory,
		OS:          a.OS,
		Status:      a.Status,
		Description: a.Description,
	}
}

func assetBiz(m *model.Asset) *biz.Asset {
	return &biz.Asset{
		ID:          m.ID,
		Hostname:    m.Hostname,
		IP:          m.IP,
		Env:         m.Env,
		Region:      m.Region,
		CPU:         m.CPU,
		Memory:      m.Memory,
		OS:          m.OS,
		Status:      m.Status,
		Description: m.Description,
		CreatedAt:   m.CreatedAt,
		UpdatedAt:   m.UpdatedAt,
	}
}

// Save 创建资产
func (r *assetRepo) Save(ctx context.Context, a *biz.Asset) (*biz.Asset, error) {
	po := assetPO(a)
	if err := r.data.q.Asset.WithContext(ctx).Create(po); err != nil {
		return nil, err
	}
	return assetBiz(po), nil
}

// Update 按 ID 全量更新（map 显式列出字段，零值也会写入）。
// 不碰缓存：淘汰由 canal binlog 订阅通道统一负责（延迟双删，见 binlog.go）。
func (r *assetRepo) Update(ctx context.Context, a *biz.Asset) (*biz.Asset, error) {
	po := assetPO(a)
	info, err := r.data.q.Asset.WithContext(ctx).
		Where(r.data.q.Asset.ID.Eq(po.ID)).
		Updates(map[string]interface{}{
			"hostname":    po.Hostname,
			"ip":          po.IP,
			"env":         po.Env,
			"region":      po.Region,
			"cpu":         po.CPU,
			"memory":      po.Memory,
			"os":          po.OS,
			"status":      po.Status,
			"description": po.Description,
		})
	if err != nil {
		return nil, err
	}
	if info.RowsAffected == 0 {
		return nil, biz.ErrAssetNotFound
	}
	return r.FindByID(ctx, a.ID)
}

// Delete 软删除。不碰缓存：淘汰由 binlog 订阅通道负责。
func (r *assetRepo) Delete(ctx context.Context, id int64) error {
	info, err := r.data.q.Asset.WithContext(ctx).
		Where(r.data.q.Asset.ID.Eq(id)).
		Delete(&model.Asset{})
	if err != nil {
		return err
	}
	if info.RowsAffected == 0 {
		return biz.ErrAssetNotFound
	}
	return nil
}

// FindByID 按 ID 查询：读路径走 cachex.Aside
// （singleflight 防击穿 + TTL 抖动防雪崩 + 空值缓存防穿透）
func (r *assetRepo) FindByID(ctx context.Context, id int64) (*biz.Asset, error) {
	return cachex.Aside(ctx, r.data.rdb, &r.data.sf, AssetCacheCfg, AssetCacheCfg.Key(id),
		func(ctx context.Context) (*biz.Asset, error) {
			po, err := r.data.q.Asset.WithContext(ctx).
				Where(r.data.q.Asset.ID.Eq(id)).
				First()
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, biz.ErrAssetNotFound
				}
				return nil, err
			}
			return assetBiz(po), nil
		},
		func(err error) bool { return err == biz.ErrAssetNotFound },
		biz.ErrAssetNotFound,
	)
}

// FindByHostname 按主机名查询（唯一索引）
func (r *assetRepo) FindByHostname(ctx context.Context, hostname string) (*biz.Asset, error) {
	po, err := r.data.q.Asset.WithContext(ctx).
		Where(r.data.q.Asset.Hostname.Eq(hostname)).
		First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, biz.ErrAssetNotFound
		}
		return nil, err
	}
	return assetBiz(po), nil
}

// ListByCondition 分页 + 条件查询
func (r *assetRepo) ListByCondition(ctx context.Context, cond *biz.ListAssetCondition) ([]*biz.Asset, int64, error) {
	a := r.data.q.Asset
	qx := a.WithContext(ctx)

	if cond.Env != "" {
		qx = qx.Where(a.Env.Eq(cond.Env))
	}
	if cond.Keyword != "" {
		like := "%" + cond.Keyword + "%"
		qx = qx.Where(field.Or(a.Hostname.Like(like), a.IP.Like(like)))
	}

	total, err := qx.Count()
	if err != nil {
		return nil, 0, err
	}
	pos, err := qx.
		Order(a.ID.Desc()).
		Offset(int(cond.Offset)).
		Limit(int(cond.Limit)).
		Find()
	if err != nil {
		return nil, 0, err
	}
	out := make([]*biz.Asset, 0, len(pos))
	for _, po := range pos {
		out = append(out, assetBiz(po))
	}
	return out, total, nil
}
