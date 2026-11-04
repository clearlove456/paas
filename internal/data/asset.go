package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/clearlove456/paas/internal/biz"
	"github.com/clearlove456/paas/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"gorm.io/gen/field"
	"gorm.io/gorm"
)

const (
	assetCacheKeyPrefix = "paas:asset:"
	assetCacheTTL       = 5 * time.Minute
)

func assetCacheKey(id int64) string {
	return fmt.Sprintf("%s%d", assetCacheKeyPrefix, id)
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

// Update 按 ID 全量更新（map 显式列出字段，零值也会写入）
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
	// 更新成功后先淘汰缓存，再回读最新数据（回读会重新填充缓存）
	r.data.rdb.Del(ctx, assetCacheKey(a.ID))
	return r.FindByID(ctx, a.ID)
}

// Delete 软删除
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
	// 删除成功后淘汰缓存
	r.data.rdb.Del(ctx, assetCacheKey(id))
	return nil
}

// FindByID 按 ID 查询：Cache-Aside 模式，优先读 Redis，未命中回源 MySQL 并回填
func (r *assetRepo) FindByID(ctx context.Context, id int64) (*biz.Asset, error) {
	key := assetCacheKey(id)
	// 1. 读缓存：命中直接返回；Redis 故障不阻塞业务，记日志后回源
	if raw, err := r.data.rdb.Get(ctx, key).Result(); err == nil {
		var a biz.Asset
		if json.Unmarshal([]byte(raw), &a) == nil {
			return &a, nil
		}
	} else if err != redis.Nil {
		log.Warnf("asset cache get failed, fallback to db: %v", err)
	}

	// 2. 回源数据库
	po, err := r.data.q.Asset.WithContext(ctx).
		Where(r.data.q.Asset.ID.Eq(id)).
		First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, biz.ErrAssetNotFound
		}
		return nil, err
	}
	out := assetBiz(po)

	// 3. 回填缓存，失败只记日志不影响主流程
	if data, err := json.Marshal(out); err == nil {
		if err := r.data.rdb.Set(ctx, key, data, assetCacheTTL).Err(); err != nil {
			log.Warnf("asset cache set failed: %v", err)
		}
	}
	return out, nil
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
