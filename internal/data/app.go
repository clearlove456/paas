package data

import (
	"context"
	"errors"
	"time"

	"github.com/clearlove456/paas/internal/biz"
	"github.com/clearlove456/paas/internal/data/model"
	"github.com/clearlove456/paas/pkg/cachex"

	"gorm.io/gorm"
)

// appCacheCfg 应用缓存策略：与资产一致（5min + 2min 抖动，空值 60s）
var appCacheCfg = cachex.Config{
	Prefix:  "paas:app:",
	TTL:     5 * time.Minute,
	Jitter:  2 * time.Minute,
	NullTTL: 60 * time.Second,
}

type appRepo struct {
	data *Data
}

// NewAppRepo 实现 biz.AppRepo 接口
func NewAppRepo(d *Data) biz.AppRepo {
	return &appRepo{data: d}
}

func appPO(a *biz.App) *model.App {
	return &model.App{
		ID:            a.ID,
		Name:          a.Name,
		Description:   a.Description,
		RepoURL:       a.RepoURL,
		Image:         a.Image,
		Replicas:      a.Replicas,
		ContainerPort: a.ContainerPort,
		Env:           a.Env,
		Status:        a.Status,
	}
}

func appBiz(m *model.App) *biz.App {
	return &biz.App{
		ID:            m.ID,
		Name:          m.Name,
		Description:   m.Description,
		RepoURL:       m.RepoURL,
		Image:         m.Image,
		Replicas:      m.Replicas,
		ContainerPort: m.ContainerPort,
		Env:           m.Env,
		Status:        m.Status,
		CreatedAt:     m.CreatedAt,
		UpdatedAt:     m.UpdatedAt,
	}
}

// Save 创建应用
func (r *appRepo) Save(ctx context.Context, a *biz.App) (*biz.App, error) {
	po := appPO(a)
	if err := r.data.q.App.WithContext(ctx).Create(po); err != nil {
		return nil, err
	}
	return appBiz(po), nil
}

// Update 按 ID 全量更新。不碰缓存：淘汰由 binlog 订阅通道负责（延迟双删）。
func (r *appRepo) Update(ctx context.Context, a *biz.App) (*biz.App, error) {
	po := appPO(a)
	info, err := r.data.q.App.WithContext(ctx).
		Where(r.data.q.App.ID.Eq(po.ID)).
		Updates(map[string]interface{}{
			"name":           po.Name,
			"description":    po.Description,
			"repo_url":       po.RepoURL,
			"image":          po.Image,
			"replicas":       po.Replicas,
			"container_port": po.ContainerPort,
			"env":            po.Env,
			"status":         po.Status,
		})
	if err != nil {
		return nil, err
	}
	if info.RowsAffected == 0 {
		return nil, biz.ErrAppNotFound
	}
	return r.FindByID(ctx, a.ID)
}

// Delete 软删除。不碰缓存：淘汰由 binlog 订阅通道负责。
func (r *appRepo) Delete(ctx context.Context, id int64) error {
	info, err := r.data.q.App.WithContext(ctx).
		Where(r.data.q.App.ID.Eq(id)).
		Delete(&model.App{})
	if err != nil {
		return err
	}
	if info.RowsAffected == 0 {
		return biz.ErrAppNotFound
	}
	return nil
}

// FindByID 按 ID 查询：读路径走 cachex.Aside（三防同资产）
func (r *appRepo) FindByID(ctx context.Context, id int64) (*biz.App, error) {
	return cachex.Aside(ctx, r.data.rdb, &r.data.sf, appCacheCfg, appCacheCfg.Key(id),
		func(ctx context.Context) (*biz.App, error) {
			po, err := r.data.q.App.WithContext(ctx).
				Where(r.data.q.App.ID.Eq(id)).
				First()
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, biz.ErrAppNotFound
				}
				return nil, err
			}
			return appBiz(po), nil
		},
		func(err error) bool { return err == biz.ErrAppNotFound },
		biz.ErrAppNotFound,
	)
}

// FindByName 按应用名查询（唯一索引）
func (r *appRepo) FindByName(ctx context.Context, name string) (*biz.App, error) {
	po, err := r.data.q.App.WithContext(ctx).
		Where(r.data.q.App.Name.Eq(name)).
		First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, biz.ErrAppNotFound
		}
		return nil, err
	}
	return appBiz(po), nil
}

// ListByCondition 分页 + 条件查询
func (r *appRepo) ListByCondition(ctx context.Context, cond *biz.ListAppCondition) ([]*biz.App, int64, error) {
	a := r.data.q.App
	qx := a.WithContext(ctx)

	if cond.Env != "" {
		qx = qx.Where(a.Env.Eq(cond.Env))
	}
	if cond.Keyword != "" {
		qx = qx.Where(a.Name.Like("%" + cond.Keyword + "%"))
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
	out := make([]*biz.App, 0, len(pos))
	for _, po := range pos {
		out = append(out, appBiz(po))
	}
	return out, total, nil
}
