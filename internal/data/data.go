package data

import (
	"context"
	"fmt"
	"time"

	"github.com/clearlove456/paas/internal/conf"
	"github.com/clearlove456/paas/internal/data/model"
	"github.com/clearlove456/paas/internal/data/query"
	"github.com/clearlove456/paas/pkg/gormx"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewAssetRepo, NewAppRepo)

// Data 持有 DB/Redis 连接与 gorm-gen 生成的类型安全 Query
type Data struct {
	db  *gorm.DB
	rdb *redis.Client
	q   *query.Query
	sf  singleflight.Group // 缓存击穿防护：合并同 key 并发回源
}

// NewData 连接 MySQL/Redis，自动建表，初始化 Query
func NewData(c *conf.Data, logger log.Logger) (*Data, func(), error) {
	helper := log.NewHelper(log.With(logger, "module", "data"))

	db, err := gorm.Open(mysql.Open(c.Database.Source), &gorm.Config{
		Logger: gormx.NewLogger(logger),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("connect mysql: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, nil, fmt.Errorf("get sql.DB: %w", err)
	}
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)

	// 开发期用 AutoMigrate 建表；生产环境应换成版本化迁移（Phase 4 引入）
	if err := db.AutoMigrate(&model.Asset{}, &model.App{}); err != nil {
		return nil, nil, fmt.Errorf("automigrate: %w", err)
	}

	// Redis：详情缓存 / 之后的分布式锁
	redisOpts := &redis.Options{
		Addr:     c.Redis.Addr,
		Password: c.Redis.Password,
	}
	if c.Redis.ReadTimeout != nil {
		redisOpts.ReadTimeout = c.Redis.ReadTimeout.AsDuration()
	}
	if c.Redis.WriteTimeout != nil {
		redisOpts.WriteTimeout = c.Redis.WriteTimeout.AsDuration()
	}
	rdb := redis.NewClient(redisOpts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, nil, fmt.Errorf("connect redis: %w", err)
	}

	cleanup := func() {
		helper.Info("closing the data resources")
		_ = sqlDB.Close()
		_ = rdb.Close()
	}
	return &Data{db: db, rdb: rdb, q: query.Use(db)}, cleanup, nil
}

// Query 暴露给 repo 实现使用
func (d *Data) Query() *query.Query { return d.q }

// Redis 暴露给 consumer 等组件使用
func (d *Data) Redis() *redis.Client { return d.rdb }
