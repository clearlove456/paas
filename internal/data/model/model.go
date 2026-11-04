// Package model 存放数据库 PO（Persistent Object）。
// PO 只描述表结构，不承载业务语义；业务对象在 biz 层定义。
// 改完 struct 后执行 `make gen-orm` 重新生成查询代码，启动时 AutoMigrate 同步表结构。
package model

import (
	"time"

	"gorm.io/gorm"
)

// Asset 主机资产表
type Asset struct {
	ID          int64          `gorm:"column:id;primaryKey;autoIncrement"`
	Hostname    string         `gorm:"column:hostname;size:128;uniqueIndex;not null"`
	IP          string         `gorm:"column:ip;size:64;not null"`
	Env         string         `gorm:"column:env;size:32;index;not null;default:dev"`
	Region      string         `gorm:"column:region;size:64"`
	CPU         int32          `gorm:"column:cpu;not null;default:1"`
	Memory      int32          `gorm:"column:memory;not null;default:1"`
	OS          string         `gorm:"column:os;size:128"`
	Status      string         `gorm:"column:status;size:32;index;not null;default:online"`
	Description string         `gorm:"column:description;size:512"`
	CreatedAt   time.Time      `gorm:"column:created_at"`
	UpdatedAt   time.Time      `gorm:"column:updated_at"`
	DeletedAt   gorm.DeletedAt `gorm:"column:deleted_at;index"` // 软删除
}

// TableName 表名
func (Asset) TableName() string { return "assets" }

// App 应用表
type App struct {
	ID            int64          `gorm:"column:id;primaryKey;autoIncrement"`
	Name          string         `gorm:"column:name;size:128;uniqueIndex;not null"`
	Description   string         `gorm:"column:description;size:512"`
	RepoURL       string         `gorm:"column:repo_url;size:255"`
	Image         string         `gorm:"column:image;size:255"`
	Replicas      int32          `gorm:"column:replicas;not null;default:1"`
	ContainerPort int32          `gorm:"column:container_port;not null;default:8080"`
	Env           string         `gorm:"column:env;size:32;index;not null;default:dev"`
	Status        string         `gorm:"column:status;size:32;index;not null;default:draft"`
	CreatedAt     time.Time      `gorm:"column:created_at"`
	UpdatedAt     time.Time      `gorm:"column:updated_at"`
	DeletedAt     gorm.DeletedAt `gorm:"column:deleted_at;index"`
}

// TableName 表名
func (App) TableName() string { return "apps" }
