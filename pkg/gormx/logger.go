// Package gormx 提供 GORM 与 Kratos 生态的集成组件。
package gormx

import (
	"context"
	"errors"
	"time"

	"github.com/go-kratos/kratos/v2/log"

	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// LogAdapter 把 GORM 的日志桥接到 Kratos logger，
// 让 SQL 日志与业务日志统一在一套体系里，方便以后接 Loki/ELK。
type LogAdapter struct {
	logger          *log.Helper
	level           gormlogger.LogLevel
	slowThresholdMs int64
}

// NewLogger 返回一个实现了 gorm logger.Interface 的 Kratos 日志适配器。
// 默认 Warn 级别、慢查询阈值 200ms。
func NewLogger(klog log.Logger) gormlogger.Interface {
	return &LogAdapter{
		logger:          log.NewHelper(log.With(klog, "module", "gorm")),
		level:           gormlogger.Warn,
		slowThresholdMs: 200,
	}
}

// LogMode 动态调整日志级别
func (l *LogAdapter) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	newLogger := *l
	newLogger.level = level
	return &newLogger
}

// Info 信息日志
func (l *LogAdapter) Info(_ context.Context, msg string, args ...interface{}) {
	if l.level >= gormlogger.Info {
		l.logger.Infof(msg, args...)
	}
}

// Warn 警告日志
func (l *LogAdapter) Warn(_ context.Context, msg string, args ...interface{}) {
	if l.level >= gormlogger.Warn {
		l.logger.Warnf(msg, args...)
	}
}

// Error 错误日志
func (l *LogAdapter) Error(_ context.Context, msg string, args ...interface{}) {
	if l.level >= gormlogger.Error {
		l.logger.Errorf(msg, args...)
	}
}

// Trace 记录每条 SQL 的耗时；慢查询告警，NotFound 降级为 debug
func (l *LogAdapter) Trace(_ context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.level <= gormlogger.Silent {
		return
	}
	elapsed := time.Since(begin)
	sql, rows := fc()
	switch {
	case err != nil && errors.Is(err, gorm.ErrRecordNotFound):
		l.logger.Debugf("sql: %s | rows: %d | %v", sql, rows, elapsed)
	case err != nil:
		l.logger.Errorf("sql: %s | rows: %d | %v | error: %v", sql, rows, elapsed, err)
	case elapsed.Milliseconds() > l.slowThresholdMs:
		l.logger.Warnf("SLOW SQL: %s | rows: %d | %v", sql, rows, elapsed)
	default:
		l.logger.Debugf("sql: %s | rows: %d | %v", sql, rows, elapsed)
	}
}
