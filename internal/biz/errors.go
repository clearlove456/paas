package biz

import (
	"fmt"

	v1 "github.com/clearlove456/paas/api/paas/v1"

	"github.com/go-kratos/kratos/v2/errors"
)

// withMsg 基于已有错误替换用户可读信息，code/reason 保持不变
func withMsg(e *errors.Error, format string, args ...interface{}) *errors.Error {
	return errors.New(int(e.Code), e.Reason, fmt.Sprintf(format, args...))
}

// 业务错误在 api/paas/v1/error_reason.proto 中统一定义（make api 生成构造函数），
// 这里挂成包级变量方便各层引用。
var (
	// 资产相关
	ErrAssetNotFound     = v1.ErrorAssetNotFound("资产不存在")
	ErrAssetHostConflict = v1.ErrorAssetHostConflict("主机名已存在")
	ErrAssetInvalid      = v1.ErrorAssetInvalid("资产参数不合法")

	// 应用相关
	ErrAppNotFound     = v1.ErrorAppNotFound("应用不存在")
	ErrAppNameConflict = v1.ErrorAppNameConflict("应用名已存在")
	ErrAppInvalid      = v1.ErrorAppInvalid("应用参数不合法")

	// 应用发布相关
	ErrAppNotDeployed    = v1.ErrorAppNotDeployed("应用尚未部署")
	ErrAppDeployTimeout  = v1.ErrorAppDeployTimeout("等待应用就绪超时")
	ErrAppRollbackFailed = v1.ErrorAppRollbackFailed("回滚失败")
	ErrAppDeployFailed   = v1.ErrorAppDeployFailed("发布失败")
)
