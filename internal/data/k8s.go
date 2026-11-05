package data

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	v1 "github.com/clearlove456/paas/api/paas/v1"
	"github.com/clearlove456/paas/internal/biz"
	"github.com/clearlove456/paas/internal/conf"
	"github.com/clearlove456/paas/pkg/k8sx"
)

// paas 语义常量（写死）：namespace 规则与资源标签
const (
	namespacePrefix = "paas-" // dev/staging/prod -> paas-dev / paas-staging / paas-prod
	labelManagedBy  = "app.kubernetes.io/managed-by"
	labelAppID      = "paas.im/app-id"
)

// allEnvs 全部环境，发布前清理其他环境里的同名资源（env 变更 = 跨 namespace 迁移）
var allEnvs = []string{"dev", "staging", "prod"}

// appDeployer biz.AppDeployer 实现：包装 pkg/k8sx，负责 paas 语义
// （namespace 派生、label 注入、App -> Workload 转换、错误码映射）
type appDeployer struct {
	k8s      *k8sx.Client
	disabled bool // kubernetes.enabled=false 时的降级实例，方法一律报错
}

// NewAppDeployer 构建部署器。kubernetes 未启用时返回降级实例（服务照常启动），
// 启用时构建客户端并做连通性检查，失败即启动失败
func NewAppDeployer(c *conf.Kubernetes) (biz.AppDeployer, error) {
	if c == nil || !c.Enabled {
		return &appDeployer{disabled: true}, nil
	}
	cli, err := k8sx.New(&k8sx.Config{Kubeconfig: c.Kubeconfig})
	if err != nil {
		return nil, fmt.Errorf("init k8s client: %w", err)
	}
	return &appDeployer{k8s: cli}, nil
}

// namespaceOf env -> namespace
func namespaceOf(env string) string { return namespacePrefix + env }

// workloadOf App -> k8sx.Workload
func workloadOf(a *biz.App) *k8sx.Workload {
	return &k8sx.Workload{
		Name:      a.Name,
		Namespace: namespaceOf(a.Env),
		Image:     a.Image,
		Replicas:  a.Replicas,
		Port:      a.ContainerPort,
		Labels: map[string]string{
			labelManagedBy: "paas",
			labelAppID:     strconv.FormatInt(a.ID, 10),
		},
	}
}

// errDisabled 未启用 k8s 时的统一报错
func errDisabled() error {
	return v1.ErrorAppDeployFailed("Kubernetes 未启用，请在 config.yaml 开启 kubernetes.enabled")
}

// Deploy 发布编排：清理其他环境残留 -> ensure 命名空间 -> Deployment -> Service -> 等待就绪
func (d *appDeployer) Deploy(ctx context.Context, a *biz.App) error {
	if d.disabled {
		return errDisabled()
	}
	w := workloadOf(a)

	// env 变更时旧环境的资源要清掉，否则同名应用在新旧环境各跑一份
	for _, env := range allEnvs {
		if env != a.Env {
			if err := d.k8s.DeleteWorkload(ctx, namespaceOf(env), a.Name); err != nil {
				return mapDeployErr(err)
			}
		}
	}

	if err := d.k8s.EnsureNamespace(ctx, w.Namespace); err != nil {
		return mapDeployErr(err)
	}
	if err := d.k8s.EnsureDeployment(ctx, w); err != nil {
		return mapDeployErr(err)
	}
	if err := d.k8s.EnsureService(ctx, w); err != nil {
		return mapDeployErr(err)
	}
	return mapDeployErr(d.k8s.WaitDeploymentReady(ctx, w.Namespace, w.Name))
}

// TearDown 删除 Deployment + Service
func (d *appDeployer) TearDown(ctx context.Context, a *biz.App) error {
	if d.disabled {
		return errDisabled()
	}
	// 不确定之前部署在哪个环境，全环境清理一遍（NotFound 忽略，幂等）
	var lastErr error
	for _, env := range allEnvs {
		if err := d.k8s.DeleteWorkload(ctx, namespaceOf(env), a.Name); err != nil {
			lastErr = err
		}
	}
	return mapDeployErr(lastErr)
}

// Scale 扩缩容 + 等待就绪
func (d *appDeployer) Scale(ctx context.Context, a *biz.App, replicas int32) error {
	if d.disabled {
		return errDisabled()
	}
	ns := namespaceOf(a.Env)
	if err := d.k8s.Scale(ctx, ns, a.Name, replicas); err != nil {
		return mapDeployErr(err)
	}
	return mapDeployErr(d.k8s.WaitDeploymentReady(ctx, ns, a.Name))
}

// Rollback 回滚上一个版本 + 等待就绪
func (d *appDeployer) Rollback(ctx context.Context, a *biz.App) error {
	if d.disabled {
		return errDisabled()
	}
	ns := namespaceOf(a.Env)
	if err := d.k8s.RollbackUndo(ctx, ns, a.Name); err != nil {
		return mapDeployErr(err)
	}
	return mapDeployErr(d.k8s.WaitDeploymentReady(ctx, ns, a.Name))
}

// Status 实时读取部署状态
func (d *appDeployer) Status(ctx context.Context, a *biz.App) (*biz.AppDeployStatus, error) {
	if d.disabled {
		return nil, errDisabled()
	}
	st, err := d.k8s.GetStatus(ctx, namespaceOf(a.Env), a.Name)
	if err != nil {
		return nil, mapDeployErr(err)
	}
	out := &biz.AppDeployStatus{
		Namespace:  namespaceOf(a.Env),
		Desired:    st.Desired,
		Updated:    st.Updated,
		Ready:      st.Ready,
		Available:  st.Available,
		Image:      st.Image,
		NodePort:   st.NodePort,
		Conditions: make([]biz.DeployCondition, 0, len(st.Conditions)),
	}
	for _, c := range st.Conditions {
		out.Conditions = append(out.Conditions, biz.DeployCondition{
			Type: c.Type, Status: c.Status, Reason: c.Reason, Message: c.Message,
		})
	}
	return out, nil
}

// mapDeployErr k8sx 哨兵错误 -> 业务错误码（proto 生成的构造函数）
func mapDeployErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, k8sx.ErrNotFound):
		return v1.ErrorAppNotDeployed("应用尚未部署或 K8s 资源已被删除")
	case errors.Is(err, k8sx.ErrWaitTimeout):
		return v1.ErrorAppDeployTimeout("等待应用就绪超时（K8s 资源已保留，可重试发布或查询部署状态）")
	case errors.Is(err, k8sx.ErrNoRollbackTarget):
		return v1.ErrorAppRollbackFailed("没有可回滚的历史版本")
	default:
		return v1.ErrorAppDeployFailed("K8s 操作失败: %v", err)
	}
}
