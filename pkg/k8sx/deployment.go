package k8sx

import (
	"context"
	"fmt"
	"maps"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
)

// 滚动参数（写死）：保留 10 个历史 RS 供回滚；
// 集群侧 180s 判定滚动失败，与客户端 3min 等待窗口对齐
const (
	revisionHistoryLimit = int32(10)
	progressDeadlineSecs = int32(180)
)

// EnsureDeployment 确保 Deployment 存在且与 Workload 一致（幂等）：
// 不存在则创建；存在则只改本组件管理的字段，API Server 回填的默认值不动。
func (c *Client) EnsureDeployment(ctx context.Context, w *Workload) error {
	_, err := c.cs.AppsV1().Deployments(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := c.cs.AppsV1().Deployments(w.Namespace).Create(ctx, buildDeployment(w), metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create deployment %s/%s: %w", w.Namespace, w.Name, err)
		}
		return nil
	}
	if err != nil {
		return err
	}

	// 已存在：Get -> 只改管理的字段 -> 无 diff 不 Update -> 冲突时 re-Get 重放
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		live, err := c.cs.AppsV1().Deployments(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !mutateDeployment(live, w) {
			return nil
		}
		_, err = c.cs.AppsV1().Deployments(w.Namespace).Update(ctx, live, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("update deployment %s/%s: %w", w.Namespace, w.Name, err)
	}
	return nil
}

// Scale 用 scale 子资源扩缩容（只带 replicas，不与模板更新抢 ResourceVersion）
func (c *Client) Scale(ctx context.Context, namespace, name string, replicas int32) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, err := c.cs.AppsV1().Deployments(namespace).UpdateScale(ctx, name,
			&autoscalingv1.Scale{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
				Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
			}, metav1.UpdateOptions{})
		return err
	})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("%w: deployment %s/%s", ErrNotFound, namespace, name)
	}
	return err
}

// WaitDeploymentReady 轮询直到就绪；超时返回 ErrWaitTimeout，
// 集群侧判定滚动失败（ProgressDeadlineExceeded）时立即返回
func (c *Client) WaitDeploymentReady(ctx context.Context, namespace, name string) error {
	deadline := time.Now().Add(c.WaitTimeout)
	for {
		dep, err := c.cs.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("%w: deployment %s/%s", ErrNotFound, namespace, name)
		}
		if err != nil {
			return err
		}
		if deploymentReady(dep) {
			return nil
		}
		if msg, failed := progressFailed(dep); failed {
			return fmt.Errorf("%w: %s/%s %s", ErrWaitTimeout, namespace, name, msg)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: %s/%s", ErrWaitTimeout, namespace, name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.PollInterval):
		}
	}
}

// DeleteWorkload 删除 Deployment 和 Service（幂等，NotFound 忽略）
func (c *Client) DeleteWorkload(ctx context.Context, namespace, name string) error {
	if err := c.cs.AppsV1().Deployments(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete deployment %s/%s: %w", namespace, name, err)
	}
	if err := c.cs.CoreV1().Services(namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete service %s/%s: %w", namespace, name, err)
	}
	return nil
}

// deploymentReady 新一代模板全部更新且副本全部可用（replicas=0 时恒就绪）
func deploymentReady(d *appsv1.Deployment) bool {
	return d.Generation <= d.Status.ObservedGeneration &&
		d.Status.UpdatedReplicas == *d.Spec.Replicas &&
		d.Status.AvailableReplicas == *d.Spec.Replicas
}

// progressFailed 集群侧判定滚动失败
func progressFailed(d *appsv1.Deployment) (string, bool) {
	for _, cond := range d.Status.Conditions {
		if cond.Type == appsv1.DeploymentProgressing && cond.Reason == "ProgressDeadlineExceeded" {
			return cond.Message, true
		}
	}
	return "", false
}

// buildDeployment 按 Workload 全新构建
func buildDeployment(w *Workload) *appsv1.Deployment {
	labels := templateLabels(w)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      w.Name,
			Namespace: w.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas:                ptr.To(w.Replicas),
			RevisionHistoryLimit:    ptr.To(revisionHistoryLimit),
			ProgressDeadlineSeconds: ptr.To(progressDeadlineSecs),
			Selector:                &metav1.LabelSelector{MatchLabels: map[string]string{LabelName: w.Name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  w.Name,
						Image: w.Image,
						Ports: []corev1.ContainerPort{{ContainerPort: w.Port, Protocol: corev1.ProtocolTCP}},
					}},
				},
			},
		},
	}
}

// mutateDeployment 把期望值写进 live Deployment，返回是否有变化。
// 只动自己管理的字段；selector 永不修改（不可变，且由资源名派生天然稳定）
func mutateDeployment(d *appsv1.Deployment, w *Workload) bool {
	changed := false
	if *d.Spec.Replicas != w.Replicas {
		*d.Spec.Replicas = w.Replicas
		changed = true
	}
	if len(d.Spec.Template.Spec.Containers) == 0 || len(d.Spec.Template.Spec.Containers[0].Ports) != 1 {
		return changed // 非本组件创建的结构，不动容器
	}
	ctr := &d.Spec.Template.Spec.Containers[0]
	if ctr.Image != w.Image {
		ctr.Image = w.Image
		changed = true
	}
	if ctr.Ports[0].ContainerPort != w.Port {
		ctr.Ports[0].ContainerPort = w.Port
		changed = true
	}
	want := templateLabels(w)
	if !maps.Equal(d.Spec.Template.Labels, want) {
		d.Spec.Template.Labels = want
		d.Labels = want
		changed = true
	}
	return changed
}
