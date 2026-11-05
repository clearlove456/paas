package k8sx

import (
	"context"
	"fmt"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"
)

// revisionAnnotation deployment controller 维护在 ReplicaSet 上的版本号
const revisionAnnotation = "deployment.kubernetes.io/revision"

// RollbackUndo 回滚到上一个发布版本（等价 kubectl rollout undo）：
// 按 Deployment selector 找出全部历史 ReplicaSet，取 revision 次大的，
// 把它的 Pod 模板整体拷回 Deployment，由控制器滚动回去。
// 取「次大」而非「当前减一」：回滚本身也会推进 revision，多次回滚后减一不成立。
func (c *Client) RollbackUndo(ctx context.Context, namespace, name string) error {
	dep, err := c.cs.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("%w: deployment %s/%s", ErrNotFound, namespace, name)
	}
	if err != nil {
		return err
	}

	rsList, err := c.cs.AppsV1().ReplicaSets(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.SelectorFromSet(dep.Spec.Selector.MatchLabels).String(),
	})
	if err != nil {
		return fmt.Errorf("list replicasets: %w", err)
	}

	// revision 最大的是当前版本，次大的是回滚目标
	var maxRS, prevRS *appsv1.ReplicaSet
	maxRev, prevRev := int64(-1), int64(-1)
	for i := range rsList.Items {
		rs := &rsList.Items[i]
		rev, ok := parseRevision(rs)
		if !ok {
			continue // 无版本注解的对象（手工创建等），跳过
		}
		switch {
		case rev > maxRev:
			prevRev, prevRS = maxRev, maxRS
			maxRev, maxRS = rev, rs
		case rev > prevRev:
			prevRev, prevRS = rev, rs
		}
	}
	if prevRS == nil {
		return fmt.Errorf("%w: deployment %s/%s has single revision %d", ErrNoRollbackTarget, namespace, name, maxRev)
	}

	// 整个 Pod 模板深拷回（镜像/端口/标签整套），冲突时重试
	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		fresh, err := c.cs.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		fresh.Spec.Template = *prevRS.Spec.Template.DeepCopy()
		_, err = c.cs.AppsV1().Deployments(namespace).Update(ctx, fresh, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("rollback %s/%s: %w", namespace, name, err)
	}
	return nil
}

// parseRevision 解析 RS 上的 revision 注解，缺失或非法返回 ok=false
func parseRevision(rs *appsv1.ReplicaSet) (int64, bool) {
	v, ok := rs.Annotations[revisionAnnotation]
	if !ok {
		return 0, false
	}
	rev, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return rev, true
}
