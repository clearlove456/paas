package k8sx

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetStatus 聚合 Deployment 与 Service 的实时状态（直读，无缓存）
func (c *Client) GetStatus(ctx context.Context, namespace, name string) (*WorkloadStatus, error) {
	dep, err := c.cs.AppsV1().Deployments(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("%w: deployment %s/%s", ErrNotFound, namespace, name)
	}
	if err != nil {
		return nil, err
	}

	st := &WorkloadStatus{
		Desired:   *dep.Spec.Replicas,
		Updated:   dep.Status.UpdatedReplicas,
		Ready:     dep.Status.ReadyReplicas,
		Available: dep.Status.AvailableReplicas,
	}
	if len(dep.Spec.Template.Spec.Containers) > 0 {
		st.Image = dep.Spec.Template.Spec.Containers[0].Image
	}

	// Service 可能被手工删掉：NodePort 记 0，不算错误
	svc, err := c.cs.CoreV1().Services(namespace).Get(ctx, name, metav1.GetOptions{})
	switch {
	case err == nil && len(svc.Spec.Ports) > 0:
		st.NodePort = svc.Spec.Ports[0].NodePort
	case err != nil && !apierrors.IsNotFound(err):
		return nil, err
	}

	for _, cond := range dep.Status.Conditions {
		if cond.Type == appsv1.DeploymentProgressing || cond.Type == appsv1.DeploymentAvailable {
			st.Conditions = append(st.Conditions, WorkloadCondition{
				Type:    string(cond.Type),
				Status:  string(cond.Status),
				Reason:  cond.Reason,
				Message: cond.Message,
			})
		}
	}
	return st, nil
}
