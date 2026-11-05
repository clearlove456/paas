package k8sx

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/retry"
)

// EnsureService 确保 NodePort 类型的 Service 存在且端口一致（幂等）。
// NodePort 由集群自动分配；更新时永不回写（K8s 不允许把已分配端口改回 0）
func (c *Client) EnsureService(ctx context.Context, w *Workload) error {
	_, err := c.cs.CoreV1().Services(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      w.Name,
				Namespace: w.Namespace,
				Labels:    templateLabels(w),
			},
			Spec: corev1.ServiceSpec{
				Type:     corev1.ServiceTypeNodePort,
				Selector: map[string]string{LabelName: w.Name},
				Ports: []corev1.ServicePort{{
					Name:       "http",
					Port:       w.Port,
					TargetPort: intstr.FromInt32(w.Port),
				}},
			},
		}
		if _, err := c.cs.CoreV1().Services(w.Namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("create service %s/%s: %w", w.Namespace, w.Name, err)
		}
		return nil
	}
	if err != nil {
		return err
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		live, err := c.cs.CoreV1().Services(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !mutateService(live, w) {
			return nil
		}
		_, err = c.cs.CoreV1().Services(w.Namespace).Update(ctx, live, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("update service %s/%s: %w", w.Namespace, w.Name, err)
	}
	return nil
}

// mutateService 同步端口；NodePort 不动。端口结构非本组件创建时不修改
func mutateService(s *corev1.Service, w *Workload) bool {
	if len(s.Spec.Ports) != 1 {
		return false
	}
	changed := false
	p := &s.Spec.Ports[0]
	if p.Port != w.Port {
		p.Port = w.Port
		changed = true
	}
	if p.TargetPort.IntValue() != int(w.Port) {
		p.TargetPort = intstr.FromInt32(w.Port)
		changed = true
	}
	return changed
}
