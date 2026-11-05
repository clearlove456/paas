package k8sx

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

// testClient fake clientset + 调小的等待参数（等待类用例不拖慢测试）
func testClient(objs ...runtime.Object) *Client {
	c := NewWithClientset(fake.NewSimpleClientset(objs...))
	c.PollInterval = time.Millisecond
	c.WaitTimeout = 50 * time.Millisecond
	return c
}

func testWorkload() *Workload {
	return &Workload{
		Name: "demo", Namespace: "paas-dev",
		Image: "nginx:1.25", Replicas: 2, Port: 8080,
		Labels: map[string]string{"app.kubernetes.io/managed-by": "paas", "paas.im/app-id": "1"},
	}
}

func TestEnsureDeploymentCreate(t *testing.T) {
	c := testClient()
	w := testWorkload()
	if err := c.EnsureDeployment(context.Background(), w); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	dep, err := c.cs.AppsV1().Deployments(w.Namespace).Get(context.Background(), w.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// selector 由 name 派生
	if got := dep.Spec.Selector.MatchLabels[LabelName]; got != w.Name {
		t.Errorf("selector = %q, want %q", got, w.Name)
	}
	// 模板标签 = 业务标签 + name 标签
	want := templateLabels(w)
	for k, v := range want {
		if dep.Spec.Template.Labels[k] != v {
			t.Errorf("template label %s = %q, want %q", k, dep.Spec.Template.Labels[k], v)
		}
	}
	ctr := dep.Spec.Template.Spec.Containers[0]
	if ctr.Image != w.Image || *dep.Spec.Replicas != w.Replicas || ctr.Ports[0].ContainerPort != w.Port {
		t.Errorf("deployment fields mismatch: image=%s replicas=%d port=%d",
			ctr.Image, *dep.Spec.Replicas, ctr.Ports[0].ContainerPort)
	}
	if *dep.Spec.RevisionHistoryLimit != revisionHistoryLimit {
		t.Errorf("revisionHistoryLimit = %d, want %d", *dep.Spec.RevisionHistoryLimit, revisionHistoryLimit)
	}
}

func TestEnsureDeploymentUpdateAndIdempotent(t *testing.T) {
	c := testClient()
	w := testWorkload()
	// 先创建 v1
	if err := c.EnsureDeployment(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	// 更新到 v2 + 3 副本
	w.Image, w.Replicas = "nginx:1.26", 3
	if err := c.EnsureDeployment(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	dep, _ := c.cs.AppsV1().Deployments(w.Namespace).Get(context.Background(), w.Name, metav1.GetOptions{})
	if got := dep.Spec.Template.Spec.Containers[0].Image; got != "nginx:1.26" {
		t.Errorf("image = %q, want nginx:1.26", got)
	}
	if *dep.Spec.Replicas != 3 {
		t.Errorf("replicas = %d, want 3", *dep.Spec.Replicas)
	}
	// 同参数再跑一遍（幂等）：无报错、字段保持
	if err := c.EnsureDeployment(context.Background(), w); err != nil {
		t.Fatal(err)
	}
	dep2, _ := c.cs.AppsV1().Deployments(w.Namespace).Get(context.Background(), w.Name, metav1.GetOptions{})
	if got := dep2.Spec.Template.Spec.Containers[0].Image; got != "nginx:1.26" || *dep2.Spec.Replicas != 3 {
		t.Errorf("no-op ensure changed fields: image=%s replicas=%d", got, *dep2.Spec.Replicas)
	}
}

func TestEnsureServiceNodePortPreserved(t *testing.T) {
	c := testClient()
	w := testWorkload()
	ctx := context.Background()
	// 创建：NodePort=0 交给集群分配（fake 不会分配）
	if err := c.EnsureService(ctx, w); err != nil {
		t.Fatal(err)
	}
	svc, _ := c.cs.CoreV1().Services(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
	if svc.Spec.Type != corev1.ServiceTypeNodePort {
		t.Errorf("type = %s, want NodePort", svc.Spec.Type)
	}
	if svc.Spec.Ports[0].NodePort != 0 {
		t.Errorf("created NodePort = %d, want 0 (auto assign)", svc.Spec.Ports[0].NodePort)
	}
	// 模拟集群已分配 30123，然后改端口再 ensure：NodePort 必须保留
	svc.Spec.Ports[0].NodePort = 30123
	if _, err := c.cs.CoreV1().Services(w.Namespace).Update(ctx, svc, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	w.Port = 9090
	if err := c.EnsureService(ctx, w); err != nil {
		t.Fatal(err)
	}
	svc2, _ := c.cs.CoreV1().Services(w.Namespace).Get(ctx, w.Name, metav1.GetOptions{})
	p := svc2.Spec.Ports[0]
	if p.NodePort != 30123 {
		t.Errorf("NodePort = %d, want 30123 (never rewritten)", p.NodePort)
	}
	if p.Port != 9090 || p.TargetPort.IntValue() != 9090 {
		t.Errorf("port/targetPort = %d/%d, want 9090/9090", p.Port, p.TargetPort.IntValue())
	}
}

// readyDep 构造指定就绪状态的 Deployment（单容器单端口）
func readyDep(namespace, name, image string, replicas int32, ready bool) *appsv1.Deployment {
	d := buildDeployment(&Workload{Name: name, Namespace: namespace, Image: image, Replicas: replicas, Port: 8080})
	d.Generation = 1
	d.Status.ObservedGeneration = 1
	if ready {
		d.Status.UpdatedReplicas = replicas
		d.Status.ReadyReplicas = replicas
		d.Status.AvailableReplicas = replicas
	}
	return d
}

func TestWaitDeploymentReady(t *testing.T) {
	ctx := context.Background()

	// 已就绪：立即返回
	c := testClient(readyDep("paas-dev", "demo", "nginx:1", 2, true))
	if err := c.WaitDeploymentReady(ctx, "paas-dev", "demo"); err != nil {
		t.Fatalf("ready: %v", err)
	}

	// 滚动失败（ProgressDeadlineExceeded）：快速失败
	c = testClient(readyDep("paas-dev", "demo", "nginx:1", 2, false))
	dep, _ := c.cs.AppsV1().Deployments("paas-dev").Get(ctx, "demo", metav1.GetOptions{})
	dep.Status.Conditions = []appsv1.DeploymentCondition{{
		Type: appsv1.DeploymentProgressing, Status: corev1.ConditionFalse,
		Reason: "ProgressDeadlineExceeded", Message: " ReplicaSet has timed out progressing",
	}}
	if _, err := c.cs.AppsV1().Deployments("paas-dev").UpdateStatus(ctx, dep, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitDeploymentReady(ctx, "paas-dev", "demo"); !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("progress-failed: got %v, want ErrWaitTimeout", err)
	}

	// 一直不就绪：到达 deadline 后 ErrWaitTimeout
	c = testClient(readyDep("paas-dev", "demo", "nginx:1", 2, false))
	if err := c.WaitDeploymentReady(ctx, "paas-dev", "demo"); !errors.Is(err, ErrWaitTimeout) {
		t.Fatalf("timeout: got %v, want ErrWaitTimeout", err)
	}

	// 不存在：ErrNotFound
	c = testClient()
	if err := c.WaitDeploymentReady(ctx, "paas-dev", "demo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: got %v, want ErrNotFound", err)
	}
}

// historyRS 构造历史 ReplicaSet；rev 为空表示无 revision 注解（脏数据）
func historyRS(namespace, name, image, rev string) *appsv1.ReplicaSet {
	labels := map[string]string{LabelName: name}
	annotations := map[string]string{}
	if rev != "" {
		annotations[revisionAnnotation] = rev
	}
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name + "-" + image, // RS 名按模板区分，模拟不同 hash
			Namespace:   namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: buildDeployment(&Workload{Name: name, Namespace: namespace, Image: image, Replicas: 1, Port: 8080}).Spec.Template,
		},
	}
}

func TestRollbackUndo(t *testing.T) {
	ctx := context.Background()

	// 版本历史 v1/v2/v3：当前模板是 v3（rev 3），回滚应选中 v2（次大 revision）
	rs1 := historyRS("paas-dev", "demo", "nginx:1", "1")
	rs2 := historyRS("paas-dev", "demo", "nginx:2", "2")
	rs3 := historyRS("paas-dev", "demo", "nginx:3", "3")
	// 无注解的脏 RS，应被跳过
	dirty := historyRS("paas-dev", "demo", "nginx:0", "")

	c := testClient(readyDep("paas-dev", "demo", "nginx:3", 1, true), rs1, rs2, rs3, dirty)
	if err := c.RollbackUndo(ctx, "paas-dev", "demo"); err != nil {
		t.Fatal(err)
	}
	dep, _ := c.cs.AppsV1().Deployments("paas-dev").Get(ctx, "demo", metav1.GetOptions{})
	if got := dep.Spec.Template.Spec.Containers[0].Image; got != "nginx:2" {
		t.Errorf("rolled back to %q, want nginx:2", got)
	}

	// 只有一个版本：无可回滚
	c = testClient(readyDep("paas-dev", "demo", "nginx:1", 1, true), rs1)
	if err := c.RollbackUndo(ctx, "paas-dev", "demo"); !errors.Is(err, ErrNoRollbackTarget) {
		t.Fatalf("single revision: got %v, want ErrNoRollbackTarget", err)
	}

	// Deployment 不存在
	c = testClient()
	if err := c.RollbackUndo(ctx, "paas-dev", "demo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing deployment: got %v, want ErrNotFound", err)
	}
}

func TestDeleteWorkloadIdempotent(t *testing.T) {
	c := testClient()
	ctx := context.Background()
	for i := 0; i < 2; i++ { // 删两次都不报错
		if err := c.DeleteWorkload(ctx, "paas-dev", "demo"); err != nil {
			t.Fatalf("delete #%d: %v", i+1, err)
		}
	}
}

func TestGetStatus(t *testing.T) {
	ctx := context.Background()
	dep := readyDep("paas-dev", "demo", "nginx:1.27", 3, true)
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "demo", Namespace: "paas-dev"},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Ports:    []corev1.ServicePort{{Port: 8080, TargetPort: intstr.FromInt32(8080), NodePort: 31234}},
		},
	}
	c := testClient(dep, svc)

	st, err := c.GetStatus(ctx, "paas-dev", "demo")
	if err != nil {
		t.Fatal(err)
	}
	if st.Desired != 3 || st.Updated != 3 || st.Ready != 3 || st.Available != 3 {
		t.Errorf("replicas = %d/%d/%d/%d, want 3/3/3/3", st.Desired, st.Updated, st.Ready, st.Available)
	}
	if st.Image != "nginx:1.27" || st.NodePort != 31234 {
		t.Errorf("image=%s nodePort=%d", st.Image, st.NodePort)
	}

	// Service 缺失：NodePort=0 不报错
	c2 := testClient(readyDep("paas-dev", "demo", "nginx:1", 1, true))
	st2, err := c2.GetStatus(ctx, "paas-dev", "demo")
	if err != nil {
		t.Fatal(err)
	}
	if st2.NodePort != 0 {
		t.Errorf("nodePort = %d, want 0", st2.NodePort)
	}

	// Deployment 缺失：ErrNotFound
	c3 := testClient()
	if _, err := c3.GetStatus(ctx, "paas-dev", "demo"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing deployment: got %v, want ErrNotFound", err)
	}
}
