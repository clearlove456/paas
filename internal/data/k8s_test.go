package data

import (
	"context"
	"errors"
	"testing"
	"time"

	v1 "github.com/clearlove456/paas/api/paas/v1"
	"github.com/clearlove456/paas/internal/biz"
	"github.com/clearlove456/paas/pkg/k8sx"

	"k8s.io/client-go/kubernetes/fake"
)

// testDeployer fake clientset 构建的部署器（等待参数调小）
func testDeployer() *appDeployer {
	c := k8sx.NewWithClientset(fake.NewSimpleClientset())
	c.PollInterval = time.Millisecond
	c.WaitTimeout = 30 * time.Millisecond
	return &appDeployer{k8s: c}
}

func TestWorkloadOf(t *testing.T) {
	w := workloadOf(&biz.App{ID: 7, Name: "user-center", Image: "nginx:1.25", Replicas: 3, ContainerPort: 8080, Env: "staging"})
	if w.Namespace != "paas-staging" || w.Name != "user-center" || w.Image != "nginx:1.25" || w.Replicas != 3 || w.Port != 8080 {
		t.Errorf("workload mapping mismatch: %+v", w)
	}
	if w.Labels[labelManagedBy] != "paas" || w.Labels[labelAppID] != "7" {
		t.Errorf("labels = %v", w.Labels)
	}
}

func TestDisabledDeployer(t *testing.T) {
	d, err := NewAppDeployer(nil)
	if err != nil {
		t.Fatal(err)
	}
	// 未启用：服务照常启动，但发布动作报错
	if err := d.Deploy(context.Background(), &biz.App{ID: 1, Env: "dev"}); !v1.IsAppDeployFailed(err) {
		t.Fatalf("deploy on disabled: got %v, want APP_DEPLOY_FAILED", err)
	}
}

// Deploy 末尾的 WaitDeploymentReady 在 fake 上必然超时（fake 不会把 status 置为就绪），
// 所以断言超时错误的同时验证资源编排：旧环境清理、新环境建好
func TestDeployEnvMigration(t *testing.T) {
	ctx := context.Background()
	d := testDeployer()

	// 应用先部署在 dev，然后改环境到 prod
	app := &biz.App{ID: 1, Name: "demo", Image: "nginx:1", Replicas: 1, ContainerPort: 8080, Env: "dev"}
	if err := d.Deploy(ctx, app); !v1.IsAppDeployTimeout(err) {
		t.Fatalf("first deploy: got %v, want APP_DEPLOY_TIMEOUT (fake 无法就绪)", err)
	}

	app.Env = "prod"
	if err := d.Deploy(ctx, app); !v1.IsAppDeployTimeout(err) {
		t.Fatalf("migrate deploy: got %v, want APP_DEPLOY_TIMEOUT", err)
	}

	// 旧环境资源已清理
	if _, err := d.k8s.GetStatus(ctx, namespaceOf("dev"), "demo"); !errors.Is(err, k8sx.ErrNotFound) {
		t.Errorf("paas-dev should be cleaned, got %v", err)
	}
	// 新环境 Deployment 已创建
	if _, err := d.k8s.GetStatus(ctx, namespaceOf("prod"), "demo"); err != nil {
		t.Errorf("paas-prod should exist: %v", err)
	}
}

func TestMapDeployErr(t *testing.T) {
	cases := []struct {
		in   error
		want func(error) bool
	}{
		{k8sx.ErrNotFound, v1.IsAppNotDeployed},
		{k8sx.ErrWaitTimeout, v1.IsAppDeployTimeout},
		{k8sx.ErrNoRollbackTarget, v1.IsAppRollbackFailed},
		{errors.New("boom"), v1.IsAppDeployFailed},
		{nil, func(error) bool { return false }},
	}
	for _, c := range cases {
		got := mapDeployErr(c.in)
		if c.in == nil {
			if got != nil {
				t.Errorf("nil in, got %v", got)
			}
			continue
		}
		if !c.want(got) {
			t.Errorf("mapDeployErr(%v) = %v, reason mismatch", c.in, got)
		}
	}
}
