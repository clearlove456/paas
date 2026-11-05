// Package k8sx 通用 Kubernetes 客户端封装：面向「应用部署」场景的
// Deployment/Service/Namespace 操作，幂等、可等待、可回滚。
// 不含任何业务语义（namespace 规则、label 约定由调用方决定）。
package k8sx

import (
	"fmt"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// 发布等待参数（写死；做成 Client 字段是为了单测能调小）
const (
	DefaultPollInterval = 2 * time.Second
	DefaultWaitTimeout  = 3 * time.Minute
)

// Config 客户端构建配置
type Config struct {
	Kubeconfig string // kubeconfig 文件绝对路径；为空则 in-cluster
}

// Client K8s 操作客户端
type Client struct {
	cs           kubernetes.Interface // 接口类型，便于测试注入 fake clientset
	PollInterval time.Duration
	WaitTimeout  time.Duration
}

// New 构建客户端并做一次连通性检查，
// 把 kubeconfig 错误/集群连不上提前暴露到启动期（fail-fast）
func New(cfg *Config) (*Client, error) {
	var rc *rest.Config
	var err error
	if cfg.Kubeconfig != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("load k8s config: %w", err)
	}
	rc.Timeout = 5 * time.Second // 单请求超时；等待就绪是客户端轮询，单次 Get 很快

	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("build clientset: %w", err)
	}
	if _, err := cs.Discovery().ServerVersion(); err != nil {
		return nil, fmt.Errorf("connect k8s cluster: %w", err)
	}
	return &Client{cs: cs, PollInterval: DefaultPollInterval, WaitTimeout: DefaultWaitTimeout}, nil
}

// NewWithClientset 用现成 clientset 构建（测试注入 fake 用）
func NewWithClientset(cs kubernetes.Interface) *Client {
	return &Client{cs: cs, PollInterval: DefaultPollInterval, WaitTimeout: DefaultWaitTimeout}
}
