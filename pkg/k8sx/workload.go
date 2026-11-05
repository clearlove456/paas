package k8sx

import "errors"

// LabelName k8sx 自动注入的标准标签，同时作为 Deployment/Service 的 selector。
// 资源名不可变 → selector 永不变化 → 不存在 selector 不可变导致的更新冲突
const LabelName = "app.kubernetes.io/name"

// Workload 一个可部署工作负载的中性描述：调用方与 K8s 对象之间的边界类型
type Workload struct {
	Name      string            // 资源名（dns-1123），同时作为 Deployment/Service 名
	Namespace string
	Image     string            // 完整镜像地址（含 tag）
	Replicas  int32
	Port      int32             // 容器监听端口
	Labels    map[string]string // 业务附加标签（managed-by、app-id 等）
}

// WorkloadStatus 部署实时状态（Deployment + Service 聚合，直读 API Server）
type WorkloadStatus struct {
	Desired    int32 // spec.replicas
	Updated    int32 // status.updatedReplicas（已更新到最新模板的副本）
	Ready      int32 // status.readyReplicas
	Available  int32 // status.availableReplicas
	Image      string
	NodePort   int32 // service ports[0].nodePort，未分配为 0
	Conditions []WorkloadCondition
}

// WorkloadCondition 就绪条件（Progressing / Available）
type WorkloadCondition struct {
	Type    string
	Status  string
	Reason  string
	Message string
}

// 哨兵错误：实现方应映射成各自的业务错误码
var (
	ErrNotFound         = errors.New("k8sx: resource not found")
	ErrWaitTimeout      = errors.New("k8sx: wait ready timeout")
	ErrNoRollbackTarget = errors.New("k8sx: no earlier revision to roll back to")
)

// templateLabels Workload 标签 + 自动注入的 name 标签（Deployment 模板/Service 共用）
func templateLabels(w *Workload) map[string]string {
	l := make(map[string]string, len(w.Labels)+1)
	for k, v := range w.Labels {
		l[k] = v
	}
	l[LabelName] = w.Name
	return l
}
