# paas — 云原生应用管理平台

一个为学习运维开发 / 云原生而生的 PaaS 平台（简化版 Rancher/Spug）：从 CMDB 资产管理起步，
逐步演进到 K8s 应用发布、GitOps 流水线、监控告警，最终自研 Operator。

## 技术栈

| 层 | 选型 |
|---|---|
| 后端框架 | Go + Kratos v2（DDD 分层 / wire 依赖注入 / proto API） |
| ORM | GORM + gorm-gen（类型安全链式查询，代码生成） |
| 存储 | MySQL 8.4（远端服务器 Docker 部署） |
| API | HTTP(JSON) + gRPC 双协议，由 proto 一份定义生成 |
| 前端 | React + TypeScript + Vite + Ant Design（独立仓库 `paas-web`） |

## Roadmap

- [x] **Phase 1** 平台骨架：CMDB 主机资产管理 + 应用管理 CRUD（当前）
- [ ] **Phase 2** 容器化：Dockerfile / Helm Chart，部署进 K8s（kind）
- [ ] **Phase 3** 发布系统：client-go 一键部署 / 扩缩容 / 回滚 / 日志
- [ ] **Phase 4** CI/CD：GitHub Actions 构建镜像 + ArgoCD GitOps
- [ ] **Phase 5** 可观测：Prometheus 指标采集 + Grafana 看板 + 告警推送
- [ ] **Phase 6** Operator：Kubebuilder 自研 Redis Operator

## 项目结构（Kratos 分层）

```
api/paas/v1/         # proto API 定义（一份定义，生成 HTTP+gRPC）
cmd/server/          # 程序入口 + wire 依赖注入
cmd/gen/             # gorm-gen 代码生成器
internal/
  conf/              # 配置结构（conf.proto）
  server/            # HTTP/gRPC server 装配、中间件链
  service/           # DTO <-> BO 转换，薄层
  biz/               # 业务逻辑 + repository 接口定义（依赖倒置核心）
  data/              # repository 实现：GORM + gen 生成的类型安全查询
    model/           # PO（表结构）
    query/           # gorm-gen 生成，勿手改
configs/             # 运行时配置（真实配置不入库，见 config.example.yaml）
third_party/         # proto 依赖（google/api 等）
```

依赖方向：`server → service → biz ← data`，biz 定义接口、data 实现。

## 快速开始

```bash
# 首次准备：复制配置并填入数据库连接
cp configs/config.example.yaml configs/config.yaml

# 生成代码（改了 proto / model 后执行）
make api        # proto -> pb/http/grpc + openapi.yaml
make config     # conf.proto -> conf.pb.go
make gen-orm    # model struct -> internal/data/query
make wire-gen   # 依赖注入

# 本地运行（HTTP :8000 / gRPC :9000）
go run ./cmd/server -conf ./configs

# 验证
curl http://127.0.0.1:8000/api/v1/assets
```

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | /api/v1/assets | 创建资产 |
| PUT | /api/v1/assets/{id} | 更新资产 |
| DELETE | /api/v1/assets/{id} | 删除资产（软删除） |
| GET | /api/v1/assets/{id} | 资产详情 |
| GET | /api/v1/assets?page=1&page_size=20&env=prod&keyword=web | 分页查询 |
| POST | /api/v1/apps | 创建应用 |
| PUT | /api/v1/apps/{id} | 更新应用 |
| DELETE | /api/v1/apps/{id} | 删除应用 |
| GET | /api/v1/apps/{id} | 应用详情 |
| GET | /api/v1/apps | 分页查询 |

完整契约见 `openapi.yaml`（`make api` 自动生成）。

## 错误码

Kratos 风格：`{"code":404,"reason":"ASSET_NOT_FOUND","message":"资产不存在"}`

| reason | HTTP | 说明 |
|---|---|---|
| ASSET_NOT_FOUND | 404 | 资产不存在 |
| ASSET_HOST_CONFLICT | 409 | 主机名已存在 |
| ASSET_INVALID | 400 | 资产参数不合法 |
| APP_NOT_FOUND | 404 | 应用不存在 |
| APP_NAME_CONFLICT | 409 | 应用名已存在 |
| APP_INVALID | 400 | 应用参数不合法 |
