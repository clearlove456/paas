package service

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "github.com/clearlove456/paas/api/paas/v1"
	"github.com/clearlove456/paas/internal/biz"

	"github.com/go-kratos/kratos/v2/log"
)

// AppService 应用服务
type AppService struct {
	v1.UnimplementedAppServiceServer

	uc  *biz.AppUsecase
	log *log.Helper
}

// NewAppService 注入 usecase
func NewAppService(uc *biz.AppUsecase, logger log.Logger) *AppService {
	return &AppService{uc: uc, log: log.NewHelper(log.With(logger, "module", "service/app"))}
}

// appPB biz 对象转 pb
func appPB(a *biz.App) *v1.App {
	if a == nil {
		return nil
	}
	return &v1.App{
		Id:            a.ID,
		Name:          a.Name,
		Description:   a.Description,
		RepoUrl:       a.RepoURL,
		Image:         a.Image,
		Replicas:      a.Replicas,
		ContainerPort: a.ContainerPort,
		Env:           a.Env,
		Status:        a.Status,
		CreatedAt:     timestamppb.New(a.CreatedAt),
		UpdatedAt:     timestamppb.New(a.UpdatedAt),
	}
}

// CreateApp 创建应用
func (s *AppService) CreateApp(ctx context.Context, req *v1.CreateAppRequest) (*v1.CreateAppReply, error) {
	a := req.GetApp()
	s.log.Infof("CreateApp: %s", a.GetName())
	out, err := s.uc.CreateApp(ctx, &biz.App{
		Name:          a.GetName(),
		Description:   a.GetDescription(),
		RepoURL:       a.GetRepoUrl(),
		Image:         a.GetImage(),
		Replicas:      a.GetReplicas(),
		ContainerPort: a.GetContainerPort(),
		Env:           a.GetEnv(),
		Status:        a.GetStatus(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateAppReply{App: appPB(out)}, nil
}

// UpdateApp 更新应用
func (s *AppService) UpdateApp(ctx context.Context, req *v1.UpdateAppRequest) (*v1.UpdateAppReply, error) {
	a := req.GetApp()
	out, err := s.uc.UpdateApp(ctx, &biz.App{
		ID:            a.GetId(),
		Name:          a.GetName(),
		Description:   a.GetDescription(),
		RepoURL:       a.GetRepoUrl(),
		Image:         a.GetImage(),
		Replicas:      a.GetReplicas(),
		ContainerPort: a.GetContainerPort(),
		Env:           a.GetEnv(),
		Status:        a.GetStatus(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.UpdateAppReply{App: appPB(out)}, nil
}

// DeleteApp 删除应用
func (s *AppService) DeleteApp(ctx context.Context, req *v1.DeleteAppRequest) (*v1.DeleteAppReply, error) {
	return &v1.DeleteAppReply{}, s.uc.DeleteApp(ctx, req.GetId())
}

// GetApp 查询应用
func (s *AppService) GetApp(ctx context.Context, req *v1.GetAppRequest) (*v1.GetAppReply, error) {
	out, err := s.uc.GetApp(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetAppReply{App: appPB(out)}, nil
}

// ListApp 分页查询应用
func (s *AppService) ListApp(ctx context.Context, req *v1.ListAppRequest) (*v1.ListAppReply, error) {
	items, total, err := s.uc.ListApp(ctx, req.GetPage(), req.GetPageSize(), req.GetEnv(), req.GetKeyword())
	if err != nil {
		return nil, err
	}
	reply := &v1.ListAppReply{Total: total, Items: make([]*v1.App, 0, len(items))}
	for _, a := range items {
		reply.Items = append(reply.Items, appPB(a))
	}
	return reply, nil
}

// PublishApp 发布应用
func (s *AppService) PublishApp(ctx context.Context, req *v1.PublishAppRequest) (*v1.PublishAppReply, error) {
	s.log.Infof("PublishApp: id=%d", req.GetId())
	out, err := s.uc.PublishApp(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.PublishAppReply{App: appPB(out)}, nil
}

// StopApp 停止应用
func (s *AppService) StopApp(ctx context.Context, req *v1.StopAppRequest) (*v1.StopAppReply, error) {
	s.log.Infof("StopApp: id=%d", req.GetId())
	out, err := s.uc.StopApp(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.StopAppReply{App: appPB(out)}, nil
}

// ScaleApp 扩缩容
func (s *AppService) ScaleApp(ctx context.Context, req *v1.ScaleAppRequest) (*v1.ScaleAppReply, error) {
	s.log.Infof("ScaleApp: id=%d replicas=%d", req.GetId(), req.GetReplicas())
	out, err := s.uc.ScaleApp(ctx, req.GetId(), req.GetReplicas())
	if err != nil {
		return nil, err
	}
	return &v1.ScaleAppReply{App: appPB(out)}, nil
}

// RollbackApp 回滚上一个版本
func (s *AppService) RollbackApp(ctx context.Context, req *v1.RollbackAppRequest) (*v1.RollbackAppReply, error) {
	s.log.Infof("RollbackApp: id=%d", req.GetId())
	out, err := s.uc.RollbackApp(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.RollbackAppReply{App: appPB(out)}, nil
}

// GetAppDeployStatus 查询部署实时状态
func (s *AppService) GetAppDeployStatus(ctx context.Context, req *v1.GetAppDeployStatusRequest) (*v1.GetAppDeployStatusReply, error) {
	a, st, err := s.uc.GetDeployStatus(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	reply := &v1.GetAppDeployStatusReply{
		AppId:            a.ID,
		Env:              a.Env,
		Namespace:        st.Namespace,
		DesiredReplicas:  st.Desired,
		UpdatedReplicas:  st.Updated,
		ReadyReplicas:    st.Ready,
		AvailableReplicas: st.Available,
		Image:            st.Image,
		NodePort:         st.NodePort,
		Conditions:       make([]*v1.DeployCondition, 0, len(st.Conditions)),
	}
	for _, c := range st.Conditions {
		reply.Conditions = append(reply.Conditions, &v1.DeployCondition{
			Type: c.Type, Status: c.Status, Reason: c.Reason, Message: c.Message,
		})
	}
	return reply, nil
}
