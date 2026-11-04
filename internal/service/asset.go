package service

import (
	"context"

	v1 "github.com/clearlove456/paas/api/paas/v1"
	"github.com/clearlove456/paas/internal/biz"

	"github.com/go-kratos/kratos/v2/log"
)

// AssetService 资产服务：实现 proto 生成的接口，只做 DTO 转换和调用 biz
type AssetService struct {
	v1.UnimplementedAssetServiceServer

	uc  *biz.AssetUsecase
	log *log.Helper
}

// NewAssetService 注入 usecase
func NewAssetService(uc *biz.AssetUsecase, logger log.Logger) *AssetService {
	return &AssetService{uc: uc, log: log.NewHelper(log.With(logger, "module", "service/asset"))}
}

// assetPB biz 对象转 pb（PO 不允许穿透到 service）
func assetPB(a *biz.Asset) *v1.Asset {
	if a == nil {
		return nil
	}
	return &v1.Asset{
		Id:          a.ID,
		Hostname:    a.Hostname,
		Ip:          a.IP,
		Env:         a.Env,
		Region:      a.Region,
		Cpu:         a.CPU,
		Memory:      a.Memory,
		Os:          a.OS,
		Status:      a.Status,
		Description: a.Description,
		CreatedAt:   timePB(a.CreatedAt),
		UpdatedAt:   timePB(a.UpdatedAt),
	}
}

// CreateAsset 创建资产
func (s *AssetService) CreateAsset(ctx context.Context, req *v1.CreateAssetRequest) (*v1.CreateAssetReply, error) {
	a := req.GetAsset()
	s.log.Infof("CreateAsset: %s", a.GetHostname())
	out, err := s.uc.CreateAsset(ctx, &biz.Asset{
		Hostname:    a.GetHostname(),
		IP:          a.GetIp(),
		Env:         a.GetEnv(),
		Region:      a.GetRegion(),
		CPU:         a.GetCpu(),
		Memory:      a.GetMemory(),
		OS:          a.GetOs(),
		Status:      a.GetStatus(),
		Description: a.GetDescription(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.CreateAssetReply{Asset: assetPB(out)}, nil
}

// UpdateAsset 更新资产
func (s *AssetService) UpdateAsset(ctx context.Context, req *v1.UpdateAssetRequest) (*v1.UpdateAssetReply, error) {
	a := req.GetAsset()
	out, err := s.uc.UpdateAsset(ctx, &biz.Asset{
		ID:          a.GetId(),
		Hostname:    a.GetHostname(),
		IP:          a.GetIp(),
		Env:         a.GetEnv(),
		Region:      a.GetRegion(),
		CPU:         a.GetCpu(),
		Memory:      a.GetMemory(),
		OS:          a.GetOs(),
		Status:      a.GetStatus(),
		Description: a.GetDescription(),
	})
	if err != nil {
		return nil, err
	}
	return &v1.UpdateAssetReply{Asset: assetPB(out)}, nil
}

// DeleteAsset 删除资产
func (s *AssetService) DeleteAsset(ctx context.Context, req *v1.DeleteAssetRequest) (*v1.DeleteAssetReply, error) {
	return &v1.DeleteAssetReply{}, s.uc.DeleteAsset(ctx, req.GetId())
}

// GetAsset 查询资产
func (s *AssetService) GetAsset(ctx context.Context, req *v1.GetAssetRequest) (*v1.GetAssetReply, error) {
	out, err := s.uc.GetAsset(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	return &v1.GetAssetReply{Asset: assetPB(out)}, nil
}

// ListAsset 分页查询资产
func (s *AssetService) ListAsset(ctx context.Context, req *v1.ListAssetRequest) (*v1.ListAssetReply, error) {
	items, total, err := s.uc.ListAsset(ctx, req.GetPage(), req.GetPageSize(), req.GetEnv(), req.GetKeyword())
	if err != nil {
		return nil, err
	}
	reply := &v1.ListAssetReply{Total: total, Items: make([]*v1.Asset, 0, len(items))}
	for _, a := range items {
		reply.Items = append(reply.Items, assetPB(a))
	}
	return reply, nil
}
