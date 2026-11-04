//go:build wireinject
// +build wireinject

// The build tag makes sure the stub is not built in the final build.

package main

import (
	"github.com/clearlove456/paas/internal/biz"
	"github.com/clearlove456/paas/internal/conf"
	"github.com/clearlove456/paas/internal/data"
	"github.com/clearlove456/paas/internal/data/consumer"
	"github.com/clearlove456/paas/internal/server"
	"github.com/clearlove456/paas/internal/service"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// wireApp init kratos application.
func wireApp(*conf.Server, *conf.Data, log.Logger) (*kratos.App, func(), error) {
	panic(wire.Build(server.ProviderSet, data.ProviderSet, biz.ProviderSet, service.ProviderSet, consumer.ProviderSet, newApp))
}
