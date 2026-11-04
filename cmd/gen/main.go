// gorm-gen 代码生成器：基于 internal/data/model 中的 struct
// 生成类型安全的链式查询代码到 internal/data/query。
// 用法: go run ./cmd/gen  (或 make gen-orm)
package main

import (
	"fmt"

	"gorm.io/gen"

	"github.com/clearlove456/paas/internal/data/model"
)

func main() {
	g := gen.NewGenerator(gen.Config{
		OutPath: "./internal/data/query",
		Mode:    gen.WithoutContext,
	})

	// struct-first 模式：不需要连数据库，直接分析 model 生成
	g.ApplyBasic(model.Asset{}, model.App{})

	g.Execute()
	fmt.Println("gorm query code generated -> internal/data/query")
}
