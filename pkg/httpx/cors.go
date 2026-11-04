// Package httpx 提供与业务无关的通用 HTTP 工具。
package httpx

import (
	stdhttp "net/http"
)

// CORSFilter 开发期跨域支持：前端 dev server (localhost:5173) 调用后端 API。
// 可作为 Kratos http.Filter 直接使用；生产环境应由网关/Nginx 统一处理跨域。
func CORSFilter(next stdhttp.Handler) stdhttp.Handler {
	return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == stdhttp.MethodOptions {
			w.WriteHeader(stdhttp.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
