package biz

// 分页参数规约
const (
	defaultPageSize = 20
	maxPageSize     = 100
)

// normalizePage 把 page/page_size 换算成 SQL 的 offset/limit，page 从 1 开始。
// pageSize 超过上限返回 ok=false，避免一次拉全表。
func normalizePage(page, pageSize int64) (offset, limit int32, ok bool) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = defaultPageSize
	}
	if pageSize > maxPageSize {
		return 0, 0, false
	}
	return int32((page - 1) * pageSize), int32(pageSize), true
}
