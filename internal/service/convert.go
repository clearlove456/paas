package service

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// timePB time.Time 转 pb Timestamp；零值返回 nil，避免前端拿到 1970-01-01
func timePB(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}
