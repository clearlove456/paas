package rmqx

import (
	"encoding/json"
	"strconv"
)

// canal flatMessage：canal-server 投递到 MQ 的扁平 JSON 格式（canal.mq.flatMessage=true，每行一条）。
// 本包只负责解析这个消息格式，不与 canal 建立任何连接。
type canalFlat struct {
	Data     []map[string]string `json:"data"` // 受影响行的字段值（字符串化）
	Database string              `json:"database"`
	Table    string              `json:"table"`
	Type     string              `json:"type"` // INSERT / UPDATE / DELETE / ERASE ...
	IsDDL    bool                `json:"isDdl"`
}

// CanalEvent 从消息中解析出的一行变更（主键已提取）
type CanalEvent struct {
	Database string
	Table    string
	Type     string
	ID       int64
}

// ParseCanal 解析 canal 投递的 binlog 消息，提取所有行事件。
// pk 为主键列名（传空默认 "id"）。
// INSERT/UPDATE/DELETE/ERASE 视为有效事件——INSERT 也要回调：
// 插入前可能被查过并缓存了空值标记（穿透防护），新数据必须让空值失效。
func ParseCanal(body []byte, pk string) []CanalEvent {
	if pk == "" {
		pk = "id"
	}
	var ev canalFlat
	if err := json.Unmarshal(body, &ev); err != nil || ev.IsDDL || len(ev.Data) == 0 {
		return nil
	}
	switch ev.Type {
	case "INSERT", "UPDATE", "DELETE", "ERASE":
	default:
		return nil
	}
	out := make([]CanalEvent, 0, len(ev.Data))
	for _, row := range ev.Data {
		id, err := strconv.ParseInt(row[pk], 10, 64)
		if err != nil {
			continue
		}
		out = append(out, CanalEvent{Database: ev.Database, Table: ev.Table, Type: ev.Type, ID: id})
	}
	return out
}
