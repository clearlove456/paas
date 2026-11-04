#!/usr/bin/env bash
# 部署 canal-server（远端服务器）：订阅 MySQL binlog → Kafka
# 缓存一致性兜底通道：DB 变更 → canal → Kafka(paas-canal) → 服务内 canalx 订阅淘汰缓存
# 幂等可重复执行（Properties 追加覆盖，后者生效）。用法: make canal
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/db.env" ]]; then
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/db.env"
fi

DB_HOST="${DB_HOST:-124.220.12.234}"
DB_SSH_USER="${DB_SSH_USER:-ubuntu}"
DB_ROOT_PASS="${DB_ROOT_PASS:?缺少 DB_ROOT_PASS，请配置 scripts/db.env}"
CANAL_PASS="${CANAL_PASS:-Canal!Paas2026}"
CANAL_IMAGE="${CANAL_IMAGE:-canal/canal-server:v1.1.7}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-172.17.0.1:9092}"
CANAL_TOPIC="${CANAL_TOPIC:-paas-canal}"

SSH="ssh -o BatchMode=yes ${DB_SSH_USER}@${DB_HOST}"

echo ">> 1/4 创建 canal MySQL 账号（binlog 订阅权限）"
$SSH "sudo docker exec -i mysql mysql -uroot -p'${DB_ROOT_PASS}'" <<SQL
CREATE USER IF NOT EXISTS 'canal'@'%' IDENTIFIED BY '${CANAL_PASS}';
ALTER USER 'canal'@'%' IDENTIFIED BY '${CANAL_PASS}';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
FLUSH PRIVILEGES;
SQL

echo ">> 2/4 启动 canal-server 容器（内存限制 768m，适配 2C3.6G 小机器）"
$SSH "sudo docker rm -f canal-server >/dev/null 2>&1 || true; \
sudo docker run -d --name canal-server --memory=768m --restart=unless-stopped \
  -e JAVA_OPTS='-Xms256m -Xmx512m' ${CANAL_IMAGE} >/dev/null && echo started"

echo ">> 3/4 写入配置（canal=kafka 模式，只订阅 paas 库）"
sleep 12
$SSH "sudo docker exec canal-server bash -c 'cat >> /home/admin/canal-server/conf/canal.properties <<\"EOF\"

# --- paas overrides（追加覆盖，Properties 后者生效） ---
canal.serverMode = kafka
kafka.bootstrap.servers = ${KAFKA_BOOTSTRAP}
EOF'"
$SSH "sudo docker exec canal-server bash -c 'cat >> /home/admin/canal-server/conf/example/instance.properties <<\"EOF\"

# --- paas overrides ---
canal.instance.master.address = mysql:3306
canal.instance.dbUsername = canal
canal.instance.dbPassword = ${CANAL_PASS}
canal.instance.filter.regex = paas\\\\..*
canal.mq.topic = ${CANAL_TOPIC}
EOF'"

echo ">> 4/4 重启 canal-server 生效"
$SSH "sudo docker restart canal-server >/dev/null && sleep 10 && \
sudo docker logs canal-server --tail 5 2>&1 | tail -5; \
sudo docker ps --filter name=canal-server --format '状态: {{.Status}}'"

echo ">> 完成。验证: 改一条数据后在服务器上执行"
echo "   sudo docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic ${CANAL_TOPIC} --from-beginning --max-messages 2 --timeout-ms 15000"
