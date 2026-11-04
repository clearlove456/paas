#!/usr/bin/env bash
# 初始化 paas 数据库：建库、建账号、授权（幂等，可重复执行）
# 用法: make db
# 凭据来源: 环境变量优先，其次 scripts/db.env（不入 git）
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 本地私有配置文件（含密码），不存在则全部依赖环境变量
if [[ -f "$SCRIPT_DIR/db.env" ]]; then
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/db.env"
fi

DB_HOST="${DB_HOST:-124.220.12.234}"
DB_SSH_USER="${DB_SSH_USER:-ubuntu}"
DB_NAME="${DB_NAME:-paas}"
DB_USER="${DB_USER:-paas}"
DB_ROOT_PASS="${DB_ROOT_PASS:?缺少 MySQL root 密码，请设置 DB_ROOT_PASS 或配置 scripts/db.env}"
DB_PASS="${DB_PASS:?缺少 paas 账号密码，请设置 DB_PASS 或配置 scripts/db.env}"

echo ">> 初始化 ${DB_HOST} 上的数据库 ${DB_NAME} ..."

ssh -o BatchMode=yes "${DB_SSH_USER}@${DB_HOST}" \
  "sudo docker exec -i mysql mysql -uroot -p'${DB_ROOT_PASS}'" <<SQL
CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS '${DB_USER}'@'%' IDENTIFIED BY '${DB_PASS}';
ALTER USER '${DB_USER}'@'%' IDENTIFIED BY '${DB_PASS}';
GRANT ALL PRIVILEGES ON \`${DB_NAME}\`.* TO '${DB_USER}'@'%';
FLUSH PRIVILEGES;
SQL

echo ">> 完成：${DB_USER}@${DB_HOST}/${DB_NAME} 已就绪"
