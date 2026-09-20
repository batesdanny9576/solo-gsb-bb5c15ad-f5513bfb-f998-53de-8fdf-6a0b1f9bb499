# 设备告警事件段聚合服务

基于 FastAPI + SQLite 的告警汇聚服务：客户端上报离散告警事件，服务按 **source + 可配置静默间隔** 将乱序到达的事件归入同一事件段（segment），并保证 **event_id 幂等去重**——重复提交不改变任何计数。

## 快速开始

一条命令完成依赖安装、数据库初始化与服务启动：

```bash
docker compose up --build
```

服务地址：**http://localhost:8000**（交互式 API 文档：http://localhost:8000/docs）

数据库文件在首次启动时自动建表初始化，并通过命名卷 `alerts-data` 持久化。

## 配置项

均通过环境变量配置（可在 `docker-compose.yml` 的 `environment` 中修改）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SILENCE_INTERVAL_SECONDS` | `300` | 静默间隔（秒）。同一 source 下，事件时间与事件段边界相距不超过该值即归入同一段；超过则开新段 |
| `DATABASE_PATH` | `/data/alerts.db`（容器）/ `data/alerts.db`（本地） | SQLite 数据库文件路径 |

## API

### 提交事件 `POST /events`

```bash
curl -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "edge-gateway-01",
    "event_id": "evt-0001",
    "occurred_at": "2026-09-20T08:00:00Z",
    "severity": "warning"
  }'
```

- `severity` 取值：`debug` / `info` / `warning` / `error` / `critical`（大小写不敏感，`warn`、`fatal` 为别名）
- `occurred_at` 为 ISO 8601 时间；不带时区时按 UTC 处理
- 响应：`{"deduplicated": false, "segment": {...}}`；重复提交同一 `(source, event_id)` 时 `deduplicated: true`，计数不变

### 查询事件段详情 `GET /segments/{id}`

```bash
curl http://localhost:8000/segments/1
```

```json
{
  "id": 1,
  "source": "edge-gateway-01",
  "started_at": "2026-09-20T08:00:00+00:00",
  "ended_at": "2026-09-20T08:04:11+00:00",
  "max_severity": "critical",
  "event_count": 3,
  "revision": 2
}
```

### 列出事件段 `GET /segments`（可选 `?source=` 过滤）

```bash
curl 'http://localhost:8000/segments?source=edge-gateway-01'
```

### 健康检查 `GET /healthz`

## 归段与 revision 规则

- 新事件的 `occurred_at` 落在某段 `[start - silence, end + silence]` 窗口内即并入该段；同时命中多个段（迟到事件桥接）时自动合并为一段，事件数累加。
- 段的 `revision` 初始为 1；**仅当**新事件导致段的起止边界或最高级别发生变化时递增，仅事件数增加不递增。
- `(source, event_id)` 唯一约束保证幂等：重复提交直接返回原段，不改计数、不改 revision。

## 本地开发与测试

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest tests/ -v                 # 全部测试（核心逻辑 + API）
uvicorn app.main:app --reload    # 本地起服务，默认 http://127.0.0.1:8000
```

核心归段逻辑（`app/service.py`）仅依赖标准库，无第三方依赖也可验证：

```bash
python3 -m unittest tests.test_service -v
```

## 项目结构

```
app/
  main.py      # FastAPI 路由与生命周期（启动时幂等建表）
  service.py   # 归段/合并/去重核心逻辑
  schemas.py   # 请求/响应模型与校验
  severity.py  # 告警级别定义与排序
  db.py        # SQLite 连接、schema、事务
  config.py    # 环境变量配置
tests/         # 单元测试与 API 测试
Dockerfile / docker-compose.yml
```
