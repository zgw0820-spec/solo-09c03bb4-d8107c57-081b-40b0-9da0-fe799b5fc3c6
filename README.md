# Prometheus Remote Write 1.0 双副本选主接收器

两台 Prometheus（副本 A / B）同时采集同一目标，各自携带 `cluster` 与 `replica`
外部标签，以 **Remote Write 1.0（snappy + protobuf 普通样本）** 推送到本服务。
接收器按 `cluster` 选出唯一供数副本，剥除 `replica` 标签后逐序列存入 SQLite，
并在供数副本停报时自动接替。

## 做成的功能

### 1. Remote Write 1.0 接收
- `POST /api/v1/write`，校验 `X-Prometheus-Remote-Write-Version: 1.0` 与
  `Content-Type: ... application/x-protobuf ... encoding=snappy`。
- 手写 protobuf 子集编解码（`receiver/app/remote_write.py`），接收器自身不依赖
  protobuf 运行时；snappy 压缩用 `cramjam`。
- 普通样本（`Sample{double value, int64 timestamp_ms}`），未知字段（如 2.x 的
  metadata）直接跳过，保持前向兼容。
- 畸形/截断载荷返回 400；空请求体按 204 noop 处理。

### 2. 按 cluster 选供数副本（`receiver/app/db.py`）
- 每个 `cluster` 在 `cluster_state` 中只记录一个 `active_replica`。
- 首个带**新点**的副本成为初始供数方（SELECTED 事件）。
- 供数方未陈旧时，另一副本的批次一律拒绝（角色 `standby`，HTTP 仍为 204，
  响应头 `X-Ingest-Accepted: 0`），不写任何样本。
- **停报接替**：供数副本超过 `STALE_AFTER_SECONDS`（默认 10 秒）没有写入新点
  即判陈旧；此时另一副本必须带来**晚于该序列尾点**的新点才能接替（TAKEOVER）。
  只重发旧点不允许接替。
- 陈旧计时只由“成功写入新点”刷新（`replica_liveness.last_write_ms`），
  重发、心跳、standby 批次都不会续期。

### 3. 逐序列存储与尾点规则
- 剥除 `replica` 后对剩余标签做规范化 JSON + SHA-256 指纹，因此 A/B 写入的是
  **同一条序列**；存库标签中不含 `replica`。
- 每条序列仅接收时间戳严格晚于当前尾点的样本；`ts <= tail` 计为 stale 丢弃，
  重发不入库、不续期。同批次重复时间戳去重，主键
  `(series_id, timestamp_ms)` 兜底幂等。
- SQLite 表：`series`、`samples`（WITHOUT ROWID 复合主键）、`cluster_state`、
  `replica_liveness`、`events`。

### 4. 切换与入库原子、重启沿用
- 一次 `BEGIN IMMEDIATE` 事务内完成“角色判定 + 样本插入 + 活跃副本切换 +
  事件记录”，中途任何异常整体回滚（已用注入故障的测试验证）。
- SQLite 开启 WAL；进程被 SIGKILL 后重启，已提交的 `cluster_state` 与样本
  完整保留（已用崩溃重启脚本验证），重启后继续沿用上次成功写入的状态与
  新鲜/陈旧判定。

### 5. Docker Compose 一键演示
- `docker compose up --build` 启动 4 个服务：
  - `receiver`（本服务，Web 与 API 在 8000 端口，SQLite 存于命名卷）
  - `prom-a` / `prom-b`（prom/prometheus:v2.54.1，`protocol_version: 1.0`，
    外部标签 `cluster=demo, replica=A|B`，5 秒抓取）
  - `demo`（FastAPI 模拟目标，持续递增 `demo_requests_total` 计数器）
- 接收器挂载 `/var/run/docker.sock`，网页按钮通过 Docker SDK 对
  `rw-demo-prom-a` / `rw-demo-prom-b` 执行 `pause` / `unpause`。

### 6. 网页控制台（`http://localhost:8000/`）
- 各 cluster 当前供数副本、供数/陈旧状态、最近新点时间。
- 两个演示副本的容器状态与**暂停 / 恢复**按钮。
- 序列数、样本总数、切换事件数；逐序列 SVG 计数走势图（最近 240 点）。
- 最近 30 条 SELECTED / TAKEOVER 事件，页面 2 秒自动刷新。

## 目录结构

```
.
├── docker-compose.yml          # receiver + prom-a + prom-b + demo
├── prometheus/
│   ├── prometheus-a.yml        # external_labels replica=A, remote_write 1.0
│   └── prometheus-b.yml        # external_labels replica=B
├── receiver/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # FastAPI：/api/v1/write 与 Web/API
│       ├── remote_write.py     # 手写 RW1.0 protobuf + snappy 编解码
│       ├── db.py               # SQLite 存储与选主/接替状态机
│       ├── dockerctl.py        # Docker pause/unpause 封装
│       ├── config.py           # 环境变量配置
│       └── static/index.html   # 演示网页
├── demo/
│   ├── Dockerfile
│   └── app.py                  # 本地模拟 metrics 目标
├── tests/                      # 31 个 pytest 用例
└── scripts/                    # 实跑冒烟与崩溃重启脚本
```

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DB_PATH` | `/data/receiver.db` | SQLite 文件路径 |
| `STALE_AFTER_SECONDS` | `10` | 副本判陈旧的无新点秒数 |
| `REQUIRE_REMOTE_WRITE_HEADER` | `1` | 是否强制 RW1.0 协议头 |
| `DOCKER_BASE_URL` | `unix:///var/run/docker.sock` | Docker 控制入口 |
| `REPLICA_CONTAINERS` | `A=rw-demo-prom-a,B=rw-demo-prom-b` | 副本与容器名映射 |

## HTTP 接口

| 方法/路径 | 说明 |
| --- | --- |
| `POST /api/v1/write` | Remote Write 1.0 入口；响应头回传 `X-Ingest-Role`（initial/active/takeover/standby/rejected/noop）、`X-Ingest-Accepted`、`X-Ingest-Stored`、`X-Ingest-Stale` |
| `GET /api/status` | 各 cluster 活跃副本、陈旧标记、两副本活跃时间 |
| `GET /api/series` | 序列清单、样本数、尾点 |
| `GET /api/series/{id}/samples?limit=240` | 逐序列样本（走势图数据源） |
| `GET /api/events` | 选主/接替事件 |
| `GET /api/replicas` / `POST /api/replicas/{A|B}/{pause|resume}` | 演示副本容器控制 |
| `GET /healthz` | 健康检查 |

## 本地测试与实跑

```bash
# 单元 + HTTP 测试（31 个）
python3 -m pytest tests/ -q

# 真实 uvicorn 进程冒烟：首写 -> standby 拒绝 -> 旧批重发 -> 停报接替
# 先启动：DB_PATH=/tmp/smoke/receiver.db uvicorn app.main:app --port 8080 \
#         --app-dir receiver
SMOKE_BASE=http://127.0.0.1:8080 python3 -I scripts/smoke_live.py

# SIGKILL 崩溃 -> 同库重启 -> 状态沿用且 standby 规则仍生效
python3 -I scripts/crash_restart_test.py
```

## 实测结果（本仓库开发环境）

- `pytest`：**31 passed**，覆盖编解码往返、特殊浮点/UTF-8、坏载荷、
  首写选主、standby 拒绝、10 秒陈旧接替、“陈旧但无新点拒绝接替”、
  重发不续期、事务注入故障原子回滚、SIGKILL 后状态持久化、多 cluster
  独立、并发写不损坏、HTTP 协议头校验等。
- 真实 uvicorn 进程冒烟（`scripts/smoke_live.py`）：
  - A 首批 → `role=initial, stored=2`；
  - B 随即写入 → `role=standby, accepted=0`；
  - A 原样重发 → `stored=0, stale=2`，`last_write_ms` 不变；
  - 等待 10.5 秒后 B 带新点 → `role=takeover, stored=1`，
    库存序列仅 1 条、样本值 `[1.0, 2.0, 3.0]`，事件表含 SELECTED 与 TAKEOVER。
- 崩溃重启（`scripts/crash_restart_test.py`，120 秒窗口）：SIGKILL 后
  WAL 文件留存，重启即恢复；活跃副本仍为 A，窗口内 B 仍判 standby，
  样本数保持 1，标签中无 `replica`。

## 未完成 / 限制项

1. **本沙箱无 Docker 守护进程且无法访问 Docker Hub 镜像仓库**，
   `docker compose up --build` 与真实 Prometheus 对发未能在本机执行；
   已通过 YAML 校验、镜像标签/SKD 可用性核对，以及用字节兼容的
   RW1.0 编码器对真实 uvicorn 进程发请求完成等价验证。首次在有 Docker 的
   环境运行时需要联网拉取 `prom/prometheus:v2.54.1` 与 `python:3.11-slim`。
2. 仅实现 Remote Write **1.0 普通样本**；exemplars、原生直方图、
   RW2.0 的 metadata/created-timestamp 不在范围内（未知字段会被跳过而非报错）。
3. 接收器假定一个请求内所有序列属于同一 `(cluster, replica)`，混合请求
   返回 400；Prometheus 按 external_labels 发送时天然满足。
4. 演示网页的暂停/恢复依赖挂载 docker.socket 的 Compose 部署；脱离该部署时
   接口返回 503，不影响写入链路。
5. SQLite 面向单机演示量级，未做分片/保留策略（retention）；如需长期运行
   应增加样本过期清理任务。
