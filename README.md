# 天文瞬变事件警报与后续观测

只使用Python标准库和SQLite的模块化服务，默认端口`8338`。支持全天巡天来源、候选事件去重、坐标与亮度测量合并、优先级计算、观测申请、望远镜排程、撤回、重分类、修正、观测队冲突、角色权限和审计。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：状态机、优先级、测量合并和排程冲突约束。
- `src/repository.py`：SQLite持久化、乐观锁、幂等和审计查询。
- `src/service.py`：用例编排、权限校验和版本控制。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8338
```

## 核心对象

`source`为巡天来源，`candidate`为瞬变候选，`telescope`为望远镜，`observation`为后续观测申请。

## 广播账（订阅 / 投递 / 回执）

台站按来源和亮度订阅预警。`subscription`为订阅（`station_id` + `source_id` + `max_magnitude`，只推送星等亮于阈值的候选），`delivery`为投递，`broadcast_batch`为批次。

- 候选`triage`（确认）时按当前订阅生成投递；同一候选同一台站只发一次（部分唯一索引保证）。
- 台站`confirm`（确认）或`reject`（拒收）后留回执；非台站本人不能回执（`403`），两人同时回执只成一份（乐观锁，`409`）。
- 发送失败保留待投递项，按原批次`retry`重试，不重建、不重复。
- 候选`withdraw`作废未回执投递；`reclassify`或订阅阈值改动后，未回执的投递作废并按新依据重算（新批次），已回执的保留。
- 服务启动时自动回填：已确认但无投递的候选补为未投递（幂等）；待投递项持久化，重启后可继续。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `GET /api/audit`
- `POST /api/subscriptions`、`GET /api/subscriptions`、`GET /api/subscriptions/<id>`
- `POST /api/subscriptions/<id>/actions`（改阈值 / 停用，触发重算）
- `GET /api/deliveries`（可按`candidate_id`/`station_id`/`status`/`batch_id`过滤）、`GET /api/deliveries/<id>`
- `POST /api/deliveries/<id>/actions`（`confirm` / `reject` / `mark_sent` / `mark_failed` / `retry`）
- `POST /api/broadcast/backfill`、`GET /api/broadcast/pending`
- `GET /api/broadcast/batches`、`POST /api/broadcast/batches/<id>/retry`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

轨道和观测计划使用简化的字符串时间窗比较，不包含真实天文历表、可见性预报、望远镜控制系统和观测数据存储。
