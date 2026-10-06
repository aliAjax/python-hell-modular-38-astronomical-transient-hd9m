# 天文瞬变事件警报与后续观测

只使用Python标准库和SQLite的模块化服务，默认端口`8338`。支持全天巡天来源、候选事件去重、坐标与亮度测量合并、优先级计算、观测申请、望远镜排程、撤回、重分类、修正、观测队冲突、角色权限和审计。台站按来源和亮度阈值订阅预警，候选确认后自动生成投递并记录回执，形成可审计的广播账。

## 模块结构

- `app.py`：参数解析、依赖组装和服务生命周期（启动时恢复并续传待投递项）。
- `src/domain.py`：角色、领域异常和数据对象。
- `src/rules.py`：状态机、优先级、测量合并、排程冲突约束和订阅亮度匹配。
- `src/repository.py`：SQLite持久化、乐观锁、幂等和审计查询。
- `src/service.py`：用例编排、权限校验、版本控制和广播账（生成、作废、重算、派发、回填）。
- `src/http_api.py`：JSON接口和统一错误响应。
- `src/audit.py`：操作审计。
- `static/index.html`：最小演示页面。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8338
```

## 核心对象

`source`为巡天来源，`candidate`为瞬变候选，`telescope`为望远镜，`observation`为后续观测申请，`subscription`为台站预警订阅，`delivery`为预警投递（广播账条目）。

## 广播账

- 台站通过`subscription`按`source_id`（可空，表示全部来源）和`max_magnitude`（亮于该阈值即匹配）订阅；同一台站同一来源范围只能有一份订阅。
- 候选`classify`（确认）时按当前有效订阅生成`delivery`（状态`pending`），记录`batch_id`和匹配依据快照；同一候选同一台站只生成一条。
- `POST /api/dispatch`（可按`batch_id`限定原批次）把待投递项发给台站：成功转`delivered`，失败保留`pending`并记录`last_error`与`attempts`，重试复用原行不产生重复。派发前会先认领为`sending`，崩溃遗留的认领在服务重启时自动回到`pending`继续走。
- 台站对`delivered`的投递执行`acknowledge`或`reject`（需`reason`）留下回执；只有本台站账号（`X-User-Id`等于`station_id`）或`admin`可以回执，并发确认只有一份成功。
- 候选撤回、重分类或订阅阈值改动后，未确认的投递（`pending`/`sending`/`delivered`）作废（`voided`，保留记录）并按新依据重算；已有回执的投递照旧保留，也不会因此重发。
- 升级时，已确认但没有任何广播记录的历史候选在服务启动时自动回填为`pending`投递。

## 接口

- `GET /health`
- `GET /api/<kind>`，可用`?status=`过滤
- `GET /api/entities/<id>`
- `POST /api/<kind>`
- `POST /api/entities/<id>/actions`
- `POST /api/dispatch`，可选`{"batch_id": "..."}`按原批次重试
- `GET /api/audit`

身份通过`X-User-Id`和`X-Role`请求头传入。可选`Idempotency-Key`防止重复创建。`delivery`由系统生成，不能通过`POST /api/<kind>`手工创建。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

轨道和观测计划使用简化的字符串时间窗比较，不包含真实天文历表、可见性预报、望远镜控制系统和观测数据存储。投递的发送方为可注入的回调，默认实现直接视为成功，未对接真实台站通道。
