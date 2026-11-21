# 06 Custom 未完成响应的会话清理

状态：open。归属：fork。级别：维护优化。

2026-11-21 对照官方 main `3f87e92` 与 fork `e5f4287`。官方 `33a9e21` 对 setup 失败、转发任务 join 失败等出口执行 custom session abandon。本地 `a0d34a5` 等改动也管理未完成响应与上传的退出；需要确认真正共用的清理路径。

[官方会话清理](https://github.com/cloudflare/pingora/commit/33a9e21)、[本地 Custom 契约覆盖](../../review/custom/custom-request-contract-coverage.md)

## 处理范围

追踪 `proxy_to_custom_upstream` 的会话所有权、调用方及重试出口，区分底层 session 的 abandon 与发送给应用的终止事件。检查上游 cache-fill 取消与重试是否已经接管清理，再决定哪些本地分支可以移除。

## 验收

验证 setup/join 失败、未完成响应、成功响应、缓存填充已取消后重试。分别断言 session abandon 次数、复用决定和应用终止事件，记录能合并及应独立保留的出口。
