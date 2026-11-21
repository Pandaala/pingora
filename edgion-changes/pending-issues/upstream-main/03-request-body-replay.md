# 03 路由前请求体缓冲与重试重放

状态：open。归属：cross-repository。级别：维护优化。

2026-11-21 对照官方 main `3f87e92` 与 fork `e5f4287`。官方 `early_body_buffer` 和本地 `bf53590` 都能在路由前保留完整请求体并用于重试。普通 retry buffer 的分块存储也有上游优化；它与可供应用访问的完整快照是两个用途。

[early body buffer](https://github.com/cloudflare/pingora/commit/da2af11)、[retry buffer 分块](https://github.com/cloudflare/pingora/commit/8b91e00)、[本地缓冲契约](../../features/request-body-buffering.md)

## 处理范围

比较捕获时机、大小限制、完整快照所有权和每次重试读取方式，判断哪些路径可以共用。逐项检查前缀接实时数据、请求 trailer、取消或截断后的 replay 资格及应用替换 body 后的 framing。

## 验收

使用 `test_request_body_seam` 及相关 core 测试对照 H1/H2、完整/前缀、重试、取消与 trailer；核对 Edgion 路由前后插件读取快照的时机。记录可复用路径与必须保留的行为。
