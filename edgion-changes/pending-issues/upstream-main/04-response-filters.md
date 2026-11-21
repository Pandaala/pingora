# 04 异步响应 body 和 trailer 回调

状态：open。归属：cross-repository。级别：维护优化。

2026-11-21 对照官方 main `3f87e92` 与 fork `e5f4287`。官方与本地 `a0d34a5` 均可等待上游 body/trailer 回调。官方还把下游 `response_body_filter` 改成 async；本地上游回调有额外的 `ResponseBodySink`、输出限额和终止事件。

[异步响应过滤](https://github.com/cloudflare/pingora/commit/018abe0)、[接口说明](https://github.com/cloudflare/pingora/commit/6cc56b6)、[本地流式契约](../../features/response-body-streaming.md)

## 处理范围

逐个对照 `ProxyHttp` 的上下游 body/trailer 签名、默认实现与调用位置。优先沿用官方 await 位置，明确 sink、多块输出、cache 顺序和 trailer 前终止事件所需的扩展。

## 验收

运行 `test_upstream_response_body_sink` 和 terminal dispatch 回归，验证 live/cache hit、Trailer/Done、异常退出、默认路径及输出上限。编译 Edgion 实际的响应过滤器实现并记录适配。
