# 02 TLS offload 握手取消

状态：open。归属：fork。级别：维护优化。

2026-11-21 对照官方 main `3f87e92` 与 fork `e5f4287`。本地 `565c88f` 已移植官方 abort-on-drop 的 BoringSSL/offload runtime 部分，并有额外回归测试。官方改动还覆盖 rustls 与 s2n。

[取消 offload task](https://github.com/cloudflare/pingora/commit/4487f7b)、[本地来源与测试](../../features/downstream-tls-offload.md)

## 处理范围

同步对应代码时以官方 offload helper 和各后端调用为基线，对比本地 runtime 退出、线程释放、feature wiring 和测试后，只保留仍有独立价值的差异。

## 验收

验证成功握手、调用方取消、回调中超时后客户端观察到关闭、独立 runtime 的线程回收。记录各适用 TLS feature 的构建和回归结果，列明保留的本地差异。
