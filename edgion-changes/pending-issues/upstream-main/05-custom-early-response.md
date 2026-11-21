# 05 Custom 上传失败后的提前响应

状态：open。归属：fork。级别：维护优化。

2026-11-21 对照官方 main `3f87e92` 与 fork `e5f4287`。官方 `7de472d` 在 custom 上传失败时继续传递完整提前响应。本地 custom pump 也处理提前响应与上传放弃，并为应用派发终止事件。触发条件与完成判定需要逐分支核对。

[官方提前响应修复](https://github.com/cloudflare/pingora/commit/7de472d)、[本地评审记录](../../review/custom/custom-upstream-early-response-abandonment.md)

## 处理范围

对比 custom writer 失败、响应已选定、响应已完整三种状态，以及升级后半关闭的行为；沿用官方错误优先级和 writer 处理，确认本地应用生命周期的最小补充。

## 验收

验证完整提前响应可送达、不完整响应不会被标为正常结束、阻塞 writer 可退出，应用 `Abandoned` 恰好派发一次。对照官方 custom 场景及本地 `custom_request_contract`，记录对应结果。
