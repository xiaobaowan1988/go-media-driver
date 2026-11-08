# Go-Media-Driver 架构重写与演进计划

**项目目标**:为自定义 Raft 共识引擎开发一个纯 Go 语言实现、无锁、零分配、基于 `sendmsg` 微批处理的极低延迟 UDP 通信中间件,完全兼容 AWS EKS (VPC CNI) 标准网络架构。

> 规则:代码与计划不一致时,必须先更新本计划,再改代码。

---

## 阶段一:内存 IPC 与零拷贝基石 (Weeks 1-2)
**目标**:脱离网络层,在本地进程间通过 `/dev/shm` 建立无锁的 LogBuffer 机制。

- [x] **共享内存映射机制**
  - 使用 `golang.org/x/sys/unix` 封装底层 `Mmap` 和 `Munmap`。
  - 实现跨进程的内存文件挂载,确保 Raft 业务端和 Media Driver 进程映射到相同的物理内存页。
- [x] **LogBuffer 结构与 CPU 对齐**
  - 定义标准 Aeron 内存布局:`Active Term` + `Dirty Term` + `Clean Term` + `MetaData`。
  - **性能调优**:在 Go 状态结构体中强行插入 `_ [56]byte` 补齐 64 字节缓存行(Cache Line Padding),彻底杜绝发送端与接收端并发更新偏移量时的 False Sharing(伪共享)。
- [x] **无锁原子操作库封装**
  - 基于 `sync/atomic` 封装带有顺序一致性内存屏障的读取与写入。
  - 开发 `TryClaim` 和 `Commit` 接口,支持 Raft 日志以 CAS(Compare-And-Swap)方式原子追加。

## 阶段二:数据面网络 I/O 极速改造 (Weeks 3-4)
**目标**:抛弃 Go 标准 `net` 库,基于 `sendmsg` 实现免陷入调度器的极速收发。

- [x] **原生非阻塞 Socket 接管**
  - 使用 `unix.Socket` 创建纯净的 IPv4 UDP Socket,并通过 `unix.SetNonblock` 强制开启非阻塞模式。
- [x] **Sender 发送线程 (微批处理)**
  - 构建 Busy Spin(自旋)死循环。
  - 预先分配 `[]unix.Iovec` 与 `[]unix.Mmsghdr` 数组。
  - 从 LogBuffer 扫描连续块(每个数据报 ≤ MTU,可含多个相邻帧),将物理指针直接挂载到 `Iovec`,以**一次 `sendmmsg` 系统调用发出整批数据报**(sendmsg 的批量形态,微批处理的本体),内核直接从共享内存聚集发送,消除用户态数据拼接拷贝。
  - 偏差说明:`unix.SendmsgN` 每次只发一个数据报且每次调用会构造 iovec,无法满足"预分配 Iovec + 零分配 + 批量",故改用 `unix.RawSyscall6(SYS_SENDMMSG)`;Socket 为非阻塞,不会阻塞线程,所以用 RawSyscall(不经调度器 entersyscall)。
- [x] **Receiver 接收线程 (直接拷贝)**
  - 构建 Busy Spin(自旋)死循环,拉取数据。偏差说明:`unix.Recvfrom` 每次返回新的 `Sockaddr`(堆分配),故改用 `RawSyscall6(SYS_RECVFROM)` + 预分配的 `RawSockaddrAny` 记录对端地址。
  - 使用 `unsafe.Pointer` 极速强转提取 Aeron Header 序列号(termID/termOffset)。
  - 计算 Inbound LogBuffer 的绝对物理地址,直接拷贝入位(`fastMemoryCopy` 即 Go 的 `copy`,运行时 memmove);先拷贝帧体,最后原子写入帧长度使其对读者可见;重复帧(重传)与已消费区域的迟到帧被丢弃。

## 阶段三:控制面与流控状态机 (Weeks 5-6)
**目标**:实现网络异常处理、丢包重传与滑动窗口机制。

- [x] **Conductor 线程 (指挥官)**
  - 管理 IPC 命令通道(新建 Publication/Subscription)。
  - 周期性执行资源垃圾回收,清理并轮转 Dirty Term。
- [x] **NAK 抑制与重传机制 (Negative Acknowledgement)**
  - 接收端序列号连续性校验:发现 Term Offset 跳跃时,立即在 Tx 队列生成 NAK 控制报文。
  - 发送端重传缓冲区:收到 NAK 后,直接根据偏移量回溯 LogBuffer,定位历史 Raft 日志并重新发送。
- [x] **Status Message (SM) 滑动窗口**
  - 接收端定时发射 SM 报文,同步当前 Consumer Position 和可用 Receiver Window。
  - 发送端实时计算背压(Backpressure),窗口耗尽时拒绝 Raft 状态机的 `TryClaim` 请求,迫使业务层主动降速。

## 阶段四:彻底击穿 Go 调度器 (Week 7)
**目标**:消除 Go 运行时 (Runtime) 带来的微秒级延迟抖动。

- [x] **OS 物理线程绑定 (Thread Pinning)**
  - 在 Sender 和 Receiver 的 Goroutine 入口处注入 `runtime.LockOSThread()`。
  - 引入 `unix.SchedSetaffinity`,将这两个数据面 Goroutine 硬绑定到特定的物理 CPU 核心上(隔离核),杜绝调度器抢占。
- [x] **热路径零堆分配 (Zero Allocation)**
  - 结合 `go build -gcflags="-m"` 执行严格的逃逸分析。
  - 确保 Sender/Receiver 循环内 **0 次堆内存分配**(无 `make`、无指针切片逃逸、无接口类型断言),完全避开 GC 扫描。

---

## 设计决策(随实现补充,代码与此处不一致时先改此处)

### 阶段一
- 包结构:`shm`(mmap)、`protocol`(帧格式,Aeron 32 字节 DATA 头,小端)、`logbuffer`(日志缓冲)。
- 文件布局:`[term0][term1][term2][metadata 4096B]`,term 长度为 2 的幂(64KiB ~ 1GiB)。metadata 内每个热点计数器独占 64 字节缓存行(`V int64` + `_ [56]byte`)。
- Aeron 的 Active/Dirty/Clean 角色由 `ActiveTermCount` 动态推导:active=`count%3`,下一个要启用的槽位(`(count+1)%3`)必须处于 clean 状态才能轮转。每个槽位有状态机 `clean → active → dirty → clean`,由 CAS 保护,清理方(Conductor/消费者)只清理"已被消费者位置越过"的 dirty 槽位。
- Tail 计数器打包为 `termID<<32 | termOffset`。**追加使用 CAS 循环**(而非 fetch-add),因此永不越过 term 末尾;放不下时由 CAS 获胜者写 padding 帧并轮转。
- `TryClaim(length)` 返回新位置或哨兵错误(`ErrBackPressured`/`ErrAdminAction`/`ErrMaxMessageLength`,预分配,不产生堆分配);`Commit` 以原子 store 写入帧长度(读端以"长度>0"判定可见)。
- 第一版**不做分片**:单帧负载上限 `MTU-32`,更大的 Raft 条目由上层拆分。
- `PublisherLimit`(metadata 字段)用于背压:默认 `MaxInt64`,阶段三由驱动按 SM 更新。

### 阶段二
- 包 `udp`:原始非阻塞 IPv4 UDP Socket(`unix.Socket` + `unix.SetNonblock`),批量发送(Add/Flush)与接收均零分配;EAGAIN 以"0 个"返回而非错误。
- `logbuffer.ScanBlock(pos, maxLen)`:从 `pos` 起收集 ≤ maxLen 的连续已提交帧(不跨 term、不含 padding;padding 帧单独以 32 字节头发送,头里的 frame_length 为整段 padding 长度)。MTU 必须是 32 的倍数。
- `logbuffer.Insert(frame)`:接收端重建;term 窗口为 `[active-1, active+1]`,`active+1` 需槽位已清理才会激活;顺序为"先查重复(长度>0)→再读 ConsumerPosition 判断是否已消费",以避免把迟到帧写进已清理的槽位。
- 协议简化:Sender 的 Socket 对接收端 `connect`;接收端用 `recvfrom` 学到对端地址(阶段三回 SM/NAK 用)。阶段二尚无流控,测试中用"消费者推进 PublisherLimit"代替 SM。
- 驱动类型:`driver.NetworkPublication`(发送侧状态)、`driver.Sender`(duty cycle `DoWork`/`Run`)、`driver.NetworkSubscription`、`driver.Receiver`。空闲策略 `IdleStrategy` 支持 BusySpin(默认)/Yield/Sleep,测试里用 Yield 以免在小核数机器上饿死别的 goroutine。

### 阶段三
- **控制帧**(`protocol`,均 32 字节,头 8 字节与 DATA 共用:length/version/flags/type):`NAK{session,stream,termID,termOffset,length}`、`SM{session,stream,consumerTermID,consumerTermOffset,receiverWindow}`。
- **接收端(`NetworkSubscription`,仅接收线程访问)**:维护 `hwm`(见过的最高位置,来自 DATA 帧或心跳)与 `complete`(连续已收位置,由 `LogBuffer.ContiguousEnd` 增量推进)。`hwm > complete` 即为丢包:`LogBuffer.GapEnd` 扫描零长度区得到缺口,**立即**发 NAK;同一缺口在 `NakInterval` 内抑制重复 NAK(NAK 抑制),超时仍未补齐则重发。SM 每 `SMInterval` 或消费位置前进 ≥ window/4 时发送,携带订阅者位置(`ConsumerPosition`)与 window。对端地址由 `recvfrom` 学得。
- **发送端(`NetworkPublication`,仅发送线程访问)**:同一个 connected socket 也收控制帧。SM → `consumerPos = max(...)`,`PublisherLimit = consumerPos + window`(背压:窗口耗尽时 `TryClaim` 返回 `ErrBackPressured`);初始 limit = 配置的初始窗口。NAK → 校验区间在 `[consumerPos, senderPos]` 内后进入固定大小的重传队列(同起点在 `RetransmitLinger` 内去重),每个 duty cycle 重传一批(直接从 LogBuffer 回溯,复用 `ScanBlock`)。空闲超过 `HeartbeatInterval` 发送只含头的 DATA 帧(`FrameLength=0`,term/offset=senderPos),使尾部丢包也能被发现、使接收端学到对端地址。
- 约束:`window ≤ termLength/2`,保证发送端最多领先消费者半个 term,三个 term 轮转不会死锁;接收端 `Insert` 以"槽位已清理"为激活下一个 term 的门槛。
- **Conductor**(`driver.Conductor`):消费 `<dir>/to-driver.ipc`(一个 LogBuffer,客户端多生产者 CAS 追加,Conductor 单消费者)里的命令 `AddPublication/AddSubscription/Remove`;每个客户端自带响应 LogBuffer 文件(路径写在命令里,驱动单写者、客户端单读者并负责清理),响应 `READY{path}/OK/ERROR{msg}`。周期性 housekeeping:对发布日志以 `consumerPos`(SM)、对订阅日志以 `ConsumerPosition` 调用 `CleanConsumed` 轮转 Dirty Term;移除的发布/订阅在 Sender/Receiver 经过 2 个 duty cycle 的宽限后才关闭映射。
- 简化:不实现 SETUP 握手(两端 term 长度/初始 termID/MTU 由命令静态配置,Raft 集群成员本来就是静态已知);一个驱动只有一个接收端点(`ReceiveIP:ReceivePort`),按 (session,stream) 分发;发布侧每个 publication 一个 connected socket。
- 测试:`TestLossy*` 用用户态 UDP 代理注入丢包/重复/乱序(双向),验证按序无丢失、堆内存不增长、窗口耗尽时背压。

### 阶段四
- **线程绑定**(`driver/pin.go`):Sender/Receiver goroutine 入口先 `runtime.LockOSThread()`,再用 `unix.SchedSetaffinity(0, cpuset)`(pid 0 = 调用线程)绑核。goroutine 退出时**不** `UnlockOSThread`,让 Go 运行时销毁该线程,避免把已改过亲和性的线程还回线程池。`Config.PinThreads` 为 true 时才绑核(`SenderCPU`/`ReceiverCPU`);无论是否绑核,数据面线程都会 LockOSThread。绑核失败(CPU 不在允许集合内等)使 `Start` 返回错误。隔离核本身需要运维配合(内核参数 `isolcpus=`/`nohz_full=`、cgroup cpuset、EKS 上用 static CPU manager policy 且 Guaranteed QoS),代码侧不负责。
- **零分配验证分三层**:① `TestHotPathZeroAlloc`:单线程驱动完整流水线(TryClaim→Commit→Sender→UDP→Receiver→Poll),并通过"偷走部分数据报"制造丢包,使 NAK/SM/重传/心跳路径都在测量范围内,`testing.AllocsPerRun` 必须为 0;② `cmd/escapecheck`:对热路径函数清单做 AST 检查(禁止 `make`/`new`/`append`/`&T{}`/切片或 map 字面量/闭包/类型断言/type switch),并解析 `go build -gcflags=-m` 输出,清单函数体内出现 `escapes to heap`/`moved to heap` 即失败;③ `scripts/escape-check.sh` 把②暴露给 CI。
- 热路径清单:`logbuffer`:TryClaim/Commit/Poll/ScanBlock/Insert/ContiguousEnd/GapEnd/CleanConsumed/rotate;`udp`:Add/Flush/Recv/Send/SendTo;`driver`:Sender.DoWork、NetworkPublication.{doWork,send,pollControl,onSM,onNak,retransmitOne,heartbeat}、Receiver.DoWork、NetworkSubscription.{onDatagram,housekeeping}。

### 里程碑与验收标准
| 里程碑 | 核心产出 | 验收标准 |
| :--- | :--- | :--- |
| **M1: IPC 联通** ✅ | LogBuffer 结构与 CAS 写机制 | 假 Media Driver 进程与 Raft 进程本地内存读写达到千万级 QPS。实测(4 核沙箱,生产者/消费者两个 goroutine,32B 负载):1600~2100 万 msgs/s,0 allocs;跨进程正确性由 `TestCrossProcess` 覆盖。 |
| **M2: I/O 跑通** ⚠️部分验证 | Sendmsg/Recvfrom 自旋机制 | Sender/Receiver 单核 CPU 占用 100%,跨机完成百万级 QPS 裸发。已验证:单机 127.0.0.1 回环,64B 帧、Yield 空闲策略,约 960~1060 万 msgs/s,按序无丢失(`TestSendReceiveInOrder`,10 万条跨 3 个 term)。**未验证:跨机(EKS)、BusySpin 下单核 100% 占用(需阶段四绑核后在独立机器上测)。** |
| **M3: 协议完善** ✅ | 重传与背压控制逻辑 | 丢包注入环境下,系统不发生 OOM,接收端严格按序组装日志。`TestLossyStrictOrderAndBoundedMemory`:双向 5% 丢包 + 2% 重复 + 5% 乱序,6.5 万条变长消息严格按序、无损坏,堆增长 < 8MB(日志为固定大小 mmap,不占堆);`TestBackPressureWhenReceiverStalls` 验证窗口耗尽时 `TryClaim` 返回 `ErrBackPressured`、消费者追上后恢复。丢包下吞吐受 NAK 周期(1ms)限制,约 5~6k msgs/s,属已知特性。 |

---

## 已知限制与后续(未纳入四个阶段)
- 未做跨机/EKS 实测:M2 的"跨机百万级 QPS"与"单核 100%"、以及绑核对尾延迟抖动的实际收益,需在带隔离核的真实机器上测量。
- 单帧不分片(负载 ≤ MTU-32);EKS VPC CNI 的 MTU 通常为 9001,可把 MTU 配到 8960 以内以容纳更大的 Raft 条目。
- 无 SETUP 握手、无多接收端/多播、无驱动崩溃恢复(客户端崩溃后其 resp 文件需外部清理)、无 SM 丢失之外的拥塞控制。
- 数据面使用 Linux 专有接口(sendmmsg、sched_setaffinity、/dev/shm),仅支持 Linux;帧格式为小端主机序(amd64/arm64)。

## 变更记录
- 2026-11-08:初始计划入库。
- 2026-11-08:阶段四完成(`PinThread`/`Config.PinThreads`、`TestHotPathZeroAlloc`、`cmd/escapecheck`;逃逸分析发现并修复了错误路径上 Errno 装箱的堆分配)。
- 2026-11-08:阶段三完成(Conductor、NAK/重传、SM 滑动窗口与背压、心跳、客户端库 `client`)。
- 2026-11-08:阶段二完成(回环验证;跨机待测);偏差:sendmmsg 与原始 recvfrom 代替 SendmsgN/Recvfrom。
- 2026-11-08:阶段一完成;补充阶段一设计决策(CAS 追加、槽位状态机、单帧不分片)。
