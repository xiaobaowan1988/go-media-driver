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

- [ ] **Conductor 线程 (指挥官)**
  - 管理 IPC 命令通道(新建 Publication/Subscription)。
  - 周期性执行资源垃圾回收,清理并轮转 Dirty Term。
- [ ] **NAK 抑制与重传机制 (Negative Acknowledgement)**
  - 接收端序列号连续性校验:发现 Term Offset 跳跃时,立即在 Tx 队列生成 NAK 控制报文。
  - 发送端重传缓冲区:收到 NAK 后,直接根据偏移量回溯 LogBuffer,定位历史 Raft 日志并重新发送。
- [ ] **Status Message (SM) 滑动窗口**
  - 接收端定时发射 SM 报文,同步当前 Consumer Position 和可用 Receiver Window。
  - 发送端实时计算背压(Backpressure),窗口耗尽时拒绝 Raft 状态机的 `TryClaim` 请求,迫使业务层主动降速。

## 阶段四:彻底击穿 Go 调度器 (Week 7)
**目标**:消除 Go 运行时 (Runtime) 带来的微秒级延迟抖动。

- [ ] **OS 物理线程绑定 (Thread Pinning)**
  - 在 Sender 和 Receiver 的 Goroutine 入口处注入 `runtime.LockOSThread()`。
  - 引入 `unix.SchedSetaffinity`,将这两个数据面 Goroutine 硬绑定到特定的物理 CPU 核心上(隔离核),杜绝调度器抢占。
- [ ] **热路径零堆分配 (Zero Allocation)**
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

### 里程碑与验收标准
| 里程碑 | 核心产出 | 验收标准 |
| :--- | :--- | :--- |
| **M1: IPC 联通** ✅ | LogBuffer 结构与 CAS 写机制 | 假 Media Driver 进程与 Raft 进程本地内存读写达到千万级 QPS。实测(4 核沙箱,生产者/消费者两个 goroutine,32B 负载):1600~2100 万 msgs/s,0 allocs;跨进程正确性由 `TestCrossProcess` 覆盖。 |
| **M2: I/O 跑通** ⚠️部分验证 | Sendmsg/Recvfrom 自旋机制 | Sender/Receiver 单核 CPU 占用 100%,跨机完成百万级 QPS 裸发。已验证:单机 127.0.0.1 回环,64B 帧、Yield 空闲策略,约 960~1060 万 msgs/s,按序无丢失(`TestSendReceiveInOrder`,10 万条跨 3 个 term)。**未验证:跨机(EKS)、BusySpin 下单核 100% 占用(需阶段四绑核后在独立机器上测)。** |
| **M3: 协议完善** | 重传与背压控制逻辑 | 丢包注入环境下,系统不发生 OOM,接收端严格按序组装日志。 |

---

## 变更记录
- 2026-11-08:初始计划入库。
- 2026-11-08:阶段二完成(回环验证;跨机待测);偏差:sendmmsg 与原始 recvfrom 代替 SendmsgN/Recvfrom。
- 2026-11-08:阶段一完成;补充阶段一设计决策(CAS 追加、槽位状态机、单帧不分片)。
