# Go-Media-Driver 架构重写与演进计划

**项目目标**:为自定义 Raft 共识引擎开发一个纯 Go 语言实现、无锁、零分配、基于 `sendmsg` 微批处理的极低延迟 UDP 通信中间件,完全兼容 AWS EKS (VPC CNI) 标准网络架构。

> 规则:代码与计划不一致时,必须先更新本计划,再改代码。

---

## 阶段一:内存 IPC 与零拷贝基石 (Weeks 1-2)
**目标**:脱离网络层,在本地进程间通过 `/dev/shm` 建立无锁的 LogBuffer 机制。

- [ ] **共享内存映射机制**
  - 使用 `golang.org/x/sys/unix` 封装底层 `Mmap` 和 `Munmap`。
  - 实现跨进程的内存文件挂载,确保 Raft 业务端和 Media Driver 进程映射到相同的物理内存页。
- [ ] **LogBuffer 结构与 CPU 对齐**
  - 定义标准 Aeron 内存布局:`Active Term` + `Dirty Term` + `Clean Term` + `MetaData`。
  - **性能调优**:在 Go 状态结构体中强行插入 `_ [56]byte` 补齐 64 字节缓存行(Cache Line Padding),彻底杜绝发送端与接收端并发更新偏移量时的 False Sharing(伪共享)。
- [ ] **无锁原子操作库封装**
  - 基于 `sync/atomic` 封装带有顺序一致性内存屏障的读取与写入。
  - 开发 `TryClaim` 和 `Commit` 接口,支持 Raft 日志以 CAS(Compare-And-Swap)方式原子追加。

## 阶段二:数据面网络 I/O 极速改造 (Weeks 3-4)
**目标**:抛弃 Go 标准 `net` 库,基于 `sendmsg` 实现免陷入调度器的极速收发。

- [ ] **原生非阻塞 Socket 接管**
  - 使用 `unix.Socket` 创建纯净的 IPv4 UDP Socket,并通过 `unix.SetNonblock` 强制开启非阻塞模式。
- [ ] **Sender 发送线程 (微批处理)**
  - 构建 Busy Spin(自旋)死循环。
  - 预先分配 `[]unix.Iovec` 数组。
  - 从 LogBuffer 扫描连续块,将物理指针直接挂载到 `Iovec`,调用 `unix.SendmsgN` 触发网卡 DMA 聚集发送(Scatter/Gather),消除用户态数据拼接拷贝。
- [ ] **Receiver 接收线程 (直接拷贝)**
  - 构建 Busy Spin(自旋)死循环,使用 `unix.Recvfrom` 拉取数据。
  - 使用 `unsafe.Pointer` 极速强转提取 Aeron Header 序列号。
  - 计算 Inbound LogBuffer 的绝对物理地址,通过纯 CPU 缓存完成 `fastMemoryCopy`。

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

### 里程碑与验收标准
| 里程碑 | 核心产出 | 验收标准 |
| :--- | :--- | :--- |
| **M1: IPC 联通** | LogBuffer 结构与 CAS 写机制 | 假 Media Driver 进程与 Raft 进程本地内存读写达到千万级 QPS。 |
| **M2: I/O 跑通** | Sendmsg/Recvfrom 自旋机制 | Sender/Receiver 单核 CPU 占用 100%,跨机完成百万级 QPS 裸发。 |
| **M3: 协议完善** | 重传与背压控制逻辑 | 丢包注入环境下,系统不发生 OOM,接收端严格按序组装日志。 |

---

## 变更记录
- 2026-11-08:初始计划入库。
