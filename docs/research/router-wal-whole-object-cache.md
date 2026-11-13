# RouterWAL 整对象读取缓存方案

更新日期：2026-11-14。状态：已改为 Future 通知后统一重试 cache 的简化实现，rebase 到 main 后完整验证通过；RAT 任务被仓库配置禁用。实现入口为 ObjectWALReadCache；共享消费型缓存、首次读取 retainedSlice、重复范围回源、TreeRangeSet 未读跟踪、trim 前缀排除、2 秒未访问过期，以及按堆大小折算的总容量和在途加载上限均已接入。

## 背景与访问模式

RouterWAL 的小范围读取经 DefaultReader 进入 AbstractObjectStorage，后者只合并当前等待队列中的请求，不复用已经发出或完成的范围读取。连续记录的请求分多轮到达时，会产生碎片化 GET，甚至再次读取前一轮已经覆盖的范围。

在三个 AZ 各有一个节点 A、B、C 的模型下，B 写入的 Object-B 按目标节点整理记录，各目标的数据在 bulk 内相邻。默认本地写入模式是 ROUTER_CHANNEL，因此对象可以包含发往 A、B、C 的三类数据。

| Object-B 中的数据 | A 的读取路径 |
| --- | --- |
| B 转发给 A | RouterInV2 → RouterChannel.get |
| B 在本节点处理 | A replay B 的 ConfirmWAL → LinkRecordDecoder → RouterChannel.get |
| B 转发给 C | A replay C 的 ConfirmWAL → LinkRecordDecoder → RouterChannel.get |

正常持续订阅并成功处理这些记录时，A 最终需要 Object-B 中的全部业务记录，但请求通过不同链路、不同批次到达。这个模式支持首次访问时整对象加载，而不是等待各批请求恰好同时进入范围合并队列。

该结论以 A 订阅 B/C 为前提，订阅关系由 main-to-proxy 映射决定。DIRECT 模式下 B 的本地数据绕过 RouterWAL；启动、失败请求、订阅变更和 replay 过载丢弃也可能使部分记录不被读取，因此不能依赖全部读完作为唯一驱逐条件。

## 缓存归属与边界

在 RouterWAL 的 DefaultReader 路径上，首次请求对象中的任意记录时下载整个对象，采用消费型缓存：每段字节只向调用方提供一次共享的 retainedSlice；已经消费或与已消费范围重叠的请求走独立的对象存储范围读取。RouterIn 和 LinkRecordDecoder 经同一个 ChannelProvider 获取同一 owner 的 RouterChannel，可以共享对应 Reader 的缓存。

共享缓存 ObjectWALReadCache 由 DefaultRouterChannelProvider 初始化并持有，通过构造函数注入其创建的各个 ObjectWALService，再交给对应 DefaultReader 使用。同一个 provider 下所有 owner node、所有 bucket 共用一份容量预算和加载去重管理。普通 ConfirmWAL 不注入该缓存，保留现有读取行为。ObjectWALService 收敛为默认三参数与完整五参数两个构造函数；DefaultReader 收敛为使用 ObjectWALConfig 的三参数与带缓存四参数两个构造函数。

管理方式沿用 MultiBucketsRouterChannel.cache 的 Guava Cache 思路：已加载条目按实际字节计重、expireAfterAccess(2, SECONDS) 兜底、removalListener 统一释放缓存持有的引用。另行维护共享的在途加载表，对同 key 的 miss 去重并限制在途资源。缓存项保存对象 buffer、固定大小、未读 RangeSet 和释放状态；不能沿用首次命中即转移整个对象所有权的 take 语义。

容量计算沿用 MultiBucketsRouterChannel.cacheMaxWeight 的向上取整方式，HEAP 使用 Systems.HEAP_MEMORY_SIZE：units = max(1, ceil(HEAP / 6 GiB))，总容量 = units × 100 MiB，在途整对象加载上限 = units × 2。例如 HEAP 为 6/12/24 GiB 时，对应 100/200/400 MiB 缓存及 2/4/8 个整对象加载。不增加超大对象专门策略。

同 key 加载先去重，只有实际发起整对象 GET 的加载者占用一个名额，等待同一加载的请求不重复占用。新对象 miss 时非阻塞尝试取得名额，达到上限则直接退化为原有 DefaultReader 范围读取，不为加载名额排队，不阻塞 Reader EventLoop；降级响应不放入对象缓存。检查同 key 在途加载与竞争名额的过程须协调，避免同对象重复整对象加载或留下无人完成的 Future。加载成功、失败和异常路径均须归还名额。provider 关闭时协调进行中的加载释放资源。该上限约束缓存 miss 的整对象加载，不是对所有底层范围 GET 的新并发限制；降级范围请求仍受原有对象存储并发控制。范围回源响应复制后返回，隔离 AbstractObjectStorage 合并重叠 GET 时的共享可写字节；正常缓存领取仍为零复制。

provider 统一负责缓存关闭，单个 WAL 关闭不关闭共享缓存。缓存 close 后新读取直接旁路到 storage，不抛缓存关闭异常、不新建整对象加载；关闭前已经发出的整对象 GET 不再将结果入缓存，释放结果并完成加载 Future，等待者重试时也走 range 回源。storage 的实际失败仍正常传播。运行期间依赖 Guava 在写入和部分读取时执行自动维护，不新增周期清理任务；关闭时显式 invalidateAll 并 cleanUp。

### 命中快路径与锁边界

- cache 命中：无需全局锁，仅在对应 CachedObject 上原子领取 slice 并更新未读范围。
- loading 命中：ConcurrentHashMap 查找后订阅 Load 的 CompletableFuture<Void>，无需全局锁。Load 不保留 request 列表，没有 READY 状态或 add/drain 协调。
- Load 仅负责整对象 GET 和完成通知。先登记 loading，再调用 Load.start，避免同步完成的 storage 回调早于登记。
- 成功时先放入 cache，再从 loading 移除自己，最后在全局锁外完成 Future。每个等待者随后调用 read(retryReadLoad=true)，统一从 cache 领取记录。
- retryReadLoad=true 时，cache 未命中就直接 range 回源，绝不再创建或等待 Load；这覆盖准入时立即淘汰、过期、读完驱逐及 close，避免循环下载整个对象。
- 加载失败时先移除 loading，再完成加载 Future。等待者通过 handle 忽略该加载异常，仍执行一次 read(retryReadLoad=true)，由 range 回源兜底；range 也失败时，才向调用方传播实际的 range 错误。
- 两者均未命中时，才进入全局短锁复查并登记新加载、检查并发额度。实际 GET、逐条领取和 Future 完成均在全局锁外。

- 并发 miss 共享一个加载 Future；失败后允许重新加载。
- 仅在 RouterWAL 场景启用，不默认改变 ConfirmWAL 等其他 DefaultReader 使用场景。
- 缓存 key 覆盖存储命名空间、bucket、完整对象路径；实例级缓存可由实例本身隔离部分命名空间。
- 缓存有总字节容量限制，并计入或限制在途加载内存，避免只限制已完成条目。
- 对象名中的 64 MiB 对齐 offset 跨度不是对象实际大小，整对象读取应读取到对象结尾。
- 不复制正常首次读取的记录。下游会修改已交付 slice 的字节，因此该范围交付后立即标记为已消费，绝不再从这个缓存项提供重叠范围；所有缓存内解析仅限尚未交付的字节。重复读取回源，不依赖修改的幂等性。
- 淘汰只移除缓存持有的引用；已经交给调用方的数据仍须有效，并有明确引用计数所有权。

## 对象加载与读取流程

1. 根据 owner、epoch、逻辑 offset 定位对象，使用完整存储身份查询共享缓存。
2. 命中时在缓存项锁内检查请求范围是否完整包含于未读集合；若是，获取 retainedSlice 并移除对应未读区间。这些操作必须原子执行。
   若请求已被消费或部分重叠，整个请求走对象存储 range GET，不混用缓存与回源字节，也不覆盖或回填当前缓存项。所有范围回源结果复制后返回，确保底层合并产生的重叠 slice 不会在多个调用方之间共享修改。
3. 未命中时优先复用该 key 的在途加载 Future。若无在途加载，非阻塞尝试取得整对象加载名额：成功则注册共享 Future 并执行从对象起点到结尾的读取；失败则执行原有范围读取，不排队、不填充对象缓存。保留现有布局兼容和 not-found fallback 行为。
4. Load 加载成功后解析并校验对象头及连续 trim 前缀，初始化未读集合，不预扫描全部业务 records；将对象放入 cache，移除 loading，完成 Future。
5. 无论加载 Future 成功或失败，等待者均通过 handle(...).thenCompose(...) 执行 read(retryReadLoad=true)，命中 cache 则领取 slice，否则直接 range 回源。不手动创建中间结果 Future，也不考虑调用方主动 complete/cancel 内部返回 Future 的场景。
6. 未读集合为空时立即按 key 和缓存项实例匹配移除，避免误删同 key 的新条目。

## 未读范围与主动驱逐

采用 Guava TreeRangeSet<Long> 维护未读范围。当前 writer 将对象头与连续 records 直接拼接，无 footer，也不实际填充到文件名的 64 MiB 对齐跨度。对象 records 区域为 [header.size(), header.size() + header.length())，再排除连续 trim 前缀，剩余范围初始化为未读。

首次成功取得并交付 retainedSlice 时移除请求的对象内 [start, end)，不等待下游业务处理完成；这表示该范围已被领取，不保证业务 append 成功。集合为空即提前驱逐。不建立 record bitmap。区间集合支持乱序并避免重复计数；重复或部分重叠请求按回源规则处理。下载整个对象或合并 GET 覆盖某段字节，都不代表这些记录已经被领取。

“已读完”表示预期的首次读取都已由缓存提供，并不证明未来绝不会重试。全部读完后立即驱逐，不增加额外保留期；未来重新访问允许重新 GET。

## Trim 触发记录处理

DefaultWriter.trim0 会追加 streamId=-1、epoch=-1、baseOffset=0、count=0、payload 为空的记录，以触发对象上传并把 trimOffset 持久化到对象头。该记录不对业务暴露；DefaultReader 的批量读取和 RecoverIterator 均跳过它。把整个 body 初始化为未读，会使含 trim 记录的对象无法仅靠正常业务读取达到全覆盖。

当前 RouterWAL 普通记录的 streamId 为目标 nodeId，writer 按 streamId 和 baseOffset 排序。因此 trim 记录位于对象 records 区域的前缀。整对象加载时，仅识别并跳过连续的 trim 前缀，然后把剩余 body 初始化为未读；遇到第一条普通记录即可停止，不扫描全部 records。一个对象可能有多个 trim 记录，必须逐条核验标记、字段、长度与边界，不能无条件跳过固定 57 字节。若只有 trim 记录，则不保留缓存。

## 被动驱逐与资源生命周期

| 触发条件 | 行为 |
| --- | --- |
| 未读 RangeSet 为空 | 立即主动移除 |
| 连续 2 秒无读取访问 | expireAfterAccess 过期 |
| 已加载缓存超过总字节容量 | Guava Cache 按容量策略淘汰 |
| provider 关闭 | 清理缓存，新读取旁路 storage；原在途 GET 完成后释放且不回填 |

2 秒是空闲超时，不是从首次加载起固定存活 2 秒。条目放入缓存时开始计时，每次正常的命中读取刷新访问时间，重复读取同样刷新；内部监控或清理不应通过读取条目的方式续期。该超时只管理已加载缓存，不作为网络 GET 超时，也不取消正常进行中的加载。

Guava 在修改及部分访问时自动维护过期条目，高吞吐缓存不需要额外定时 cleanUp。到期条目不再命中，但不保证在截止瞬间执行 removalListener；整个缓存完全空闲时，过期 buffer 可能保留到后续维护或 provider 关闭。这是本方案接受的惰性释放行为。[Guava CacheBuilder 文档](https://guava.dev/releases/32.0.1-jre/api/docs/com/google/common/cache/CacheBuilder.html)

所有移除路径统一释放缓存持有的引用，读取与释放在缓存项级别协调；取得 slice 失败不更新未读范围。已交付 retainedSlice 由调用方释放，不受缓存移除影响；底层对象内存要等全部 slice 释放后才能实际回收，因此缓存容量权重不等于进程内所有存活对象 buffer 的总量。等待加载的请求也必须在条目被容量淘汰或关闭的竞态下得到明确结果，不能访问已释放 buffer 或无限重试加载。

实现验证需覆盖容量与并发公式、满额时范围降级、满额时同 key 在途复用、加载去重和名额归还、同 key 并发加载、首次 slice 的下游修改、重复与重叠回源隔离、多个 trim 前缀、读完主动移除、2 秒访问续期、容量淘汰与读取并发、slice 生命周期、失败重试及关闭后的晚到结果。

## 采样依据与限制

本次讨论使用 routerwal-1007-simple-trace.csv 和同名 txt。案例 A 为 3 次 GET，案例 B 为 34 次 GET；两者范围并集均为 [48, 8412672)，B 返回总字节量为 12,246,696。它们是挑选案例，不代表整体请求频率。采样未标记 produce/replay 用途，merge 时间也不是准确的网络发包时间。

## 实现验证记录

原始压测基线为 `86f76405b90548f490185733dc0744300fe74d39`，原缓存提交为 `ffdab31731bf2ad314f46eec474f8f999943f77a`。用户反馈该版本 GET 请求减少，但延迟明显上升，未据此认定优化有效。

2026-11-14，`codex/router-wal-object-cache` 分支 rebase 到 main 的 `fcbfb4c6cfbcde669d1242fa11c27c70c82db880`，保留主干的无序 RouterWAL 回调配置以及 source/read-purpose 指标。新一轮对比应分别构建这个主干基线和当前分支 HEAD，不应将旧基线与包含主干新变化的分支直接比较。Rebase 后未重新压测。

本实现随 DefaultRouterChannelProvider 自动启用，无配置开关；对比应保持相同堆大小、流量和拓扑。重点观察 RouterWAL GET 次数及大小分布、下载字节量、produce/replay 延迟、CPU 和直接内存。现有采样只能说明读取形态，不能作为整体收益估计。

新主干的 source/read-purpose 指标默认关闭。若比较 ROUTER_WAL 指标，需要开启 `-Dautomq.s3.metrics.source.enabled=true`；若进一步区分 produce/replay，还需开启 `-Dautomq.s3.metrics.read.purpose.enabled=true`。两边使用相同指标开关。

2026-11-12，本地执行以下命令，最终结果为 BUILD SUCCESSFUL：

```sh
./gradlew --build-cache metadata:S3UnitTest core:S3UnitTest s3stream:test rat checkstyleMain checkstyleTest spotlessJavaCheck
```

ObjectWALReadCacheTest 的 21 个测试全部通过，包含缓存与 Reader 集成、布局 fallback、并发加载去重、满额降级、重复与重叠范围隔离、2 秒访问续期、trim 前缀、容量淘汰、引用计数及关闭；快路径测试证明 cache/loading 命中不依赖全局 monitor，并覆盖完成/登记竞态、缓存发布及额度释放先于 Future 通知、失败回调立即重试、准入淘汰后仅一次 range 回源，并发重复等待者仅领取一次共享 slice，以及整对象加载/解析失败后一次 range 回源和最终 range 错误传播。DefaultRouterChannelProviderTest 验证各 owner/bucket 共用缓存且 provider 关闭后读取旁路 storage。s3stream:test 的依赖包含 SpotBugs，本次通过。构造函数收敛后的 WAL、provider 和 factory 定向测试及最终完整验证也已通过。

完整回归首次暴露 RouterOutV2Test.testDirectModeSplitsLocalAndRemotePartitions 的既有测试发布竞态：先增加完成计数、再写入结果，等待计数的线程可能看到空结果。已将该回调改为先发布结果再增加完成计数，完整验证重跑通过。RAT 命令已执行，但 build.gradle 显式设置 rat.enabled=false，因此任务 SKIPPED，不代表许可证扫描已运行。

2026-11-14，rebase 后在 JDK 17 下再次执行上述完整命令，BUILD SUCCESSFUL。补充的 Reader 集成断言确认 ROUTER_PRODUCE/SNAPSHOT_REPLAY 用途能穿过整对象加载、失败后的有限范围读取、旧布局 fallback 以及缓存复用；21 个缓存测试通过。独立审查未发现新的整合问题。

## 代码入口

- `core/src/main/java/kafka/automq/zerozone/RouterOutV2.java`：本地写入模式与 RouterChannel 写入。
- `core/src/main/java/kafka/automq/zerozone/RouterInV2.java`：produce 读取。
- `core/src/main/java/kafka/automq/zerozone/DefaultRouterChannelProvider.java`：共享 RouterChannel。
- `core/src/main/java/kafka/automq/zerozone/DefaultLinkRecordDecoder.java`：replay 读取与 records 修改。
- `core/src/main/java/kafka/automq/zerozone/SnapshotReadPartitionsManager.java`：订阅关系。
- `s3stream/src/main/java/com/automq/stream/s3/cache/SnapshotReadCache.java`：LinkRecord 回放。
- `s3stream/src/main/java/com/automq/stream/s3/wal/impl/object/ObjectWALReadCache.java`：共享缓存及对象所有权管理。
- `s3stream/src/main/java/com/automq/stream/s3/wal/impl/object/DefaultReader.java`：有限范围读取的缓存入口。
- `s3stream/src/main/java/com/automq/stream/s3/wal/impl/object/WALObjectHeader.java`：对象头和 body 长度。
- `s3stream/src/main/java/com/automq/stream/s3/operator/AbstractObjectStorage.java`：现有范围合并。
