# Flink 状态管理
<nav>
<a href="#一状态分类">一、状态分类</a><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#21-算子状态">2.1 算子状态</a><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#22-键控状态">2.2 键控状态</a><br/>
<a href="#二状态编程">二、状态编程</a><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#21-键控状态">2.1 键控状态</a><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#22-状态有效期">2.2 状态有效期</a><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#23-算子状态">2.3 算子状态</a><br/>
<a href="#三容错机制与故障恢复">三、容错机制与故障恢复</a><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#31-CheckPoint">3.1 CheckPoint</a><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#32-Savepoint">3.2 Savepoint</a><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#33-Savepoint 与 Checkpoint 的区别">3.3 Savepoint 与 Checkpoint 的区别</a><br/>
<a href="#四状态后端">四、状态后端</a><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#41-状态管理器分类">4.1 状态管理器分类</a><br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<a href="#42-配置方式">4.2 配置方式</a><br/>
</nav>


## 一、状态分类

相对于其他流计算框架，Flink 一个比较重要的特性就是其支持有状态计算。即你可以将中间的计算结果进行保存，并提供给后续的计算使用：

<div align="center"> <img width="500px" src="https://gitee.com/heibaiying/BigData-Notes/raw/master/pictures/flink-stateful-stream.png"/> </div>


具体而言，Flink 又将状态 (State) 分为 Keyed State 与 Operator State：

### 2.1 算子状态

算子状态 (Operator State)：顾名思义，状态是和算子进行绑定的，一个算子的状态不能被其他算子所访问到。官方文档上对 Operator State 的解释是：*each operator state is bound to one parallel operator instance*，所以更为确切的说一个算子状态是与一个并发的算子实例所绑定的，即假设算子的并行度是 2，那么其应有两个对应的算子状态：

<div align="center"> <img width="500px" src="https://gitee.com/heibaiying/BigData-Notes/raw/master/pictures/flink-operator-state.png"/> </div>

在访问上，Operator State 需要自己实现 CheckpointedFunction 或 ListCheckpointed 接口。

### 2.2 键控状态

键控状态 (Keyed State) ：是一种特殊的算子状态，即状态是根据 key 值进行区分的，Flink 会为每类键值维护一个状态实例。如下图所示，每个颜色代表不同 key 值，对应四个不同的状态实例。每个 Key 对应一个 State，即一个 Operator 实例处理多个 Key，访问相应的多个 State，并由此就衍生了 Keyed State。Keyed State 只能用在 KeyedStream 的算子中，即在整个程序中没有 keyBy 的过程就没有办法使用 KeyedStream。

<div align="center"> <img src="https://gitee.com/heibaiying/BigData-Notes/raw/master/pictures/flink-keyed-state.png"/> </div>

访问上，Keyed State 通过 RuntimeContext 访问，这需要 Operator 是一个 Rich Function。

## 二、状态编程

### 2.1 键控状态

Flink 提供了以下数据格式来管理和存储键控状态 (Keyed State)：

- **ValueState**：存储单值类型的状态。比如 Wordcount，用 Word 当 Key，State 就是它的 Count。这里面的单个值可能是数值或者字符串。作为单个值，可以使用  `update(T)` 进行更新，并通过 `T value()` 进行检索。 
- **ListState**：存储列表类型的状态。可以使用 `add(T)` 或 `addAll(List)` 添加元素；并通过 `get()` 获得整个列表。
- **ReducingState**：用于存储经过 ReduceFunction 计算后的结果，使用 `add(T)` 增加元素。
- **AggregatingState**：用于存储经过 AggregatingState 计算后的结果，使用 `add(IN)` 添加元素。
- **FoldingState**：已被标识为废弃，会在未来版本中移除，官方推荐使用 `AggregatingState` 代替。
- **MapState**：维护 Map 类型的状态，在 State 上有 put、remove 等。需要注意的是在 MapState 中的 key 和 Keyed state 中的 key 不是同一个。

以上所有增删改查方法不必硬记，在使用时通过语法提示来调用即可。这里给出一个具体的使用示例：假设我们正在开发一个监控系统，当监控数据超过阈值一定次数后，需要发出报警信息。这里之所以要达到一定次数，是因为由于偶发原因，偶尔一次超过阈值并不能代表什么，故需要达到一定次数后才触发报警，这就需要使用到 Flink 的状态编程。相关代码如下：

```java
public class ThresholdWarning extends 
    RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, List<Long>>> {

    // 通过ListState来存储非正常数据的状态
    private transient ListState<Long> abnormalData;
    // 需要监控的阈值
    private Long threshold;
    // 触发报警的次数
    private Integer numberOfTimes;

    ThresholdWarning(Long threshold, Integer numberOfTimes) {
        this.threshold = threshold;
        this.numberOfTimes = numberOfTimes;
    }

    @Override
    public void open(Configuration parameters) {
        // 通过状态名称(句柄)获取状态实例，如果不存在则会自动创建
        abnormalData = getRuntimeContext().getListState(
            new ListStateDescriptor<>("abnormalData", Long.class));
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, List<Long>>> out)
        throws Exception {
        Long inputValue = value.f1;
        // 如果输入值超过阈值，则记录该次不正常的数据信息
        if (inputValue >= threshold) {
            abnormalData.add(inputValue);
        }
        ArrayList<Long> list = Lists.newArrayList(abnormalData.get().iterator());
        // 如果不正常的数据出现达到一定次数，则输出报警信息
        if (list.size() >= numberOfTimes) {
            out.collect(Tuple2.of(value.f0 + " 超过指定阈值 ", list));
            // 报警信息输出后，清空状态
            abnormalData.clear();
        }
    }
}
```

调用自定义的状态监控，这里我们使用 a，b 来代表不同类型的监控数据，分别对其数据进行监控：

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStreamSource<Tuple2<String, Long>> tuple2DataStreamSource = env.fromElements(
    Tuple2.of("a", 50L), Tuple2.of("a", 80L), Tuple2.of("a", 400L),
    Tuple2.of("a", 100L), Tuple2.of("a", 200L), Tuple2.of("a", 200L),
    Tuple2.of("b", 100L), Tuple2.of("b", 200L), Tuple2.of("b", 200L),
    Tuple2.of("b", 500L), Tuple2.of("b", 600L), Tuple2.of("b", 700L));
tuple2DataStreamSource
    .keyBy(0)
    .flatMap(new ThresholdWarning(100L, 3))  // 超过100的阈值3次后就进行报警
    .printToErr();
env.execute("Managed Keyed State");
```

输出如下结果如下：

<div align="center"> <img src="https://gitee.com/heibaiying/BigData-Notes/raw/master/pictures/flink-state-management.png"/> </div>



### 2.2 状态有效期

以上任何类型的 keyed state 都支持配置有效期 (TTL) ，示例如下：

```java
StateTtlConfig ttlConfig = StateTtlConfig
    // 设置有效期为 10 秒
    .newBuilder(Time.seconds(10))  
    // 设置有效期更新规则，这里设置为当创建和写入时，都重置其有效期到规定的10秒
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) 
    /*设置只要值过期就不可见，另外一个可选值是ReturnExpiredIfNotCleanedUp，
     代表即使值过期了，但如果还没有被物理删除，就是可见的*/
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();
ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("abnormalData", Long.class);
descriptor.enableTimeToLive(ttlConfig);
```

### 2.3 算子状态

相比于键控状态，算子状态目前支持的存储类型只有以下三种：

- **ListState**：存储列表类型的状态。
- **UnionListState**：存储列表类型的状态，与 ListState 的区别在于：如果并行度发生变化，ListState 会将该算子的所有并发的状态实例进行汇总，然后均分给新的 Task；而 UnionListState 只是将所有并发的状态实例汇总起来，具体的划分行为则由用户进行定义。
- **BroadcastState**：用于广播的算子状态。

这里我们继续沿用上面的例子，假设此时我们不需要区分监控数据的类型，只要有监控数据超过阈值并达到指定的次数后，就进行报警，代码如下：

```java
public class ThresholdWarning extends RichFlatMapFunction<Tuple2<String, Long>, 
Tuple2<String, List<Tuple2<String, Long>>>> implements CheckpointedFunction {

    // 非正常数据
    private List<Tuple2<String, Long>> bufferedData;
    // checkPointedState
    private transient ListState<Tuple2<String, Long>> checkPointedState;
    // 需要监控的阈值
    private Long threshold;
    // 次数
    private Integer numberOfTimes;

    ThresholdWarning(Long threshold, Integer numberOfTimes) {
        this.threshold = threshold;
        this.numberOfTimes = numberOfTimes;
        this.bufferedData = new ArrayList<>();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 注意这里获取的是OperatorStateStore
        checkPointedState = context.getOperatorStateStore().
            getListState(new ListStateDescriptor<>("abnormalData",
                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                })));
        // 如果发生重启，则需要从快照中将状态进行恢复
        if (context.isRestored()) {
            for (Tuple2<String, Long> element : checkPointedState.get()) {
                bufferedData.add(element);
            }
        }
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, 
                        Collector<Tuple2<String, List<Tuple2<String, Long>>>> out) {
        Long inputValue = value.f1;
        // 超过阈值则进行记录
        if (inputValue >= threshold) {
            bufferedData.add(value);
        }
        // 超过指定次数则输出报警信息
        if (bufferedData.size() >= numberOfTimes) {
             // 顺便输出状态实例的hashcode
             out.collect(Tuple2.of(checkPointedState.hashCode() + "阈值警报！", bufferedData));
            bufferedData.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 在进行快照时，将数据存储到checkPointedState
        checkPointedState.clear();
        for (Tuple2<String, Long> element : bufferedData) {
            checkPointedState.add(element);
        }
    }
}
```

调用自定义算子状态，这里需要将并行度设置为 1：

```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// 开启检查点机制，每个检查点时间间隔 1000ms
env.enableCheckpointing(1000);
// 设置并行度为1
DataStreamSource<Tuple2<String, Long>> tuple2DataStreamSource = env.setParallelism(1).fromElements(
    Tuple2.of("a", 50L), Tuple2.of("a", 80L), Tuple2.of("a", 400L),
    Tuple2.of("a", 100L), Tuple2.of("a", 200L), Tuple2.of("a", 200L),
    Tuple2.of("b", 100L), Tuple2.of("b", 200L), Tuple2.of("b", 200L),
    Tuple2.of("b", 500L), Tuple2.of("b", 600L), Tuple2.of("b", 700L));
tuple2DataStreamSource
    .flatMap(new ThresholdWarning(100L, 3))
    .printToErr();
env.execute("Managed Keyed State");
}
```

此时输出如下：

<div align="center"> <img src="https://gitee.com/heibaiying/BigData-Notes/raw/master/pictures/flink-operator-state-para1.png"/> </div>



在上面的调用代码中，我们将程序的并行度设置为 1，可以看到三次输出中状态实例的 hashcode 全是一致的，证明它们都同一个状态实例。假设将并行度设置为 2，此时输出如下：

<div align="center"> <img src="https://gitee.com/heibaiying/BigData-Notes/raw/master/pictures/flink-operator-state-para2.png"/> </div>



可以看到此时两次输出中状态实例的 hashcode 是不一致的，代表它们不是同一个状态实例，这也就是上文提到的，一个算子状态是与一个并发的算子实例所绑定的。同时这里只输出两次，是因为在并发处理的情况下，线程 1 可能拿到 5 个非正常值，线程 2 可能拿到 4 个非正常值，因为要大于 3 次才能输出，所以在这种情况下就会出现只输出两条记录的情况，所以需要将程序的并行度设置为 1。

## 三、容错机制与故障恢复

### 3.1 CheckPoint

为了使 Flink 的状态具有良好的容错性，Flink 提供了检查点机制 (CheckPoint)。通过检查点机制，Flink 定期在数据流上生成 checkpoint barrier ，当某个算子收到 barrier 时，即会基于当前状态生成一份快照，然后再将该 barrier 传递到下游算子，下游算子接收到该 barrier 后，也基于当前状态生成一份快照，依次传递直至到最后的 Sink 算子上。当出现异常后，Flink 就可以根据最近的一次的快照数据将所有算子恢复到先前的状态。

<div align="center"> <img src="https://gitee.com/heibaiying/BigData-Notes/raw/master/pictures/flink-stream-barriers.png"/> </div>


简单地说，就是 Checkpoint 会定时制作分布式快照，对程序中的状态进行备份。当发生故障时，将整个作业的所有 Task 都回到最近一次成功 Checkpoint 中的状态，然后从那个点开始继续处理数据。

如果要从 Checkpoint 恢复，必要条件是**数据源需要支持数据重新发送**。Checkpoint 恢复后， Flink 提供两种一致性语义，一种是恰好一次，一种是至少一次。在做 Checkpoint 时，可根据 Barries 对齐来判断是恰好一次还是至少一次，如果对齐，则为恰好一次，否则没有对齐即为至少一次。如果作业是单线程处理，也就是说 Barries 是不需要对齐的；如果只有一个 Checkpoint 在做，不管什么时候从 Checkpoint 恢复，都会恢复到刚才的状态；如果有多个节点，假如一个数据的 Barries 到了，另一个 Barries 还没有来，内存中的状态如果已经存储。那么这 2 个流是不对齐的，恢复的时候其中一个流可能会有重复。

**开启 Checkpoint**

默认情况下，检查点机制是关闭的，需要在程序中进行开启：

```java
env.enableCheckpointing(1000) 
```
意思是每个 Checkpoint 的事件间隔为 1 秒。Checkpoint 做的越频繁，恢复时追数据就会相对减少，同时 Checkpoint 相应的也会有一些 IO 消耗。

```java
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE) 
```

设置了 Exactly_Once 语义，并且需要 Barries 对齐，这样可以保证消息不会丢失也不会重复。


```java
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
```
2 个 Checkpoint 之间最少是要等 500ms，也就是刚做完一个 Checkpoint。比如某个 Checkpoint 做了 700ms，按照原则过 300ms 应该是做下一个 Checkpoint，因为设置了 1000ms 做一次 Checkpoint 的，但是中间的等待时间比较短，不足 500ms 了，需要多等 200ms，因此以这样的方式防止 Checkpoint 太过于频繁而导致业务处理的速度下降。

```java
// 设置执行 Checkpoint 操作时的超时时间
env.getCheckpointConfig().setCheckpointTimeout(60000);
```
表示做 Checkpoint 多久超时，如果 Checkpoint 在 1min 之内尚未完成，说明 Checkpoint 超时失败。

```java
// 设置最大并发执行的检查点的数量
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
```
表示同时有多少个 Checkpoint 在做快照，这个可以根据具体需求去做设置。

```java
// 将检查点持久化到外部存储
env.getCheckpointConfig().enableExternalizedCheckpoints(
    ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
```
表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint 会在整个作业 Cancel 时被删除。Checkpoint 是作业级别的保存点。

```java
env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
```
如果有更近的保存点时，是否将作业回退到该检查点。


上面讲过，除了故障恢复之外，还需要可以手动去调整并发重新分配这些状态。手动调整并发，必须要重启作业并会提示 Checkpoint 已经不存在，那么作业如何恢复数据？

### 3.2 Savepoint

Flink 还有另外一个机制是 SavePoint。Savepoint 是检查点机制的一种特殊的实现，它允许你通过手工的方式来触发 Checkpoint，并将结果持久化存储到指定路径中。当作业失败时，可以从外部恢复，主要用于避免 Flink 集群在重启或升级时导致状态丢失。示例如下：

```shell
# 触发指定 id 的作业的 Savepoint，并将结果存储到指定目录下
bin/flink savepoint :jobId [:targetDirectory]
```

更多命令和配置可以参考官方文档：[Savepoint]( https://ci.apache.org/projects/flink/flink-docs-release-1.9/zh/ops/state/savepoints.html )

### 3.3 Savepoint 与 Checkpoint 的区别

Savepoint 与 Checkpoint 有什么区别呢？

- 从触发管理方式来讲，Checkpoint 由 Flink 自动触发并管理，而 Savepoint 由用户手动触发并人工管理；

- 从用途来讲，Checkpoint 在 Task 发生异常时快速恢复，例如网络抖动或超时异常，而 Savepoint 有计划地进行备份，使作业能停止后再恢复，例如修改代码、调整并发；

- 最后从特点来讲，Checkpoint 比较轻量级，作业出现问题会自动从故障中恢复，在作业停止后默认清除；而 Savepoint 比较持久，以标准格式存储，允许代码或配置发生改变，恢复需要启动作业手动指定一个路径恢复。

## 四、状态后端

### 4.1 状态管理器分类

默认情况下，所有的状态都存储在 JVM 的堆内存中，在状态数据过多的情况下，这种方式很有可能导致内存溢出，因此 Flink 该提供了其它方式来存储状态数据，这些存储方式统一称为状态后端 (或状态管理器)：

<div align="center"> <img src="https://gitee.com/heibaiying/BigData-Notes/raw/master/pictures/flink-checkpoints-backend.png"/> </div>



主要有以下三种：

#### 1. MemoryStateBackend

默认的方式，即基于 JVM 的堆内存进行存储。这种存储状态本身存储在 TaskManager 节点也就是执行节点内存中的，因为内存有容量限制，所以单个 State maxStateSize 默认 5M，且需要注意 maxStateSize <= akka.framesize 默认 10M。Checkpoint 存储在 JobManager 内存中，因此总大小不超过 JobManager 的内存。

**推荐使用的场景**：本地测试、几乎无状态的作业，比如 ETL、JobManager 不容易挂，或挂掉影响不大的情况。不推荐在生产场景使用。

#### 2. FsStateBackend

基于文件系统进行存储，可以是本地文件系统，也可以是 HDFS 等分布式文件系统。 需要注意而是虽然选择使用了 FsStateBackend ，但 State 仍然是存储在 TaskManager 的内存中的，但不会像 MemoryStateBackend 有 5M 的设置上限。Checkpoint 存储在外部文件系统（本地或 HDFS），打破了总大小 JobManager 内存的限制。容量限制上，单 TaskManager 上 State 总量不超过它的内存，总大小不超过配置的文件系统容量。

**推荐使用的场景**：常规使用状态的作业、例如分钟级窗口聚合或 join、需要开启 HA 的作业。

#### 3. RocksDBStateBackend

RocksDBStateBackend 是 Flink 内置的第三方状态管理器。RocksDB 是一个 key/value 的内存存储系统，和其他的 key/value 一样，先将状态放到内存中，如果内存快满时，则写入到磁盘中，但需要注意 RocksDB 不支持同步的 Checkpoint。

不过 RocksDB 支持增量的 Checkpoint，也是目前唯一增量 Checkpoint 的 Backend，意味着每次用户不需要将所有状态都写进去，将增量的改变的状态写进去即可。它的 Checkpoint 存储在外部文件系统（本地或 HDFS），其容量限制只要单个 TaskManager 上 State 总量不超过它的内存 + 磁盘，单 Key 最大 2G，总大小不超过配置的文件系统容量即可。

RocksDB 比起全文件系统的方式，其读取速率更快；比起全内存的方式，其存储空间更大，因此它是一种比较均衡的方案。

**推荐使用的场景**：超大状态的作业，例如天级窗口聚合、需要开启 HA 的作业、最好是对状态读写性能要求不高的作业。

### 4.2 配置方式

Flink 支持使用两种方式来配置后端管理器：

**第一种方式**：基于代码方式进行配置，只对当前作业生效：

```java
// 配置 FsStateBackend
env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints"));
// 配置 RocksDBStateBackend
env.setStateBackend(new RocksDBStateBackend("hdfs://namenode:40010/flink/checkpoints"));
```

配置 RocksDBStateBackend 时，需要额外导入下面的依赖：

```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb_2.11</artifactId>
    <version>1.9.0</version>
</dependency>
```

**第二种方式**：基于 `flink-conf.yaml` 配置文件的方式进行配置，对所有部署在该集群上的作业都生效：

```yaml
state.backend: filesystem
state.checkpoints.dir: hdfs://namenode:40010/flink/checkpoints
```



> 注：本篇文章所有示例代码下载地址：[flink-state-management]( https://github.com/heibaiying/BigData-Notes/tree/master/code/Flink/flink-state-management)



## 参考资料

+ [Working with State](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/state/state.html)
+ [Checkpointing](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/state/checkpointing.html)
+ [Savepoints](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/state/savepoints.html#savepoints)
+ [State Backends](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/state/state_backends.html)
+ Fabian Hueske , Vasiliki Kalavri . 《Stream Processing with Apache Flink》.  O'Reilly Media .  2019-4-30 












<div align="center"> <img  src="https://gitee.com/heibaiying/BigData-Notes/raw/master/pictures/weixin-desc.png"/> </div>