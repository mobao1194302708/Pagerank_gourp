# 作业提交模板

```bash

.
└── code/
    ├── giraph-impl/         # Giraph 实现模块
    │   ├── src/             #  Java 源码
    │   ├── pom.xml          # 定义了 Giraph 和 Hadoop 依赖
    │   └── run-giraph.sh    # 封装了提交 Giraph 任务至 YARN 的 shell 脚本
    ├── mapreduce-impl/      # MapReduce 实现模块
    │   ├── src/             # 实现 PageRank 迭代逻辑的 Mapper/Reducer 源码
    │   ├── pom.xml          # 定义了 Hadoop 相关依赖
    │   └── run-mr.sh        # 封装了循环提交 MapReduce 作业的迭代脚本
    ├── input_data/          # 实验输入数据集
    │   └── README.md        # 
    ├── output_data/         # 实验结果存储
    │   ├── metrics.csv      # 指标数据
    │   ├── mapreduce_log_500.txt      # Mapreduce的500MB运行日志
    │   └── README.md        #
    ├── scripts/             # 公共脚本
    │   ├── plot_results.py  # Python 脚本：读取 metrics.csv 并生成性能对比图表
    │   ├── random_data.py   # Python 脚本：用于随机生成不同规模的数据
    │   └── .empty           
    ├── images/              # 存放 README.md 使用的静态图片
    │   └── README.md        # 
    └── README.md            # 项目主说明文档
```

## 研究目的
比较Giraph和MapReduce运行PageRank算法的差异

## 研究内容
对比分析Giraph和MapReduce在执行PageRank算法等图迭代计算任务时的差异，深入
理解Giraph所采用的BSP（Bulk Synchronous Parallel）模型的设计理念及其在图计算中的优势。
重点探讨两者在数据通信方式、任务调度机制及迭代开销等方面的不同，以及这些差异对算法性能
与可扩展性的影响。
### 1. 迭代方式
- Giraph: 图迭代计算视为一个任务，节点工作进程常驻，只需要在任务开始和结束时从HDFS上读取输入，写入输出
- MapReduce: 一轮迭代视为一个任务，迭代过程会反复启动和销毁任务进程，每轮迭代都需要从HDFS上读取输入、写入输入
25m~500m
    - 对比Giraph和MapReduce的任务启动延时(Start Time - Submited Time)
    - 对比Giraph和MapReduce读写HDFS的字节数
    - 对比Giraph和MapReduce读写HDFS的用时

### 2. 数据与计算模型
- Giraph：以图数据结构为数据模型，采用以顶点为中心的计算模型，在顶点上定义计算方法，顶点完成计算后沿着顶点之间的边传输消息。支持图数据的直接表示，输入数据经过加载后形成图结构数据，驻留在内存中，数据通信只需传输Rank值，不需要传输图结构。
- MapReduce：以键值数据结构作为数据模型，计算分成两个阶段：map:对键值转化和分组 ；reduce:对相同键的数据进行合并。无法直接表示图数据，只能将图数据转化为键值形式，这会导致部分数据冗余存储。加之MapReduce是无状态计算，图结构不会驻留内存，每轮迭代需要在HDFS上加载和写入图结构数据，每次Shuffle不仅需要传输Rank值，还要传输图结构。
25m~500m
    - 对比Giraph发送的消息字节数和MapReduce的Shuffle字节数。
    - 对比Giraph和MapReduce的内存开销。
    - 绘制MapReduceHDFS读写字节数与任务规模的关系。
### 3. 数据通信:
- Giraph:采用BSP同步和Message消息传递，不同顶点之间并发处理，每个顶点的compute()计算完成后就可以调用sendMessage()发送消息传递数据，最终通过BSP同步所以顶点的计算和通信，Giraph能够更充分地利用I/O资源。
- MapReduce: 采用Shuffle机制实现Map任务和Reduce任务之间的数据传输，一个Map任务需要完成全部图顶点的处理才能进入Shuffle阶段，最先被处理的图顶点将被阻塞直到Map任务完成，此过程将会导致I/O资源的浪费。
100m 迭代5轮
    - 对比Giraph和MapReduce在运行时的网络I/O使用情况。
25m~500m
    - 对比MapReduce的Shuffle耗时和Giraph每个SuperStep的栅栅同步时间(Barrier)

### 4.**可拓展性差异**
-  数据可拓展性：固定Worker数量，增加图规模
Metrics : 总作业时间、系统总体资源占用率
-  水平可拓展性: 固定数据规模(大规模图)，增加节点数量
Metrics : 加速比 ，资源利用率 

## 实验
### 实验环境
节点数：4个
#### 节点配置
Hadoop/Giraph 集群节点配置汇总表

|**节点角色**|**节点名称 (Hostname)**|**CPU**|**内存 (RAM)**|**操作系统**|**公网带宽**|**磁盘配置**|
|---|---|---|---|---|---|---|
|**Hadoop Master**|`hadoop-master`|8核|16 GiB|Ubuntu 24.04 LTS|10 Mbps|50GB 增强型 SSD|
|**Hadoop Slave 1**|`hadoop-slave1`|8核|16 GiB|Ubuntu 24.04 LTS|10 Mbps|50GB 增强型 SSD|
|**Hadoop Slave 2**|`hadoop-slave2`|8核|16 GiB|Ubuntu 24.04 LTS|10 Mbps|50GB 增强型 SSD|
|**ZooKeeper**|`zookeeper-node`|2核|2 GiB|Ubuntu 24.04 LTS|3 Mbps|50GB 通用型 SSD|

### 软件环境配置表

|**配置项目**|**详细内容**|
|---|---|
|**部署方式**|Docker 部署 (手工制作镜像)|
|**x86 镜像地址**|`docker pull frestrain/pagerank:x86`|
|**Arm 镜像地址**|`docker pull frestrain/pagerank:arm`|
|**操作系统**|Ubuntu 18.04|
|**Hadoop 版本**|3.3.6|
|**JDK 版本**|OpenJDK 11|
|**Giraph 版本**|Giraph 2.7|


### 实验负载
#### 1.数据结构（邻接表）
|**字段**|**含义**|**示例**|**作用**|
|---|---|---|---|
|**节点 ID (Node ID)**|当前页面的唯一标识|`0`|标识当前权重所属的对象|
|**PR Score**|节点的当前重要性分数|`0.0000025`|迭代计算的核心数值，随迭代更新|
|**当前节点的指向的节点**|该页面指向的所有链接|`124543, 172009...`|决定了 PR 值将分发给哪些节点|
#### 2.使用的数据集（pagerank数据）
|**文件名称**|**原始大小（字节）**|**说明大小 (MB)**|**备注**|
|---|---|---|---|
|`random_pagerank_data_100.txt`|110,056,298|约 104.96 MB|中型测试数据集|
|`random_pagerank_data_300.txt`|346,925,030|约 330.85 MB|大型数据集|
|`random_pagerank_data_500.txt`|583,623,430|约 556.59 MB|超大型数据集|
|`web-Google-PR-Init.txt`|50,598,489|约 48.25 MB|初始化/真实提取数据|
### 实验步骤
列出执行实验的关键步骤，并对关键步骤进行截图，如 MapReduce / Spark / Flink 部署成功后的进程信息、作业执行成功的信息等，**截图能够通过显示用户账号等个性化信息佐证实验的真实性**。

#### 第一阶段：环境搭建与准备
1. **基础集群部署：** 搭建Hadoop分布式集群，配置 Hadoop 的 yarn-site.xml 和 mapred-site.xml。确保其 Zookeeper 能够正常协调各 Worker。如下是Hadoop的Overview。
<img src="code/images/hdfs.png" width="300" alt="HDFS">

2. **环境检查：** 环境环境验证 java -version、hadoop version 和 giraph 命令是否正常。
3. **测试:** 在单机下跑pagerank和 Giraph 自带的 SimpleShortestPathsComputation，确保计算框架链路通畅。
4. 如下是Master和Slave的JPS截图

![JPS](code/images/master_jps.png)
![JPS](code/images/slave_jps.png)


#### 第二阶段：算法编程实现
1. **MapReduce 版实现：**
   
2. **Giraph (BSP) 版实现：**
---
#### 第三阶段：数据集准备与预处理
1. **数据选取：** 准备三个不同规模的图数据集以及真实提取数据集
    - 将原始数据转换为邻接表格式（确保Mapreduce和Giraph在同一负载下)
    - 确保两套系统使用相同的输入源，以保证实验的公平性。
---
#### 第四阶段：实验运行与数据采集
1. **预实验：** 在小规模数据集上测试，验证两个版本PageRank的计算结果是否一致。
2. **正式实验：**
    - **维度 A（迭代次数）：** 固定数据集，改变迭代次数（如 10, 20, 50 次），记录执行时间。
    - **维度 B（数据规模）：** 固定迭代次数，改变节点和边数，观察作业的可扩展性。
3. **性能指标采集：**
    - **作业总时长：** 从提交到任务结束的时间。
    - **迭代间隙开销：** 记录 MapReduce 频繁读写 HDFS 产生的 I/O 耗时。
    - **网络通信量：** 记录 Giraph 在 BSP 同步点（Barrier）前后的网络流量。
4. **数据收集**
    - 我们部署了JobHIstory来监控Mapreduce的每一轮迭代与Giraph的运行指标。
    - ![部分运行历史](code/images/jobhistory.png)
    - ![部分运行历史](code/images/single_state.png)

---

#### 第五阶段：对比分析与结论撰写
1. **执行效率分析：** * 对比发现 Giraph 在迭代计算中由于 **数据驻留内存** 和 **无需频繁启动 Job**，其性能通常远优于 MapReduce。
2. **编程复杂度总结：**
    - 分析 Giraph “以顶点为中心”的编程模式是否比 MapReduce 的拆分逻辑更直观。
3. **BSP 模型深度剖析：**
    - 结合实验数据，解释 **超级步（Superstep）** 机制如何通过内存消息传递取代了 MapReduce 繁重的 **Shuffle-to-Disk** 过程。
4. **可扩展性讨论：**
    - 观察当数据规模超出内存容量时，Giraph 的表现是否会大幅下降（触发 Spill），从而分析其局限性。
### 实验结果与分析
#### 1.迭代方式
##### 1.1 对比Giraph和MapReduce的任务启动延时(Start Time - Submited Time)
![读写对比图](code/images/启动时间与总时间对比图.png)
##### 1.2 对比Giraph和MapReduce读写HDFS的字节数和读写时间
![读写对比图](code/images/HDFS读写与磁盘读写对比图.png)
#### 2.数据与计算模型
##### 2.1 对比Giraph发送的消息字节数和MapReduce的Shuffle字节数
![读写对比图](code/images/datastastic&datamodel/data_analysis/cmp_2.png)
##### 2.2 对比Giraph和MapReduce的内存开销
![读写对比图](code/images/datastastic&datamodel/data_analysis/cmp_1.png)
##### 2.3 绘制MapReduceHDFS读写字节数与任务规模的关系
![](code/images/datastastic&datamodel/data_analysis/cmp_3.png)
#### 3.数据通信
##### 3.1 对比Giraph和MapReduce在运行时的网络I/O使用情况
![读写对比图](code/images/网络流量对比.jpg)
##### 3.2 对比MapReduce的Shuffle耗时和Giraph每个SuperStep的栅栅同步时间(Barrier)
![读写对比图](code/images/通信等待时间对比.jpg)

### 结论
#### 1.迭代方式
##### 1.1 任务启动延时与调度开销
- Giraph 具有极低的调度延迟：实验结果显示，Giraph 的任务启动延时（Start Time - Submitted Time）始终保持在极低水平（约 2-3 秒），且不随数据集规模增加而变化。
- MapReduce 调度开销随规模显著增长：相比之下，MapReduce 的累计调度延时远高于 Giraph，并随数据集增大而线性上升（从 50 秒增长至 65 秒以上）。这验证了图 1 中提到的：MapReduce 每轮迭代反复启动/销毁进程导致了巨大的调度负担，而 Giraph 的进程常驻模式有效消除了这一开销。
##### 1.2 存储与网络 I/O 效率
- HDFS 读写压力对比：在处理 500MB 数据集时，MapReduce 产生的 HDFS 读写总量超过 10000MB，而 Giraph 仅产生极少量（数百 MB）的 HDFS I/O。
- 本地磁盘（Shuffle I/O）开销：MapReduce 在本地磁盘上的 Shuffle I/O 呈指数级增长，在 500MB 时达到 $10^4$ GB 级别的对数刻度压力，而 Giraph 仅维持在 $10^1$ GB 级别以下。
- 结论支持：这说明 Giraph 仅在任务开始和结束时进行 HDFS 交互的策略，极大减轻了网络和存储系统的负载，避免了 MapReduce 每轮迭代强制读写 HDFS 带来的性能瓶颈。
#### 2.数据与计算模型
通过对比 Giraph 与 MapReduce 在不同数据规模（25MB ~ 500MB）下的PageRank迭代计算实验，得出以下核心结论：
##### 2.1 任务调度与启动效率
- Giraph 具有极高的调度稳定性：由于 Giraph 采用进程常驻模式，其任务启动延时始终保持在 2-3 秒 的极低水平，不随数据规模增长。
- MapReduce 调度开销沉重：MapReduce 每一轮迭代都需要重新启动和销毁进程，导致累计调度延时高达 50-65 秒。这验证了图一中关于“反复启动销毁任务进程”是导致 MapReduce 性能低下的关键因素。
##### 2.2 I/O 交互与磁盘压力
- HDFS 访问频率差异：MapReduce 在每轮迭代中强制读写 HDFS，导致其 HDFS 读写总量在 500MB 规模下突破了 10000 MB；而 Giraph 仅在起止阶段交互，I/O 压力减小了数个数量级。
- 本地磁盘（Shuffle）压力：MapReduce 的 Shuffle 过程产生了海量的本地磁盘 I/O（对数坐标下达到 $10^4$ GB 级别），而 Giraph 由于数据驻留内存，其磁盘 I/O 极低。
##### 2.3 数据模型与通信效率
- 数据表达精简度：Giraph 采用以顶点为中心的图数据模型，通信时仅传输 Rank 值，不需要传输图结构。
- 网络带宽利用率：实验显示，Giraph 发送的消息字节数仅为 MapReduce Shuffle 字节数的 26.8% ~ 27.3%。这表明 Giraph 的通信模型比 MapReduce 的键值对模型在处理图数据时更加高效、精简。
##### 2.4 内存开销与总体性能
- 内存利用率：虽然 Giraph 将数据驻留内存，但其总内存开销始终低于 MapReduce。例如在 500MB 规模下，Giraph 开销为 22396 MB，而 MapReduce 为 23552 MB。
- 性能崩溃现象：随着数据集增大，MapReduce 的总执行时间呈指数级飙升（最高超过 20000 秒），而 Giraph 仅呈现缓慢的线性增长。
#### 3.数据通信
通过对比 Giraph 与 MapReduce 在 PageRank 算法下的实验表现（数据集范围 25MB 至 500MB），本实验得出以下核心结论：
#### 3.1 数据通信与同步开销
- 消息传递更精简：Giraph 仅需传输顶点的 Rank 值，不传输图结构。实验显示其网络传输数据量仅为 MapReduce Shuffle 字节数的 26.8% ~ 27.3%。
- 吞吐与并发表现：Giraph 基于 BSP 同步模型，网络吞吐波动更频繁但峰值更高（最高超 6,000 KB/s），相比 MapReduce 受限于 Shuffle 阶段的阻塞，Giraph 能够更充分地利用 I/O 资源。
- 同步等待时间：MapReduce 的平均 Shuffle 耗时随规模增长极快（最高 213.3 秒），而 Giraph 的 SuperStep 栅栏同步时间更短且增长受控。
Giraph 在图迭代计算场景下具有压倒性优势。它通过常驻工作进程解决了调度延时问题，通过内存驻留图结构解决了 HDFS 频繁读写问题，并通过 BSP 消息模型解决了通信冗余问题。相比传统的 MapReduce 批处理模型，Giraph 能够在大规模迭代任务中提供更高的吞吐量和更稳定的执行预期。
### 分工
尽可能详细地写出每个人的具体工作和贡献度，并按贡献度大小进行排序。





