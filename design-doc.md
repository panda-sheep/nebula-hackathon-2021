# 要解决什么问题

问题1： 悬挂边

问题2： 出度、入度统计值

问题3： 优化图结构的访问速度

# 问题有什么价值？

## 问题1： 悬挂边

```
(src)-[edge]->           有起点，有边，没有终点
-[edge]->(dst)           没有起点，有边，有终点
-[edge]->               没有起点，没有终点，有边
```

> 在图论上，这是不合理的。但在nebula中物理上是存在的。

### 原因

因为图元素建模的时候，EdgeA_Out 的存在性和 srcVertex 是两个独立的key。有可能发生： 

1. graphd 只插入了边 EdgeA，没插入 srcVertx
2. 删除 Vertex 和边的操作不原子。导致垃圾 key 的存在。 边key没删成功，把点key删成功了。
3. Drop Tag 的时候，最后一个 Tag

![image](https://user-images.githubusercontent.com/50101159/145525464-05e899a2-3ca0-4bd4-8e54-f0bb78ed5bc4.png)


## 问题2： 出度、入度统计值

可以用于：

1. 判断一个点是否是超级节点？
2. 找到有哪些超级节点？
3. 最短路径等算法的时候，默认BFS，走到超级节点就先暂停BFS，改成DFS或者采样。
4. 执行计划CBO的时候，count是Cost的一个重要参数。
5. 删除一个超级节点（和周围的边），rpc超时参数值设置。

### 原因：
![image](https://user-images.githubusercontent.com/50101159/145525833-03bfa058-cbeb-482f-9fe8-7b5f57cac6c2.png)
```
EdgeA_Out 是单独的Key
EdgeA_Out1
EdgeA_Out2
EdgeA_Out3
…
EdgeA_Outn
```
对于每个点，不做 prefix scan 不知道 n 是多少。扫描，或者建索引。

## 问题3：优化图结构的访问速度

决大部分的图计算，只需要 edge key 的信息——图结构——以及一两个property（weight）。行式存储格式，SSD 扫描时带有基本无用的 value。

> BTW KV-seperation也是一种提升图结构访问的方式。


## 不解决的问题

- 分布式事务的能力

- TOSS 导致的逻辑边，正向和反向访问的最终一致性

- 并发的隔离性

- 点、边等数据模型: 点是否要有Tag/Lable.
 
- online DDL：DDL 不能实时感知，meta client时延问题。

- graphd, storaged 双进程 rpc 时延大的问题


## 其他设计目标

- 平滑升级，不依赖工具升级。

- 不更改通信协议

- 增加语法。

- Change: 悬挂边插入会报错。

# 业界方案参考

## JanusGraph (TitanDB)

![image](https://user-images.githubusercontent.com/50101159/145526122-f4896cce-dd05-4878-8b09-5b1a05522eda.png)

JanusGraph 将一个点的ID作为KEY, “<所有属性>,<所有出边>，<所有入边>” 作为一个超级大的value

好处：

1.	不会有悬挂边
2.	Value里面可以增加一个count字段，统计出度和入度

坏处：

1.	The downside is that each edge has to be stored twice - once for each end vertex of the edge.
2.	一个边的修改，涉及到两个超大value的修改。—读取到内存，中间位置的（内存）插入，写回硬盘
3.	两个vertexID key的能否原子操作，由其存储引擎决定（HBASE，Cassandra）。Nebula 中的类似操作，叫做TOSS（见下）。

## Neo4j 的简略结构

![image](https://user-images.githubusercontent.com/50101159/145526558-500bb76d-ced0-4b8a-b167-afc91ad64cb5.png)

![image](https://user-images.githubusercontent.com/50101159/145526612-e14a110f-b0b0-41bd-abc7-2e26d9cd1d94.png)

点(Label)、边(图结构)、属性是各自独立的文件, Neo4j 自己实现这些硬盘文件的可靠性（以及Raft）

## B-tree

略

## KV-seperation (WiscKey)

略

## nebula-TOSS 

![image](https://user-images.githubusercontent.com/50101159/145527387-148e0eca-a68f-4b9a-808b-6eda8ebdb9a0.png)

保证EdgeA_Out 和 EdgeA_In的（两个机器上的）**最终一致性**

下文仍使用TOSS的逻辑，但有改动

# 解决方法详述

## 建模方式

### storage 原来的数据格式

![image](https://user-images.githubusercontent.com/50101159/145525464-05e899a2-3ca0-4bd4-8e54-f0bb78ed5bc4.png)


### vertex data 

| Key |   Value |
| -   |  - | 
| PartitionId  VertexId  TagId | Property values ,   value的开头含有当前的schema信息 |
| type(1byte NebulaKeyType::kVertex)_PartID(3bytes)_VertexID(8bytes)_TagID(4bytes)   需要vidlen来指定，不足补'\0'  | - |

index 

|  key |   Value |
| -  | - |
| type(NebulaKeyType::kIndex)_PartitionId  IndexId   Index binary  nullableBit   VertexId   | - |    
需要vidlen来指定，不足补'\0'

### edge data

| key | value |
| -   | - |
| partId + srcId + edgeType + edgeRank + dstId + version   需要vidlen来指定，不足补'\0' |  Property values |
|    type(1byte)_PartID(3bytes)_VertexID(8bytes)_EdgeType(4bytes)_Rank(8bytes)_otherVertexId(8bytes)_version(1) | - |

index 

| key | value |
| -   | - |
| PartitionId  IndexId  Index binary  nullableBit   SrcVertexId  EdgeRank  DstVertexId | - |


## 

![image](https://user-images.githubusercontent.com/50101159/145528316-47b31ced-a276-4e03-ae97-8e09b7943920.png)

