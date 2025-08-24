# raft-demo
raft协议的一个简单实现，目前主要是针对三个子问题进行了简单实现，很多复杂的逻辑都没有，例如客户端监听，。集群配置变更，状态机相关等等都还没有，后面有空了再弄吧。

# 要求
- 已安装Go
- Linux环境

# 编译
```shell
go build -o raft-demo main.go
```

# 运行
运行我们直接在本地起多个进程来模拟不同的节点。启动三个节点进行选举，为了方便观察每个节点的状态，我们可以在三个不同的窗口输入如下的命令。

```shell
./raft-demo -id 1 -port 8001 -peers 1:localhost:8001,2:localhost:8002,3:localhost:8003
./raft-demo -id 2 -port 8002 -peers 1:localhost:8001,2:localhost:8002,3:localhost:8003
./raft-demo -id 3 -port 8003 -peers 1:localhost:8001,2:localhost:8002,3:localhost:8003
```

假设node-1成为了Leader，Leader-1会一直打印如下日志，表示收到客户端的日志
```
Leader 1 updated commit index to 62
Leader 1 added log: 7 (term 122)
Leader 1 updated commit index to 69
Leader 1 added log: 5 (term 122)
Leader 1 updated commit index to 74
```

对于其他Follower，会收到Leader日志复制的请求，然后复制到log里
```
Node 2 received logs: 9
Node 2 updated commit index to 103
Node 2 received logs: 4
Node 2 updated commit index to 107
Node 2 received logs: 6
Node 2 updated commit index to 113
Node 2 received logs: 8
Node 2 updated commit index to 121
Node 2 received logs: 6
Node 2 updated commit index to 127
```