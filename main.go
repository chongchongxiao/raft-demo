package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 节点角色定义
type State int

const (
	Follower  State = iota // 跟随者
	Candidate              // 候选者
	Leader                 // 领导者
)

// 角色状态的字符串表示
func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RPC请求/响应结构

// RequestVoteArgs: 选举请求参数
type RequestVoteArgs struct {
	Term         int // 候选者的任期
	CandidateID  int // 候选者ID
	LastLogIndex int // 候选者最后一条日志的索引
	LastLogTerm  int // 候选者最后一条日志的任期
}

// RequestVoteReply: 选举响应
type RequestVoteReply struct {
	Term        int  // 当前任期，供候选者更新
	VoteGranted bool // 是否同意投票
}

// AppendEntriesArgs: 日志追加/心跳请求参数
type AppendEntriesArgs struct {
	Term         int      // 领导者的任期
	LeaderID     int      // 领导者ID
	PrevLogIndex int      // 前一条日志的索引
	PrevLogTerm  int      // 前一条日志的任期
	Entries      []string // 要追加的日志条目（心跳时为空）
	LeaderCommit int      // 领导者的提交索引
}

// AppendEntriesReply: 日志追加/心跳响应
type AppendEntriesReply struct {
	Term    int  // 当前任期，供领导者更新
	Success bool // 操作是否成功
	Match   int  // 匹配的日志索引（用于快速回溯）
}

// RaftNode 核心结构
type RaftNode struct {
	ID          int            // 预定义好的节点ID
	peers       map[int]string // 节点ID到地址的映射
	state       State          // 当前节点状态
	currentTerm int            // 当前任期
	votedFor    int            // 已投票给哪个节点
	log         []string       // 日志条目
	lastLogTerm int            // 最后一条日志的任期

	commitIndex int // 已提交的日志索引
	lastApplied int // 已应用到状态机的日志索引,状态机相关的逻辑暂未实现

	// 领导者特有状态
	nextIndex  map[int]int // 发送给每个节点的下一个日志索引
	matchIndex map[int]int // 每个节点已复制的最高日志索引

	// 控制变量
	mu        sync.Mutex
	stateCh   chan State
	heartbeat chan struct{}

	// 计时器
	electionTimeout time.Duration // 心跳超时时间
	heartbeatTick   time.Duration // 心跳间隔
}

// NewRaftNode 创建一个新的Raft节点
func NewRaftNode(id int, peers map[int]string) *RaftNode {
	return &RaftNode{
		ID:              id,
		peers:           peers,
		state:           Follower,
		votedFor:        0,
		log:             []string{},
		nextIndex:       make(map[int]int),
		matchIndex:      make(map[int]int),
		stateCh:         make(chan State, 1), // 带缓冲避免阻塞
		heartbeat:       make(chan struct{}),
		electionTimeout: time.Duration(150+rand.Intn(150)) * time.Millisecond,
		heartbeatTick:   50 * time.Millisecond,
	}
}

// Start 初始化并启动Raft节点
func (n *RaftNode) Start() {
	fmt.Printf("Node %d starting in %s state\n", n.ID, n.state)

	// 初始化领导者状态，使用日志长度作为初始nextIndex
	logLen := len(n.log)
	for peerID := range n.peers {
		n.nextIndex[peerID] = logLen
		n.matchIndex[peerID] = 0
	}

	// 启动主状态机循环
	go n.runStateMachine()
}

// runStateMachine 管理状态转换的主循环
func (n *RaftNode) runStateMachine() {
	for {
		select {
		case newState := <-n.stateCh:
			n.mu.Lock()
			oldState := n.state
			n.state = newState
			n.mu.Unlock()
			fmt.Printf("Node %d transitioning from %s to %s\n", n.ID, oldState, newState)
			n.handleCurrentState()
		default:
			n.handleCurrentState()
		}
	}
}

// handleCurrentState 分发到相应的状态处理函数
func (n *RaftNode) handleCurrentState() {
	switch n.state {
	case Follower:
		n.runFollower()
	case Candidate:
		n.runCandidate()
	case Leader:
		n.runLeader()
	}
}

// runFollower 跟随者状态的主要逻辑
func (n *RaftNode) runFollower() {
	fmt.Printf("Node %d running as %s\n", n.ID, Follower)

	// 重置选举计时器
	electionTimer := time.NewTimer(n.electionTimeout)
	defer electionTimer.Stop()

	for {
		select {
		case <-n.heartbeat:
			// 收到心跳，重置计时器
			if !electionTimer.Stop() {
				<-electionTimer.C
			}
			electionTimer.Reset(n.electionTimeout)
			// fmt.Printf("Node %d received heartbeat, resetting election timer\n", n.ID)
		case <-electionTimer.C:
			// 选举超时，成为候选者
			n.mu.Lock()
			n.state = Candidate
			n.stateCh <- Candidate
			n.mu.Unlock()
			return
		}
	}
}

// runCandidate 候选者状态的主要逻辑
func (n *RaftNode) runCandidate() {
	fmt.Printf("Node %d running as %s\n", n.ID, Candidate)

	// 开始新的选举
	n.startElection()

	// 等待选举结果或超时，每次超时时间都随机生成
	electionTimer := time.NewTimer(time.Duration(150+rand.Intn(150)) * time.Millisecond)
	defer electionTimer.Stop()

	for {
		select {
		case <-electionTimer.C:
			// 选举超时，开始新一轮的选举
			n.mu.Lock()
			n.state = Candidate // 确保仍为候选者
			n.mu.Unlock()
			n.startElection()
			electionTimer.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
		case newState := <-n.stateCh:
			// 状态已变更（成为领导者或跟随者）
			n.mu.Lock()
			n.state = newState
			n.mu.Unlock()
			return
		}
	}
}

// runLeader 领导者状态的主要逻辑
func (n *RaftNode) runLeader() {
	fmt.Printf("Node %d running as %s\n", n.ID, Leader)

	// 初始化领导者状态
	n.mu.Lock()
	logLen := len(n.log)
	for p := range n.peers {
		n.nextIndex[p] = logLen // 使用日志长度作为初始值
		n.matchIndex[p] = 0
	}
	n.mu.Unlock()

	// 发送初始心跳和各个节点保活
	n.broadcast()

	// 启动心跳计时器
	heartbeatTimer := time.NewTicker(n.heartbeatTick)
	defer heartbeatTimer.Stop()

	for {
		select {
		case <-heartbeatTimer.C:
			// 定期发送心跳
			n.broadcast()
		case newState := <-n.stateCh:
			// 状态已变更（不再是领导者）
			n.mu.Lock()
			n.state = newState
			n.mu.Unlock()
			return
		}
	}
}

// startElection 发起新的选举
func (n *RaftNode) startElection() {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.ID
	voteCount := 1 // 给自己投票
	currentTerm := n.currentTerm
	fmt.Printf("Node %d starting election for term %d\n", n.ID, currentTerm)
	n.mu.Unlock()

	// 向所有节点发送投票请求
	for peerID, addr := range n.peers {
		if peerID == n.ID {
			continue
		}

		go func(peerID int, addr string) {
			n.mu.Lock()
			args := RequestVoteArgs{
				Term:         currentTerm,
				CandidateID:  n.ID,
				LastLogIndex: len(n.log) - 1,
				LastLogTerm:  n.lastLogTerm,
			}
			n.mu.Unlock()

			var reply RequestVoteReply
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				fmt.Printf("Failed to connect to node %d: %v\n", peerID, err)
				return
			}
			defer client.Close()

			if err := client.Call("RaftService.RequestVote", args, &reply); err != nil {
				fmt.Printf("Failed to request vote from node %d: %v\n", peerID, err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// 如果任期已变更，不再作为候选者
			if n.currentTerm != currentTerm {
				return
			}

			// 处理响应
			if reply.Term > currentTerm {
				n.currentTerm = reply.Term
				n.votedFor = 0
				n.stateCh <- Follower
				return
			}

			if reply.VoteGranted {
				voteCount++
				// 检查是否获得多数票
				if voteCount > len(n.peers)/2 {
					n.stateCh <- Leader
				}
			}
		}(peerID, addr)
	}
}

// broadcast 发送心跳消息（空的AppendEntries）
func (n *RaftNode) broadcast() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	currentTerm := n.currentTerm
	n.mu.Unlock() // 移除未使用的logLen变量

	// 调试使用
	// fmt.Printf("Leader %d sending heartbeats for term %d\n", n.ID, currentTerm)

	for peerID, addr := range n.peers {
		if peerID == n.ID {
			continue
		}

		go func(peerID int, addr string) {
			n.mu.Lock()
			prevIndex := n.nextIndex[peerID] - 1
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     n.ID,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  n.lastLogTerm,
				Entries:      []string{}, // 空表示心跳
				LeaderCommit: n.commitIndex,
			}
			n.mu.Unlock()

			var reply AppendEntriesReply
			// TODO 这里可以优化一下，不用每次新建连接
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				fmt.Printf("Failed to connect to node %d: %v\n", peerID, err)
				return
			}
			defer client.Close()

			if err := client.Call("RaftService.AppendEntries", args, &reply); err != nil {
				fmt.Printf("Failed to send heartbeat to node %d: %v\n", peerID, err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > currentTerm {
				n.currentTerm = reply.Term
				n.votedFor = 0
				n.stateCh <- Follower
				return
			}
		}(peerID, addr)
	}
}

// SendLog 向所有节点发送日志条目（仅领导者可用）
func (n *RaftNode) SendLogs(logs []string) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		// 一般raft的实现中，Follower会把请求转发到Leader，这里为了简便，直接就这么实现了
		fmt.Println("Only leaders can send logs")
		return
	}

	// 添加日志到本地
	n.log = append(n.log, logs...)
	n.lastLogTerm = n.currentTerm
	currentTerm := n.currentTerm
	fmt.Printf("Leader %d added logs: %d (term %d)\n", n.ID, len(logs), currentTerm)
	n.mu.Unlock() // 移除未使用的logLen变量

	// 向所有节点同步日志
	for peerID, addr := range n.peers {
		if peerID == n.ID {
			continue
		}

		go func(peerID int, addr string) {
			n.mu.Lock()
			// 防止prevIndex下溢
			prevIndex := n.nextIndex[peerID] - 1
			// 检查索引有效性
			if prevIndex >= len(n.log) {
				fmt.Printf("Invalid prevIndex %d for log length %d\n", prevIndex, len(n.log))
				n.mu.Unlock()
				return
			}

			startIndex := prevIndex
			if prevIndex < 0 {
				startIndex = 0
			}

			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     n.ID,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  n.lastLogTerm,
				Entries:      n.log[startIndex:], // 发送从prevIndex开始的日志
				LeaderCommit: n.commitIndex,
			}
			n.mu.Unlock()

			var reply AppendEntriesReply
			client, err := rpc.DialHTTP("tcp", addr)
			if err != nil {
				fmt.Printf("Failed to connect to node %d: %v\n", peerID, err)
				return
			}
			defer client.Close()

			if err := client.Call("RaftService.AppendEntries", args, &reply); err != nil {
				fmt.Printf("Failed to send log to node %d: %v\n", peerID, err)
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// 如果任期已变更，忽略响应
			if n.currentTerm != currentTerm {
				return
			}

			if reply.Term > currentTerm {
				n.currentTerm = reply.Term
				n.votedFor = 0
				n.stateCh <- Follower
				return
			}

			if reply.Success {
				// 更新匹配索引和下一个索引
				n.matchIndex[peerID] = prevIndex + len(args.Entries)
				n.nextIndex[peerID] = n.matchIndex[peerID] + 1
				// 检查是否可以提交
				n.checkCommit()
			} else {
				// 日志不匹配，回溯索引（避免下溢）
				if reply.Match > 0 {
					n.nextIndex[peerID] = reply.Match + 1
				} else if n.nextIndex[peerID] > 0 {
					n.nextIndex[peerID]--
				}
			}
		}(peerID, addr)
	}
}

// checkCommit 如果足够多的节点已复制日志，则更新提交索引
func (n *RaftNode) checkCommit() {
	count := 1 // 自己已复制
	for _, idx := range n.matchIndex {
		if idx >= n.commitIndex {
			count++
		}
	}

	if count > len(n.peers)/2 && n.commitIndex < len(n.log)-1 {
		n.commitIndex = len(n.log) - 1
		fmt.Printf("Leader %d updated commit index to %d\n", n.ID, n.commitIndex)
	}
}

// RaftService 实现RPC方法
type RaftService struct {
	node *RaftNode
}

// RequestVote 处理投票请求（RPC方法）
func (s *RaftService) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	// 1. 处理任期更新
	if args.Term > s.node.currentTerm {
		s.node.currentTerm = args.Term
		s.node.votedFor = 0
		// 如果之前是领导者，降级为跟随者
		if s.node.state == Leader {
			s.node.stateCh <- Follower
		} else {
			s.node.state = Follower
		}
	}

	// 2. 决定是否同意投票
	reply.Term = s.node.currentTerm
	reply.VoteGranted = false

	// 只接受当前或更高任期的请求
	if args.Term < s.node.currentTerm {
		return nil
	}

	// 检查是否已投票
	hasVoted := s.node.votedFor != 0 && s.node.votedFor != args.CandidateID
	if hasVoted {
		return nil
	}

	// 检查日志是否足够新
	lastLogIndex := len(s.node.log) - 1
	logOk := args.LastLogTerm > s.node.lastLogTerm ||
		(args.LastLogTerm == s.node.lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if logOk {
		reply.VoteGranted = true
		s.node.votedFor = args.CandidateID
		// 通知跟随者收到心跳（重置选举计时器）
		select {
		case s.node.heartbeat <- struct{}{}:
		default:
			// 通道已满，无需阻塞
		}
		fmt.Printf("Node %d voted for node %d\n", s.node.ID, args.CandidateID)
	}

	return nil
}

// AppendEntries 处理日志追加/心跳请求（RPC方法）
func (s *RaftService) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	// 1. 处理任期更新
	reply.Term = s.node.currentTerm
	reply.Success = false
	reply.Match = 0

	if args.Term < s.node.currentTerm {
		return nil
	}

	// 收到领导者消息，重置选举计时器
	select {
	case s.node.heartbeat <- struct{}{}:
	default:
		// 通道已满，无需阻塞
	}

	// 更新自己的任期，必要时降级
	if args.Term > s.node.currentTerm {
		s.node.currentTerm = args.Term
		s.node.votedFor = 0
		if s.node.state == Leader {
			s.node.stateCh <- Follower
		} else {
			s.node.state = Follower
		}
	}

	// 2. 检查前序日志是否匹配
	logLen := len(s.node.log)
	if args.PrevLogIndex >= logLen {
		// 这里不匹配的日志逐步向前删除，知道和Leader的完全一致
		reply.Match = logLen - 1
		return nil
	}

	if args.PrevLogIndex > 0 {
		// TODO: 在我们的实现中，log没有记录任期信息，后续有空了再增加吧
		// 这里的任期匹配校验先直接跳过
	}

	// 3. 处理日志追加
	if len(args.Entries) > 0 {
		if args.PrevLogIndex+1 > len(s.node.log) {
			reply.Success = false
			return nil
		}
		// 截断并追加日志
		s.node.log = append(s.node.log[:args.PrevLogIndex+1], args.Entries...)
		s.node.lastLogTerm = args.Term
		fmt.Printf("Node %d received logs: %v\n", s.node.ID, len(args.Entries))
		reply.Match = args.PrevLogIndex + len(args.Entries)
	}

	// 4. 更新提交索引
	if args.LeaderCommit > s.node.commitIndex {
		newCommit := args.LeaderCommit
		if newCommit >= len(s.node.log) {
			newCommit = len(s.node.log) - 1
		}
		s.node.commitIndex = newCommit
		fmt.Printf("Node %d updated commit index to %d\n", s.node.ID, s.node.commitIndex)
	}

	reply.Success = true
	return nil
}

// parsePeers 解析节点列表
func parsePeers(peersStr string) map[int]string {
	peers := make(map[int]string)
	if peersStr == "" {
		return peers
	}

	for _, entry := range strings.Split(peersStr, ",") {
		// 预期格式：id:host:port（三部分）
		parts := strings.Split(entry, ":")
		if len(parts) != 3 {
			fmt.Printf("Invalid node format, expected 'id:host:port', skipping entry: %s\n", entry)
			continue
		}
		// 解析节点ID
		id, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			fmt.Printf("Failed to parse node ID, skipping entry: %s, error: %v\n", entry, err)
			continue
		}
		// 组合地址为host:port
		addr := fmt.Sprintf("%s:%s", parts[1], parts[2])
		peers[int(id)] = addr
	}
	return peers
}

func main() {
	// 解析命令行参数
	id := flag.Int("id", 0, "Node ID (required)")
	port := flag.Int("port", 0, "Listening port (required)")
	peers := flag.String("peers", "", "Cluster nodes, format: id:host:port,id:host:port (required)")
	flag.Parse()

	if *id == 0 || *port == 0 || *peers == "" {
		log.Fatal("Missing parameters: -id, -port, and -peers are required")
	}

	// 初始化节点
	peerMap := parsePeers(*peers)
	// 打印解析后的节点列表（调试用）
	fmt.Printf("Parsed cluster nodes: %v\n", peerMap)

	node := NewRaftNode(*id, peerMap)

	// 注册RPC服务
	rpc.Register(&RaftService{node: node})
	rpc.HandleHTTP()

	// 启动HTTP服务器
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to start listener: %v", err)
	}
	fmt.Printf("Raft node %d started, listening on port %d\n", *id, *port)

	// 启动Raft节点
	node.Start()

	// 测试：如果是领导者，模拟一下客户端向服务端发送请求
	// TODO 每个节点再启动一个监听用于接收客户端请求
	go func() {
		for {
			time.Sleep(5 * time.Second)
			node.mu.Lock()
			isLeader := node.state == Leader
			node.mu.Unlock()

			if isLeader {
				// 对于leader，每次随机生成几条日志
				num := rand.Intn(10) + 1
				logs := make([]string, num)
				for i := 0; i < num; i = i + 1 {
					logs[i] = fmt.Sprintf("test log %d-%d", i, time.Now().Unix())
				}
				node.SendLogs(logs)
			}
		}
	}()

	// 启动HTTP服务处理RPC请求
	http.Serve(listener, nil)
}
