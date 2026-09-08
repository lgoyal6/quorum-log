# quorum-log

A replicated metadata/lease log. This project **integrates** the production-grade
Raft implementation from [go.etcd.io/raft](https://github.com/etcd-io/raft)
(etcd/raft); it does not invent or reimplement the consensus algorithm.
