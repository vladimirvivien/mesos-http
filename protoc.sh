protoc  --proto_path=$GOPATH/src:.  --go_out=. ./mesos/mesos/mesos.proto
protoc  --proto_path=$GOPATH/src:.  --go_out=. ./mesos/exec/executor.proto
protoc  --proto_path=$GOPATH/src:.  --go_out=. ./mesos/sched/scheduler.proto
