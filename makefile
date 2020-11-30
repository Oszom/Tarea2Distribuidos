#················································Name Node·························································
.PHONY: runNamenode
runNamenode: 
	rm -f log.txt
	protoc -I NameNode/namenode NameNode/namenode/namenode.proto --go_out=plugins=grpc:./
	go run NameNode/server.go

.PHONY: compileNamenode
compileNamenode:
	protoc -I NameNode/namenode NameNode/namenode/namenode.proto --go_out=plugins=grpc:./
#··················································································································
#················································Data Node·························································
.PHONY: runDatanode
runDatanode: 
	rm -rf libro
	protoc -I DataNode/datanode DataNode/datanode/datanode.proto --go_out=plugins=grpc:./
	go run DataNode/server.go

.PHONY: compileDatanode
compileDatanode:
	protoc -I DataNode/datanode DataNode/datanode/datanode.proto --go_out=plugins=grpc:./
	
#··················································································································
#················································Cliente···························································
.PHONY: runCliente
runCliente: 
	go run Cliente/client.go
#··················································································································

.PHONY: rn
rn:
	git pull
	rm -f log.txt
	protoc -I NameNode/namenode NameNode/namenode/namenode.proto --go_out=plugins=grpc:./
	go run NameNode/server.go

.PHONY: rc
rc:
	git pull
	go run Cliente/client.go

.PHONY: rd
rd:
	rm -rf libro
	protoc -I DataNode/datanode DataNode/datanode/datanode.proto --go_out=plugins=grpc:./
	go run DataNode/server.go
