.PHONY: arreglarPath
arreglarPath:
	export PATH="$PATH:$(go env GOPATH)/bin"

#················································Logistica·························································
.PHONY: runLogistica
runLogistica: 
	protoc -I Logistica/logistica/ Logistica/logistica/logistica.proto --go_out=plugins=grpc:./
	go run Logistica/server.go

.PHONY: compileLogistica
compileLogistica:
	protoc -I Logistica/logistica/ Logistica/logistica/logistica.proto --go_out=plugins=grpc:./
#protoc -I Logistica\logistica\ Logistica\logistica\logistica.proto --go_out=plugins=grpc:.\
#··················································································································
#················································Finanzas·························································
.PHONY: runFinanzas
runFinanzas: 
	protoc -I Finanzas/finanza/ Finanzas/finanza/finanza.proto --go_out=plugins=grpc:./
	go run Finanzas/server.go

.PHONY: compileFinanzas
compileFinanzas:
	protoc -I Finanzas/finanza/ Finanzas/finanza/finanza.proto --go_out=plugins=grpc:./
#protoc -I Logistica\logistica\ Logistica\logistica\logistica.proto --go_out=plugins=grpc:Logistica\logistica\
#··················································································································
#················································Cliente···························································
.PHONY: runCliente
runCliente: 
	go run Cliente/client.go
#··················································································································
#················································Camion····························································
.PHONY: runCamion
runCamion: 
	go run Camiones/camion.go
#protoc -I Camiones\camion\ Camiones\camion\camion.proto --go_out=plugins=grpc:Camiones\camion
#··················································································································