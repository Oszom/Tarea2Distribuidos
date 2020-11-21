package main

import (

	"Tarea2/NameNode/namenode"

	//"bufio"
	//"fmt"
	"log"
	"net"
	//"os"
	//"strings"
	"sync"

	"google.golang.org/grpc"
)

func GetOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

func servirServidor(wg *sync.WaitGroup, namenodeServer  *namenode.ServerNamenode, puerto string) {
	lis, err := net.Listen("tcp", ":"+puerto)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", puerto, err)
	}
	grpcServer := grpc.NewServer()

	namenode.RegisterNameNodeServiceServer(grpcServer, namenodeServer)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server over port %s: %v", puerto, err)
	}
}

func main(){
	var wg sync.WaitGroup

	log.Printf("El IP del Namenode actual es: %v", GetOutboundIP())

	sr := namenode.ServerRepartidor{}
	sn := namenode.ServerNamenode{}

	portsLibres = []string{"9000","9001","9002","9003"}
	for i, s := range portsLibres {
		we.Add(1)
	} 

	
}