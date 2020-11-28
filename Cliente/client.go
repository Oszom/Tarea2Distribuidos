package main

import (
	"io/ioutil"
	"regexp"
	"time"

	//"Tarea2/NameNode/namenode"
	"Tarea2/DataNode/datanode"
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	Roedor "Tarea2/Cliente/PicadorCriminalMutilador"

	"google.golang.org/grpc"
)

func pruebaMandar(conn *grpc.ClientConn) {
	readerTipo := bufio.NewReader(os.Stdin)
	fmt.Printf("Ingrese nombre del archivo: ")
	filePath, _ := readerTipo.ReadString('\n')
	filePath = strings.TrimSuffix(filePath, "\n")
	filePath = strings.TrimSuffix(filePath, "\r")

	regex, _ := regexp.Compile("\\/?([^\\/]+\\..+)$")

	fileName := regex.FindStringSubmatch(filePath)[1]

	partes := Roedor.Cortar(filePath)
	c := datanode.NewDatanodeServiceClient(conn)

	stream, _ := c.SubirArchivo(context.Background())

	waitc := make(chan struct{})

	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}

			if err != nil {
				log.Fatalf("Error al recibir un mensaje: %v", err)
			}
			log.Printf("El server retorna el siguiente mensaje: %v", in.Message)
		}
	}()

	var mensaje datanode.Chunk

	for i := 0; i < len(partes); i++ {
		chunkBytes, errBytes := ioutil.ReadFile(partes[i])

		if errBytes != nil {
			fmt.Print(errBytes)
		}

		mensaje = datanode.Chunk{
			Content:        chunkBytes,
			NombreChunk:    partes[i],
			NombreOriginal: fileName,
			Parte:          int32(i),
		}

		if err := stream.Send(&mensaje); err != nil {
			log.Fatalf("Failed to send a note: %v", err)
		}

		os.Remove(partes[i])

		time.Sleep(1 * time.Second)
	}

	stream.CloseSend()
	<-waitc
}

func pruebaPropuesta(conn *grpc.ClientConn) {

	c := datanode.NewDatanodeServiceClient(conn)

	stream, _ := c.VerificarPropuesta(context.Background())

	waitc := make(chan struct{})

	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}

			if err != nil {
				log.Fatalf("Error al recibir un mensaje: %v", err)
			}
			log.Printf("El server retorna el siguiente mensaje: %v %v", in.Capacidad, in.FalloRandom)
		}
	}()

	var mensaje datanode.CantidadChunks

	mensaje = datanode.CantidadChunks{
		Chunks: int32(2),
	}

	if err := stream.Send(&mensaje); err != nil {
		log.Fatalf("Failed to send a note: %v", err)
	}

	stream.CloseSend()
	<-waitc
}

func main() {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist58:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
	}

	defer conn.Close()

	pruebaMandar(conn)

}
