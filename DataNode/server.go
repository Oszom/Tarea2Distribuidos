package main

import (
	"Tarea2/DataNode/datanode"
	"Tarea2/NameNode/namenode"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"syscall"
	"time"

	//"bufio"
	//"fmt"
	"log"
	"net"

	//"os"
	//"strings"
	"sync"

	wr "github.com/mroth/weightedrand"
	"google.golang.org/grpc"
)

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
		Funciones relacionadas a iniciación del servidor
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP
}

func servirServidor(wg *sync.WaitGroup, datanodeServer *DatanodeServer, puerto string) {
	lis, err := net.Listen("tcp", ":"+puerto)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", puerto, err)
	}
	grpcServer := grpc.NewServer()

	datanode.RegisterDatanodeServiceServer(grpcServer, datanodeServer)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server over port %s: %v", puerto, err)
	}
}

func main() {

	var wg sync.WaitGroup

	log.Printf("El IP del Namenode actual es: %v", getOutboundIP())

	sr := DatanodeServer{}
	//sn := namenode.ServerNamenode{}

	wg.Add(1)
	go servirServidor(&wg, &sr, "9000")
	wg.Wait()

}

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
		Definiciones de structs para
		el uso en los servidores
		de datanode.
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

//DatanodeServer is
type DatanodeServer struct {
	hola int
}

//Propuesta is
type Propuesta struct {
	Chunk          string
	NombreOriginal string
	Ubicacion      int
}

//IntentoPropuesta is
type IntentoPropuesta struct {
	Chunk   int32
	Maquina string
}

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
		Definiciones de los metodos del servidor gRPC
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

//SubirArchivo is
func (dn *DatanodeServer) SubirArchivo(stream datanode.DatanodeService_SubirArchivoServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		mensaje := in.Content
		nameFile := in.NombreChunk

		if _, err12 := os.Stat("/libro"); os.IsNotExist(err12) {
			errFolder := os.Mkdir("libro", 0755)
			if errFolder != nil {
				//log.Printf(err)
			}
		}

		Andres := ioutil.WriteFile("libro/"+nameFile, mensaje, 0644)
		if Andres != nil {
			log.Printf("%v", Andres)
		}

		log.Printf("Chunk %s recibido con exito.", in.NombreChunk)

		//Se envia la respuesta al cliente
		if err := stream.Send(&datanode.UploadStatus{
			Message: "Chunk recibido",
			Code:    datanode.UploadStatusCode_Ok}); err != nil {
			return err
		}

	}
}

//VerificarPropuesta is
func (dn *DatanodeServer) VerificarPropuesta(stream datanode.DatanodeService_VerificarPropuestaServer) error {
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		var stat syscall.Statfs_t

		wd, err := os.Getwd()

		syscall.Statfs(wd, &stat)

		// Available blocks * size per block = available space in bytes
		trocitos := stat.Bavail * uint64(stat.Bsize) / (250 * (1 << 10))

		// Probabilidad aleatoria de que el datanode falle porque se siente mal
		rand.Seed(time.Now().UTC().UnixNano())
		eleccion, _ := wr.NewChooser(
			wr.Choice{Item: true, Weight: 1},
			wr.Choice{Item: false, Weight: 9},
		)
		haFallado := eleccion.Pick().(bool)

		//Se envia la respuesta al cliente
		if err := stream.Send(&datanode.IsAlive{
			Capacidad:   int32(trocitos),
			FalloRandom: haFallado,
		}); err != nil {
			return err
		}

	}
}

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
				Funciones auxiliares
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

func primeraPropuesta(nChunks int) []namenode.Propuesta {

	var listaPropuesta []namenode.Propuesta

	maquinasDisponibles := []string{"dist58", "dist59", "dist60"}

	for i := 0; i < nChunks; i++ {
		posMaq := i % len(maquinasDisponibles)
		listaPropuesta = append(listaPropuesta, namenode.Propuesta{
			NumChunk: int32(i),
			Maquina:  maquinasDisponibles[posMaq],
		})
	}

	return listaPropuesta

}

func generarPropuesta(propuesta []Propuesta) {

}
