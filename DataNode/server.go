package main

import (
	"Tarea2/DataNode/datanode"
	"Tarea2/NameNode/namenode"
	"context"
	"io"

	//"io/ioutil"
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
	NumChunk    int32
	NombreLibro string
	Maquina     string
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
	archivoChunks := make(map[string][]byte)
	var nombreLibro string
	//Recibo los chunks de parte del cliente
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		nombreLibro = in.NombreOriginal
		archivoChunks[in.NombreChunk] = in.Content
	}

	log.Printf("Deje de recibir archivos")

	//Generar la estructura de datos de las propuestas
	listaPropuestaInicial := primeraPropuesta(len(archivoChunks), nombreLibro)

	//Se envia la propuesta al namenode
	listaPropuestaValida, errorConn := propuestaNamenode(listaPropuestaInicial)

	log.Printf("La propuesta obtenida desde el namenode es: %v \n Acaso hubo un error por timeout?: %v", listaPropuestaValida, errorConn)

	//Se envia la respuesta al cliente
	if err := stream.Send(&datanode.UploadStatus{
		Message: "Libro Subido con Exito",
		Code:    datanode.UploadStatusCode_Ok}); err != nil {
		return err
	}

	return nil
}

/*
	VerificarPropuesta le responde al namenode si este es capaz de recibir los chunks
	designados en la propuesta generada para repartir los chunks
*/
func (dn *DatanodeServer) VerificarPropuesta(stream datanode.DatanodeService_VerificarPropuestaServer) error {
	for {

		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		log.Printf("El namenode me pregunto si acepto la propuesta con un numero de %d Chunks", in.Chunks)

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

/* func -> wea para enviar entre datanodes

nameFile := in.NombreChunk

mensaje := in.Content


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
*/

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
				Funciones auxiliares
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

//Genera la propuesta inicial
func primeraPropuesta(nChunks int, nombreLibro string) []namenode.Propuesta {

	var listaPropuesta []namenode.Propuesta

	maquinasDisponibles := []string{"dist58", "dist59", "dist60"}

	for i := 0; i < nChunks; i++ {
		posMaq := i % len(maquinasDisponibles)
		listaPropuesta = append(listaPropuesta, namenode.Propuesta{
			NumChunk:    int32(i),
			Maquina:     maquinasDisponibles[posMaq],
			NombreLibro: nombreLibro,
		})
	}

	log.Print("Genere la primera propuesta")

	return listaPropuesta

}

//Manda la propuesta inicial al namenode y recibe una propuesta valida
func propuestaNamenode(propuesta []namenode.Propuesta) ([]Propuesta, bool) {

	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist57:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
	}

	defer conn.Close()

	c := namenode.NewNameNodeServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	if cancel != nil {
		log.Print(cancel)
	}

	stream, err := c.MandarPropuesta(ctx)

	if err != nil {
		//Error por timeout
		return propuesta, true
	}

	waitc := make(chan []Propuesta)
	//Dejamos que un thread reciba la propuesta del namenode
	go func() {
		var listaPropuestaNamenode []Propuesta
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				waitc <- listaPropuestaNamenode
				close(waitc)
				return
			}

			if err != nil {
				log.Fatalf("Error al recibir un mensaje: %v", err)
			}

			listaPropuestaNamenode = append(listaPropuestaNamenode,
				Propuesta{
					NumChunk:    in.NumChunk,
					Maquina:     in.Maquina,
					NombreLibro: in.NombreLibro,
				})
		}
	}()
	var mensajePropuesta namenode.Propuesta
	//Enviamos la propuesta al Namenode
	for i := 0; i < len(propuesta); i++ {
		mensajePropuesta = namenode.Propuesta{
			NumChunk:    propuesta[i].NumChunk,
			Maquina:     propuesta[i].Maquina,
			NombreLibro: propuesta[i].NombreLibro,
		}
		if err := stream.Send(&mensajePropuesta); err != nil {
			log.Fatalf("Failed to send a note: %v", err)
		}
	}

	stream.CloseSend()
	retornoDatanode := <-waitc
	return retornoDatanode, false

}
