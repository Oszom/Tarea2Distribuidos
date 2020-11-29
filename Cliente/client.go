package main

import (
	"io/ioutil"
	"regexp"
	"time"

	"Tarea2/DataNode/datanode"
	"Tarea2/NameNode/namenode"
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

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

	Structs Auxiliares usados para ordenar los libros obtenidos

/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

type ubicacionesChunks struct {
	numeroChunk    int32
	ubicacionChunk string
}

//LibrosMaquinas is
type LibrosMaquinas struct {
	nombreLibro string
	Chunks      []ubicacionesChunks
}

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

	Funciones para cada una de las funcialidades del cliente

/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

func enviarArchivo() {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist58:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
	}

	defer conn.Close()

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

func getListaLibros() []LibrosMaquinas {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist57:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
	}

	defer conn.Close()

	c := namenode.NewNameNodeServiceClient(conn)

	stream, _ := c.GetListaLibros(context.Background())

	waitc := make(chan []LibrosMaquinas)

	go func() {

		var listaLibros []LibrosMaquinas

		for {
			in, err := stream.Recv()
			if err == io.EOF {
				waitc <- listaLibros
				close(waitc)
				return
			}

			if err != nil {
				log.Fatalf("Error al recibir un mensaje: %v", err)
			}

			infoCurrLibro := parserLibroActual(in)

			listaLibros = append(listaLibros, LibrosMaquinas{
				nombreLibro: in.NombreLibro,
				Chunks:      infoCurrLibro,
			})

		}
	}()

	var mensaje namenode.EmptyMessage

	if err := stream.Send(&mensaje); err != nil {
		log.Fatalf("Failed to send a note: %v", err)
	}

	stream.CloseSend()
	librosActuales := <-waitc
	return librosActuales
}

func descargarChunk(maquina string, nombreChunk string) {

	//Descargo un chunk en especifico de un datanode

}

func parserLibroActual(libroActual *namenode.LibroEnSistema) []ubicacionesChunks {

	var chunksArchivos []ubicacionesChunks

	for i := 0; i < len(libroActual.Chunks); i++ {
		chunkActual := libroActual.Chunks[i]

		chunksArchivos = append(chunksArchivos, ubicacionesChunks{
			numeroChunk:    chunkActual.NumChunk,
			ubicacionChunk: chunkActual.Maquina,
		})
	}

	return chunksArchivos
}

func descargarLibro(novelaErotica LibrosMaquinas) {

	//Itero por los chunks y los bajo a su propia carpeta

	//Si un chunk falta, falla toda la operacion

	//Los pego y borro despues de unirlos

	//Aviso por pantalla

}

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

	Funcion main

/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

func main() {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist58:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
	}

	defer conn.Close()

	enviarArchivo()

}
