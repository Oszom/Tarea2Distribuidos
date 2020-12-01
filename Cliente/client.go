package main

import (
	"io/ioutil"
	"regexp"
	"time"

	Roedor "Tarea2/Cliente/PicadorCriminalMutilador"
	"Tarea2/DataNode/datanode"
	"Tarea2/NameNode/namenode"
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

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

func subirLibro() {
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
			_, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}

			if err != nil {
				log.Fatalf("Error al recibir un mensaje: %v", err)
			}
			//log.Printf("El server retorna el siguiente mensaje: %v", in.Message)
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

func descargarChunk(maquina string, numChunk int32, nombreLibro string) {

	//Descargo un chunk en especifico de un datanode

	//log.Printf("Le voy a pedir el chunk %d a la maquina %s. Deseame suerte", numChunk, maquina)

	var conn *grpc.ClientConn
	conn, err := grpc.Dial(maquina+":9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
	}

	defer conn.Close()

	c := datanode.NewDatanodeServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	if cancel != nil {
		//
	}
	stream, err := c.ObtenerChunk(ctx)

	if err != nil {
		log.Fatalf("El datanode no responde, error de timeout.")
	}

	waitc := make(chan struct{})

	go func() {

		var Chunksito datanode.Chunk

		for { //Aquí se reciben los chunks
			in, err := stream.Recv()
			if err == io.EOF {
				close(waitc)
				return
			}

			if err != nil {
				log.Fatalf("Error al recibir un mensaje: %v", err)
			}

			Chunksito = datanode.Chunk{
				Content:        in.Content,
				NombreChunk:    in.NombreChunk,
				NombreOriginal: in.NombreOriginal,
				Parte:          in.Parte,
			}

			contenidoChunk := Chunksito.Content
			errorsito := ioutil.WriteFile(Chunksito.NombreChunk, contenidoChunk, 0644)
			if errorsito != nil {
				fmt.Println(errorsito)
			}
		}
	}()
	var mensaje datanode.Propuesta
	mensaje = datanode.Propuesta{
		NumChunk:    numChunk,
		Maquina:     maquina,
		NombreLibro: nombreLibro,
	}

	if err := stream.Send(&mensaje); err != nil {
		log.Fatalf("Failed to send a note: %v", err)
	}

	stream.CloseSend()
	<-waitc
	return

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
	listaChunks := novelaErotica.Chunks

	libro := novelaErotica.nombreLibro
	for i := 0; i < len(listaChunks); i++ {
		maquina := listaChunks[i].ubicacionChunk
		numChunk := listaChunks[i].numeroChunk
		descargarChunk(maquina, numChunk, libro)
	}

	Roedor.Juntar(libro, uint64(len(listaChunks)))
	fmt.Println("Libro descargado con éxito!")
}

func parsearListado(listado []LibrosMaquinas) []string {
	var enumeraciones []string
	enumeraciones = append(enumeraciones, "Listado de libros disponibles en el sistema.\n\n")
	for i := 0; i < len(listado); i++ {
		numerito := strconv.Itoa(i + 1)
		enumeraciones = append(enumeraciones, numerito+" - "+listado[i].nombreLibro+"\n")
	}
	return enumeraciones
}

func timeTrackSubida(start time.Time) {
	elapsed := time.Since(start)
	log.Printf("Subir el libro tomo %s", elapsed)
}

func timeTrackBajada(start time.Time, libro string) {
	elapsed := time.Since(start)
	log.Printf("Descargar el libro %s tomo %s", libro, elapsed)
}

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

	Funcion main

/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

func main() {
	for {
		decision := bufio.NewReader(os.Stdin)
		fmt.Printf("¿Que tarea desea realizar?\n1) Subir Libro\n2) Descargar Libro \n")
		choice, _ := decision.ReadString('\n')
		choice = strings.TrimSuffix(choice, "\n")
		choice = strings.TrimSuffix(choice, "\r")
		var comienzoSubida time.Time
		switch choice {
		case "1":
			comienzoSubida = time.Now()
			subirLibro()
			fmt.Println("Tiempo transcurrido en segundos es: " + fmt.Sprintf("%f", time.Since(comienzoSubida).Seconds()))
			//	timeTrackSubida(comienzoSubida)
		case "2":
			lista := getListaLibros()
			opciones := parsearListado(lista)
			for i := 0; i < len(opciones); i++ {
				fmt.Println(opciones[i])
			}
			inputValido := true
			var librilloInt int
			for inputValido {
				libroADescargar := bufio.NewReader(os.Stdin)
				fmt.Println("Elije el número de uno de los libros anteriores que deseas descargar: ")
				librillo, _ := libroADescargar.ReadString('\n')
				librillo = strings.TrimSuffix(librillo, "\n")
				librillo = strings.TrimSuffix(librillo, "\r")
				librilloInt, _ = strconv.Atoi(librillo)
				if librilloInt > 0 && librilloInt < len(opciones) {

					inputValido = false
				} else {
					fmt.Println("Por favor elija un número que esté dentro del rango indicado: ")
					inputValido = true
				}
			}
			libroElegido := lista[librilloInt-1]
			finsubida := time.Since(comienzoSubida)
			descargarLibro(libroElegido)

			//timeTrackBajada(comienzoBajada, libroElegido.nombreLibro)
		default:
			fmt.Printf("Por favor, ingrese una de las opciones indicadas (1 ó 2)\n")

		}
	}

}
