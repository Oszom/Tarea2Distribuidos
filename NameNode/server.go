package main

import (
	"Tarea2/DataNode/datanode"
	"Tarea2/NameNode/namenode"
	"bufio"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"
	//"bufio"
	//"fmt"
	//"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"sync"

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

func servirServidor(wg *sync.WaitGroup, namenodeServer *ServerNamenode, puerto string) {
	lis, err := net.Listen("tcp", ":"+puerto)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", puerto, err)
	}
	grpcServer := grpc.NewServer()

	namenode.RegisterNameNodeServiceServer(grpcServer, namenodeServer)

	log.Printf("El Namenode esta listo para recibir conexiones.")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server over port %s: %v", puerto, err)
	}
}

func main() {

	var wg sync.WaitGroup

	log.Printf("El IP del Namenode actual es: %v", getOutboundIP())

	sn := ServerNamenode{}

	wg.Add(1)
	go servirServidor(&wg, &sn, "9000")
	wg.Wait()

}

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
		Definiciones de structs para
		el uso en los servidores
		de namenode.
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

//ServerNamenode is
type ServerNamenode struct {
	hola int
}

//IntentoPropuesta is
type IntentoPropuesta struct {
	Chunk       int32
	Maquina     string
	NombreLibro string
}

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
		Definiciones de los metodos del servidor gRPC
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

//MandarPropuesta is
func (sr *ServerNamenode) MandarPropuesta(stream namenode.NameNodeService_MandarPropuestaServer) error {

	var listaPropuesta []IntentoPropuesta
	var ElementoPropuesta IntentoPropuesta

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		ElementoPropuesta = IntentoPropuesta{
			Chunk:       in.NumChunk,
			Maquina:     in.Maquina,
			NombreLibro: in.NombreLibro,
		}
		listaPropuesta = append(listaPropuesta, ElementoPropuesta)
	}

	log.Printf("La propuesta recibida es %v\n", listaPropuesta)

	propuestasOrdenadas := compilarPropuestasMaquinas(listaPropuesta)
	resultadoPropuestas := chequearPropuesta(propuestasOrdenadas)

	existenFalsos := false
	if !resultadoPropuestas["dist58"] {
		existenFalsos = true
	}
	if !resultadoPropuestas["dist59"] {
		existenFalsos = true
	}
	if !resultadoPropuestas["dist60"] {
		existenFalsos = true
	}
	var propuestaAEnviar []IntentoPropuesta
	if existenFalsos {
		newPropuesta := nuevaPropuesta(resultadoPropuestas, len(listaPropuesta), listaPropuesta[0].NombreLibro) //Propuesta Namenode
		textoAEscribir := formatearTexto(newPropuesta)
		escribirLog("log.txt", textoAEscribir)
		propuestaAEnviar = newPropuesta

	} else {
		textoAEscribir := formatearTexto(listaPropuesta) //Propuesta del Datanode
		escribirLog("log.txt", textoAEscribir)
		propuestaAEnviar = listaPropuesta
		//Escribir en el log las propuestas
		//Mandar propuesta al Datanode
	}

	log.Printf("La propuesta a enviar es %v\n", propuestaAEnviar)

	for i := 0; i < len(propuestaAEnviar); i++ {

		if err := stream.Send(&namenode.Propuesta{
			NumChunk:    propuestaAEnviar[i].Chunk,
			Maquina:     propuestaAEnviar[i].Maquina,
			NombreLibro: propuestaAEnviar[i].NombreLibro}); err != nil {
			return err
		}
	}

	//Se envia la respuesta al cliente

	return nil
}

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
				Funciones auxiliares
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

type struct LibrosMaquinas {
	string nombreLibro,
	[]ubicacionesChunks Chunks,
}

type struct ubicacionesChunks {
	int32 numeroChunk,
	string ubicacionChunk,
}

func obtenerListadeLibros() []string {
	var texto []string
	var listaLibros []string
	file, err := os.Open("NameNode/log.txt")

	if err != nil {
		log.Fatalf("Fallo al abrir el archivo.")
	}

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		if len(scanner.Text()) != 0 {
			texto = append(texto, scanner.Text())
		}
	}

	file.Close()
	count := 0

	nombreLibro := strings.Split(texto[0], " ")[0]
	nPartes, _ := strconv.Atoi(strings.Split(texto[0], " ")[2])
	listaLibros = append(listaLibros, nombreLibro)
	for i := 0; i < len(texto); i++ {
		if count == nPartes+1 {
			nombreLibro = strings.Split(texto[i], " ")[0]
			listaLibros = append(listaLibros, nombreLibro)
			nPartes, _ = strconv.Atoi(strings.Split(texto[i], " ")[2])
			count = 0
		}
		count = count + 1

	}
	return listaLibros
}

func formatearTexto(propuesta []IntentoPropuesta) []string {
	textoCompleto := []string{}
	textoCompleto = append(textoCompleto, propuesta[0].NombreLibro+" Cantidad_Partes "+fmt.Sprint(len(propuesta))+"\n")
	for i := 0; i < len(propuesta); i++ {
		linea := propuesta[i].NombreLibro + "_parte_" + fmt.Sprintf("%d", propuesta[i].Chunk) + " " + propuesta[i].Maquina + "\n"
		textoCompleto = append(textoCompleto, linea)
	}
	return textoCompleto

}

func escribirLog(archivo string, texto []string) {
	file, err := os.OpenFile(archivo, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Error creando el archivo: %s", err)
	}

	datawriter := bufio.NewWriter(file)

	for _, data := range texto {
		_, _ = datawriter.WriteString(data + "\n")
	}

	datawriter.Flush()
	file.Close()
}

func compilarPropuestasMaquinas(azucar []IntentoPropuesta) map[string]int {

	harina := make(map[string]int)
	harina["dist58"] = 0
	harina["dist59"] = 0
	harina["dist60"] = 0

	var levadura IntentoPropuesta
	huevo := 1

	for i := 0; i < len(azucar); i++ {
		levadura = azucar[i]
		harina[levadura.Maquina] = harina[levadura.Maquina] + huevo
	}

	return harina

}

func nuevaPropuesta(propuestaAnterior map[string]bool, nChunks int, nombreLibro string) []IntentoPropuesta {

	var listaPropuesta []IntentoPropuesta

	var maquinasDisponibles []string

	if propuestaAnterior["dist58"] {
		maquinasDisponibles = append(maquinasDisponibles, "dist58")
	}
	if propuestaAnterior["dist59"] {
		maquinasDisponibles = append(maquinasDisponibles, "dist59")
	}
	if propuestaAnterior["dist60"] {
		maquinasDisponibles = append(maquinasDisponibles, "dist60")
	}

	for i := 0; i < nChunks; i++ {
		posMaq := i % len(maquinasDisponibles)
		listaPropuesta = append(listaPropuesta, IntentoPropuesta{
			Chunk:       int32(i),
			Maquina:     maquinasDisponibles[posMaq],
			NombreLibro: nombreLibro,
		})
	}

	return listaPropuesta

}

func chequearPropuesta(propuestita map[string]int) map[string]bool {

	maquinasDatanode := []string{"dist58", "dist59", "dist60"}
	quienSeQuedaConLaCocaDeMaradona := make(map[string]bool)

	for i := 0; i < len(maquinasDatanode); i++ {
		maqActual := maquinasDatanode[i]
		chunksAUsar := propuestita[maqActual]

		quieroHacerUnaLinea := consultaDatanode(maqActual, chunksAUsar)
		quienSeQuedaConLaCocaDeMaradona[maqActual] = quieroHacerUnaLinea

	}

	return quienSeQuedaConLaCocaDeMaradona
}

/*
	consultaDatanode realiza una consulta a un datanode para ver si tiene
	problemas con la propuesta.

	Retorna:
	true si la acepta y false si la rechaza
*/

func consultaDatanode(nombreMaquina string, nEsposos int) bool {

	var conn *grpc.ClientConn
	conn, err := grpc.Dial(nombreMaquina+":9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
	}

	defer conn.Close()

	c := datanode.NewDatanodeServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	if cancel != nil {
		log.Print(cancel)
	}

	stream, err := c.VerificarPropuesta(ctx)

	if err != nil {
		//Error por timeout
		return false
	}

	waitc := make(chan bool)

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

			//Veo si ocurrio un error random
			if in.FalloRandom {
				waitc <- false
			} else {
				if in.Capacidad > int32(nEsposos) {
					waitc <- true
				} else {
					waitc <- false
				}
			}

		}
	}()

	var mensaje datanode.CantidadChunks

	mensaje = datanode.CantidadChunks{
		Chunks: int32(nEsposos),
	}

	if err := stream.Send(&mensaje); err != nil {
		log.Fatalf("Failed to send a note: %v", err)
	}

	stream.CloseSend()
	retornoDatanode := <-waitc
	return retornoDatanode

}
