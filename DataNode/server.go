package main

import (
	"Tarea2/DataNode/datanode"
	"Tarea2/NameNode/namenode"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"bufio"
	"time"

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

	log.Printf("El Datanode esta listo para recibir conexiones.")

	datanode.RegisterDatanodeServiceServer(grpcServer, datanodeServer)

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server over port %s: %v", puerto, err)
	}
}

func main() {

	var wg sync.WaitGroup

	log.Printf("El IP del Datanode actual es: %v", getOutboundIP())
	log.Printf("El hostname es: %s", getHostname())
	isDistributed := false
	tieneElPabloLaRazon := true

	if strings.Contains(getHostname(), "dist58") {
		for tieneElPabloLaRazon{
			decision := bufio.NewReader(os.Stdin)
			fmt.Printf("¿Con que algoritmo de exclusion quiere que funcione el sistema?\n1) Distribuido\n2) Centralizado \n")
			choice, _ := decision.ReadString('\n')
			choice = strings.TrimSuffix(choice, "\n")
			choice = strings.TrimSuffix(choice, "\r")
			switch choice {
			case "1":
				isDistributed = true
				tieneElPabloLaRazon = false
				break
			case "2":
				isDistributed = false
				tieneElPabloLaRazon = false
				break
			default:
				fmt.Printf("Por favor, ingrese una de las opciones indicadas (1 ó 2)\n")
			}
		}
	}

	sr := DatanodeServer{}
	sr.isDistribuido = isDistributed

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
	isDistribuido bool
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

	var listaPropuestaValida []Propuesta
	var errorConn error

	if dn.isDistribuido {
		listaPropuestaValida, errorConn = manejoPropuestaDistribuida(len(listaPropuestaInicial), nombreLibro)
	} else {
		//Se envia la propuesta al namenode
		listaPropuestaValida, errorConn = propuestaNamenode(listaPropuestaInicial)
	}

	if errorConn != nil {
		return errorConn
	}

	log.Printf("La propuesta obtenida es: %v \n Acaso hubo un error por timeout?: %v", listaPropuestaValida, errorConn)

	for i := 0; i < len(listaPropuestaValida); i++ {
		actualChunk := listaPropuestaValida[i]
		if actualChunk.Maquina != "dist58" {
			nombreChunkActual := actualChunk.NombreLibro + "_parte_" + fmt.Sprintf("%d", actualChunk.NumChunk)
			mandarChunk(archivoChunks[nombreChunkActual], actualChunk.Maquina, nombreChunkActual, nombreLibro)
		} else {
			if _, err12 := os.Stat("libro/"); os.IsNotExist(err12) {
				errFolder := os.Mkdir("libro", 0755)
				if errFolder != nil {
					//log.Printf(err)
				}
				if _, err12 := os.Stat("libro/" + nombreLibro); os.IsNotExist(err12) {
					errFolder := os.Mkdir("libro/"+nombreLibro, 0755)
					if errFolder != nil {
						//log.Printf(err)
					}

				}

			}

			chunkName := nombreLibro + "_parte_" + fmt.Sprintf("%d", actualChunk.NumChunk)

			Andres := ioutil.WriteFile("libro/"+nombreLibro+"/"+chunkName, archivoChunks[chunkName], 0644)
			if Andres != nil {
				log.Printf("%v", Andres)
			}

			log.Printf("Me llego el chunk %s", chunkName)
		}
	}

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

		if haFallado {
			log.Printf("[Entra Carlos Pinto a la escena, llena de humo]\nNada hizo pensar al datanode que le ocurriria un fallo en este mismo momento.")
		}

		//Se envia la respuesta al cliente
		if err := stream.Send(&datanode.IsAlive{
			Capacidad:   int32(trocitos),
			FalloRandom: haFallado,
		}); err != nil {
			return err
		}

	}
}

//CompartirArchivoDatanode corresponde a la funcion encargada de recibir uno de los chunks desde un datanode a otro
func (dn *DatanodeServer) CompartirArchivoDatanode(stream datanode.DatanodeService_CompartirArchivoDatanodeServer) error {
	for {

		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		/*
			Hago algo con lo que recibo
		*/

		nameFile := in.NombreChunk

		mensaje := in.Content

		if _, err12 := os.Stat("libro/"); os.IsNotExist(err12) {
			errFolder := os.Mkdir("libro", 0755)
			if errFolder != nil {
				//log.Printf(err)
			}
			if _, err12 := os.Stat("libro/" + in.NombreOriginal); os.IsNotExist(err12) {
				errFolder := os.Mkdir("libro/"+in.NombreOriginal, 0755)
				if errFolder != nil {
					//log.Printf(err)
				}

			}

		}

		Andres := ioutil.WriteFile("libro/"+in.NombreOriginal+"/"+nameFile, mensaje, 0644)
		if Andres != nil {
			log.Printf("Falle aqui %v", Andres)
		}

		log.Printf("Me llego el chunk %s", nameFile)

		//Se envia la respuesta al cliente
		if err := stream.Send(&datanode.UploadStatus{
			Message: "Chunk recibido con exito",
			Code:    datanode.UploadStatusCode_Ok,
		}); err != nil {
			return err
		}

	}
}

//ObtenerChunk is my first love, the one i can't live without. The one who stole my heart away and broke it wihout remorse
func (dn *DatanodeServer) ObtenerChunk(stream datanode.DatanodeService_ObtenerChunkServer) error {

	for {

		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		/*
			Hago algo con lo que recibo
		*/

		pathArchivo := "libro/" + in.NombreLibro + "/" + in.NombreLibro + "_parte_" + fmt.Sprintf("%d", in.NumChunk)

		log.Printf("Me pidieron el archivo con el siguiente path:\n%s",pathArchivo)

		if fileExists(pathArchivo) {

			chunkBytes, errBytes := ioutil.ReadFile(pathArchivo)

			if errBytes != nil {
				return errBytes
			}

			NombreChunk := in.NombreLibro + "_parte_" + fmt.Sprintf("%d", in.NumChunk)

			if err := stream.Send(&datanode.Chunk{
				Content:        chunkBytes,
				NombreChunk:    NombreChunk,
				NombreOriginal: in.NombreLibro,
				Parte:          in.NumChunk,
			}); err != nil {
				return err
			}

		} else {
			return errors.New("Archivo no encontrado")
		}

		//Se envia la respuesta al cliente

	}
}

/*
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
				Funciones auxiliares
/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
*/

func manejoPropuestaDistribuida(nChunks int, nombreLibro string) ([]Propuesta, error) {

	var nuevaPropuesta []Propuesta

	var primeraPropuesta []Propuesta

	maquinasDisponibles := []string{"dist58", "dist59", "dist60"}

	for i := 0; i < nChunks; i++ {
		posMaq := i % len(maquinasDisponibles)
		primeraPropuesta = append(primeraPropuesta, Propuesta{
			NumChunk:    int32(i),
			Maquina:     maquinasDisponibles[posMaq],
			NombreLibro: nombreLibro,
		})
	}

	//Map donde se muestra si la maquina aceota la propuesta
	namenodeAprueba := make(map[string]bool)
	var maquinasQueSiPueden []string

	namenodeAprueba["dist58"] = false
	namenodeAprueba["dist59"] = false
	namenodeAprueba["dist60"] = false

	papa2020 := true

	for i := 0; i < len(namenodeAprueba); i++ {

		maquinaActual := maquinasDisponibles[i]
		var sipoApruebo bool

		if strings.Contains(maquinaActual, "dist58") {

			sipoApruebo = autoApruebo(int(primeraPropuesta[i].NumChunk))

		} else {

			sipoApruebo = consultaDatanode(maquinaActual, primeraPropuesta[i].NumChunk)

		}

		namenodeAprueba[maquinaActual] = sipoApruebo
		if sipoApruebo{
			maquinasQueSiPueden = append(maquinasQueSiPueden,maquinaActual)
		}
		papa2020 = papa2020 && sipoApruebo

	}

	if papa2020{
		nuevaPropuesta = primeraPropuesta
	} else {
		if len(maquinasQueSiPueden) == 0 {
			return []Propuesta{}, errors.New("Ninguna de las maquinas puede aceptar la propuesta")
		} else {
			for i := 0; i < nChunks; i++ {
				posMaq := i % len(maquinasQueSiPueden)
				nuevaPropuesta= append(nuevaPropuesta, Propuesta{
					NumChunk:       int32(i),
					Maquina:     maquinasDisponibles[posMaq],
					NombreLibro: nombreLibro,
				})
			}
		}
	}

	//Segunda Propuesta

	mandarAlLog(nuevaPropuesta)

	return nuevaPropuesta, nil

}

func autoApruebo(nChunks int) bool {

	log.Printf("El namenode me pregunto si acepto la propuesta con un numero de %d Chunks", nChunks)

	var stat syscall.Statfs_t

	wd, _ := os.Getwd()

	syscall.Statfs(wd, &stat)

	// Available blocks * size per block = available space in bytes
	trocitos := stat.Bavail * uint64(stat.Bsize) / (250 * (1 << 10))

	// Probabilidad aleatoria de que el datanode falle porque se siente mal
	rand.Seed(time.Now().UTC().UnixNano())
	eleccion, _ := wr.NewChooser(
		wr.Choice{Item: false, Weight: 1},
		wr.Choice{Item: true, Weight: 9},
	)
	haFallado := eleccion.Pick().(bool)

	return haFallado && int(trocitos) > nChunks
}

func compilarPropuestasMaquinas(azucar []Propuesta) map[string]int {

	harina := make(map[string]int)
	harina["dist58"] = 0
	harina["dist59"] = 0
	harina["dist60"] = 0

	var levadura Propuesta
	huevo := 1

	for i := 0; i < len(azucar); i++ {
		levadura = azucar[i]
		harina[levadura.Maquina] = harina[levadura.Maquina] + huevo
	}

	return harina

}

func consultaDatanode(nombreMaquina string, nEsposos int32) bool {

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
func propuestaNamenode(propuesta []namenode.Propuesta) ([]Propuesta, error) {

	var conn *grpc.ClientConn
	conn, err := grpc.Dial("dist57:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
	}

	defer conn.Close()

	c := namenode.NewNameNodeServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

	if cancel != nil {
		//
	}

	stream, err := c.MandarPropuesta(ctx)

	if err != nil {
		log.Fatalf("El namenode no respondio a tiempo")
	}

	if err != nil {
		var propuestaARetornar []Propuesta
		for i := 0; i < len(propuesta); i++ {
			propuestaARetornar = append(propuestaARetornar, Propuesta{
				NumChunk:    propuesta[i].NumChunk,
				Maquina:     propuesta[i].Maquina,
				NombreLibro: propuesta[i].NombreLibro,
			})

		}

		//Error por timeout
		return propuestaARetornar, errors.New("Timeout en la conexion")
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
	return retornoDatanode, nil

}

func mandarChunk(chunkActual []byte, maquinaDestino string, NombreChunk string, nombreOriginal string) bool {

	var conn *grpc.ClientConn
	conn, err := grpc.Dial(maquinaDestino+":9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
		return false
	}

	defer conn.Close()

	c := datanode.NewDatanodeServiceClient(conn)

	stream, _ := c.CompartirArchivoDatanode(context.Background())

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

	mensaje = datanode.Chunk{
		Content:        chunkActual,
		NombreChunk:    NombreChunk,
		NombreOriginal: nombreOriginal,
		Parte:          int32(0),
	}

	if err := stream.Send(&mensaje); err != nil {
		log.Fatalf("Failed to send a note: %v", err)
		return false
	}

	stream.CloseSend()
	<-waitc
	return true
}

func mandarAlLog(azucar []Propuesta) error {
	//Que significa esta funcion?

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

	stream, err := c.AlmacenarPropuesta(ctx)

	if err != nil {
		//Error por timeout
		return errors.New("Fallo la comunicación con el namenode")
	}	

	//No lo se, pero me la recomendaron

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

		}
	}()

	var mensaje namenode.Propuesta

	for i := 0; i < len(azucar); i++ {

		mensaje = namenode.Propuesta{
			NumChunk: int32(i),
			Maquina: azucar[i].Maquina,
			NombreLibro: azucar[i].NombreLibro,
		}

		if err := stream.Send(&mensaje); err != nil {
			log.Fatalf("Failed to send a note: %v", err)
		}

	}

	stream.CloseSend()
	return nil
}

/*
	fileExists verifica si el archivo definido por filename existe
	y retorna true si existe o false si no
*/
//Codigo obtenido desde https://golangcode.com/check-if-a-file-exists/
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

//Codigo obtenido para
func getHostname() string {

	name, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return name
}

/*

//	En este codigo se responde por cada una de las peticiones que
//	ha llegado

for {

	in, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}


	//Hago algo con lo que recibo


	//Se envia la respuesta al cliente
	if err := stream.Send(&datanode.UploadStatus{
		Message: "Chunk recibido con exito",
		Code:    datanode.UploadStatusCode_Ok,
		}); err != nil {
			return err
		}

	}

	return nil
}

*/
