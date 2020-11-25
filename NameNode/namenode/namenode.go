package namenode

import (
	context "context"
	"io"
	"time"

	"Tarea2/Datanode/datanode"
	"log"

	grpc "google.golang.org/grpc"
)

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

//MandarPropuesta is
func (sr *ServerNamenode) MandarPropuesta(stream NameNodeService_MandarPropuestaServer) error {
	var listaPropuesta []IntentoPropuesta
	var ElementoPropuesta IntentoPropuesta
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
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

	propuestasOrdenadas := compilarPropuestasMaquinas(listaPropuesta)
	resultadoPropuestas := chequearPropuesta(propuestasOrdenadas)

	existenFalsos = false
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

	for i:=0 ; i<len(propuestaAEnviar); i++ {

		if err := stream.Send(&Propuesta{
			NumChunk: propuestaAEnviar[i].Chunk,
			Maquina:     propuestaAEnviar[i].Maquina,
			NombreLibro:  propuestaAEnviar[i].NombreLibro); err != nil {
			return err
		}
	}

	stream.CloseSend()
	//Se envia la respuesta al cliente
	

	return nil
}
func formatearTexto(propuesta []IntentoPropuesta) []string {
	var textoCompleto []string
	textoCompleto.append(textoCompleto, propuesta[0].NombreLibro+" Cantidad_Partes "+len(propuesta))
	for i := 0; i < len(IntentoPropuesta); i++ {
		linea := propuesta[i].NombreLibro + "_parte_" + propuesta[i].Chunk + " " + propuesta[i].Maquina + "\n"
		textoCompleto.append(textoCompleto, linea)
	}
	return textoCompleto

}

func escribirLog(archivo string, texto []string) {
	file, err := os.Openfile(archivo, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
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

	Retorna
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

	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

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
