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
	Chunk   int32
	Maquina string
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
			Chunk:   in.NumChunk,
			Maquina: in.Maquina,
		}
		listaPropuesta = append(listaPropuesta, ElementoPropuesta)
	}

	propuestasOrdenadas := compilarPropuestasMaquinas(listaPropuesta)

	//Se envia la respuesta al cliente
	if err := stream.Send(&UploadStatus{
		Message: "Chunk recibido",
		Code:    UploadStatusCode_Ok}); err != nil {
		return err
	}

	return nil
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

func nuevaPropuesta(propuestaAnterior map[string]bool, nChunks int) []IntentoPropuesta {

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
			Chunk:   int32(i),
			Maquina: maquinasDisponibles[posMaq],
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
