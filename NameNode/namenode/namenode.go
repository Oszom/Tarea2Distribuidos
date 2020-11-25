package namenode

import (
	context "context"
	"flag"
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
	Chunk   string
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

func chequearPropuesta(propuestita []IntentoPropuesta) bool {

	maquinasDatanode := []string{"dist58", "dist59", "dist60"}

	for i := 0; i < len(maquinasDatanode); i++ {

	}

	return false
}

func newConnDatanode(nombreMaquina string, matrimonio IntentoPropuesta) bool {

	var conn *grpc.ClientConn
	conn, err := grpc.Dial(nombreMaquina+":9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
	}

	defer conn.Close()

	c := datanode.NewDatanodeServiceClient(conn)

	var deadlineMs = flag.Int("deadline_ms", 20*1000, "Default deadline in milliseconds.")

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*deadlineMs)*time.Millisecond)

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
