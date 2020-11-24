package namenode

import (
	"io"
	"log"

	"golang.org/x/net/context"
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

func chequearPropuesta(propuestita []IntentoPropuesta) bool {

}
