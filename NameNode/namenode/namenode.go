package namenode

import (
	"io"
	"log"

	"golang.org/x/net/context"
)

type ServerNamenode struct {
	hola int
}

func (nm *ServerNamenode) PruebaFuncionalidad(ctx context.Context, empty *EmptyMessage) (*EmptyMessage, error) {
	log.Printf("Esta es una prueba")

	return &EmptyMessage{}, nil
}

/*
	Definiciones del servidor encargado de repartir los puertos del namenode
*/

type ServerRepartidor struct {
	puertosDisponibles []*string
	puertosTotales     []*string
}

func (sr *ServerRepartidor) RedirigirCliente(ctx context.Context, empty *EmptyMessage) (*PuertoAConectar, error) {

	newPort := sr.puertosDisponibles[0]
	sr.puertosDisponibles = sr.puertosDisponibles[1:]

	return &PuertoAConectar{Puerto: *newPort}, nil

}

func (sr *ServerRepartidor) PrintContext(stream SpreaderService_PrintContextServer) error {

	log.Printf("Hola, veremos como se la come el pablo")

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		log.Printf("%s", in.Msg)

		if err := stream.Send(in); err != nil {
			return err
		}
	}
}
