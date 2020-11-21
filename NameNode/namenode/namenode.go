package namenode

import (
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
	puertosTotales []*string
}

func (sr *ServerRepartidor) RedirigirCliente (ctx context.Context, empty *EmptyMessage) (*PuertoAConectar,error){

	newPort := sr.puertosDisponibles[0]
	sr.puertosDisponibles = sr.puertosDisponibles[1:]

	return &PuertoAConectar{Puerto:*newPort}, nil

}