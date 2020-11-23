package datanode

import (
	"io"
	"log"

	"golang.org/x/net/context"
)

type DatanodeServer struct {}

func (*dn DatanodeServer) SubirArchivo (stream DatanodeService_SubirArchivoServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		/*
			Hago algo con lo que recibo con in
		*/
		log.Printf("%s", in.Msg)


		//Se envia la respuesta
		if err := stream.Send(in); err != nil {
			return err
		}
	}
}