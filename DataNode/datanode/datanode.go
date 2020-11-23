package datanode

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	//"golang.org/x/net/context"
)

//DatanodeServer is
type DatanodeServer struct {
	hola int
}

//SubirArchivo is
func (dn *DatanodeServer) SubirArchivo(stream DatanodeService_SubirArchivoServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		mensaje := in.Content
		nameFile := in.NombreChunk

		if _, err12 := os.Stat("/libro"); os.IsNotExist(err12) {
			errFolder := os.Mkdir("libro", 0755)
			fmt.Printf("El andres se la come")
			if errFolder != nil {
				log.Fatal(err)
			}
		}

		Andres := ioutil.WriteFile("libro/"+nameFile+"andresql", mensaje, 0644)
		if Andres != nil {
			log.Fatal(Andres)
		}
		/*
			Hago algo con lo que recibo con in
		*/
		log.Printf("%s", in.NombreChunk)

		//Se envia la respuesta
		if err := stream.Send(&UploadStatus{
			Message: "Chunk recibido",
			Code:    UploadStatusCode_Ok}); err != nil {
			return err
		}

	}
}
