package datanode

import (
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
			if errFolder != nil {
				//log.Printf(err)
			}
		}

		Andres := ioutil.WriteFile("libro/"+nameFile, mensaje, 0644)
		if Andres != nil {
			log.Printf("%v", Andres)
		}
		/*
			Hago algo con lo que recibo con in
		*/
		log.Printf("Chunk %s recibido con exito.", in.NombreChunk)

		//Se envia la respuesta
		if err := stream.Send(&UploadStatus{
			Message: "Chunk recibido",
			Code:    UploadStatusCode_Ok}); err != nil {
			return err
		}

	}
}
