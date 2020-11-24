package datanode

import (
	"Tarea2/NameNode/namenode"
	"io"
	"io/ioutil"
	"log"
	"os"
	"syscall"
	//"golang.org/x/net/context"
)

//DatanodeServer is
type DatanodeServer struct {
	hola int
}

type Propuesta struct {
	Chunk          string
	NombreOriginal string
	Ubicacion      int
}

func generarPropuesta(propuesta []Propuesta) {

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

		log.Printf("Chunk %s recibido con exito.", in.NombreChunk)

		//Se envia la respuesta al cliente
		if err := stream.Send(&UploadStatus{
			Message: "Chunk recibido",
			Code:    UploadStatusCode_Ok}); err != nil {
			return err
		}

	}
}

//VerificarPropuesta is
func (dn *DatanodeServer) VerificarPropuesta(stream DatanodeService_VerificarPropuestaServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		var stat syscall.Statfs_t

		wd, err := os.Getwd()

		syscall.Statfs(wd, &stat)

		// Available blocks * size per block = available space in bytes
		fmt.Println(stat.Bavail * uint64(stat.Bsize))

		//Se envia la respuesta al cliente
		if err := stream.Send(&UploadStatus{
			Message: "Chunk recibido",
			Code:    UploadStatusCode_Ok}); err != nil {
			return err
		}

	}
}
