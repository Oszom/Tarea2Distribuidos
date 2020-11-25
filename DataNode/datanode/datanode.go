package datanode

import (
	"Tarea2/NameNode/namenode"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"syscall"
	"time"

	wr "github.com/mroth/weightedrand"
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

//IntentoPropuesta is
type IntentoPropuesta struct {
	Chunk   int32
	Maquina string
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
		_, err := stream.Recv()
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
		trocitos := stat.Bavail * uint64(stat.Bsize) / (250 * (1 << 10))

		// Probabilidad aleatoria de que el datanode falle porque se siente mal
		rand.Seed(time.Now().UTC().UnixNano())
		eleccion, _ := wr.NewChooser(
			wr.Choice{Item: true, Weight: 1},
			wr.Choice{Item: false, Weight: 9},
		)
		haFallado := eleccion.Pick().(bool)

		//Se envia la respuesta al cliente
		if err := stream.Send(&IsAlive{
			Capacidad:   int32(trocitos),
			FalloRandom: haFallado,
		}); err != nil {
			return err
		}

	}
}

func primeraPropuesta(nChunks int) []namenode.Propuesta {

	var listaPropuesta []namenode.Propuesta

	maquinasDisponibles := []string{"dist58", "dist59", "dist60"}

	for i := 0; i < nChunks; i++ {
		posMaq := i % len(maquinasDisponibles)
		listaPropuesta = append(listaPropuesta, namenode.Propuesta{
			NumChunk: int32(i),
			Maquina:  maquinasDisponibles[posMaq],
		})
	}

	return listaPropuesta

}
