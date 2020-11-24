package main

import (
	Roedor "Tarea2/Cliente/PicadorCriminalMutilador"
	//"Tarea2/NameNode/namenode"
	"Tarea2/DataNode/datanode"
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
)

//Nombre del paquete que posee las funciones de generar chunks y unirlos

//"io"

/*

func main() {

	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("Ingrese nombre de la maquina donde se encuentra alojado logistica: ")
	ip, _ := reader.ReadString('\n')
	ip = strings.TrimSuffix(ip, "\n")
	ip = strings.TrimSuffix(ip, "\r")

	readerTipo := bufio.NewReader(os.Stdin)
	fmt.Printf("Ingrese Tipo de cliente (pyme/retail): ")
	tipo, _ := readerTipo.ReadString('\n')
	tipo = strings.TrimSuffix(tipo, "\n")
	tipo = strings.TrimSuffix(tipo, "\r")

	readerTiempo := bufio.NewReader(os.Stdin)
	fmt.Printf("Ingrese tiempo de espera entre ordenes (en segundos): ")
	tiempo, _ := readerTiempo.ReadString('\n')
	tiempo = strings.TrimSuffix(tiempo, "\n")
	tiempo = strings.TrimSuffix(tiempo, "\r")

	var conn *grpc.ClientConn
	conn, err := grpc.Dial(ip+":9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
	}

	defer conn.Close()

	c := logistica.NewLogisticaServiceClient(conn)

	var message1 logistica.OrdenCliente
	var message2 logistica.SeguimientoCliente
	for {

		if tipo == "pyme" {
			readerDecision := bufio.NewReader(os.Stdin)
			fmt.Printf("Indique que quiere realizar (Pedido/Seguimiento)")
			decision, _ := readerDecision.ReadString('\n')
			decision = strings.TrimSuffix(decision, "\n")
			decision = strings.TrimSuffix(decision, "\r")

			if decision == "Pedido" {
				readerPedido := bufio.NewReader(os.Stdin)
				fmt.Printf("Ingrese su pedido con el siguiente formato: id,producto,valor,tienda,destino,prioritario: ")
				pedido, _ := readerPedido.ReadString('\n')
				pedido = strings.TrimSuffix(pedido, "\n")
				pedido = strings.TrimSuffix(pedido, "\r")
				pedidoListo := strings.Split(pedido, ",")

				valorEntero, cosita := strconv.ParseInt(pedidoListo[2], 10, 64)
				if cosita != nil {
					log.Fatalf("Error al covertir el valor a entero: %s", err)
				}

				prioritarioEntero, cosito := strconv.ParseInt(pedidoListo[5], 10, 64)
				if cosito != nil {
					log.Fatalf("Error al covertir prioritario a entero: %s", err)
				}
				message1 = logistica.OrdenCliente{
					Id:          pedidoListo[0],
					Producto:    pedidoListo[1],
					Valor:       valorEntero,
					Tienda:      pedidoListo[3],
					Destino:     pedidoListo[4],
					Prioritario: prioritarioEntero,
				}
				response1, err1 := c.NuevaOrden(context.Background(), &message1)
				if err1 != nil {
					log.Fatalf("Problema al procesar su orden: %s", err)
				}
				log.Printf("El numero de seguimiento del producto %s es: %d", response1.Producto, response1.Seguimiento)

			} else {
				readerSeguimiento := bufio.NewReader(os.Stdin)
				fmt.Printf("Ingrese codigo seguimiento")
				seguimiento, _ := readerSeguimiento.ReadString('\n')
				seguimiento = strings.TrimSuffix(seguimiento, "\n")
				seguimiento = strings.TrimSuffix(seguimiento, "\r")
				seguimientoEntero, cosite := strconv.ParseInt(seguimiento, 10, 64)
				if cosite != nil {
					log.Fatalf("Error al covertir seguimiento a entero: %s", err)
				}

				message2 = logistica.SeguimientoCliente{
					Seguimiento: seguimientoEntero,
					Estado:      "---",
					Producto:    "---",
				}

				response2, err2 := c.InformarSeguimiento(context.Background(), &message2)
				if err2 != nil {
					log.Fatalf("Problema al consultar por su producto: %s", err)
				}
				log.Printf("Tu pedido se encuentra en %s", response2.Estado)

			}

		} else {
			readerPedido := bufio.NewReader(os.Stdin)
			fmt.Printf("Ingrese su pedido con el siguiente formato: id,producto,valor,tienda,destino: ")
			pedido, _ := readerPedido.ReadString('\n')
			pedido = strings.TrimSuffix(pedido, "\n")
			pedido = strings.TrimSuffix(pedido, "\r")
			pedidoListo := strings.Split(pedido, ",")

			valorEntero, cosita := strconv.ParseInt(pedidoListo[2], 10, 64)
			if cosita != nil {
				log.Fatalf("Error al covertir el valor a entero: %s", err)
			}
			message1 = logistica.OrdenCliente{
				Id:          pedidoListo[0],
				Producto:    pedidoListo[1],
				Valor:       valorEntero,
				Tienda:      pedidoListo[3],
				Destino:     pedidoListo[4],
				Prioritario: -1,
			}
			response1, err1 := c.NuevaOrden(context.Background(), &message1)
			if err1 != nil {
				log.Fatalf("Problema al procesar su orden: %s", err)
			}
			log.Printf("El numero de seguimiento del producto %s es: %d", response1.Producto, response1.Seguimiento)
		}

	}

}

*/

func main() {
	var conn *grpc.ClientConn
	conn, err := grpc.Dial(":9000", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("no se pudo conectar: %s", err)
	}

	defer conn.Close()

	readerTipo := bufio.NewReader(os.Stdin)
	fmt.Printf("Ingrese nombre del archivo: ")
	file, _ := readerTipo.ReadString('\n')
	file = strings.TrimSuffix(file, "\n")
	file = strings.TrimSuffix(file, "\r")

	partes := Roedor.Cortar(file)
	c := datanode.NewDatanodeServiceClient(conn)

	stream, err := c.SubirArchivo(context.Background())

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
			log.Printf("El server retorna el siguiente mensaje: %v", in.Message)
		}
	}()

	var mensaje datanode.Chunk

	for i := 0; i < len(partes); i++ {
		chunkBytes, errBytes := ioutil.ReadFile(partes[i])

		if errBytes != nil {
			fmt.Print(errBytes)
		}

		mensaje = datanode.Chunk{
			Content:        chunkBytes,
			NombreChunk:    partes[i],
			NombreOriginal: file,
			Parte:          int32(i),
		}

		if err := stream.Send(&mensaje); err != nil {
			log.Fatalf("Failed to send a note: %v", err)
		}

		os.Remove(partes[i])

		time.Sleep(1 * time.Second)
	}

	stream.CloseSend()
	<-waitc

	/*
		c := namenode.NewSpreaderServiceClient(conn)

		stream, err := c.PrintContext(context.Background())
		//log.Printf("%v, %v", response, err)

		readerTipo := bufio.NewReader(os.Stdin)
		fmt.Printf("1 o 2, que significará, tu lo déscúbrírás: ")
		tipo, _ := readerTipo.ReadString('\n')
		tipo = strings.TrimSuffix(tipo, "\n")
		tipo = strings.TrimSuffix(tipo, "\r")

		var ingredientes [6]string

		if tipo == "1"{
			ingredientes = [6]string{"mayo", "mostaza", "palta", "tomate", "miel", "aji"}
		} else {
			ingredientes = [6]string{"azucar rubia", "extracto de vainilla", "manjar", "leche", "crema batida", "chocolate"}
		}



		waitc := make(chan struct{})

		go func() {
			for{
				in, err := stream.Recv()
				if err == io.EOF {
					close(waitc)
					return
				}

				if err != nil {
					log.Fatalf("Error al recibir un mensaje: %v", err)
				}
				log.Printf("El server retorna el siguiente mensaje: %v", in.Msg)
			}
		} ()

		var mensaje namenode.Saludines

		for _, note := range ingredientes {

			mensaje = namenode.Saludines{Msg: "El pablo se la come con " + note}

			if err := stream.Send(&mensaje); err != nil {
				log.Fatalf("Failed to send a note: %v", err)
			}

			time.Sleep(1 * time.Second)
		}

		stream.CloseSend()
		<-waitc
	*/
}
