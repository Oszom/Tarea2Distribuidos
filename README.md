# Tarea2Distribuidos

><h1> El nombre de la funcion/variable debe partir con una letra mayuscula para ser considerado como exportado</h1>
><h1> Al definir parametros en una función, estos se definen como (nombre tipo)</h1>

# Ubicacion de los nodos

* _DataNode 1_ -> `dist58`
* _DataNode 2_ -> `dist59`
* _DataNode 3_ -> `dist60`
* _NameNode_ -> `dist57`

- La contraseña nueva para las máquinas virtuales es la siguiente: 123momiaes
- Para correr cada Datanode utilizar el siguiente comando: make runDatanode
- Para correr el Namenode utilizar el siguiente comando: make runNamenode
- Para correr el cliente utilizar el siguiente comando: make runCliente

-   Se usa el nombre de las maquinas en vez de su ip en el log para referirse a ellas.
-   El DataNode almacenado en la máquina 'dist58' es el que siempre va a recibir los chunks por parte del cliente,
    el cual se va a contactar con el Namenode, al iniciar este, se va a consultar si se quiere usar el algoritmo centralizado o distribuido. 
-   Es necesario correr primero el Namenode y luego los Datanode antes de correr el Cliente.
-   Los fallos que consideramos bajo los cuales no se acepte una propuesta de parte de un Datanode son: Que un Datanode  se encuentre caído, que un Datanode no tenga el espacio suficiente para almacenar la cantidad de Chunks que se le proponen y un error aleatorio con una probabilidad de un 10%.
- Se debe haber subido al menos un archivo antes de intentar descargar un libro.
- No subir 2 archivos con el mismo nombre.
- Cuando corre el Namenode se borra el archivo log.txt automáticamente.
- Favor de ingresar solo número al momento de seleccionar un libro a descargar.