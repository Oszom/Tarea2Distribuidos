# Tarea2Distribuidos

><h1> El nombre de la funcion/variable debe partir con una letra mayuscula para ser considerado como exportado</h1>
><h1> Al definir parametros en una función, estos se definen como (nombre tipo)</h1>


Para compilar `protocolbuffer` se debe correr lo siguiente

```bash
protoc --go_out=. --go_opt=paths=source_relative \
--go-grpc_out=. --go-grpc_opt=paths=source_relative \
ARCHIVO_PROTO
```

