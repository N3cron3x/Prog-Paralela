# Microservicio de Actores en AWS Lambda

Proyecto academico que implementa el Modelo de Actores con Akka Typed
(Java) y lo expone como una funcion serverless en AWS Lambda detras de
API Gateway.

## Componentes principales

| Componente | Ubicacion | Rol |
|---|---|---|
| SupervisorActor | src/main/java/.../actors/SupervisorActor.java | Crea el pool de workers, aplica la estrategia de reinicio y reparte tareas |
| WorkerActor | src/main/java/.../actors/WorkerActor.java | Procesa tareas SUM y CONCAT, y contiene el punto de fallo simulado |
| Boot | src/main/java/.../Boot.java | Crea el ActorSystem una sola vez por contenedor de Lambda |
| TaskLambdaHandler | src/main/java/.../lambda/TaskLambdaHandler.java | Punto de entrada HTTP, traduce JSON a mensajes de actor y viceversa |

## Requisitos

* JDK 11
* Maven 3.8 o superior
* AWS CLI configurado con credenciales validas (solo para el paso de despliegue)
* Opcional: AWS SAM CLI para probar la funcion en local antes de subirla

## Compilar

```
mvn clean package
```

Esto genera el jar final en `target/actor-lambda-project.jar`, ya
empaquetado con todas sus dependencias gracias al plugin shade.

## Probar el sistema de actores en local, sin AWS

```
mvn test
```

Las pruebas en `SupervisorActorTest` levantan el sistema de actores
con el TestKit de Akka, envian una tarea valida y verifican la
respuesta, y ademas envian una tarea con `simulateFailure: true` para
comprobar que el pool sigue respondiendo a peticiones posteriores.

## Probar el handler de Lambda en local con SAM

Con AWS SAM CLI instalado, y usando el jar generado en el paso anterior:

```
sam local invoke TaskFunction --event events/sum_event.json
sam local invoke TaskFunction --event events/concat_event.json
sam local invoke TaskFunction --event events/fail_event.json
```

El tercer evento contiene `simulateFailure: true`. La respuesta
esperada es un codigo 504, porque el worker que recibio esa tarea se
reinicia antes de poder contestar. Una invocacion posterior con un
evento normal confirma que el pool de workers sigue funcionando con
normalidad.

## Desplegar en AWS Lambda

1. Crear la funcion (una sola vez):

```
aws lambda create-function \
  --function-name actor-lambda-project \
  --runtime java11 \
  --role arn:aws:iam::TU_CUENTA:role/TU_ROL_DE_EJECUCION \
  --handler com.university.actorlambda.lambda.TaskLambdaHandler::handleRequest \
  --memory-size 512 \
  --timeout 15 \
  --zip-file fileb://target/actor-lambda-project.jar
```

2. Actualizar el codigo tras cambios posteriores:

```
aws lambda update-function-code \
  --function-name actor-lambda-project \
  --zip-file fileb://target/actor-lambda-project.jar
```

3. Conectar la funcion a un endpoint HTTP publico creando una API en
   API Gateway (REST API o HTTP API), con integracion de tipo proxy
   Lambda, y asociando un metodo POST a esta funcion.

## Formato de la peticion HTTP

```json
{
  "taskId": "tarea-001",
  "type": "SUM",
  "operands": ["10", "25", "7"],
  "simulateFailure": false
}
```

`type` acepta `SUM` (suma numerica de `operands`) o `CONCAT` (union de
los strings en `operands`). `simulateFailure` en true fuerza un error
dentro del worker para observar el reinicio automatico.

## Formato de la respuesta HTTP

```json
{
  "taskId": "tarea-001",
  "success": true,
  "result": "42.0",
  "errorMessage": null,
  "processedBy": "worker-0"
}
```

Codigos de estado usados: 200 exito, 400 JSON invalido, 422 error de
negocio (por ejemplo, un operando no numerico en una tarea SUM), 500
error interno, 504 timeout esperando al worker, tipicamente asociado a
un fallo simulado seguido de reinicio.

## Notas de diseño

El `ActorSystem` se guarda en un campo estatico dentro de `Boot` para
aprovechar la reutilizacion de contenedores que hace AWS Lambda entre
invocaciones consecutivas. Crear un `ActorSystem` nuevo en cada
peticion seria costoso y ademas romperia el pool de workers, ya que
cada arranque perderia el estado y las referencias existentes.

El patron ask (`AskPattern.ask`) es el puente entre el mundo sincrono
que espera API Gateway y el mundo asincrono y basado en mensajes del
sistema de actores. El handler bloquea solo su propio hilo mientras
espera el resultado, sin bloquear al sistema de actores en si.
