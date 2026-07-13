package com.university.actorlambda.lambda;

import akka.actor.typed.javadsl.AskPattern;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.university.actorlambda.Boot;
import com.university.actorlambda.actors.SupervisorActor;
import com.university.actorlambda.dto.HttpResultDto;
import com.university.actorlambda.messages.TaskRequest;
import com.university.actorlambda.messages.TaskResult;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Funcion serverless que actua como puerta de entrada del microservicio.
 *
 * Flujo completo de una peticion:
 *   1. API Gateway invoca este handler con el cuerpo JSON en el evento.
 *   2. El JSON se deserializa a un objeto TaskRequest.
 *   3. Se envia la tarea al SupervisorActor usando el patron ask, que
 *      convierte la comunicacion asincrona basada en mensajes en un
 *      CompletionStage que Lambda puede esperar de forma sincrona.
 *   4. El supervisor despacha la tarea a un worker del pool.
 *   5. Si el worker responde a tiempo, se devuelve 200 con el resultado.
 *   6. Si el worker falla (por ejemplo, un fallo simulado) o no responde
 *      dentro del timeout, se devuelve un error controlado en vez de
 *      dejar que la excepcion se propague sin control, tal como se
 *      espera de una arquitectura tolerante a fallos.
 */
public class TaskLambdaHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Duration TIMEOUT_ASK = Duration.ofSeconds(5);

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        context.getLogger().log("Peticion recibida: " + input.getBody());

        TaskRequest taskRequest;
        try {
            taskRequest = MAPPER.readValue(input.getBody(), TaskRequest.class);
        } catch (Exception ex) {
            return respuesta(400, new HttpResultDto(null, false, null,
                    "Cuerpo JSON invalido: " + ex.getMessage(), null));
        }

        try {
            CompletionStage<TaskResult> futuro = AskPattern.ask(
                    Boot.supervisor(),
                    replyTo -> new SupervisorActor.Dispatch(taskRequest, replyTo),
                    TIMEOUT_ASK,
                    Boot.sistema().scheduler());

            // Se bloquea la invocacion de Lambda hasta tener el resultado,
            // dado que el contrato con API Gateway es sincrono. Internamente
            // Akka sigue trabajando de forma asincrona y no bloqueante.
            TaskResult resultado = futuro.toCompletableFuture().get(
                    TIMEOUT_ASK.getSeconds(), TimeUnit.SECONDS);

            HttpResultDto dto = new HttpResultDto(
                    resultado.getTaskId(),
                    resultado.isSuccess(),
                    resultado.getResult(),
                    resultado.getErrorMessage(),
                    resultado.getProcessedBy());

            int codigo = resultado.isSuccess() ? 200 : 422;
            return respuesta(codigo, dto);

        } catch (TimeoutException e) {
            // Este es el caso tipico cuando el worker elegido lanzo el
            // fallo simulado: el actor se reinicia pero nunca llega a
            // responder al mensaje que estaba procesando en el momento
            // del fallo, asi que el ask expira.
            context.getLogger().log("Timeout esperando respuesta del worker: " + e.getMessage());
            return respuesta(504, new HttpResultDto(taskRequest.getTaskId(), false, null,
                    "El worker no respondio a tiempo, probablemente se reinicio por un fallo", null));

        } catch (ExecutionException e) {
            context.getLogger().log("Error de ejecucion en el sistema de actores: " + e.getMessage());
            return respuesta(500, new HttpResultDto(taskRequest.getTaskId(), false, null,
                    "Error interno procesando la tarea: " + e.getCause(), null));

        } catch (Exception e) {
            context.getLogger().log("Error inesperado: " + e.getMessage());
            return respuesta(500, new HttpResultDto(taskRequest.getTaskId(), false, null,
                    "Error inesperado: " + e.getMessage(), null));
        }
    }

    private APIGatewayProxyResponseEvent respuesta(int codigo, HttpResultDto cuerpo) {
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");

        String cuerpoJson;
        try {
            cuerpoJson = MAPPER.writeValueAsString(cuerpo);
        } catch (Exception e) {
            cuerpoJson = "{\"success\": false, \"errorMessage\": \"No se pudo serializar la respuesta\"}";
        }

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(codigo)
                .withHeaders(headers)
                .withBody(cuerpoJson);
    }
}
