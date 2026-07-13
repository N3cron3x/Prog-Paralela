package com.university.actorlambda.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.university.actorlambda.exceptions.SimulatedWorkerFailure;
import com.university.actorlambda.messages.TaskRequest;
import com.university.actorlambda.messages.TaskResult;

import java.util.List;

/**
 * Actor worker. Cada instancia procesa una tarea a la vez y responde
 * al remitente (replyTo) con el resultado.
 *
 * Puntos clave del Modelo de Actores presentes aqui:
 *   1. El estado interno (en este caso, un contador de tareas procesadas)
 *      solo lo toca este actor, nunca se comparte memoria con otros hilos.
 *   2. La unica forma de comunicarse con el actor es enviandole mensajes
 *      inmutables (Process), nunca invocando metodos directamente.
 *   3. Si ocurre un error no controlado, el actor no intenta "arreglarlo"
 *      por su cuenta. Deja que la supervision del padre decida que hacer.
 */
public class WorkerActor extends AbstractBehavior<WorkerActor.Command> {

    // Contrato de mensajes que este actor entiende
    public interface Command {
    }

    /**
     * Unico mensaje que procesa el worker: contiene la tarea y una
     * referencia a quien debe recibir la respuesta (patron ask).
     */
    public static final class Process implements Command {
        public final TaskRequest request;
        public final ActorRef<TaskResult> replyTo;

        public Process(TaskRequest request, ActorRef<TaskResult> replyTo) {
            this.request = request;
            this.replyTo = replyTo;
        }
    }

    private int tareasProcesadas = 0;

    public static Behavior<Command> create() {
        return Behaviors.setup(WorkerActor::new);
    }

    private WorkerActor(ActorContext<Command> context) {
        super(context);
        getContext().getLog().info("Worker creado: {}", getContext().getSelf().path());
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Process.class, this::onProcess)
                .build();
    }

    private Behavior<Command> onProcess(Process msg) {
        String nombreWorker = getContext().getSelf().path().name();

        // Punto de inyeccion de fallo intencional, requerido por la
        // asignacion para probar la supervision y el reinicio automatico.
        if (msg.request.isSimulateFailure()) {
            getContext().getLog().warn(
                    "Se solicito un fallo simulado en {} para la tarea {}",
                    nombreWorker, msg.request.getTaskId());
            throw new SimulatedWorkerFailure(nombreWorker, msg.request.getTaskId());
        }

        tareasProcesadas++;

        String resultado;
        try {
            resultado = procesar(msg.request);
        } catch (Exception ex) {
            // Errores de validacion (por ejemplo, un numero mal formado)
            // se responden como fallo de negocio, no como caida del actor.
            msg.replyTo.tell(TaskResult.failed(
                    msg.request.getTaskId(), ex.getMessage(), nombreWorker));
            return this;
        }

        getContext().getLog().info(
                "{} proceso la tarea {} (total acumulado: {})",
                nombreWorker, msg.request.getTaskId(), tareasProcesadas);

        msg.replyTo.tell(TaskResult.ok(msg.request.getTaskId(), resultado, nombreWorker));
        return this;
    }

    private String procesar(TaskRequest request) {
        List<String> operandos = request.getOperands();

        switch (request.getType()) {
            case SUM:
                double suma = 0;
                for (String operando : operandos) {
                    suma += Double.parseDouble(operando.trim());
                }
                return String.valueOf(suma);

            case CONCAT:
                return String.join("", operandos);

            default:
                throw new IllegalArgumentException("Tipo de tarea no soportado: " + request.getType());
        }
    }
}
