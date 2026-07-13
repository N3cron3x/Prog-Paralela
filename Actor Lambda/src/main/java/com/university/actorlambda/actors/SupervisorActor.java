package com.university.actorlambda.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.SupervisorStrategy;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.university.actorlambda.messages.TaskRequest;
import com.university.actorlambda.messages.TaskResult;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Actor supervisor. Es el padre de todos los workers y el unico punto
 * de entrada del microservicio hacia el sistema de actores.
 *
 * Responsabilidades:
 *   1. Crear y mantener un pool fijo de workers al arrancar.
 *   2. Envolver cada worker con una estrategia de supervision, de modo
 *      que si uno lanza una excepcion, Akka lo reinicia automaticamente
 *      en lugar de tumbar todo el sistema.
 *   3. Repartir las tareas entrantes entre los workers usando un
 *      esquema round robin simple.
 */
public class SupervisorActor extends AbstractBehavior<SupervisorActor.Command> {

    public interface Command {
    }

    /**
     * Mensaje publico que recibe el supervisor desde fuera del sistema
     * de actores (en este proyecto, desde el handler de Lambda).
     */
    public static final class Dispatch implements Command {
        public final TaskRequest request;
        public final ActorRef<TaskResult> replyTo;

        public Dispatch(TaskRequest request, ActorRef<TaskResult> replyTo) {
            this.request = request;
            this.replyTo = replyTo;
        }
    }

    private static final int TAMANO_POOL = 3;

    private final List<ActorRef<WorkerActor.Command>> workers;
    private int siguienteWorker = 0;

    public static Behavior<Command> create() {
        return Behaviors.setup(SupervisorActor::new);
    }

    private SupervisorActor(ActorContext<Command> context) {
        super(context);
        this.workers = new ArrayList<>();

        for (int i = 0; i < TAMANO_POOL; i++) {
            String nombre = "worker-" + i;

            // Behaviors.supervise es la pieza central de la tolerancia a
            // fallos del Modelo de Actores. Cualquier excepcion no
            // capturada dentro del worker es interceptada aqui: en vez
            // de propagarse y matar al proceso, provoca un reinicio del
            // actor con estado limpio. withLimit evita reinicios
            // infinitos si el worker falla de forma persistente.
            Behavior<WorkerActor.Command> comportamientoSupervisado =
                    Behaviors.supervise(WorkerActor.create())
                            .onFailure(
                                    SupervisorStrategy.restart()
                                            .withLimit(10, Duration.ofSeconds(30)));

            ActorRef<WorkerActor.Command> worker =
                    getContext().spawn(comportamientoSupervisado, nombre);

            // Vigila la señal de muerte definitiva del hijo, por si el
            // limite de reinicios se agota y el worker queda detenido.
            getContext().watch(worker);

            workers.add(worker);
        }

        getContext().getLog().info("Supervisor iniciado con {} workers", TAMANO_POOL);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Dispatch.class, this::onDispatch)
                .build();
    }

    private Behavior<Command> onDispatch(Dispatch msg) {
        ActorRef<WorkerActor.Command> workerElegido = workers.get(siguienteWorker);
        siguienteWorker = (siguienteWorker + 1) % workers.size();

        getContext().getLog().info(
                "Despachando tarea {} hacia {}",
                msg.request.getTaskId(), workerElegido.path().name());

        workerElegido.tell(new WorkerActor.Process(msg.request, msg.replyTo));
        return this;
    }
}
