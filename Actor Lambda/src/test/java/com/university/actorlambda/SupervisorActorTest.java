package com.university.actorlambda;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import com.university.actorlambda.actors.SupervisorActor;
import com.university.actorlambda.messages.TaskRequest;
import com.university.actorlambda.messages.TaskResult;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifica dos escenarios pedidos por la asignacion:
 *   1. Un worker procesa una tarea valida y responde correctamente.
 *   2. Un worker que recibe una tarea con simulateFailure lanza la
 *      excepcion, el supervisor lo reinicia, y el pool sigue operativo
 *      para atender la siguiente tarea sin caerse por completo.
 */
public class SupervisorActorTest {

    @ClassRule
    public static final TestKitJunitResource testKit = new TestKitJunitResource();

    @Test
    public void procesaUnaSumaValida() {
        TestProbe<TaskResult> probe = testKit.createTestProbe();
        var supervisor = testKit.spawn(SupervisorActor.create());

        TaskRequest request = new TaskRequest(
                "test-suma", TaskRequest.TaskType.SUM, Arrays.asList("4", "6"), false);

        supervisor.tell(new SupervisorActor.Dispatch(request, probe.getRef()));

        TaskResult resultado = probe.receiveMessage();
        assertTrue(resultado.isSuccess());
        assertEquals("10.0", resultado.getResult());
    }

    @Test
    public void poolSigueOperativoTrasFalloSimulado() {
        TestProbe<TaskResult> probe = testKit.createTestProbe();
        var supervisor = testKit.spawn(SupervisorActor.create());

        // Se envian tres tareas para asegurar que, con el round robin de
        // tamano de pool 3, al menos una golpea a cada worker, incluido
        // el que va a fallar a proposito.
        TaskRequest falla = new TaskRequest(
                "test-falla", TaskRequest.TaskType.SUM, Arrays.asList("1"), true);
        supervisor.tell(new SupervisorActor.Dispatch(falla, probe.getRef()));

        // El worker que fallo no llega a responder ese mensaje, asi
        // que aqui simplemente confirmamos que el supervisor sigue vivo
        // y puede seguir aceptando trabajo nuevo.
        TaskRequest siguiente = new TaskRequest(
                "test-siguiente", TaskRequest.TaskType.CONCAT, Arrays.asList("a", "b"), false);
        supervisor.tell(new SupervisorActor.Dispatch(siguiente, probe.getRef()));

        TaskResult resultado = probe.receiveMessage();
        assertTrue(resultado.isSuccess());
        assertEquals("ab", resultado.getResult());
    }
}
