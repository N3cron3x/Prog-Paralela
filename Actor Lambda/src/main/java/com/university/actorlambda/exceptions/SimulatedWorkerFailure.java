package com.university.actorlambda.exceptions;

/**
 * Excepcion que un worker lanza a proposito cuando la peticion trae
 * el flag simulateFailure en true. Al no ser capturada dentro del
 * actor, Akka la interpreta como un fallo real y aplica la estrategia
 * de supervision configurada en SupervisorActor (reinicio del worker).
 */
public class SimulatedWorkerFailure extends RuntimeException {

    public SimulatedWorkerFailure(String workerName, String taskId) {
        super("Fallo simulado en " + workerName + " al procesar la tarea " + taskId);
    }
}
