package com.university.actorlambda.messages;

import java.io.Serializable;
import java.util.List;

/**
 * Representa la peticion de trabajo que llega desde el cliente HTTP.
 *
 * El campo "type" define que operacion ejecuta el worker: SUM para sumar
 * una lista de numeros o CONCAT para unir una lista de strings.
 *
 * El campo "simulateFailure" permite forzar, a proposito, un error dentro
 * del worker. Esto existe unicamente para demostrar que el supervisor
 * detecta el fallo y reinicia al actor afectado, tal como pide la
 * asignacion.
 */
public class TaskRequest implements Serializable {

    public enum TaskType {
        SUM,
        CONCAT
    }

    private String taskId;
    private TaskType type;
    private List<String> operands;
    private boolean simulateFailure;

    // Constructor vacio requerido por Jackson para poder deserializar el JSON
    public TaskRequest() {
    }

    public TaskRequest(String taskId, TaskType type, List<String> operands, boolean simulateFailure) {
        this.taskId = taskId;
        this.type = type;
        this.operands = operands;
        this.simulateFailure = simulateFailure;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public TaskType getType() {
        return type;
    }

    public void setType(TaskType type) {
        this.type = type;
    }

    public List<String> getOperands() {
        return operands;
    }

    public void setOperands(List<String> operands) {
        this.operands = operands;
    }

    public boolean isSimulateFailure() {
        return simulateFailure;
    }

    public void setSimulateFailure(boolean simulateFailure) {
        this.simulateFailure = simulateFailure;
    }
}
