package com.university.actorlambda.messages;

import java.io.Serializable;

/**
 * Respuesta que un worker envia de vuelta al llamador una vez procesada
 * la tarea. Incluye el nombre del actor que respondio, lo cual es util
 * en los logs para comprobar que, tras un reinicio, el worker sigue
 * siendo el mismo actor logico aunque su estado interno se haya
 * reiniciado.
 */
public class TaskResult implements Serializable {

    private String taskId;
    private boolean success;
    private String result;
    private String errorMessage;
    private String processedBy;

    public TaskResult() {
    }

    public TaskResult(String taskId, boolean success, String result, String errorMessage, String processedBy) {
        this.taskId = taskId;
        this.success = success;
        this.result = result;
        this.errorMessage = errorMessage;
        this.processedBy = processedBy;
    }

    public static TaskResult ok(String taskId, String result, String processedBy) {
        return new TaskResult(taskId, true, result, null, processedBy);
    }

    public static TaskResult failed(String taskId, String errorMessage, String processedBy) {
        return new TaskResult(taskId, false, null, errorMessage, processedBy);
    }

    public String getTaskId() {
        return taskId;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getResult() {
        return result;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String getProcessedBy() {
        return processedBy;
    }
}
