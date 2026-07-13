package com.university.actorlambda.dto;

/**
 * Forma final que toma la respuesta antes de convertirse en JSON.
 * Se mantiene separada de TaskResult para no acoplar el formato de
 * salida HTTP con el mensaje interno que usan los actores.
 */
public class HttpResultDto {

    public String taskId;
    public boolean success;
    public String result;
    public String errorMessage;
    public String processedBy;

    public HttpResultDto() {
    }

    public HttpResultDto(String taskId, boolean success, String result, String errorMessage, String processedBy) {
        this.taskId = taskId;
        this.success = success;
        this.result = result;
        this.errorMessage = errorMessage;
        this.processedBy = processedBy;
    }
}
