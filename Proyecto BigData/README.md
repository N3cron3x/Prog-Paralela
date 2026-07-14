# Aplicacion Hibrida de Procesamiento Big Data en Entorno Serverless

## Estructura del repositorio

```
bigdata-project/
├── gpu-preprocessing/       CUDA + OpenMP + funcion serverless de normalizacion
├── spark-processing/        Pipelines RDD y DataFrame + funcion serverless disparadora
└── akka-orchestrator/        Sistema de actores Akka Typed + funcion serverless orquestadora
```

## 1. gpu-preprocessing

Requiere una maquina con GPU NVIDIA, CUDA Toolkit y un compilador con soporte OpenMP.

```
cd gpu-preprocessing
make
```

Esto genera `libnormalize.so`. Este archivo, junto con `lambda_handler.py` y `requirements.txt`,
se empaqueta como imagen de contenedor para AWS Lambda (o backend equivalente con GPU),
ya que Lambda no ofrece GPU nativa.

Prueba local del handler:
```
python3 -c "from lambda_handler import handler; print(handler({'data':[1,2,3,4,5]}, None))"
```

## 2. spark-processing

`rdd_pipeline.py` y `dataframe_pipeline.py` se suben a un bucket S3 usado como carpeta de scripts.

`spark_trigger_lambda.py` se despliega como funcion Lambda estandar con las variables de entorno:
- `EMR_CLUSTER_ID`: id del cluster EMR donde correran los steps de Spark
- `RESULTS_BUCKET`: bucket S3 donde se leen inputs y se escriben resultados

Prueba local de un pipeline (requiere pyspark instalado):
```
pip install -r requirements.txt pyspark
spark-submit rdd_pipeline.py s3://bucket/input.txt s3://bucket/output/rdd
spark-submit dataframe_pipeline.py s3://bucket/input.txt s3://bucket/output/dataframe
```

## 3. akka-orchestrator

Requiere sbt y JDK 11+.

```
cd akka-orchestrator
sbt assembly
```

El jar resultante en `target/scala-2.13/bigdata-orchestrator-assembly-1.0.jar` se sube como
funcion Lambda con handler `orchestrator.LambdaHandler::handleRequest` y las variables de entorno:
- `GPU_FUNCTION_NAME`
- `SPARK_TRIGGER_FUNCTION_NAME`
- `RESULTS_BUCKET`

El endpoint HTTP se expone conectando API Gateway a esta funcion mediante integracion proxy.
Cuerpo esperado del POST:
```json
{ "jobId": "job-001", "data": [1.0, 2.5, 3.7, 4.2] }
```

## Flujo end to end

1. API Gateway recibe el POST y lo pasa al `LambdaHandler`.
2. El `ValidationActor` valida el input y lo envia al `GpuPreprocessActor`.
3. El `GpuPreprocessActor` invoca la funcion serverless de GPU (con reintentos) y pasa el resultado al `SparkJobActor`.
4. El `SparkJobActor` dispara los steps RDD y DataFrame en EMR y hace polling de los resultados en S3.
5. El `ResultAnalyzerActor` calcula el speedup y lo pasa al `ResponseActor`.
6. El `ResponseActor` devuelve la respuesta final al cliente HTTP.
