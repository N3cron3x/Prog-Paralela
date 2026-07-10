#!/usr/bin/env python3
"""
wordcount_spark.py
-------------------
Job de PySpark que implementa Word Count usando dos enfoques:
  1) RDD (API de bajo nivel)
  2) DataFrame (API estructurada con Catalyst)

Compara tiempos de ejecución y calcula el speedup entre ambos enfoques.

Uso:
    spark-submit wordcount_spark.py --input /ruta/al/archivo.txt --output /ruta/salida
    spark-submit wordcount_spark.py --generate-sample   # genera un dataset sintético de ~100MB

Autor: Luis Muñoz
"""

import argparse
import csv
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# =========================================================================
# 1. GENERACIÓN DE UN DATASET SINTÉTICO (solo si no se cuenta con uno propio)
# =========================================================================
def generar_dataset_sintetico(ruta_salida: str, tamano_objetivo_mb: int = 100) -> str:
    """
    Genera un archivo de texto de gran tamaño repitiendo un corpus base.
    Útil para pruebas cuando no se dispone de un dataset real de ~100MB.
    """
    corpus_base = (
        "Spark es un motor de procesamiento distribuido que permite analizar "
        "grandes volumenes de datos de manera rapida y eficiente utilizando "
        "memoria en lugar de disco. Los RDD Resilient Distributed Datasets "
        "representan la abstraccion original de Spark mientras que los "
        "DataFrame ofrecen una API estructurada optimizada por el motor "
        "Catalyst y el ejecutor Tungsten para mejorar el rendimiento general "
        "del procesamiento de datos distribuidos en clusters de computadoras.\n"
    )

    tamano_objetivo_bytes = tamano_objetivo_mb * 1024 * 1024
    print(f"Generando dataset sintetico de ~{tamano_objetivo_mb}MB en: {ruta_salida}")

    with open(ruta_salida, "w", encoding="utf-8") as f:
        escrito = 0
        while escrito < tamano_objetivo_bytes:
            f.write(corpus_base)
            escrito += len(corpus_base.encode("utf-8"))

    print("Dataset generado correctamente.")
    return ruta_salida


# =========================================================================
# 2. INICIALIZACIÓN DE SPARK
# =========================================================================
def crear_spark_session(nombre_app: str = "WordCount-RDD-vs-DataFrame") -> SparkSession:
    """Crea la SparkSession (y de forma implicita el SparkContext asociado)."""
    spark = (
        SparkSession.builder.appName(nombre_app)
        .config("spark.sql.shuffle.partitions", "8")  # ajustar segun recursos disponibles
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # reduce ruido en consola
    return spark


# =========================================================================
# 3. WORD COUNT CON RDD
# =========================================================================
def wordcount_rdd(spark: SparkSession, ruta_entrada: str, ruta_salida: str) -> float:
    """
    Implementa Word Count usando la API de RDD clasica:
    flatMap -> map -> reduceByKey.
    Retorna el tiempo de ejecucion en segundos.
    """
    sc = spark.sparkContext

    inicio = time.time()

    # Carga el archivo como RDD de lineas
    lineas_rdd = sc.textFile(ruta_entrada)

    resultado_rdd = (
        lineas_rdd
        .flatMap(lambda linea: linea.split())              # divide cada linea en palabras
        .map(lambda palabra: palabra.lower().strip(".,;:!?\"'()"))  # normaliza
        .filter(lambda palabra: palabra != "")              # descarta tokens vacios
        .map(lambda palabra: (palabra, 1))                  # mapea a pares (palabra, 1)
        .reduceByKey(lambda a, b: a + b)                    # suma ocurrencias por clave
        .sortBy(lambda par: par[1], ascending=False)        # ordena por frecuencia descendente
    )

    # Accion: trae el resultado agregado al driver (collect).
    # El resultado de un Word Count es pequeno (una fila por palabra unica),
    # aunque el dataset de entrada sea grande, por lo que traerlo al driver es seguro.
    resultado_local = resultado_rdd.collect()
    conteo_total = len(resultado_local)

    # Guarda resultado con I/O nativo de Python (evita depender de winutils/Hadoop
    # en Windows, ya que saveAsTextFile requiere el filesystem de Hadoop incluso
    # para escribir en disco local).
    os.makedirs(ruta_salida, exist_ok=True)
    ruta_archivo = os.path.join(ruta_salida, "resultado_rdd.txt")
    with open(ruta_archivo, "w", encoding="utf-8") as f:
        for palabra, conteo in resultado_local:
            f.write(f"{palabra}\t{conteo}\n")

    tiempo_total = time.time() - inicio

    print(f"[RDD] Palabras unicas encontradas: {conteo_total}")
    print(f"[RDD] Tiempo de ejecucion: {tiempo_total:.2f} segundos")
    print(f"[RDD] Top 10 palabras mas frecuentes: {resultado_local[:10]}")

    return tiempo_total


# =========================================================================
# 4. WORD COUNT CON DATAFRAMES
# =========================================================================
def wordcount_dataframe(spark: SparkSession, ruta_entrada: str, ruta_salida: str) -> float:
    """
    Implementa Word Count usando la API estructurada de DataFrame:
    explode -> split -> groupBy -> count -> orderBy.
    Retorna el tiempo de ejecucion en segundos.
    """
    inicio = time.time()

    # Lee el archivo como DataFrame de lineas (una columna "value" por linea)
    df_lineas = spark.read.text(ruta_entrada)

    df_palabras = (
        df_lineas
        .select(F.explode(F.split(F.col("value"), r"\s+")).alias("palabra_raw"))
        .select(F.lower(F.regexp_replace("palabra_raw", r"[.,;:!?\"'()]", "")).alias("palabra"))
        .filter(F.col("palabra") != "")  # descarta tokens vacios
    )

    resultado_df = (
        df_palabras
        .groupBy("palabra")
        .count()
        .orderBy(F.col("count").desc())
    )

    # Accion: trae el resultado agregado al driver (collect), por la misma
    # razon que en el enfoque RDD: el resultado final es pequeno.
    resultado_local = resultado_df.collect()
    conteo_total = len(resultado_local)

    # Guarda resultado en CSV con el modulo csv nativo de Python (evita
    # depender de winutils/Hadoop en Windows para el escritor distribuido).
    os.makedirs(ruta_salida, exist_ok=True)
    ruta_archivo = os.path.join(ruta_salida, "resultado_dataframe.csv")
    with open(ruta_archivo, "w", newline="", encoding="utf-8") as f:
        escritor = csv.writer(f)
        escritor.writerow(["palabra", "count"])
        for fila in resultado_local:
            escritor.writerow([fila["palabra"], fila["count"]])

    tiempo_total = time.time() - inicio

    print(f"[DataFrame] Palabras unicas encontradas: {conteo_total}")
    print(f"[DataFrame] Tiempo de ejecucion: {tiempo_total:.2f} segundos")
    print("[DataFrame] Top 10 palabras mas frecuentes:")
    for fila in resultado_local[:10]:
        print(f"  {fila['palabra']}: {fila['count']}")

    return tiempo_total


# =========================================================================
# 5. COMPARACIÓN DE RENDIMIENTO
# =========================================================================
def comparar_rendimiento(tiempo_rdd: float, tiempo_df: float) -> None:
    """Calcula y muestra el speedup relativo entre DataFrame y RDD."""
    speedup = tiempo_rdd / tiempo_df if tiempo_df > 0 else float("inf")

    print("\n" + "=" * 60)
    print("COMPARACION DE RENDIMIENTO: RDD vs DataFrame")
    print("=" * 60)
    print(f"{'Enfoque':<15}{'Tiempo (s)':<15}")
    print(f"{'RDD':<15}{tiempo_rdd:<15.2f}")
    print(f"{'DataFrame':<15}{tiempo_df:<15.2f}")
    print("-" * 60)
    print(f"Speedup (DataFrame vs RDD): {speedup:.2f}x")
    if speedup > 1:
        print("=> DataFrame fue mas rapido gracias al optimizador Catalyst "
              "y a la ejecucion en Tungsten (codegen, formato binario compacto).")
    else:
        print("=> RDD fue mas rapido en esta ejecucion (posible en datasets "
              "pequenos donde el overhead de planificacion de Catalyst no se amortiza).")
    print("=" * 60 + "\n")


# =========================================================================
# 6. FUNCIÓN PRINCIPAL
# =========================================================================
def main():
    parser = argparse.ArgumentParser(description="Word Count con RDD y DataFrame en PySpark")
    parser.add_argument("--input", type=str, default="dataset.txt",
                        help="Ruta del archivo de texto de entrada")
    parser.add_argument("--output", type=str, default="salida",
                        help="Directorio base de salida para los resultados")
    parser.add_argument("--generate-sample", action="store_true",
                        help="Genera un dataset sintetico de ~100MB antes de ejecutar")
    parser.add_argument("--sample-size-mb", type=int, default=100,
                        help="Tamano en MB del dataset sintetico a generar")
    args = parser.parse_args()

    ruta_entrada = args.input
    if args.generate_sample or not os.path.exists(ruta_entrada):
        generar_dataset_sintetico(ruta_entrada, args.sample_size_mb)

    ruta_salida_rdd = os.path.join(args.output, "resultado_rdd")
    ruta_salida_df = os.path.join(args.output, "resultado_dataframe")

    spark = crear_spark_session()

    try:
        tiempo_rdd = wordcount_rdd(spark, ruta_entrada, ruta_salida_rdd)
        tiempo_df = wordcount_dataframe(spark, ruta_entrada, ruta_salida_df)
        comparar_rendimiento(tiempo_rdd, tiempo_df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
