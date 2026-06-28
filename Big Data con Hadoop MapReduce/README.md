# WordCount MapReduce — UNIBE Programación Paralela y Distribuida

## Estructura del proyecto

```
wordcount/
├── pom.xml
├── src/main/java/mapreduce/
│   └── WordCount.java        ← Mapper + Combiner + Reducer + Driver
├── scripts/
│   ├── hdfs_setup.sh         ← Configuración inicial de HDFS (ejecutar 1 vez)
│   ├── run.sh                ← Compilar, subir archivos y ejecutar el job
│   └── benchmark.sh          ← Comparar rendimiento con 1, 2 y 4 reducers
└── input/                    ← Coloca aquí los .txt a procesar
```

## Requisitos

| Herramienta | Versión mínima | Verificar con          |
|-------------|----------------|------------------------|
| Java JDK    | 8              | `java -version`        |
| Maven       | 3.6            | `mvn -version`         |
| Hadoop      | 3.x            | `hadoop version`       |
| pdftotext   | cualquiera     | `pdftotext -v`         |

### Variables de entorno necesarias

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

---

## Paso a paso

### 1. Configurar HDFS (una sola vez)

```bash
chmod +x scripts/*.sh
./scripts/hdfs_setup.sh
```

### 2. Convertir PDFs a texto y colocarlos en `input/`

```bash
for f in /ruta/tus/pdfs/*.pdf; do
    pdftotext "$f" "input/$(basename "${f%.pdf}").txt"
done
```

### 3. Ejecutar el job

```bash
./scripts/run.sh ./input
```

Los resultados se guardan en `output/` localmente y el top 20 de palabras
más frecuentes se imprime en consola.

### 4. Benchmark (análisis de rendimiento)

```bash
./scripts/benchmark.sh
```

Ejecuta el job 3 veces con 1, 2 y 4 reducers. Genera `benchmark_results.tsv`
con los tiempos comparativos.

---

## Arquitectura del job

```
Archivos HDFS
     │
     ▼
  MAPPER (WordMapper)
  ├── Lee línea por línea (TextInputFormat)
  ├── Normaliza a minúsculas y elimina caracteres no alfabéticos
  └── Emite (palabra, 1) por cada token
     │
     ▼
  COMBINER (IntSumReducer)        ← pre-agrega localmente
  └── Reduce tráfico en la fase Shuffle & Sort
     │
     ▼
  REDUCER (WordReducer)
  └── Suma todos los valores por clave → emite (palabra, total)
     │
     ▼
  Salida HDFS → part-r-00000 ...
```

## Notas sobre rendimiento

- **Combiner**: Activado por defecto. Pre-agrega conteos en cada mapper
  antes del shuffle, reduciendo el volumen de datos transferidos por red.
- **Número de reducers**: Más reducers = más paralelismo, pero mayor
  overhead de coordinación. Óptimo depende del tamaño del dataset.
- **Particionado**: Hadoop usa `HashPartitioner` por defecto; garantiza
  que la misma palabra siempre va al mismo reducer.
