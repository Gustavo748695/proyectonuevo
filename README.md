-- Cargar los datos desde el archivo CSV
data = LOAD '/user/cloudera/pig_wordcount/comentario.csv' 
    USING PigStorage(',') 
    AS (id:int, cliente:chararray, direccion:chararray, comentario:chararray);

-- Extraer la columna comentario
comentarios = FOREACH data GENERATE comentario;

-- Separar cada comentario en palabras
palabras = FOREACH comentarios GENERATE FLATTEN(TOKENIZE(comentario)) AS palabra;

-- Agrupar por palabra
palabras_agrupadas = GROUP palabras BY palabra;

-- Contar las repeticiones de cada palabra
cuentas = FOREACH palabras_agrupadas GENERATE group AS palabra, COUNT(palabras) AS total;

-- Ordenar por frecuencia descendente
ordenadas = ORDER cuentas BY total DESC;

-- Tomar solo las 3 palabras más frecuentes
top3 = LIMIT ordenadas 3;

-- Guardar el resultado en HDFS
STORE top3 INTO '/user/cloudera/pig_wordcount/out' USING PigStorage(',');

#!/bin/bash
# 0_preparar_hdfs.sh

# Crear un directorio en HDFS si no existe
hdfs dfs -mkdir -p /user/cloudera/pig_wordcount

# Subir el archivo CSV a HDFS
hdfs dfs -put /home/cloudera/workspace/hadoop_course-master/pig/wordcount/comentario.csv /user/cloudera/pig_wordcount/

# Verificar si el archivo se subió correctamente
hdfs dfs -ls /user/cloudera/pig_wordcount

