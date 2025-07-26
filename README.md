-- Cargar los datos desde el archivo CSV
data = LOAD '/user/cloudera/pig_wordcount/comentario.csv' 
    USING PigStorage(',') 
    AS (id:int, cliente:chararray, direccion:chararray, comentario:chararray);

-- Extraer la columna comentario
comentarios = FOREACH data GENERATE comentario;

-- Separar cada comentario en palabras
palabras = FOREACH comentarios GENERATE FLATTEN(TOKENIZE(comentario)) AS palabra;

-- Filtrar las palabras para que solo contengan caracteres alfabéticos
palabras_filtradas = FILTER palabras BY (chararray) (wordmatches(palabra, '^[a-zA-Z]+$'));

-- Convertir las palabras a minúsculas para normalizar
palabras_normalizadas = FOREACH palabras_filtradas GENERATE LOWER(palabra) AS palabra;

-- Agrupar las palabras por su valor
palabras_agrupadas = GROUP palabras_normalizadas BY palabra;

-- Contar las repeticiones de cada palabra
cuentas = FOREACH palabras_agrupadas GENERATE group AS palabra, COUNT(palabras_normalizadas) AS total;

-- Ordenar por frecuencia descendente
ordenadas = ORDER cuentas BY total DESC;

-- Tomar solo las 3 palabras más frecuentes
top3 = LIMIT ordenadas 3;

-- Guardar el resultado en HDFS
STORE top3 INTO '/user/cloudera/pig_wordcount/out' USING PigStorage(',');

