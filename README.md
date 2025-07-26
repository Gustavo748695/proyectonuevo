-- Cargar los datos desde el archivo CSV
data = LOAD '/user/cloudera/pig_wordcount/comentario.csv' 
    USING PigStorage(',') 
    AS (id:int, cliente:chararray, direccion:chararray, comentario:chararray);

-- Extraer los comentarios
comentarios = FOREACH data GENERATE comentario;

-- Tokenizar los comentarios en palabras
palabras = FOREACH comentarios GENERATE FLATTEN(TOKENIZE(comentario)) AS palabra;

-- Filtrar solo palabras alfabéticas
palabras_limpias = FILTER palabras BY (wordmatches(palabra, '^[a-zA-ZáéíóúÁÉÍÓÚñÑ]+$'));

-- Normalizar (minúsculas)
palabras_norm = FOREACH palabras_limpias GENERATE LOWER(palabra) AS palabra;

-- Contar la frecuencia de cada palabra
agrupadas = GROUP palabras_norm BY palabra;
conteo = FOREACH agrupadas GENERATE group AS palabra, COUNT(palabras_norm) AS total;

-- Ordenar por cantidad descendente
ordenadas = ORDER conteo BY total DESC;

-- Tomar las 3 más frecuentes
top3 = LIMIT ordenadas 3;

-- Unir las 3 palabras más frecuentes en un solo string/comentario
top3_como_comentario = FOREACH (GROUP top3 ALL) GENERATE CONCAT(CONCAT(FLATTEN(top3.palabra).$0, ' '), CONCAT(FLATTEN(top3.palabra).$1, CONCAT(' ', FLATTEN(top3.palabra).$2))) AS comentario;

-- Guardar resultado
STORE top3_como_comentario INTO '/user/cloudera/pig_wordcount/out' USING PigStorage(',');

