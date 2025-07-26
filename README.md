-- Cargar los datos del archivo CSV
data = LOAD '/user/cloudera/pig_wordcount/comentarios.csv'
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

-- Tomar solo las 3 más repetidas
top3 = LIMIT ordenadas 3;

-- Guardar el resultado
STORE top3 INTO '/user/cloudera/pig_wordcount/out' USING PigStorage(',');

