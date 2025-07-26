-- Cargar el archivo sin encabezado
comentarios_raw = LOAD '/user/hadoop/comentarios.csv' 
    USING PigStorage(',') 
    AS (id:int, cliente:chararray, direccion:chararray, comentario:chararray);

-- Extraer solo la columna de comentarios y tokenizar
palabras = FOREACH comentarios_raw GENERATE FLATTEN(TOKENIZE(LOWER(comentario))) AS palabra;

-- Limpiar palabras (quitar comas o puntos, si existieran)
limpias = FOREACH palabras GENERATE REPLACE(palabra, '[^a-záéíóúñü]+', '') AS palabra;

-- Filtrar vacíos
palabras_validas = FILTER limpias BY palabra IS NOT NULL AND palabra != '';

-- Contar ocurrencias
grupo_palabras = GROUP palabras_validas BY palabra;
conteo = FOREACH grupo_palabras GENERATE group AS palabra, COUNT(palabras_validas) AS total;

-- Ordenar por frecuencia
ordenadas = ORDER conteo BY total DESC;

-- Sacar las 3 más repetidas
top3 = LIMIT ordenadas 3;

-- Mostrar resultado
DUMP top3;
