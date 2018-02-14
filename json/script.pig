orders = LOAD '/home/lukas/workshops/pig/data_sample.json' USING JsonLoader('name:chararray,Items:bag{(uuid:map[s:chararray])}');
orders = FOREACH orders GENERATE $0, FLATTEN(Items.uuid) AS uuid;
orders = FOREACH orders GENERATE $0, uuid#'s' as uuid;
STORE orders INTO 'flatten.json' USING JsonStorage();


