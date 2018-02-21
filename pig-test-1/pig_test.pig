orders = LOAD ${input1} USING JsonLoader('name:chararray,Items:bag{(uuid:map[s:chararray])}');
orders = FOREACH orders GENERATE $0, FLATTEN(Items.uuid) AS uuid;
${output1} = FOREACH orders GENERATE $0, uuid#'s' as uuid;