orders = LOAD '$INPUT' USING JsonLoader('name:chararray,Items:bag{(uuid:tuple(s:chararray))}');
orders = FOREACH orders GENERATE name, FLATTEN(Items);
orders = FOREACH orders GENERATE name, uuid.s;
STORE orders INTO '$OUTPUT' USING PigStorage(',');


