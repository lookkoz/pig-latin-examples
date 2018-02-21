register ../libs/*.jar;


orders = LOAD '$INPUT' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') as myMap:map[];
orders = foreach orders generate FLATTEN(myMap#'Items') as items, myMap#'Count' as count;
orders = foreach orders generate (chararray)items#'redemptionId'#'S' as redemptionId, (chararray)items#'uuid'#'S' as uuid, (chararray)items#'status'#'S', (chararray)count;

STORE orders INTO '$OUTPUT' USING PigStorage(',');

