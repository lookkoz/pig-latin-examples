/**
 * This script works with sample.json
 * Keep watch on the last parameter which points to atomic data type
 * For DDB export made with EMR these are lower case and for AWS CLI DDB export these are all
 * upper case. Input file must be cleared from blank lines.
 */


register ../libs/*.jar;
define NestedLoader com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true');

records = LOAD '$INPUT' USING NestedLoader() AS (data: map[]);

/*
records = foreach records generate data#'Items' as data:map[];

orders = foreach records generate data#'uuid'#'s' as order_id:chararray, data#'user'#'m'#'uuid'#'s' as user_id:chararray, data#'redemptionId'#'s' as redemption_id:chararray,data#'user'#'m'#'tenant'#'s' as tenant_namekey:chararray, data#'total'#'m'#'value'#'n' as total_value:int, data#'total'#'m'#'currency'#'s' as total_currency:chararray, data#'status'#'s' as status:chararray, data#'date'#'s' as date:chararray;

products = foreach records generate data#'uuid' as order_id:map[], data#'products' as products:map[];
products = foreach products generate order_id#'s' as order_id:chararray, FLATTEN(products#'l') as products:map[];
products = foreach products generate order_id, products#'m' as products:map[];

items = foreach products generate order_id, products#'items' as items:map[];
items = foreach items generate order_id, FLATTEN(items#'l') as l:map[];
items = foreach items generate order_id, l#'m' as m:map[];
items = foreach items generate order_id, m#'code' as code:map[], m#'uuid' as item_id:map[];
items = foreach items generate order_id, code#'s' as code:chararray, item_id#'s' as item_id:chararray;

products = foreach products generate order_id, products#'uuid' as product_id:map[], products#'quantity' as quantity:map[];
products = foreach products generate order_id, product_id#'s' as product_id:chararray, quantity#'n' as quantity:int;

--dump orders;
--dump products;
--dump items;
--describe orders;
--describe products;
--describe items;

STORE items INTO 'ITEMS_OUT' USING PigStorage(',');
STORE orders INTO 'ORDERS_OUT' USING PigStorage(',');
STORE products INTO 'PRODUCTS_OUT' USING PigStorage(',');
*/