/**
 * This script works with the schema like porders-sample.json represents.
 * Keep watch on the last parameter which points to atomic data type
 * For DDB export made with EMR these are lower case and for AWS CLI DDB export these are all
 * upper case. Input file must be cleared from blank lines.
 */


register ../libs/*.jar;
register /home/lukas/Pulpit/PERKBOX/BIGDATA/piggybank.jar;
register /home/lukas/Pulpit/PERKBOX/BIGDATA/com.mysql.jdbc_5.1.5.jar;

define NestedLoader com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad=true');

records = LOAD '$INPUT' USING NestedLoader() AS (data: map[]);
records = foreach records generate FLATTEN(data#'Items') as data:map[];
/*
orders = foreach records generate
    data#'uuid'#'S' as order_id:chararray,
    data#'redemptionId'#'S' as redemption_id:chararray,
    data#'user'#'S' as user_id:chararray,
    data#'total'#'M'#'value'#'N' as total_value:int,
    data#'total'#'M'#'currency'#'S' as total_currency:chararray,
    data#'totalVAT'#'M'#'value'#'N' as total_vat_value:float,
    data#'totalVAT'#'M'#'currency'#'S' as total_vat_currency:chararray,
    data#'totalBeforeDiscount'#'M'#'value'#'N' as total_before_discount_value:float,
    data#'totalBeforeDiscount'#'M'#'currency'#'S' as total_before_discount_currency:chararray,
    data#'totalSavings'#'M'#'value'#'N' as total_savings_value:float,
    data#'totalSavings'#'M'#'currency'#'S' as total_savings_currency:chararray,
    data#'status'#'S' as status:chararray,
    data#'date'#'S' as date:datetime;

dump orders;

*/

products = foreach records generate data#'uuid' as order_id:map[], data#'products' as products:map[];
products = foreach products generate order_id#'S' as order_id:chararray, FLATTEN(products#'L') as products:map[];
products = foreach products generate order_id, products#'M' as products:map[];
/*
items = foreach products generate order_id, products#'items' as items:map[];
items = foreach items generate order_id, FLATTEN(items#'L') as l:map[];
items = foreach items generate order_id, l#'M' as m:map[];
items = foreach items generate order_id, m#'code' as code:map[], m#'uuid' as item_id:map[];
items = foreach items generate order_id, code#'S' as code:chararray, item_id#'S' as item_id:chararray;
*/
products = foreach products generate order_id, products#'uuid' as product_id:map[], products#'quantity' as quantity:map[];
products = foreach products generate order_id, product_id#'S' as product_id:chararray, quantity#'N' as quantity:int;

STORE products INTO 'products' USING org.apache.pig.piggybank.storage.DBStorage('com.mysql.jdbc.Driver', 'jdbc:mysql://localhost:3306/pig','test', 'test', 'insert into products(product_id, order_id, quantity) values(?,?,?)');
--dump orders;
--dump products;
--dump items;
--describe orders;
--describe products;
--describe items;
/*
STORE items INTO 'ITEMS_OUT' USING PigStorage(',');
STORE orders INTO 'ORDERS_OUT' USING PigStorage(',');
STORE products INTO 'PRODUCTS_OUT' USING PigStorage(',');
*/