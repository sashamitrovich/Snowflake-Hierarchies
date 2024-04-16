use role sysadmin;
use database controlling;
use schema final;

-- if the CTE above worked, this should show cost centers with materialized paths
select * from final.costcenter;


-- let's define a stored procedure that will take the final.constcenter table and materialize the data
-- in the format suitable for BI tools

create or replace procedure convert_to_static(NEW_TABLE_NAME STRING)
  returns string
  language javascript
  as
  $$
  var sql_size_command = "select max(array_size(hierarchy)) from final.costcenter"; // + TABLE_NAME;
   var stmt = snowflake.createStatement(
         {
         sqlText: sql_size_command
         }
      );
  var res = stmt.execute();
  res.next();
  var size = parseInt(res.getColumnValue(1));

  // now compose the create table statement

  var columns= "level1 string";
  
  var i=1;
  while(i<size) {
      columns+=", level"+parseInt(i+1)+" string";
      i++;
  }
    var sql_create_table = "create or replace table " + NEW_TABLE_NAME + " (id string, " + columns + ")";

  
  // we have the create table statement
  // let's execute it
  
  var stmt = snowflake.createStatement(
         {
         sqlText: sql_create_table
         }
      );
  var res = stmt.execute();
  
  // now, let's iterate and fill the new table
  
  var sql_path_query = "select * from final.costcenter";
  var stmt = snowflake.createStatement(
         {
         sqlText: sql_path_query
         }
      );
  var res1 = stmt.execute();
  
  // we have the json paths, let's iterrate


  var sql_insert = "insert into " + NEW_TABLE_NAME + " values "  // (" + values + ")";
  for (var pos = 0; res1.next(); pos++) {
  
      var id = res1.getColumnValue(1);
      
      var value_array=res1.getColumnValue('HIERARCHY');
      
      var values = "'" + id +"'," + "'" + value_array.join("','") + "'";
      
      for(var i=value_array.length; i< size; i++) {
          values+=", NULL"
      }
      
      sql_insert += (pos===0 ? "" : ",") +  "(" + values + ")";
    
  }

  var stmt2 = snowflake.createStatement(
         {
         sqlText: sql_insert
         }
      );
  var res2 = stmt2.execute();

  $$
  ;

-- TEST if everything worked

-- just in case
drop table bi_hi;

call convert_to_static('bi_hi');

-- check for content
select * from bi_hi;

