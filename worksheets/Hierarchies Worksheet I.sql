-- PART I - run this first
-- let's import cost center raw data and traverse to materialize the paths
/*
id,name,parentid
S01000,BSP Inc,
S02000,Logistics,S01000
S02200,Energy,S02000
2210,Electricity,S02200
...
*/


use role sysadmin;
create database if not exists controlling;
use database controlling;
create schema if not exists staging;
create schema if not exists final;
use schema staging;
create warehouse if not exists controlling_wh;
use warehouse controlling_wh;

create table costcenter_raw (id string, name string, parentid string);
-- truncate table costcenter_raw; -- in case you need to delete content from the table

-- now, load from the CSV "constcenters_raw" here into the costcenter_raw table
-- either using the UI or SnowSQL

-- after the data has been loaded, the following query should return 26 rows
select * from staging.costcenter_raw;

-- the following query (recursive CTE) will find all cost centers bellow S02000 (9 rows total, the resultset includes also S02000)
-- https://docs.snowflake.com/en/user-guide/queries-hierarchical.html#using-connect-by-or-recursive-ctes-to-query-hierarchical-data


WITH RECURSIVE cc AS 
  (
    -- anchor clause (we’re starting with parent we’re interested in, using the id to identify it)

    select id, name 
    from staging.costcenter_raw
    where id = 'S02000'

    UNION ALL

    -- the recursive clause will traverse all the branches leading down from the parent specified in the anchor clause

    select costcenter_raw.id, costcenter_raw.name
    from staging.costcenter_raw, cc
    where staging.costcenter_raw.parentid = cc.id

   )
select id, name
from cc;

-- we'll add some semi-structured magic (JSON arrays: https://restfulapi.net/json-array/)
-- to materialize the paths of all cost centers leading up to the top-most parent(s)

WITH RECURSIVE create_paths AS
  (
    -- anchor (top-most parent, we're using the id to identify it)
    select '"' || parentid || '"' as parents, id, name 
    from staging.costcenter_raw
    where id = 'S01000'

    UNION ALL

    -- recursive clause will keep concatenating parent ids from the temp view "create_paths"
    select concat( ifnull(create_paths.parents || ',','') , '"' || costcenter_raw.parentid || '"'), costcenter_raw.id, costcenter_raw.name
    from staging.costcenter_raw, create_paths
    where staging.costcenter_raw.parentid = create_paths.id

   )
select id, name, parse_json( '[' || ifnull(parents || ',','')  || '"' || id || '"]')  as hierarchy
from create_paths;


-- the result contains another column, "hieraerchy", 
-- which materializes the result of recursive traversing of the whole tree

-- now let's create the table in the final schema that has materialized paths
-- so we only have to run the recursive query once

create or replace table final.costcenter as (
WITH RECURSIVE create_paths AS
  (
    -- anchor (top-most parent, we're using the id to identified it)
    select parentid as parents, id, name 
    from staging.costcenter_raw
    where id = 'S01000'

    UNION ALL

    -- recursive clause will keep concatenating parent ids from the temp view "create_paths"
    select concat( ifnull(create_paths.parents || ',','') , '"' || costcenter_raw.parentid || '"'), costcenter_raw.id, costcenter_raw.name
    from staging.costcenter_raw, create_paths
    where costcenter_raw.parentid = create_paths.id

   )
select id, name, parse_json( '[' || ifnull(parents || ',','')  || '"' || id || '"]')  as hierarchy
from create_paths
);

-- if the CTE above worked, this should show cost centers with materialized paths
select * from final.costcenter;


-- great, we've loaded and pre-calculated the paths we'll need to answer questions around costs that contribute to cost centers
-- now, open the Part II worksheet to proceed

select max(array_size(hierarchy))
from final.costcenter;

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

call convert_to_static('bi_hi');

insert into bi_hierarchy values ('S01000','S01000', NULL, NULL, NULL);
insert into bi_hierarchy values ('4220','S01000','S04000','S04100','4220');

select * from bi_hi;

drop table bi_hierarchy;

describe table bih;