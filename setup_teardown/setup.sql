-- This script will bootstrap your environment where you'll run the demo
-- Run this script using SnowSQL with a user that has the SYSADMIN role:
-- snowsql -u my_user -r sysadmin -a my_snowflake_account_identifier -f setup.sql

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
create warehouse if not exists hierarchy_wh;
use warehouse hierarchy_wh;

create table costcenter_raw (id string, name string, parentid string);

-- load the file into the table stage
put file://sample_data/costcenters_raw.csv @%costcenter_raw;

-- load the table from file on stage
copy into costcenter_raw file_format = (type = csv field_delimiter = ',' skip_header = 1);

-- sanity check
select *
from costcenter_raw;

-- now let's create the table in the final schema that has all the materialized paths
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

select *
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

-- create the flat table that for Enteprise BI tools will use
call convert_to_static('bi_hi');

select * from bi_hi;


-- now lets add the costs

-- let's create another table - it will contain costs created on different cost centers

create table cost (id string, date date, amount number, costcenterid string, description string );

-- now, load the cost csv using the UI or SnowSQL
-- load the file into the table stage
put file://sample_data/costs_raw.csv @%cost;

-- load the table from file on stage
copy into cost file_format = (type = csv field_delimiter = ',' skip_header = 1);


-- if that worked, the following query will return 26 rows
select * from cost;

alter warehouse hierarchy_wh suspend;

