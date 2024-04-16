-- https://help.sap.com/viewer/9afb6f90c86748ef9f2b17c7d857f9b3/6.00.31/de-DE/a60ad553088f4308e10000000a174cb4.html

-- PART II - run this after PART I
-- In this worksheet we'll load costs and show how we query for aggregated costs that contribute to any cost center in the hierarchy

use role sysadmin;
use database controlling;
use schema final;
use warehouse controlling_wh;

-- drop table cost;

-- let's create another table - it will contain costs created on different cost centers
create table cost (id string, date date, amount number, costcenterid string, description string );

-- now, load the cost csv using the UI or SnowSQL

-- if that worked, the following query will return 26 rows
select * from cost;

-- let's find the 'Electricity' cost center with its materialized hiearchy
select * from final.costcenter
where name='Electricity';

-- let's demonstrate how we can flatten the hierarchy
select id, name, value
from final.costcenter, lateral flatten(input => hierarchy)
where name='Electricity';


-- now that we understand what lateral flatten dows,
-- show all cost centers "belonging" to a cost center using our materialized hierarchy
-- (no need for recursive queries anymore!)
select costcenter.id, name
from costcenter, lateral flatten(input => hierarchy)
where value ='S02000';


-- let's find costs centers "under" a category 
-- (we're excluding the category by using some semi-structured built-in functions by Snowflake)
select costcenter.id, name
from costcenter, lateral flatten(input => hierarchy)
where value ='S02000' and
array_position('S02000'::variant, hierarchy)<array_size(hierarchy)-1;

-- let's join that with the costs to
-- find all costs "under" a category (excluding that level)
with costcenters as
(
  select *
  from costcenter, lateral flatten(input => hierarchy)
  where value ='S02000' and
  array_position('S02000'::variant, hierarchy)<array_size(hierarchy)-1
)
select cost.*
from cost, costcenters
where cost.costcenterid=costcenters.id;

-- let's calculate the sum of all costs "under" a category
with costcenters as
(
  select *
  from costcenter, lateral flatten(input => hierarchy)
  where value ='S02000' and
  array_position('S02000'::variant, hierarchy)<array_size(hierarchy)-1
)
select sum(amount)
from cost, costcenters
where cost.costcenterid=costcenters.id;

-- the query can also be written like this  to calculate sum of costs for a category
select sum(amount) 
from final.costcenter, final.cost, lateral flatten(input => hierarchy)
where cost.costcenterid = costcenter.id
group by value
having value='S02000';

