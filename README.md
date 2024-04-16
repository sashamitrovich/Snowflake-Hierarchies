# Hierarchies in Snowflake
This demo shows several Snowflake features that allow storing and querying SAP-style hierarchies.

Many prospects and customers are using Snowflake as data platform for
_single point of truth_ which integrates data from many different sources, including the SAP landcape.

In this scenario, one of the key aspects is using Snowflake to ingest, store and analyze SAP hieararchies. Examples of these hierarchies include multi-level bill of materials (BOMs), parts genealogy, HR organization charts and cost center organization.

A sample cost center hierarchy used in the demo:
![](/images/CostCenterHierarchy_de.png)


## Project structure

Directory [worksheets](/worksheets/) contains the SQL code to execute on Snowflake. Make sure to follow the script and load sample data from [sample_data](/sample_data/).

### Sample data

[sample_data/costcenters_raw.csv](/sample_data/costcenters_raw.csv) contains the raw SAP table with the cost center hierarchy:

| id | name | parentid |
| --- | --- | ------
 | S01000 | BSP AG | |
| S02000| Logistik | S01000 |
| S02200| Energie | S02000 |
| 2210| Strom | S02200 |
| 2220| Wasser | S02200 |
| 2230| Gas | S02200 |
| S02300| Gebäude | S02000 |
| ...| ... | ... |
| ...| ... | ... |

[sample_data/costs_raw.csv](/sample_data/costs_raw.csv) contains the costs that are created and assigned to different cost centers (meaning levels)


### [Worksheet 1](/worksheets/Hierarchies%20Worksheet%20I.sql)

In this worksheet we import the data and run a  [Recursive CTEs]([docs.snowflake.com/](https://docs.snowflake.com/en/user-guide/queries-cte.html#recursive-ctes-and-hierarchical-data)) query to traverse the raw data and materialize all the paths within the hierarchy. At the end of the worksheet, we've create a _costcenter_ table with the following content:

ID|NAME|HIERARCHY
---|---|---
S01000|BSP Inc|["S01000"]
S02000|Logistics|["S01000","S02000"]
S03000|Admin.|["S01000","S03000"]
S01100|Management|["S01000","S01100"]
S04000|Production|["S01000","S04000"]
S02200|Energy|["S01000","S02000","S02200"]
...|...|...
...|...|...

### [Worksheet 2](/worksheets/Hierarchies%20Worksheet%20II.sql)

In this worksheet we query the hierarchy from the table that we materialized in the previous worksheet. Typical questions that can be answered with this structures are: find all "children" of a given cost center (regardless on which level/depth), find all associated costs, find a sum of costs etc.

### [Worksheet 3](/worksheets/Hierarchies%20Worksheet%20III.sql)

In this worksheet we show how the cost center hierarchy can be further tranformed using a stored procedure into a format suitable to be analyzed by common BI tools. The stored procedure reads the _costcenter_ table and creates a pivot table. This table can then be analysed using, for instance, Tableau:

![tableau](images/tableau%20hierarchy%20analysis.png)
