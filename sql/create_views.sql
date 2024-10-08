/* Use this SQL script to test the output of the data */
create view region as select * from 'data/tpch/sf=10/region/*.parquet';
create view nation as select * from 'data/tpch/sf=10/nation/*.parquet';
create view part as select * from 'data/tpch/sf=10/part/*.parquet';
create view supplier as select * from 'data/tpch/sf=10/supplier/*.parquet';
create view customer as select * from 'data/tpch/sf=10/customer/*.parquet';
create view partsupp as select * from 'data/tpch/sf=10/partsupp/*.parquet';
create view orders as select * from 'data/tpch/sf=10/orders/*.parquet';
create view lineitem as select * from 'data/tpch/sf=10/lineitem/*.parquet';
