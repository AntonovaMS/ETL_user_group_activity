insert into dds.fct_deliveries (order_id ,order_ts ,delivery_id ,courier_id ,address ,delivery_ts ,rate ,sum ,tip_sum )
select order_id ,order_ts::timestamp ,delivery_id ,courier_id ,address ,delivery_ts::timestamp ,rate::smallint ,sum::numeric(14, 2) ,tip_sum::numeric(14, 2) from stg.fct_deliveries 
where delivery_ts::date = '{{ds}}';
