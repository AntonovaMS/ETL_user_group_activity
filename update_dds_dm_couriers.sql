insert into dds.dm_couriers  (id, name, active_from) 
select id, name, now() from stg.dm_couriers  
where id not in (select id from dds.dm_couriers) 
group by id,name;

TRUNCATE TABLE stg.dm_couriers;