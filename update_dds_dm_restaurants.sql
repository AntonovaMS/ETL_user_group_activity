insert into dds.dm_restaurants  (id, name, active_from) 
select id, name, now()  from stg.dm_restaurants  
where id not in (select id from dds.dm_restaurants) 
group by id,name;

TRUNCATE TABLE stg.dm_restaurants;