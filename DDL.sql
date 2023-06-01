/*---------слой STG------------*/


DROP TABLE  stg.dm_restaurants CASCADE;
DROP TABLE  stg.dm_couriers CASCADE;
DROP TABLE  stg.fct_deliveries CASCADE;

CREATE TABLE stg.dm_restaurants(
    id text,
    name text
);

CREATE TABLE stg.dm_couriers(
    id text,
    name text
);

CREATE TABLE stg.fct_deliveries(
    order_id text,
    order_ts text,
    delivery_id text,
    courier_id text,
    address text,
    delivery_ts text,
    rate text,
    sum text,
    tip_sum text
);

/*--------------слой DDS------------*/

DROP TABLE  dds.dm_restaurants CASCADE;
DROP TABLE  dds.dm_couriers CASCADE;
DROP TABLE  dds.fct_deliveries CASCADE;

CREATE TABLE dds.dm_restaurants(
    id text primary key not null,  
    name text not null,
    active_from  timestamp not null,
    active_to timestamp default '2099-01-01'::timestamp
);


CREATE TABLE dds.dm_couriers(
    id text primary key not null,  /*PK*/
    name text NOT null,
    active_from  timestamp not null,
    active_to timestamp default '2099-01-01'::timestamp
);


CREATE TABLE dds.fct_deliveries(
    order_id text not null, 
    order_ts timestamp not NULL,
    delivery_id text primary KEY,  
    courier_id text not NULL,   
    address text not NULL,
    delivery_ts timestamp not NULL,
    rate smallint default 0 CONSTRAINT positive_rate CHECK (rate >= 0),
    sum numeric(14, 2) default 0 CONSTRAINT positive_sum CHECK (sum >= 0),
    tip_sum numeric(14, 2) default 0 CONSTRAINT positive_tip_sum CHECK (tip_sum >= 0)
    /*FOREIGN KEY (order_id) REFERENCES dds.dm_orders (order_key),
    FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers (id)*/
);

/* слой CDM */
create table cdm.dm_courier_ledger(
	id serial,
	courier_id text not null,
	courier_name text, 
	settlement_year smallint, 
	settlement_month smallint,
	orders_count numeric(14, 2),
	orders_total_sum numeric(14, 2),
	rate_avg smallint,
	order_processing_fee numeric(14, 2),
	courier_order_sum numeric(14, 2),
	courier_tips_sum numeric(14, 2),
	courier_reward_sum numeric(14, 2))
	
CREATE UNIQUE INDEX update_insert_dm_courier_ledger on cdm.dm_courier_ledger (courier_id ,courier_name , settlement_year ,settlement_month);