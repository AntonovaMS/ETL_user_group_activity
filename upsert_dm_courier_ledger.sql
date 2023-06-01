
insert into cdm.dm_courier_ledger (courier_id ,courier_name , settlement_year , settlement_month ,orders_count ,orders_total_sum ,rate_avg , order_processing_fee,courier_order_sum ,courier_tips_sum ,courier_reward_sum)
select main.courier_id,dc."name" , main.settlement_year, main.settlement_month, main.orders_count, main.orders_total_sum, main.rate_avg, main.order_processing_fee, main.courier_order_sum, main.courier_tips_sum, main.courier_reward_sum 
from(select co.courier_id,
	   extract(year from order_ts) as settlement_year,
	   extract(month from order_ts) as settlement_month,
	   count(order_id) as orders_count,
	   sum(sum) as orders_total_sum,
	   avg(rate) as rate_avg,
	   sum(sum)*0.25 as order_processing_fee,
	   sum(courier_order) as courier_order_sum,
	   sum(tip_sum) as courier_tips_sum,
	   sum(courier_order) + sum(tip_sum) * 0.95 as courier_reward_sum
from dds.fct_deliveries fd
		join(select a.courier_id, r as avg_rate , su, case 
			when r < 4 then su*0.05 
				when su*0.05<= 100 then 100
			when 4 <= r and r < 4.5 then su*0.07 
				when su*0.07<=150 then 150 
			when 4.5 <= r and r < 4.9 then su*0.08 
				when su*0.08<=175 then 175
			when 4.9 <= r then su*0.1 
				when su*0.1<=200 then 200
			end as courier_order
		from (select courier_id, sum as su from dds.fct_deliveries)a
		join(select courier_id, avg(rate) as r from dds.fct_deliveries group by courier_id)b 
		on a.courier_id = b.courier_id) co
on co.courier_id = fd.courier_id 
group by co.courier_id,extract(year from order_ts), extract(month from order_ts))main
left join dds.dm_couriers dc on main.courier_id = dc.id 
order by main.settlement_year, main.settlement_month
where settlement_year = extract(year from '{{ds}}') and settlement_month = extract(month from '{{ds}}') 
on conflict (courier_id ,courier_name , settlement_year ,settlement_month) do update 
		 SET orders_count = excluded.orders_count , 
  			 orders_total_sum = excluded.orders_total_sum, 
  			 rate_avg = excluded.rate_avg, 
  			 order_processing_fee = excluded.order_processing_fee, 
  			 courier_order_sum = excluded.courier_order_sum, 
  			 courier_tips_sum = excluded.courier_tips_sum, 
  			 courier_reward_sum = excluded.courier_reward_sum
  			 
  			 