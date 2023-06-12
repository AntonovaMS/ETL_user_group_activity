-------------Шаг 7.1--------------
with user_group_messages as (
    select hk_group_id,count(distinct(message_from )) as cnt_users_in_group_with_messages
	from ANTONOVAMS2016YANDEXRU__DWH.s_dialog_info sdi 
	join ANTONOVAMS2016YANDEXRU__DWH.h_dialogs hd on sdi.hk_message_id = hd.hk_message_id 
	join ANTONOVAMS2016YANDEXRU__DWH.l_user_message lum on hd.hk_message_id = lum.hk_message_id
	join ANTONOVAMS2016YANDEXRU__DWH.l_groups_dialogs lgd on lum.hk_message_id = lgd.hk_message_id
	group by hk_group_id
)

select hk_group_id, cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10

;



--------------Шаг 7.2---------------

with user_group_log as (
    SELECT t1.*, hg.registration_dt 
	FROM(SELECT hk_group_id, count(distinct(hk_user_id)) as cnt_added_users
	FROM ANTONOVAMS2016YANDEXRU__DWH.s_auth_history AS SU
	JOIN ANTONOVAMS2016YANDEXRU__DWH.l_user_group_activity AS LUGA ON  SU.hk_l_user_group_activity = LUGA.hk_l_user_group_activity 
	WHERE EVENT = 'add'
	group by hk_group_id) AS T1
	JOIN ANTONOVAMS2016YANDEXRU__DWH.h_groups as hg ON T1.hk_group_id = hg.hk_group_id 
	order by hg.registration_dt 
	limit 10
)
select hk_group_id ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10

;

--------------------Шаг 7.3----------------------



select T1.hk_group_id, cnt_added_users, cnt_users_in_group_with_messages, ROUND(cnt_users_in_group_with_messages/cnt_added_users*100,0) as group_conversion
from(with user_group_log as (
    SELECT t1.*, hg.registration_dt 
	FROM(SELECT hk_group_id, count(distinct(hk_user_id)) as cnt_added_users
	FROM ANTONOVAMS2016YANDEXRU__DWH.s_auth_history AS SU
	JOIN ANTONOVAMS2016YANDEXRU__DWH.l_user_group_activity AS LUGA ON  SU.hk_l_user_group_activity = LUGA.hk_l_user_group_activity 
	WHERE EVENT = 'add'
	group by hk_group_id) AS T1
	JOIN ANTONOVAMS2016YANDEXRU__DWH.h_groups as hg ON T1.hk_group_id = hg.hk_group_id 
	order by hg.registration_dt 
	limit 10
)
select hk_group_id ,cnt_added_users
from user_group_log
order by cnt_added_users
limit 10) T1 
join (
    select hk_group_id,count(distinct(message_from )) as cnt_users_in_group_with_messages
	from ANTONOVAMS2016YANDEXRU__DWH.s_dialog_info sdi 
	join ANTONOVAMS2016YANDEXRU__DWH.h_dialogs hd on sdi.hk_message_id = hd.hk_message_id 
	join ANTONOVAMS2016YANDEXRU__DWH.l_user_message lum on hd.hk_message_id = lum.hk_message_id
	join ANTONOVAMS2016YANDEXRU__DWH.l_groups_dialogs lgd on lum.hk_message_id = lgd.hk_message_id
	group by hk_group_id
) T2 on T1.hk_group_id = T2.hk_group_id
order by group_conversion desc;
