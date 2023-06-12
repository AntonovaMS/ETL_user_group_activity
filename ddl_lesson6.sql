-----DDL----DDL----DDL---DDL---DDL----DDL------

        
---------------------------------STAGIN----------------------------------
drop table if exists ANTONOVAMS2016YANDEXRU__STAGING.group_log;
create table ANTONOVAMS2016YANDEXRU__STAGING.group_log
(
    group_id int PRIMARY KEY,
    user_id INT,
    user_id_from INT,
    event varchar(100),
    datetime timestamp /*название столбца было обозначенно в задании*/
)
order by group_id /*является ли здесь это приницпиальным моментом? думаю ,может быть ,этот ордер бай вообще излишен*/
SEGMENTED BY group_id all nodes 
PARTITION BY datetime::date
GROUP BY calendar_hierarchy_day(datetime::date, 3, 2);

----------------------------DWH-----------------------------------

drop table if exists ANTONOVAMS2016YANDEXRU__DWH.l_user_group_activity;

create table ANTONOVAMS2016YANDEXRU__DWH.l_user_group_activity
(
hk_l_user_group_activity int primary key,
hk_user_id bigint CONSTRAINT fk_l_user_group_activity_h_users REFERENCES ANTONOVAMS2016YANDEXRU__DWH.h_users(hK_user_id),
hk_group_id bigint CONSTRAINT fk_l_user_group_activity_h_group REFERENCES ANTONOVAMS2016YANDEXRU__DWH.h_groups(hk_group_id),
load_dt datetime,
load_src varchar(20)
) 
order by load_dt 
SEGMENTED BY hk_user_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);



drop table if exists ANTONOVAMS2016YANDEXRU__DWH.s_auth_history;

create table ANTONOVAMS2016YANDEXRU__DWH.s_auth_history
(
hk_l_user_group_activity bigint not null CONSTRAINT fk_s_auth_history_l_user_group_activity REFERENCES ANTONOVAMS2016YANDEXRU__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from int,
event  varchar(100),
event_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_l_user_group_activity all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

