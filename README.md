# ETL_user_group_activity

### Описание проекта
Требуется выявить группы соцсети с наибольшей конверсией для размещения рекламы на сторонних сайтах с целью привлечь новых пользователей.

### Навыки и инструменты
* Airflow DAG
* Vertica
* boto3
* SQL подзапросы, джоины


### Общий вывод
Реализован Airflow DAG:
- выгрузка файлов csv из бакета S3
- заполнение таблиц staging-слоя данными из файлов csv 
- скрипты миграции в таблицу связи  и наполнение сателлита БД Vertica
- Витрина данных для поиска пабликов с высокой конверсией
