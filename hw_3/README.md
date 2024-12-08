## Домашнее задание 3.
### Команда - Дмитрий Янаков, Владислав Люкшин, Данила Грашенков.

Настройка:
1) Собираем контейнер с DMP сервисом: `docker-compose build dmp_service`.
2) Выполняем `docker compose up airflow-init`, чтобы инициализировать Airflow.
3) Поднимаем всю систему с помощью `sh docker-init.sh`.
4) Заходим в UI Airflow по `localhost:8080` (Username - `airflow`, Password - `airflow`).
5) Добавляем подключение: Connection Id - `postgres_dwh`, Connection Type - `postgres`, Host - `host.docker.internal`, Database - `postgres`, Login - `postgres`, Password - `postgres`, Port - `5434`.
6) Триггерим даги!

После запуска дагов в DWH появится новая схема **presentation** и две новые таблицы, соответствующие двум ETL.

Сделали без бонусных заданий. 
