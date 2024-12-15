## Домашнее задание 4.
### Команда - Дмитрий Янаков, Владислав Люкшин, Данила Грашенков.

Настройка:
1) Билдим Superset - 'docker-compose build'.
2) Поднимаем всю систему с помощью `sh docker-init.sh`.
3) Заходим в Superset по `localhost:8088` (Username - `admin`, Password - `admin`).
4) Добавляем подключение PostgreSQL: Host - `host.docker.internal`, Port - `5434`, Database - `postgres`, Username - `postgres`, Password - `postgres`.
5) Можем создавать дэшборды с помощью данных, лежащих в DWH (предварительно заинсертив что-то в мастер).

Ссылка на демонстрацию работы 2-х дэшбордов из задания - https://drive.google.com/file/d/1onF7uUQxLDQ9T9PjfRxPDDHCfeMUxbEK/view?usp=sharing

Тестовые инсерты в мастер, на которых далее были построены дэшборды, указаны в файле **test.sql**.
