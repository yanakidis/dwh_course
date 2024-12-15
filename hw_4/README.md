## Домашнее задание 4.
### Команда - Дмитрий Янаков, Владислав Люкшин, Данила Грашенков.

Настройка:
1) Поднимаем всю систему с помощью `sh docker-init.sh`.
2) Заходим в Superset по `localhost:8088` (Username - `admin`, Password - `admin`).
3) Добавляем подключение PostgreSQL: Host - `host.docker.internal`, Port - `5434`, Database - `postgres`, Username - `postgres`, Password - `postgres`.
4) Можем создавать дэшборды с помощью данных, лежащих в DWH.

Ссылка на демонстрацию работы 2-х дэшбордов из задания - https://drive.google.com/file/d/1onF7uUQxLDQ9T9PjfRxPDDHCfeMUxbEK/view?usp=sharing

Тестовые инсерты в мастер, на которых далее были построены дэшборды, указаны в файле **test.sql**.
