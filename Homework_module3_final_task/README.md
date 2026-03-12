# ETL Pipeline: MongoDB → PostgreSQL (Airflow)

Итоговое задание по дисциплине **ETL-процессы (Модуль 3)**.

В рамках проекта реализован ETL-пайплайн для переноса данных из нереляционной базы данных **MongoDB** в **PostgreSQL** с использованием **Apache Airflow** для оркестрации процессов.

Проект включает генерацию тестовых данных, их репликацию, трансформацию и построение аналитических витрин.

---

# Архитектура проекта

Пайплайн реализован по следующей схеме:

```

MongoDB → STG → DDS → MART

```

### Описание слоёв

**MongoDB**
- источник данных
- содержит коллекции с сырыми данными

**STG (staging layer)**
- промежуточный слой в PostgreSQL
- данные приводятся к табличной структуре

**DDS (data detail store)**
- слой очищенных и преобразованных данных
- добавляются вычисляемые поля

**MART**
- аналитические витрины
- используются для анализа данных

---

# Используемые технологии

- **MongoDB** — источник данных
- **PostgreSQL** — аналитическое хранилище
- **Apache Airflow** — оркестрация ETL
- **Docker / Docker Compose** — контейнеризация
- **Python** — реализация ETL-логики

---

# Структура проекта

```

Homework_module3_final_task/
│
├── dags/
│   ├── mongo_to_postgres_etl.py
│   └── build_datamarts.py
│
├── scripts/
│   ├── generate_mongo_data.py
│   ├── mongo_to_stg.py
│   ├── stg_to_dds.py
│   └── build_marts.py
│
├── sql/
│   ├── create_tables.sql
│   └── create_marts.sql
│
├── screenshots/
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md

```

---

# Описание ETL процесса

## 1. Генерация данных

Скрипт:

```

scripts/generate_mongo_data.py

```

Создаёт тестовые данные и загружает их в MongoDB:

**Коллекции:**

- `user_sessions`
- `support_tickets`

---

## 2. Репликация данных (Mongo → STG)

Скрипт:

```

scripts/mongo_to_stg.py

```

Что происходит:

- извлечение данных из MongoDB
- базовая трансформация
- загрузка данных в PostgreSQL

Таблицы:

```

stg.user_sessions
stg.support_tickets

```

---

## 3. Построение слоя DDS

Скрипт:

```

scripts/stg_to_dds.py

```

На этом этапе вычисляются производные поля:

**user_sessions**
- `session_duration_minutes`

**support_tickets**
- `resolution_time_hours`

Таблицы:

```

dds.user_sessions
dds.support_tickets

```

---

# Аналитические витрины

В проекте реализованы две витрины.

## mart.user_activity

Содержит агрегированную информацию по активности пользователей:

- количество сессий
- средняя длительность сессии
- количество посещённых страниц
- количество действий

---

## mart.support_performance

Статистика по обращениям в поддержку:

- статус тикета
- тип проблемы
- количество тикетов
- среднее время обработки
- количество открытых тикетов

---

# Airflow DAG

В проекте реализовано два DAG.

## 1. mongo_to_postgres_etl

ETL процесс:

```

MongoDB → STG → DDS

```

Tasks:

- `mongo_to_stg`
- `stg_to_dds`

---

## 2. build_datamarts

Формирование аналитических витрин:

```

DDS → MART

```

Task:

- `build_marts`

---

# Запуск проекта

## 1. Создать файл `.env`

Пример содержимого смотрите в .env.example 

---

## 2. Запустить инфраструктуру

```

docker compose up airflow-init

docker compose up -d

```

---

## 3. Инициализировать структуры PostgreSQL

Для создания необходимой структуры таблиц для хранения данных, необходимо выполнить следующую команду:

```

docker exec -i etl_postgres psql -U airflow -d airflow < sql/create_tables.sql

```

Для создания таблиц предназначенных для аналитических витрин:

```

docker exec -i etl_postgres psql -U airflow -d airflow < sql/create_marts.sql

```

---

## 4. Сгенерировать тестовые данных MongoDB

---

docker compose exec airflow-scheduler python /opt/airflow/scripts/generate_mongo_data.py

---

## 5. Открыть Airflow

```

http://localhost:8080

```

Логин:

```

airflow

```

Пароль:

```

airflow

```

---

## 6. Запуск ETL

В Airflow UI необходимо запустить DAG:

```

mongo_to_postgres_etl

```

После этого запустить:

```

build_datamarts

```

---

# Результат

После выполнения пайплайна данные будут доступны в PostgreSQL:

**stg**
- user_sessions
- support_tickets

**dds**
- user_sessions
- support_tickets

**mart**
- user_activity
- support_performance

---