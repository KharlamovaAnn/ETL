# Лабораторная работа №3. Интеграция данных из нескольких источников. Обработка и согласование данных из разных источников

**Цель работы.** Разработать комплексное ETL-решение для интеграции данных из локальной СУБД PostgreSQL и файловых источников (CSV/Excel) в целевое хранилище MySQL. Спроектировать верхнеуровневую архитектуру аналитического решения.

## Вариант 18

Оценка эффективности КЦ. Сопоставить длительность звонка с тематикой и рейтингом оператора.

Call-центр.
PostgreSQL: Журнал звонков.
Excel: Оценки операторов (KPI).
CSV: Тематика обращений.

# Ход работы

## Шаг 1. Архитектура решения

```mermaid
graph TD
    subgraph "Source Layer"
        PG[(PostgreSQL: call_logs)]
        EX[Excel: operator_kpi.xlsx]
        CSV[CSV: call_topics.csv]
    end

    subgraph "Storage Layer"
        PDI{Pentaho PDI - ETL}
        MY[(MySQL: call_center_analytics)]
        SA[Staging Area / Transformation]
    end

    subgraph "Business Layer"
        VW[MySQL View: view_call_center_report]
        BI[Business Analytics / KPIs]
    end

    PG --> PDI
    EX --> PDI
    CSV --> PDI
    PDI --> SA
    SA --> MY
    MY --> VW
    VW --> BI
```

## Шаг 2. Создание таблицы и её заполнение в PostgreSQL:

### Создание таблицы (call_logs):

```sql
CREATE TABLE call_logs (
    call_id SERIAL PRIMARY KEY,
    operator_id INT,
    call_date TIMESTAMP,
    duration_sec INT
);
```

<img width="1847" height="766" alt="image" src="https://github.com/user-attachments/assets/177eb3da-ed85-46c1-8243-e24ce20e22ad" />


Вид таблицы:

<img width="1851" height="960" alt="image" src="https://github.com/user-attachments/assets/66c65b56-4ab2-4fbc-8fb0-0441988946a3" />



## Шаг 3. Разработка трансформации в Pentaho (Spoon)

Общий вид трансформации:

<img width="1492" height="721" alt="image" src="https://github.com/user-attachments/assets/d3028783-2a54-4e6f-89bc-e1d45c40d227" />


### Настройка основных узлов

#### Подключение PostgreSQL:

<img width="1122" height="780" alt="image" src="https://github.com/user-attachments/assets/a8321fdc-127a-4c64-8d64-af01bf506ed3" />


#### Подключение файлов:

<img width="1705" height="756" alt="image" src="https://github.com/user-attachments/assets/c8f39731-f570-4f76-b702-7bec3f25d3ee" />


#### Фильтрация:

- Условие: `duration_sec > 540` (фильтрация 1 млн строк до ~100 тыс. целевых записей).

## Шаг 4. Создание витрины данных (MySQL View)
Создание основной таблицы:
```sql
CREATE TABLE IF NOT EXISTS call_center_analytics (
    call_id INT PRIMARY KEY,
    operator_name VARCHAR(255),
    kpi_score DECIMAL(3, 2),
    topic_name VARCHAR(100),
    call_date TIMESTAMP,
    duration_sec INT
);
```

<img width="1564" height="564" alt="image" src="https://github.com/user-attachments/assets/e77a4444-3c19-4b22-9cd3-2e214ed301ad" />


Вид таблицы:

```sql
CREATE OR REPLACE VIEW view_call_center_report AS
SELECT
    topic_name,
    CASE
        WHEN kpi_score >= 4.5 THEN 'High Performing'
        WHEN kpi_score >= 3.0 THEN 'Standard'
        ELSE 'Needs Improvement'
    END AS operator_performance_category,
    AVG(duration_sec) as avg_duration,
    COUNT(*) as total_calls
FROM call_center_analytics
GROUP BY topic_name, operator_performance_category;
```

<img width="1562" height="504" alt="image" src="https://github.com/user-attachments/assets/c788bc8b-c88c-493a-82bb-a0d86712690b" />
