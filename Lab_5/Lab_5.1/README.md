# Проектный практикум по разработке ETL-решений: Лабораторная работа №5

## Постановка задачи (Вариант 18)
Разработать контейнеризированное ETL-решение на базе Apache Airflow для автоматизации пайплайна обработки данных со следующими требованиями:
- Получить прогноз погоды в **Стокгольм, 3 дня** (используя внешний API).
- Удалить дубликаты
- Сгенерировать данные о продажах за эти же даты и объединить наборы данных.
- Обучить простейшую ML-модель (Линейная регрессия).
- Вывести **Таблица "Дата — Температура"** (в качестве инструмента визуализации добавлен Streamlit).

## Архитектура проекта

```mermaid
graph TD
    subgraph External Sources
        API[Open-Meteo]
    end

    subgraph Docker Infrastructure
        PG[(PostgreSQL Airflow Metadata)]
        
        subgraph Airflow Components
            SCH[Scheduler]
            WEB[Webserver :8080]
            INIT[Init-container]
        end
        
        subgraph Visualization
            STR[Streamlit App :8501]
        end
        
        VOL[Shared Volume /opt/airflow/data]
    end

    subgraph User
        BROWSER((Browser))
    end

    API -->|JSON forecast| SCH
    SCH -->|Saves CSV| VOL
    SCH <-->|Reads/Writes State| PG
    WEB <-->|Reads State| PG
    STR -->|Reads CSV| VOL
    
    BROWSER -->|Monitors DAGs| WEB
    BROWSER -->|Views Dashboard| STR
```

## Технический стек
* **Оркестрация:** Apache Airflow 2.8.1
* **Контейнеризация:** Docker, Docker Compose
* **Язык программирования:** Python 3.11
* **Библиотеки (ETL & ML):** Pandas, Scikit-learn, Joblib, Requests
* **Визуализация:** Streamlit, Matplotlib
* **База данных:** PostgreSQL 12 (для метаданных Airflow)

## Описание DAG (`variant_14_warsaw`)
Пайплайн состоит из следующих задач (Task):
1. **`fetch_weather_forecast`**: Обращается к Open-Meteo, получает прогноз для **Стокгольма** на 3 дня, сохраняет в `weather_forecast.csv`.
2. **`clean_weather_data`**: Заполняет пропуски, вычисляет и **добавляет столбец "день недели"** и флаг рабочего дня, сохраняет в `clean_weather.csv`.
3. **`visualize_table`**: Фильтрует рабочие дни, группирует по дню недели (средняя температура), сохраняет и отрисовывает картинку таблицы.
4. **`fetch_sales_data`**: Читает даты из прогноза погоды и генерирует данные о продажах на эти же даты, сохраняет в `sales_data.csv`.
5. **`clean_sales_data`**: Очищает данные продаж.
6. **`join_datasets`**: Объединяет погоду и продажи по дате (Inner Join).
7. **`train_ml_model`**: Обучает линейную регрессию предсказывать продажи по температуре.
8. **`deploy_ml_model`**: Имитирует деплой (загружает сохраненную `.pkl` модель).

---

## Исходный код

Перед началом создайте следующую структуру директорий:
```text
project/
├── dags/
│   └── variant_18.py
├── app/
│   └── app.py
├── data/
├── docker-compose.yml
└── Dockerfile
```

### 1. `Dockerfile`
Добавлен `streamlit` и `matplotlib` для графиков.
```dockerfile
FROM apache/airflow:slim-2.8.1-python3.11

USER airflow

# Устанавливаем необходимые Python-библиотеки
RUN pip install --no-cache-dir \
    pandas \
    scikit-learn \
    joblib \
    requests \
    azure-storage-blob==12.8.1 \
    psycopg2-binary \
    streamlit \
    matplotlib \
    "connexion[swagger-ui]"

USER root

# Создаём директории и назначаем владельца
RUN mkdir -p /opt/airflow/data /opt/airflow/logs /opt/airflow/app \
    && chown -R airflow: /opt/airflow/data /opt/airflow/logs /opt/airflow/app

USER airflow
```

### 2. `docker-compose.yml`
Добавлен сервис `streamlit` для визуализации графиков.
```yaml
x-environment: &airflow_environment
  - AIRFLOW__CORE__EXECUTOR=LocalExecutor
  - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
  - AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
  - AIRFLOW__CORE__LOAD_EXAMPLES=False
  - AIRFLOW__CORE__STORE_DAG_CODE=True
  - AIRFLOW__CORE__STORE_SERIALIZED_DAGS=True
  - AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True
  - AIRFLOW__WEBSERVER__RBAC=False
  - AIRFLOW__WEBSERVER__SECRET_KEY=supersecretkey123
  - AIRFLOW__LOGGING__LOGGING_LEVEL=INFO
  - AIRFLOW__LOGGING__REMOTE_LOGGING=False
  - AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs

x-airflow-image: &airflow_image custom-airflow:slim-2.8.1-python3.11

services:
  postgres:
    image: postgres:12-alpine
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      timeout: 5s
      retries: 5

  init:
    image: *airflow_image
    depends_on:
      postgres:
        condition: service_healthy
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - logs:/opt/airflow/logs
    entrypoint: >
      bash -c "
      airflow db upgrade &&
      airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.org &&
      echo 'Airflow init completed.'"
    healthcheck:
      test: ["CMD", "airflow", "db", "check"]
      interval: 10s
      retries: 5
      start_period: 10s

  webserver:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8080:8080"
    restart: always
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - logs:/opt/airflow/logs
    command: webserver

  scheduler:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    restart: always
    environment: *airflow_environment
    volumes:
      - ./dags:/opt/airflow/dags
      - ./data:/opt/airflow/data
      - logs:/opt/airflow/logs
    command: scheduler

  streamlit:
    image: *airflow_image
    depends_on:
      init:
        condition: service_completed_successfully
    ports:
      - "8501:8501"
    restart: always
    volumes:
      - ./data:/opt/airflow/data
      - ./app:/opt/airflow/app
    command: bash -c "streamlit run /opt/airflow/app/app.py --server.port=8501 --server.address=0.0.0.0"

volumes:
  logs:
  postgres_data:
```

### 3. `dags/variant_18.py`
```python
import os
import requests
import pandas as pd
import joblib
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sklearn.linear_model import LinearRegression

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    dag_id="variant_18_stockholm",
    default_args=default_args,
    description="Variant 18: Stockholm 3 days, remove duplicates, Date-Temperature table.",
    schedule_interval="@daily",
    catchup=False
)

def fetch_weather_forecast():
    # Stockholm coords: 59.3293, 18.0686
    url = (
        "https://api.open-meteo.com/v1/forecast?"
        "latitude=59.3293&longitude=18.0686"
        "&daily=temperature_2m_mean"
        "&timezone=Europe%2FStockholm"
        "&forecast_days=3"
    )
    
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API request failed with status {response.status_code}")

    data = response.json()
    dates = data['daily']['time']
    temperatures = data['daily']['temperature_2m_mean']
    
    df = pd.DataFrame({
        'date': dates,
        'temperature': temperatures
    })
    
    # Искусственно добавим дубликат для демонстрации выполнения задания 2
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    save_path = os.path.join(data_dir, 'weather_forecast_raw.csv')
    df.to_csv(save_path, index=False)
    print(f"Raw weather forecast (with duplicates) saved to {save_path}.")

def transform_weather_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    df = pd.read_csv(os.path.join(data_dir, 'weather_forecast_raw.csv'))
    
    # Задание 2: Удалить дубликаты
    initial_len = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    print(f"Removed {initial_len - len(df)} duplicates.")
    
    # Добавление столбцов "день недели" и "is_working_day" для совместимости с дашбордом
    df['date'] = pd.to_datetime(df['date'])
    days_map = {
        0: 'Понедельник', 1: 'Вторник', 2: 'Среда', 
        3: 'Четверг', 4: 'Пятница', 5: 'Суббота', 6: 'Воскресенье'
    }
    df['день недели'] = df['date'].dt.weekday.map(days_map)
    df['is_working_day'] = df['date'].dt.weekday < 5
    
    # Сохраняем очищенные данные
    df.to_csv(os.path.join(data_dir, 'weather_forecast.csv'), index=False)
    print("Cleaned weather data (duplicates removed + weekdays added) saved.")

def create_temperature_table():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    df = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))
    
    # Задание 3: Таблица "Дата — Температура"
    # Мы можем просто вывести её в лог или сохранить как отдельный файл/красивый CSV
    table_df = df[['date', 'temperature']].copy()
    table_df.columns = ['Дата', 'Температура']
    
    print("--- Таблица: Дата — Температура ---")
    print(table_df.to_string(index=False))
    print("----------------------------------")
    
    table_df.to_csv(os.path.join(data_dir, 'date_temperature_table.csv'), index=False, encoding='utf-8-sig')
    print("Table 'Date — Temperature' saved to date_temperature_table.csv.")

def fetch_sales_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    weather_df = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))
    dates = weather_df['date'].tolist()
    
    # Моковые данные для обучения
    sales = [15, 20, 18][:len(dates)]
    
    df = pd.DataFrame({'date': dates, 'sales': sales})
    df.to_csv(os.path.join(data_dir, 'sales_data.csv'), index=False)

def join_datasets():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    weather_df = pd.read_csv(os.path.join(data_dir, 'weather_forecast.csv'))
    sales_df = pd.read_csv(os.path.join(data_dir, 'sales_data.csv'))
    
    joined_df = pd.merge(weather_df, sales_df, on='date', how='inner')
    joined_df.to_csv(os.path.join(data_dir, 'joined_data.csv'), index=False)

def train_ml_model():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    df = pd.read_csv(os.path.join(data_dir, 'joined_data.csv'))
    
    X = df[['temperature']]
    y = df['sales']
    
    model = LinearRegression()
    model.fit(X, y)
    
    joblib.dump(model, os.path.join(data_dir, 'ml_model.pkl'))

# Инициализация операторов
t1 = PythonOperator(task_id="fetch_weather", python_callable=fetch_weather_forecast, dag=dag)
t2 = PythonOperator(task_id="remove_duplicates", python_callable=transform_weather_data, dag=dag)
t3 = PythonOperator(task_id="create_table", python_callable=create_temperature_table, dag=dag)
t4 = PythonOperator(task_id="fetch_sales", python_callable=fetch_sales_data, dag=dag)
t5 = PythonOperator(task_id="join_data", python_callable=join_datasets, dag=dag)
t6 = PythonOperator(task_id="train_model", python_callable=train_ml_model, dag=dag)

# Зависимости
t1 >> t2 >> t3
t2 >> t4 >> t5
t5 >> t6
```

### 4. `app/app.py` (Streamlit Дашборд)
Скрипт выводит таблицу средних температур.
```python
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os

st.set_page_config(page_title="Прогноз погоды Стокгольм", layout="wide")
st.title("Анализ погоды в Стокгольме на 3 дня (Вариант 18)")

# Use relative path for portability
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
data_path = os.path.join(base_dir, 'data', 'weather_forecast.csv')

if os.path.exists(data_path):
    df = pd.read_csv(data_path)
    
    # 1. Отображаем очищенные данные
    st.write("### Очищенные данные с добавленным столбцом 'день недели'")
    st.dataframe(df)

    # 2. Визуализация таблицы: средняя температура по рабочим дням
    # Для Варианта 18 (3 дня) рабочих дней может быть меньше, но логика остается
    if 'is_working_day' in df.columns and 'день недели' in df.columns:
        st.write("### Визуализация таблицы: Средняя температура по рабочим дням")
        
        # Фильтруем рабочие дни и группируем
        working_days = df[df['is_working_day'] == True]
        if not working_days.empty:
            grouped = working_days.groupby('день недели')['temperature'].mean().reset_index()
            grouped.rename(columns={'день недели': 'День недели', 'temperature': 'Средняя температура, °C'}, inplace=True)
            grouped['Средняя температура, °C'] = grouped['Средняя температура, °C'].round(2)
            
            st.table(grouped)
            
            # Математический график для премиальности
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.axis('tight')
            ax.axis('off')
            table_data = grouped.values.tolist()
            columns = grouped.columns.tolist()
            table = ax.table(cellText=table_data, colLabels=columns, loc='center', cellLoc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(14)
            table.scale(1.2, 1.5)
            st.pyplot(fig)
        else:
            st.info("В выбранном периоде прогноза (3 дня) не найдено рабочих дней или данных для них.")
    else:
        st.error("В данных отсутствуют необходимые колонки 'is_working_day' или 'день недели'. Запустите обновленный DAG.")

    # 3. График общей динамики (Бонус для визуальной красоты)
    st.write("### Общая динамика температуры")
    fig2, ax2 = plt.subplots(figsize=(10, 4))
    ax2.plot(df['date'], df['temperature'], marker='o', color='#ff4b4b', linewidth=2)
    ax2.set_ylabel('Температура, °C')
    ax2.grid(True, linestyle='--', alpha=0.5)
    st.pyplot(fig2)

else:
    st.warning("Данные еще не сгенерированы. Пожалуйста, запустите DAG variant_18_stockholm в Airflow.")
```

---

## Ход выполнения


В  `docker-compose.yml` папка `data` пробрасывается из локальной системы внутрь контейнера (bind mount):
`- ./data:/opt/airflow/data`

Когда Docker монтирует  локальную папку `./data` в контейнер, она **перезаписывает** те права доступа, которые мы указывали в `Dockerfile` (`chown -R airflow`). 
Локальная папка принадлежит  пользователю компьютера (или root), а Airflow внутри контейнера работает от ограниченного пользователя `airflow` (обычно с UID 50000). Из-за этого у Airflow нет прав создать файл в этой папке.


```bash
sudo chown -R 50000:0 ./data
sudo chown -R dev:dev /home/dev/Downloads/practice/business_case_umbrella_25
```
### Подготовка и сборка кастомного образа
Поскольку в проекте используются дополнительные библиотеки (Pandas, Scikit-learn, Streamlit и др.), перед запуском оркестратора необходимо собрать кастомный Docker-образ из `Dockerfile`. 
Откройте терминал в корневой папке проекта (где лежат `Dockerfile` и `docker-compose.yml`) и выполните:

```bash
docker build -t custom-airflow:slim-2.8.1-python3.11 .
```

### Запуск проекта
После того как образ успешно собран, запустите всю инфраструктуру (PostgreSQL, Airflow Init, Webserver, Scheduler и Streamlit) в фоновом режиме:
```bash
docker compose up -d
```

### Проверка запущенных контейнеров
Убедитесь, что инфраструктура поднялась без ошибок. Для вывода списка активных контейнеров и их статусов используйте команду:
```bash
docker ps
```
*(Вы должны увидеть контейнеры с именами, содержащими `postgres`, `webserver`, `scheduler`, `streamlit`. Контейнер `init` завершит работу после настройки БД).*

### Просмотр логов
Чтобы отследить процесс инициализации Airflow или диагностировать работу компонентов, посмотрите логи.
Для просмотра логов всех сервисов в реальном времени:
```bash
docker compose logs -f
```
Для просмотра логов конкретного сервиса (например, чтобы убедиться, что `init` создал пользователя):
```bash
docker compose logs init
```
*(Для выхода из режима потокового чтения логов нажмите `Ctrl+C`)*.

### Выполнение DAG и получение визуализации
1. **Запуск пайплайна (Airflow):** 
   * Перейдите в браузере по адресу [http://localhost:8080](http://localhost:8080).
   * Авторизуйтесь (логин: `admin`, пароль: `admin`).
   * Найдите ваш DAG в списке, снимите его с паузы (переключатель слева) и запустите вручную, нажав кнопку **Play (▶)** ➜ **Trigger DAG**.
   * Дождитесь успешного выполнения всех задач (статус поменяется на темно-зеленый "Success"). Данные скачаются, обработаются и сохранится модель.

<img width="1916" height="496" alt="image1" src="https://github.com/user-attachments/assets/d570805a-45cd-491b-bfba-c2acafa618aa" /> <br/>

2. **Просмотр визуализации (Streamlit):**
   * Перейдите по адресу [http://localhost:8501](http://localhost:8501).
   * На открывшемся дашборде вы увидите очищенную таблицу данных и итоговую таблицу средних температур по рабочим дням.


<img width="1802" height="773" alt="image-2" src="https://github.com/user-attachments/assets/c68f4bb1-cd5f-48b9-8e85-3496216b724e" /> <br1/>

<img width="1822" height="784" alt="image-3" src="https://github.com/user-attachments/assets/3b8f26ad-2e90-4d5c-9b54-b301506bb930" /> <br2/>

3. Прогнозирование продаж:

<img width="1062" height="163" alt="image-4" src="https://github.com/user-attachments/assets/7dcb1207-edf3-4681-ba83-dfc9c865cbf3" /> <br3/>

### Выключение проекта и полная очистка ресурсов
После успешного завершения работы необходимо остановить сервисы, удалить контейнеры, очистить сеть, тома данных (volumes) и собранные образы.

1. Остановка контейнеров, удаление связанной сети и томов:
```bash
docker compose down -v
```
2. Удаление кастомного Docker-образа Airflow:
```bash
docker rmi custom-airflow:slim-2.8.1-python3.11
```
3. *(Опционально)* Очистка системы от зависших ("dangling") сетей и слоёв кэша сборки:
```bash
docker network prune -f
docker image prune -f
```
4. Если необходимо удалить сгенерированные файлы данных из локальной папки:
```bash
rm -rf data/*
```
