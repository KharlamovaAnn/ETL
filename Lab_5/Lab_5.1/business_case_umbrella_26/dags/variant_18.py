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
