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