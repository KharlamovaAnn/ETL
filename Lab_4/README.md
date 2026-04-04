# Лабораторная работа №4. Анализ и обработка больших данных с Dask (ETL-пайплайн)

**Вариант 18**: `Austin, TX House Listings.zip`

**Цель работы:** изучить инструменты Dask для обработки Big Data, освоить построение ETL-пайплайнов с «ленивыми вычислениями» и визуализировать графы выполнения задач (DAG).

## Шаг 1. Extract (Извлечение данных)

Для работы с набором данных используется `dask.dataframe`. Среда выполнения — Google Colab с подключением Google Drive. Настраиваем локальный кластер для параллельной обработки.

```python
import dask.dataframe as dd
from dask.distributed import Client
from dask.diagnostics import ProgressBar

# Инициализация клиента Dask (2 воркера, 2 потока на воркер)
client = Client(n_workers=2, threads_per_worker=2, processes=True)

# Чтение данных из ZIP-архива (ленивая загрузка)
file_path = '/content/drive/MyDrive/austin_house_listings.zip'
df = dd.read_csv(file_path, compression='zip', blocksize=None)
```
Результат:
<img width="1280" height="841" alt="image" src="https://github.com/user-attachments/assets/b19920c8-ab48-4903-8e8f-46c3dcc47d46" />

## Шаг 2. Transform (Трансформация и очистка данных)

Проведено профилирование качества данных (подсчет пропусков). Столбцы с пропуском более 60% удаляются автоматически (лениво). Также добавлены интерактивные графики Altair для визуализации профиля данных.

```python
# Подсчет пропущенных значений (построение графа вычислений)
missing_values = df.isnull().sum()

# Вычисление процента пропусков
mysize = df.index.size
missing_count = ((missing_values / mysize) * 100)

# Запуск реальных вычислений только для агрегированной статистики
with ProgressBar():
    missing_count_percent = missing_count.compute()

# Визуализация пропусков с помощью Altair
import altair as alt
missing_df = missing_count_percent.reset_index()
missing_df.columns = ['Column', 'Percentage']

# Формирование списка столбцов для удаления (> 60% пропусков)
columns_to_drop = list(missing_count_percent[missing_count_percent > 60].index)
print("\nУдаляемые столбцы (пропуски > 60%):", columns_to_drop)

# Ленивое удаление столбцов
df_dropped = df.drop(columns=columns_to_drop)

df_dropped.head()
```

**Результат очистки:**
Удалены избыточные и практически пустые столбцы, что повышает эффективность последующего анализа.
<img width="1680" height="838" alt="image" src="https://github.com/user-attachments/assets/e3f8edad-a51a-4d63-a10c-8d38005fd7e6" />

## Шаг 3. Load (Загрузка / Сохранение результатов)

Очищенный датасет сохраняется в формате Parquet, который обеспечивает высокую скорость чтения/записи и оптимизированное хранение в приложениях Big Data.

```python
df_dropped.to_parquet('cleaned_austin_listings.parquet', engine='pyarrow')
```
Результат:
<img width="1754" height="275" alt="image" src="https://github.com/user-attachments/assets/621361f8-5e11-4e56-aafa-b2fc458d65d1" />

## Визуализация DAG

Dask составляет граф выполнения задач (DAG) перед началом вычислений.

### 1. Простой граф (Delayed Chain)

Граф визуализирует простую цепочку операций: инкремент двух чисел и их последующее сложение.

```python
import dask.delayed as delayed

def increment(i): return i + 1
def add(x, y): return x + y

x = delayed(increment)(10)
y = delayed(increment)(20)
z = delayed(add)(x, y)

z.visualize()
print("Результат вычисления DAG:", z.compute())
```
Результат: 
<img width="350" height="275" alt="image" src="https://github.com/user-attachments/assets/b19920c8-ab48-4903-8e8f-46c3dcc47d46" />

### 2. Сложный граф (Map-Reduce Process)

Построение многоуровневого графа для имитации процесса map-reduce: поэлементная обработка списка и финальная агрегация.

```python
data = [10, 20, 30, 40, 50]
layer1 = [delayed(increment)(i) for i in data]

def square(x): return x ** 2
layer2 = [delayed(square)(j) for j in layer1]
total = delayed(sum)(layer2)

total.visualize()
print("Итоговый результат сложного DAG:", total.compute())
```
Результат:
<img width="350" height="275" alt="image" src="https://github.com/user-attachments/assets/62396996-78ae-4b71-a54e-6dea9403fa7a" />

## #5 Аналитика (Altair)

В разделе аналитики реализованы интерактивные дашборды для изучения данных:
- Распределение цен на недвижимость по городам.
- Динамика цен в зависимости от года постройки (интерактивный scatter plot).
- Профилирование продаж по времени (месяца).

```python
# Пример дашборда Altair
chart1 = alt.Chart(df_sample).mark_circle(size=60).encode(
    x=alt.X('yearBuilt:Q', title='Год постройки'),
    y=alt.Y('latestPrice:Q', title='Цена ($)'),
    color='city:N',
    tooltip=['city', 'latestPrice', 'yearBuilt']
).interactive()
chart1.display()
```
Результаты аналитики:
<img width="1011" height="615" alt="image" src="https://github.com/user-attachments/assets/f6dcfc8b-9225-403a-bb77-75c2d8300996" />
<img width="954" height="613" alt="image" src="https://github.com/user-attachments/assets/595dd4bf-3756-4717-95cc-97369e7a94c1" />
