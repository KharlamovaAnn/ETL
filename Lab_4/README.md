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

<img width="1308" height="496" alt="Снимок экрана 2026-04-07 020357" src="https://github.com/user-attachments/assets/f03e0ad2-5abf-4899-9a0f-b2852a8b825d" />


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

**Результат очистки.**
В результате мы можем видеть, что исходные данные уже предоставлены в очищенном виде, ни один столбец не привысил 60% пропусков:

<img width="1352" height="670" alt="Снимок экрана 2026-04-07 020503" src="https://github.com/user-attachments/assets/c95ba518-a66f-4149-a99d-f7e6192286c4" />


## Шаг 3. Load (Загрузка / Сохранение результатов)

Очищенный датасет сохраняется в формате Parquet, который обеспечивает высокую скорость чтения/записи и оптимизированное хранение в приложениях Big Data.

```python
df_dropped.to_parquet('cleaned_austin_listings.parquet', engine='pyarrow')
```

Результат:

<img width="661" height="166" alt="Снимок экрана 2026-04-07 020535" src="https://github.com/user-attachments/assets/a38af63c-4d25-4875-a4f6-0920c6da5beb" />


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

<img width="551" height="684" alt="Снимок экрана 2026-04-07 020554" src="https://github.com/user-attachments/assets/70a017db-a0a7-4291-992d-797bd5473f0f" />


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

<img width="850" height="730" alt="Снимок экрана 2026-04-07 020629" src="https://github.com/user-attachments/assets/10b30988-0bc7-4d4d-86a0-3eb1d1427c92" />


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

<img width="1095" height="589" alt="Снимок экрана 2026-04-07 020708" src="https://github.com/user-attachments/assets/44d79a2e-4908-4b91-9009-be6a0f4bd2de" />

<img width="950" height="651" alt="Снимок экрана 2026-04-07 020716" src="https://github.com/user-attachments/assets/9e233c59-db49-4bba-a68a-ec27e735fab1" />

