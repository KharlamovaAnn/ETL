# Лабораторная работа №2. Динамические соединения с базами данных

**Цель работы.** Получить практические навыки создания сложного ETL-процесса, включающего динамическую загрузку файлов по HTTP, нормализацию базы данных, обработку дубликатов и настройку обработки ошибок с использованием Pentaho Data Integration (PDI).

## Вариант 18

|№ |Основной фильтр для загрузки в БД	 |Доп. задание 1 (Аналитика)	 |Доп. задание 2 (Аналитика)| 
|-|--------------|------------|----|
|18| Заказы 1-го квартала  2016	| Отчет по регионам |	Анализ возвратов

# Ход работы

## Шаг 1. Подготовка базы данных

Перед запуском ETL-процесса необходимо создать структуру таблиц в вашей базе данных (mgpu_ico_etl_14). Выполните следующий SQL-скрипт через phpMyAdmin или DBeaver:
```SQL
-- 1. Таблица заказов (фактов)
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
 row_id INT PRIMARY KEY,
 order_date DATE,
 ship_date DATE,
 ship_mode VARCHAR(50),
 sales DECIMAL(10,2),
 quantity INT,
 discount DECIMAL(4,2),
 profit DECIMAL(10,2),
 returned TINYINT(1) DEFAULT 0 -- 1 = Yes, 0 = No
);
```
Пример Успешного выполнения:
<img width="1678" height="958" alt="image" src="https://github.com/user-attachments/assets/fdd1c40f-cafc-4f90-87dc-720a85c05f7a" />

```
-- 2. Таблица клиентов (измерение)
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
 id INT AUTO_INCREMENT PRIMARY KEY,
 customer_id VARCHAR(20) NOT NULL,
 customer_name VARCHAR(100),
 segment VARCHAR(50),
 country VARCHAR(100),
 city VARCHAR(100),
 state VARCHAR(100),
 postal_code VARCHAR(20),
 region VARCHAR(50),
 INDEX idx_customer_id (customer_id),
 INDEX idx_region (region)
);
```
Пример Успешногго выполнения:
<img width="1678" height="959" alt="image" src="https://github.com/user-attachments/assets/59c1ff9c-a687-49d2-8789-d75597abcd65" />

```
-- 3. Таблица продуктов (измерение)
DROP TABLE IF EXISTS products;
CREATE TABLE products (
 id INT AUTO_INCREMENT PRIMARY KEY,
 product_id VARCHAR(20) NOT NULL,
 category VARCHAR(50),
 sub_category VARCHAR(50),
 product_name VARCHAR(255),
 person VARCHAR(100),
 INDEX idx_product_id (product_id),
 INDEX idx_category (category),
 INDEX idx_subcategory (sub_category)
);
```
Пример Успешногго выполнения:
<img width="1677" height="953" alt="image" src="https://github.com/user-attachments/assets/508247b1-d70d-4418-8d12-5135b8a45188" />


```
-- 4. Индексы и настройка кодировки
ALTER TABLE orders ADD INDEX idx_order_date (order_date);
ALTER TABLE orders ADD INDEX idx_ship_date (ship_date);

-- ЗАМЕНИТЕ mgpu_ico_etl_18 на имя вашей базы данных!
ALTER DATABASE mgpu_ico_etl_18 CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE customers CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE products CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```
Пример Успешногго выполнения:
<img width="1682" height="955" alt="image" src="https://github.com/user-attachments/assets/c9bdad4e-af67-4695-a611-00ff633bbbd4" />|

<img width="1682" height="964" alt="image" src="https://github.com/user-attachments/assets/b339ede3-3d11-44c9-92d1-ab8175ce342d" />

## Шаг 2. Настройка Job (Главного задания)

**Set Variables: Создайте переменную пути к файлу.**
<img width="1851" height="986" alt="image" src="https://github.com/user-attachments/assets/30436c56-70c0-4398-b25f-dfa19e24c2f6" />

**Check File Exists: Проверка наличия файла ${CSV_FILE_PATH}.**
<img width="1634" height="920" alt="image" src="https://github.com/user-attachments/assets/8a64025a-17ad-4d13-9f2c-6414b47f8401" />

**HTTP (Download): Загрузка файла, если его нет.**
<img width="1853" height="991" alt="image" src="https://github.com/user-attachments/assets/80ee30b3-e30c-4638-ab29-370e9f3db8c9" />

**Transformation. Последовательный вызов трех трансформаций для загрузки данных.**
<img width="1845" height="987" alt="image" src="https://github.com/user-attachments/assets/93b0e4f5-2d58-41e3-ad3e-7e18d18d866b" />

## Шаг 3. Реализация Трансформаций (Transformations)
### Трансформация 1. Load Orders

**Select Values. Установите типы данных (Date format: dd.MM.yyyy для дат, Integer для ID).**
<img width="1543" height="813" alt="image" src="https://github.com/user-attachments/assets/da67bf9c-d5ba-40a3-9a3b-0fdc7f4215f1" />

**Memory Group By. Используется для дедупликации (группировка по row_id, взятие первых значений по остальным полям).**
<img width="1116" height="891" alt="image" src="https://github.com/user-attachments/assets/79948fb0-ef6d-499e-aebb-0342b2ba1984" />

**Filter Rows (Валидация)**
* Условие: order_date IS NOT NULL AND ship_date IS NOT NULL, данные за 1-ый квартал 2016
* TRUE -> Table Output (в таблицу orders).
* FALSE -> Write to Log (логирование ошибок).
<img width="1083" height="724" alt="image" src="https://github.com/user-attachments/assets/d6eb0730-c9d0-4494-8251-56466f40efbe" />

**Value Mapper. Преобразование поля Returned: Yes -> 1, No -> 0, Empty -> 0.**
<img width="918" height="401" alt="image" src="https://github.com/user-attachments/assets/e2716c4b-51d4-4676-8cb4-53f2c09ccb86" />

### Трансформация 2. Load Customers

**Select Values. Оставьте только поля, относящиеся к клиенту (customer_id, name, city и т.д.).**
<img width="1558" height="802" alt="image" src="https://github.com/user-attachments/assets/1e56c5f6-0181-47a7-b1f7-1609e623deec" />

**Memory Group By. Группировка по customer_id (устранение дублей клиентов).**
<img width="1118" height="874" alt="image" src="https://github.com/user-attachments/assets/10b6f329-36d5-4b2a-bc37-541ae6f284bf" />

**Table Output. Загрузка в таблицу customers.**
<img width="1013" height="726" alt="image" src="https://github.com/user-attachments/assets/20e79f19-520b-47f1-b0ea-608d8bc1c181" />

### Трансформация 3. Load Products

**Select Values. Оставьте поля продукта (product_id, category, name и т.д.).**
<img width="1556" height="825" alt="image" src="https://github.com/user-attachments/assets/84179f2f-b952-4fab-9073-1653388bc7bb" />


**Memory Group By. Группировка по product_id.**
<img width="1119" height="871" alt="image" src="https://github.com/user-attachments/assets/3ab8df07-dfc8-488e-b2a7-18c9d4b5b175" />


**Table Output. Загрузка в таблицу products.**
<img width="943" height="719" alt="image" src="https://github.com/user-attachments/assets/b17e94a6-a2d8-45bc-ba1f-9902639b64eb" />

## Шаг 4 Выполнение доп заданий
## Общий вид
<img width="1627" height="560" alt="image" src="https://github.com/user-attachments/assets/3ed60f03-76d3-44b8-b52c-d92a80c73553" />

### Выполнение трансформаци для 1 задания:
<img width="1847" height="638" alt="image" src="https://github.com/user-attachments/assets/05003cba-4376-4277-be64-92e2cf601ecf" />

## Общий вид
<img width="1626" height="895" alt="image" src="https://github.com/user-attachments/assets/2a7d9188-b051-46be-b62e-887cf79f6f61" />
### Выполнение трансформаци для 2 задания:
<img width="1847" height="991" alt="image" src="https://github.com/user-attachments/assets/1d44c6a6-7609-4870-8b1d-9d35ebb89896" />


## Проверка данных

### Для проверки наличия записей использовался такой скрипт: SELECT * FROM orders,customers,products LIMIT 100;
<img width="1677" height="965" alt="image" src="https://github.com/user-attachments/assets/dfb65da0-981f-4849-b348-c106518a3e1a" />

<img width="1683" height="954" alt="image" src="https://github.com/user-attachments/assets/bbd2df50-723b-4364-a871-c0280f832bad" />

<img width="1692" height="976" alt="image" src="https://github.com/user-attachments/assets/e3d9e8d6-243b-4417-b294-6517981b92d3" />


### Для проверки количества записей использовался такой запрос: SELECT COUNT(*) FROM orders,customers,products;
<img width="1679" height="493" alt="image" src="https://github.com/user-attachments/assets/cc101161-62b8-426e-b181-e0c9d20cc6dc" />

<img width="1650" height="482" alt="image" src="https://github.com/user-attachments/assets/f2294ab4-c2e4-4db0-b89c-5d0a7e4287ad" />

<img width="1682" height="404" alt="image" src="https://github.com/user-attachments/assets/f422f629-185e-49c3-8131-7d52642f180b" />

# Файлы

[Файл Job](/Files/Job%20CSV.kjb)

[Файл Transformations orders](/Files/lab_2_1_csv_orders.ktr)

[Файл Transformations products](Files/lab_2_3_csv_to_products.ktr)

[Файл Transformations customers](Files/lab_2_2_csv_to_Customers.ktr)

[Отчет по регионам](Files/zadanie_1.ktr)

[Анализ возвратов](Files/zadanie_2.ktr)
