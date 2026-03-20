# Лабораторная работа №1. Установка и настройка ETL-инструмента. Создание конвейеров данных

**Цель работы.** Изучение основных принципов работы с ETL-инструментами на примере Pentaho Data Integration (PDI), настройка среды, создание конвейера обработки данных (фильтрация, очистка, замена значений) и выгрузка результатов в базу данных MySQL.

# Задание на лабораторную работу
### Общая задача
1.  Выбрать вариант задания из таблицы ниже.
2.  Скачать CSV-датасет (если ссылка Kaggle недоступна — использовать VPN, найти зеркало, сгенерировать синтетические данные или использовать анонимизированные рабочие данные).
3.  Скачать шаблоны конвейеров для примера: [GitHub Repository](https://github.com/BosenkoTM/workshop-on-ETL/tree/main/lectures/L_01).
4.  Создать трансформацию (`.ktr`), реализующую:
    *   **CSV File Input.** Чтение данных.
    *   **Filter Rows / Value Mapper / String Operations.** Очистка данных, фильтрация битых записей, замена значений.
    *   **Table Output.** Загрузка очищенных данных в таблицу MySQL в базе `mgpu_ico_etl_XX`.
5.  Проверить результат SQL-запросом через phpMyAdmin.

### Варианты заданий
| 18 | Цифровая реклама: обработка данных кампаний. | [Digital Ads Dataset](lab_01/DigitalAd_dataset.csv)

# Выполнение лаборатоной работы

## Созданный конвейер в Spoon (общий вид)
<img width="1852" height="976" alt="image" src="https://github.com/user-attachments/assets/d0f9f333-a989-4923-a8d1-4afd28e6a277" />

## Настройки ключевых шагов (Input, Filter, Output)

### Настройка блока Input
<img width="1232" height="736" alt="image" src="https://github.com/user-attachments/assets/dd827724-f2c6-49ab-b767-d5875ce6dccf" />

### Настройка блока SelectRows
<img width="1566" height="809" alt="image" src="https://github.com/user-attachments/assets/52ddea72-51aa-4b16-a244-5e22f0e943ac" />

### Настройка блока Filter
<img width="1090" height="715" alt="image" src="https://github.com/user-attachments/assets/8dd02f39-7bec-41c4-9a90-f92a862a2775" />

### Настройка блока Output
<img width="940" height="712" alt="image" src="https://github.com/user-attachments/assets/b507dd30-694e-43b1-be53-e6b8edc045ab" />


## SQL-запросы, использованные для проверки загрузки данных, и скриншот результата SELECT из phpMyAdmin

### Для проверки наличия записей использовался такой скрипт: SELECT * FROM `digital_ad_customers` LIMIT 100;

<img width="1689" height="962" alt="image" src="https://github.com/user-attachments/assets/a5f39cb8-2a90-40d5-a655-8f86ba16d8ae" />

### Для проверки количества записей использовался такой запрос: SELECT COUNT(*) FROM `digital_ad_customers`;
<img width="1682" height="949" alt="image" src="https://github.com/user-attachments/assets/868e8b75-03b4-4f0e-adcb-deee7a792db7" />


## Файлы:
[Исходный CSV](lab_01/DigitalAd_dataset.csv)

[Файл трансформации](main/lab_01/Lab_01.ktr)

