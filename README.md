# first-yandex

Домашнее задание по Yandex Cloud (Yandex Data Proc).

## Где и как делала
- Выполнялось в Yandex Cloud, кластер Yandex Data Proc.
- Работа велась в Apache Zeppelin (PySpark, %spark.pyspark).

## Суть задания
Собрать витрину `mart_city_top_products`:
- посчитать `revenue = qty * price`
- сделать join `orders` + `users` + `products`
- посчитать метрики `orders_cnt`, `qty_sum`, `revenue_sum` по (city, product_id, product_name)
- выбрать Top-2 товаров по `revenue_sum` в каждом городе с Window
- сохранить результат в Parquet (HDFS и S3) и прочитать обратно

## Сделано сверх задания
- дополнительные проверки: схема (printSchema), количество строк (count),
  проверка Top-2 по городам, explain(True)
- скриншоты ключевых шагов — в папке `screenshots/`

## Артефакты
- `Zeppelin-1_2MJWXP924.zpln` — ноутбук Zeppelin
- `notebook.md` — читаемый код 
- `screenshots/` — подтверждение выполнения
