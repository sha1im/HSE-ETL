# Задание 1. Работа с Big Data в Yandex Data Processing

## Цель работы

Целью данной работы являлось практическое знакомство с обработкой Big Data с использованием Apache Spark и Hadoop в облачной инфраструктуре Yandex Cloud.

В рамках задания необходимо было:

* развернуть кластер Hadoop/Spark;
* загрузить данные для обработки;
* выполнить трансформацию вложенного JSON;
* записать результат в формате Parquet;
* проверить корректность записи и чтения данных.

## Развертывание инфраструктуры

Для выполнения задания был развернут кластер Yandex Data Processing.

В кластер были включены:

* Apache Hadoop (HDFS);
* Apache Spark;
* Apache Zeppelin.

Работа с PySpark выполнялась через веб-интерфейс Apache Zeppelin (`Zeppelin Web UI`).

Дополнительно был создан:

* Yandex Object Storage bucket;
* Service Account;
* Static Access Key для доступа Spark к Object Storage через S3 API.

## Создание исходного JSON-файла

Для демонстрации ETL-процесса был создан тестовый JSON-файл со вложенной структурой.

JSON содержал:

* массив `catalogs`;
* массив `offers`;
* служебное поле `version`.

После создания JSON-файл был записан в HDFS.

### Код ячейки

```python
%spark.pyspark

import json

data = {
    "catalogs": [
        {
            "id": "1234",
            "date_start": "2020-06-05",
            "date_end": "2020-06-12",
            "is_main": True,
            "image": "https://retailer1234.ru/catalogs/1234.jpg",
            "conditions": "Предложения действительны для Москвы и области",
            "offers": ["11111", "22222", "33333"],
            "target_regions": [
                "Россия, Москва",
                "Россия, Ярославская область",
                "Россия, Костромская область"
            ]
        },
        {
            "id": "5678",
            "date_start": "2020-06-05",
            "date_end": "2020-06-12",
            "is_main": True,
            "image": "https://retailer1234.ru/catalogs/5678.jpg",
            "conditions": "Предложения действительны в магазине по адресу: Владимир, улица Куйбышева, 26К",
            "offers": ["22222", "33333"],
            "target_shops": [
                "Владимир, улица Куйбышева, 26К"
            ]
        }
    ],
    "offers": [
        {
            "id": "11111",
            "barcode": "7501031311369",
            "description": "Молоко Домик в деревне 3,2% 0,93 л",
            "discount_label": "1+1",
            "image": "https://retailer1234.ru/offers/1234567.jpg",
            "price_is_from": False,
            "price_new": 50,
            "price_old": 100,
            "date_start": "2020-06-05",
            "date_end": "2020-06-10"
        },
        {
            "id": "22222",
            "barcode": "3113097501031",
            "description": "Огурцы маринованные Дядя Ваня 680 г",
            "image": "https://retailer1234.ru/offers/5671234.jpg",
            "price_is_from": False,
            "price_new": 70,
            "price_old": 75
        },
        {
            "id": "33333",
            "barcode": "1097501031133",
            "description": "Жевательная резинка Orbit в ассортименте",
            "image": "https://retailer1234.ru/offers/10486.jpg",
            "price_is_from": True,
            "price_new": 10
        }
    ],
    "version": 2
}

local_path = "/tmp/example.json"
hdfs_path = "hdfs:///tmp/etl_bigdata/example.json"

with open(local_path, "w", encoding="utf-8") as file:
    json.dump(data, file, ensure_ascii=False, indent=2)

spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
    .get(spark.sparkContext._jsc.hadoopConfiguration()) \
    .copyFromLocalFile(
        False,
        True,
        spark.sparkContext._jvm.org.apache.hadoop.fs.Path(local_path),
        spark.sparkContext._jvm.org.apache.hadoop.fs.Path(hdfs_path)
    )

print(f"JSON file successfully written to: {hdfs_path}")
```

### Вывод ячейки

```text
JSON file successfully written to: hdfs:///tmp/etl_bigdata/example.json
```

## Чтение JSON через Spark

После этого JSON-файл был прочитан с помощью Spark.

### Код ячейки

```python
%spark.pyspark

df = (
    spark.read
    .option("multiline", "true")
    .json(hdfs_path)
)

df.printSchema()
df.show(truncate=False)
```

### Вывод ячейки

```text
root
 |-- catalogs: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- conditions: string (nullable = true)
 |    |    |-- date_end: string (nullable = true)
 |    |    |-- date_start: string (nullable = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- image: string (nullable = true)
 |    |    |-- is_main: boolean (nullable = true)
 |    |    |-- offers: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- target_regions: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- target_shops: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |-- offers: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- barcode: string (nullable = true)
 |    |    |-- date_end: string (nullable = true)
 |    |    |-- date_start: string (nullable = true)
 |    |    |-- description: string (nullable = true)
 |    |    |-- discount_label: string (nullable = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- image: string (nullable = true)
 |    |    |-- price_is_from: boolean (nullable = true)
 |    |    |-- price_new: long (nullable = true)
 |    |    |-- price_old: long (nullable = true)
 |-- version: long (nullable = true)

+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------+
|catalogs                                                                                                                                                                                                                                                                                                                                                                                                                                                           |offers                                                                                                                                                                                                                                                                                                                                                                                                                                       |version|
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------+
|[{Предложения действительны для Москвы и области, 2020-06-12, 2020-06-05, 1234, https://retailer1234.ru/catalogs/1234.jpg, true, [11111, 22222, 33333], [Россия, Москва, Россия, Ярославская область, Россия, Костромская область], null}, {Предложения действительны в магазине по адресу: Владимир, улица Куйбышева, 26К, 2020-06-12, 2020-06-05, 5678, https://retailer1234.ru/catalogs/5678.jpg, true, [22222, 33333], null, [Владимир, улица Куйбышева, 26К]}]|[{7501031311369, 2020-06-10, 2020-06-05, Молоко Домик в деревне 3,2% 0,93 л, 1+1, 11111, https://retailer1234.ru/offers/1234567.jpg, false, 50, 100}, {3113097501031, null, null, Огурцы маринованные Дядя Ваня 680 г, null, 22222, https://retailer1234.ru/offers/5671234.jpg, false, 70, 75}, {1097501031133, null, null, Жевательная резинка Orbit в ассортименте, null, 33333, https://retailer1234.ru/offers/10486.jpg, true, 10, null}]|2      |
+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------+

```

## Трансформация структуры catalogs

Для преобразования вложенной структуры использовалась функция `explode`.

### Код ячейки

```python
%spark.pyspark

from pyspark.sql import functions as F

explode_catalogs = df.select(F.explode("catalogs"))

catalogs = explode_catalogs.select("col.*")

catalogs.printSchema()
catalogs.show(truncate=False)
```

### Вывод ячейки

```text
root
 |-- conditions: string (nullable = true)
 |-- date_end: string (nullable = true)
 |-- date_start: string (nullable = true)
 |-- id: string (nullable = true)
 |-- image: string (nullable = true)
 |-- is_main: boolean (nullable = true)
 |-- offers: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- target_regions: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- target_shops: array (nullable = true)
 |    |-- element: string (containsNull = true)

+------------------------------------------------------------------------------+----------+----------+----+-----------------------------------------+-------+---------------------+--------------------------------------------------------------------------+--------------------------------+
|conditions                                                                    |date_end  |date_start|id  |image                                    |is_main|offers               |target_regions                                                            |target_shops                    |
+------------------------------------------------------------------------------+----------+----------+----+-----------------------------------------+-------+---------------------+--------------------------------------------------------------------------+--------------------------------+
|Предложения действительны для Москвы и области                                |2020-06-12|2020-06-05|1234|https://retailer1234.ru/catalogs/1234.jpg|true   |[11111, 22222, 33333]|[Россия, Москва, Россия, Ярославская область, Россия, Костромская область]|null                            |
|Предложения действительны в магазине по адресу: Владимир, улица Куйбышева, 26К|2020-06-12|2020-06-05|5678|https://retailer1234.ru/catalogs/5678.jpg|true   |[22222, 33333]       |null                                                                      |[Владимир, улица Куйбышева, 26К]|
+------------------------------------------------------------------------------+----------+----------+----+-----------------------------------------+-------+---------------------+--------------------------------------------------------------------------+--------------------------------+
```

## Трансформация структуры offers

Аналогичным образом была обработана структура `offers`.

### Код ячейки

```python
%spark.pyspark

explode_offers = df.select(F.explode("offers"))

offers = explode_offers.select("col.*")

offers.printSchema()
offers.show(truncate=False)
```

### Вывод ячейки

```text
root
 |-- barcode: string (nullable = true)
 |-- date_end: string (nullable = true)
 |-- date_start: string (nullable = true)
 |-- description: string (nullable = true)
 |-- discount_label: string (nullable = true)
 |-- id: string (nullable = true)
 |-- image: string (nullable = true)
 |-- price_is_from: boolean (nullable = true)
 |-- price_new: long (nullable = true)
 |-- price_old: long (nullable = true)

+-------------+----------+----------+----------------------------------------+--------------+-----+------------------------------------------+-------------+---------+---------+
|barcode      |date_end  |date_start|description                             |discount_label|id   |image                                     |price_is_from|price_new|price_old|
+-------------+----------+----------+----------------------------------------+--------------+-----+------------------------------------------+-------------+---------+---------+
|7501031311369|2020-06-10|2020-06-05|Молоко Домик в деревне 3,2% 0,93 л      |1+1           |11111|https://retailer1234.ru/offers/1234567.jpg|false        |50       |100      |
|3113097501031|null      |null      |Огурцы маринованные Дядя Ваня 680 г     |null          |22222|https://retailer1234.ru/offers/5671234.jpg|false        |70       |75       |
|1097501031133|null      |null      |Жевательная резинка Orbit в ассортименте|null          |33333|https://retailer1234.ru/offers/10486.jpg  |true         |10       |null     |
+-------------+----------+----------+----------------------------------------+--------------+-----+------------------------------------------+-------------+---------+---------+
```

## Запись данных в HDFS в формате Parquet

Полученные DataFrame были записаны в HDFS в формате Parquet.

### Код ячейки

```python
%spark.pyspark

catalogs_hdfs_path = "hdfs:///tmp/etl_bigdata/catalogs_parquet/"
offers_hdfs_path = "hdfs:///tmp/etl_bigdata/offers_parquet/"

(
    catalogs
    .write
    .mode("overwrite")
    .parquet(catalogs_hdfs_path)
)

(
    offers
    .write
    .mode("overwrite")
    .parquet(offers_hdfs_path)
)

print(f"Catalogs written to: {catalogs_hdfs_path}")
print(f"Offers written to: {offers_hdfs_path}")
```

### Вывод ячейки

```text
Catalogs written to: hdfs:///tmp/etl_bigdata/catalogs_parquet/
Offers written to: hdfs:///tmp/etl_bigdata/offers_parquet/
```

## Проверка чтения данных из HDFS

После записи parquet-файлы были повторно прочитаны из HDFS.

### Код ячейки

```python
%spark.pyspark

catalogs_from_hdfs = spark.read.parquet(catalogs_hdfs_path)
offers_from_hdfs = spark.read.parquet(offers_hdfs_path)

print("Catalogs from HDFS:")
catalogs_from_hdfs.show(truncate=False)

print("Offers from HDFS:")
offers_from_hdfs.show(truncate=False)
```

Все данные были успешно прочитаны.

## Настройка подключения к Object Storage

Для интеграции Spark с Yandex Object Storage был настроен доступ через S3 API.

### Код ячейки

```python
%spark.pyspark

spark.conf.set("spark.hadoop.fs.s3a.endpoint", "storage.yandexcloud.net")
spark.conf.set("spark.hadoop.fs.s3a.access.key", "<ACCESS_KEY_ID>")
spark.conf.set("spark.hadoop.fs.s3a.secret.key", "<SECRET_ACCESS_KEY>")
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

print("S3/Object Storage config is set")
```

### Вывод ячейки

```text
S3/Object Storage config is set
```

## Запись данных в Object Storage

После настройки доступа данные были записаны в Yandex Object Storage.

### Код ячейки

```python
%spark.pyspark

bucket_name = "<YOUR_BUCKET_NAME>"

catalogs_s3_path = f"s3a://{bucket_name}/etl_bigdata/catalogs_parquet/"
offers_s3_path = f"s3a://{bucket_name}/etl_bigdata/offers_parquet/"

(
    catalogs
    .write
    .mode("overwrite")
    .parquet(catalogs_s3_path)
)

(
    offers
    .write
    .mode("overwrite")
    .parquet(offers_s3_path)
)

print(f"Catalogs written to: {catalogs_s3_path}")
print(f"Offers written to: {offers_s3_path}")
```

### Вывод ячейки

```text
Catalogs written to: s3a://hse-seminar-backet/etl_bigdata/catalogs_parquet/
Offers written to: s3a://hse-seminar-backet/etl_bigdata/offers_parquet/
```

## Проверка чтения данных из Object Storage

После записи parquet-файлы были повторно прочитаны из Object Storage.

### Код ячейки

```python
%spark.pyspark

catalogs_from_s3 = spark.read.parquet(catalogs_s3_path)
offers_from_s3 = spark.read.parquet(offers_s3_path)

print("Catalogs from S3/Object Storage:")
catalogs_from_s3.show(truncate=False)

print("Offers from S3/Object Storage:")
offers_from_s3.show(truncate=False)
```

### Вывод ячейки

```text
Catalogs from S3/Object Storage:
+------------------------------------------------------------------------------+----------+----------+----+-----------------------------------------+-------+---------------------+--------------------------------------------------------------------------+--------------------------------+
|conditions                                                                    |date_end  |date_start|id  |image                                    |is_main|offers               |target_regions                                                            |target_shops                    |
+------------------------------------------------------------------------------+----------+----------+----+-----------------------------------------+-------+---------------------+--------------------------------------------------------------------------+--------------------------------+
|Предложения действительны для Москвы и области                                |2020-06-12|2020-06-05|1234|https://retailer1234.ru/catalogs/1234.jpg|true   |[11111, 22222, 33333]|[Россия, Москва, Россия, Ярославская область, Россия, Костромская область]|null                            |
|Предложения действительны в магазине по адресу: Владимир, улица Куйбышева, 26К|2020-06-12|2020-06-05|5678|https://retailer1234.ru/catalogs/5678.jpg|true   |[22222, 33333]       |null                                                                      |[Владимир, улица Куйбышева, 26К]|
+------------------------------------------------------------------------------+----------+----------+----+-----------------------------------------+-------+---------------------+--------------------------------------------------------------------------+--------------------------------+

Offers from S3/Object Storage:
+-------------+----------+----------+----------------------------------------+--------------+-----+------------------------------------------+-------------+---------+---------+
|barcode      |date_end  |date_start|description                             |discount_label|id   |image                                     |price_is_from|price_new|price_old|
+-------------+----------+----------+----------------------------------------+--------------+-----+------------------------------------------+-------------+---------+---------+
|7501031311369|2020-06-10|2020-06-05|Молоко Домик в деревне 3,2% 0,93 л      |1+1           |11111|https://retailer1234.ru/offers/1234567.jpg|false        |50       |100      |
|3113097501031|null      |null      |Огурцы маринованные Дядя Ваня 680 г     |null          |22222|https://retailer1234.ru/offers/5671234.jpg|false        |70       |75       |
|1097501031133|null      |null      |Жевательная резинка Orbit в ассортименте|null          |33333|https://retailer1234.ru/offers/10486.jpg  |true         |10       |null     |
+-------------+----------+----------+----------------------------------------+--------------+-----+------------------------------------------+-------------+---------+---------+
```

## Итог

В рамках задания был реализован ETL-процесс обработки JSON данных с использованием Apache Spark.

Были выполнены:

* чтение JSON;
* обработка вложенных структур;
* трансформация данных;
* запись в Parquet;
* работа с HDFS;
* работа с Yandex Object Storage через S3 API.

Также была продемонстрирована интеграция:

* Apache Spark;
* Apache Hadoop (HDFS);
* Apache Zeppelin;
* Yandex Object Storage;
* Yandex Data Processing.

В папке first exercise можно найти исходные ячейки в форматах .ipynb и .zpln.

# Задание 2. NoSQL в ETL-процессах: Kafka → Data Transfer → Yandex StoreDoc

## Цель работы

Целью второго задания было повторить демонстрацию из вебинара «NoSQL в ETL-процессах» и настроить поставку данных в реальном времени из топика Apache Kafka в кластер Yandex StoreDoc с помощью Yandex Data Transfer.

В рамках задания была реализована следующая схема:

```text
Managed Service for Apache Kafka → Yandex Data Transfer → Yandex StoreDoc
```

Kafka использовалась как источник потоковых сообщений, Yandex Data Transfer — как сервис переноса данных, а Yandex StoreDoc — как NoSQL-хранилище для итоговых документов.

## Создание инфраструктуры

Для выполнения задания были созданы следующие ресурсы:

* кластер Yandex Managed Service for Apache Kafka;
* топик Kafka с именем `sensors`;
* пользователь Kafka `mkf-user` с правами producer и consumer;
* кластер Yandex StoreDoc;
* база данных `db1`;
* пользователь StoreDoc `mmg-user`;
* виртуальная машина Ubuntu для отправки тестовых сообщений в Kafka;
* два endpoint'а в Yandex Data Transfer;
* трансфер типа «Репликация».

Также были настроены группы безопасности для доступа к Kafka и StoreDoc по необходимым портам.

## Подготовка утилит на виртуальной машине

Для отправки тестовых сообщений в Kafka на виртуальной машине были установлены утилиты:

```bash
sudo apt update && sudo apt install --yes kafkacat
sudo apt update && sudo apt-get install --yes jq
```

Так как в используемой версии Ubuntu команда `kafkacat` доступна как `kcat`, дальнейшая отправка сообщений выполнялась через `kcat`.

Дополнительно был загружен SSL-сертификат Yandex Cloud для подключения к Kafka по защищённому соединению:

```bash
mkdir -p ~/.kafka

wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
     -O ~/.kafka/YandexInternalRootCA.crt
```

## Проверка подключения к Kafka

Перед настройкой трансфера было проверено подключение к Kafka-кластеру через SSL.

Для проверки использовался порт `9091`, так как подключение выполнялось по `SASL_SSL`.

Пример проверки доступности брокера:

```bash
nc -vz <FQDN хоста или CNAME брокера> 9091
```

После настройки группы безопасности подключение к брокеру Kafka стало успешным.

## Подготовка тестовых данных

На виртуальной машине был создан файл `sample.json` с тестовыми данными сенсоров автомобиля.

Пример данных:

```json
{
    "device_id": "iv9a94th6rzt********",
    "datetime": "2020-06-05 17:27:00",
    "latitude": 55.70329032,
    "longitude": 37.65472196,
    "altitude": 427.5,
    "speed": 0,
    "battery_voltage": 23.5,
    "cabin_temperature": 17,
    "fuel_level": null
}
{
    "device_id": "rhibbh3y08qm********",
    "datetime": "2020-06-06 09:49:54",
    "latitude": 55.71294467,
    "longitude": 37.66542005,
    "altitude": 429.13,
    "speed": 55.5,
    "battery_voltage": null,
    "cabin_temperature": 18,
    "fuel_level": 32
}
{
    "device_id": "iv9a94th6rzt********",
    "datetime": "2020-06-07 15:00:10",
    "latitude": 55.70985913,
    "longitude": 37.62141918,
    "altitude": 417.0,
    "speed": 15.7,
    "battery_voltage": 10.3,
    "cabin_temperature": 17,
    "fuel_level": null
}
```

Файл содержит три отдельных JSON-сообщения, каждое из которых далее отправлялось в Kafka topic `sensors`.

## Создание endpoint'а-источника Kafka

В Yandex Data Transfer был создан endpoint-источник для Kafka.

Основные параметры endpoint'а:

* тип источника: `Kafka`;
* тип подключения: кластер Managed Service for Apache Kafka;
* кластер: созданный Kafka-кластер;
* аутентификация: `SASL`;
* пользователь: `mkf-user`;
* топик: `sensors`;
* правила конвертации: `JSON`;
* схема данных: список полей.

В схеме были указаны поля:

* `device_id`;
* `datetime`;
* `latitude`;
* `longitude`;
* `altitude`;
* `speed`;
* `battery_voltage`;
* `cabin_temperature`;
* `fuel_level`.

Поле `device_id` было задано как ключевое.

## Создание endpoint'а-приёмника StoreDoc

Также был создан endpoint-приёмник для Yandex StoreDoc.

Основные параметры endpoint'а:

* тип приёмника: `MongoDB/Yandex StoreDoc`;
* тип инсталляции: кластер Yandex StoreDoc;
* кластер: созданный StoreDoc-кластер;
* источник аутентификации: `db1`;
* база данных: `db1`;
* пользователь: `mmg-user`;
* политика очистки: `Drop`.

После настройки endpoint'ов был создан transfer типа «Репликация».

## Создание и запуск трансфера

В Yandex Data Transfer был создан трансфер:

```text
Kafka topic sensors → Yandex StoreDoc database db1
```

После активации трансфер перешёл в статус:

```text
Реплицируется
```

Это означает, что Data Transfer начал слушать сообщения из Kafka topic `sensors` и переносить их в Yandex StoreDoc.

## Отправка данных в Kafka

Данные из файла `sample.json` были отправлены в Kafka topic `sensors` с помощью `jq` и `kcat`.

Команда отправки:

```bash
jq -rc . sample.json | kcat -P \
  -b <CNAME брокера>>:9091 \
  -t sensors \
  -k key \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanisms=SCRAM-SHA-512 \
  -X sasl.username="mkf-user" \
  -X sasl.password="<PASSWORD>" \
  -X ssl.ca.location=/home/etl-homework/.kafka/YandexInternalRootCA.crt
```

После отправки сообщения были считаны трансфером и записаны в StoreDoc.

## Проверка результата в Yandex StoreDoc

Для проверки результата использовался WebSQL.

До отправки данных база `db1` была пустой.

После успешной работы трансфера в базе появилась коллекция:

```text
sensors
```

Для проверки был выполнен запрос:

```javascript
db.sensors.find()
```

В результате в коллекции `sensors` были найдены 3 документа, соответствующие трём JSON-сообщениям из файла `sample.json`.

В документах присутствуют как исходные поля:

* `device_id`;
* `datetime`;
* `latitude`;
* `longitude`;
* `altitude`;
* `speed`;
* `battery_voltage`;
* `cabin_temperature`;
* `fuel_level`;

так и служебные поля, добавленные Data Transfer:

* `_timestamp`;
* `_partition`;
* `_offset`;
* `_idx`.

---

## Итог

В рамках второго задания была настроена потоковая поставка данных из Kafka в NoSQL-хранилище Yandex StoreDoc.

Были выполнены следующие действия:

* создан Kafka-кластер;
* создан топик `sensors`;
* создан пользователь Kafka;
* создан кластер Yandex StoreDoc;
* создана база данных `db1`;
* подготовлены тестовые JSON-сообщения;
* создан endpoint-источник Kafka;
* создан endpoint-приёмник StoreDoc;
* создан и активирован трансфер в Yandex Data Transfer;
* данные были отправлены в Kafka;
* данные были успешно перенесены в StoreDoc;
* результат был проверен через WebSQL запросом `db.sensors.find()`.

Таким образом, был реализован ETL-процесс потоковой поставки данных:

```text
Kafka → Data Transfer → Yandex StoreDoc
```

Скринщоты подтверждающие успешное выполнение второго задания можно найти в папке second exercise.