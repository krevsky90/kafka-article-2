info: https://habr.com/ru/articles/961048/

todo:
1) add flyway instead of running kafka-article-2-krev\inventory-service\src\main\resources\inventory.sql manually
2) add grafana to monitor kafka resources, offset lags etc
? 3) прикрутить Node Exporter Full dashboard к order-service
4) выяснить, какие метрики обычно требуются для спрингбут приложений, какие под это уже есть дашборды, и какие вывод можно сделать из этих метрик
5) добавить панель с моей кастомной метрикой-counter-ом
6) симитировать нагрузочное тестирование
7) научиться писать запросы к прометеусу
    типа label_values(jvm_memory_used_bytes, instance)