Для просмотра всех данных используем SQL:
Матричный вид (товары по складам):
sqlSELECT
title as Товар,
warehouse_title as Склад,
residue as Остаток
FROM `remonline-sync.remonline_inventory.warehouse_goods`
ORDER BY title, warehouse_title

Сводка по товарам:
sqlSELECT
title,
COUNT(DISTINCT warehouse*title) as складов,
SUM(residue) as общий*остаток,
ROUND(AVG(residue), 1) as средний*остаток
FROM `remonline-sync.remonline_inventory.warehouse_goods`
GROUP BY title
ORDER BY общий*остаток DESC

1. Поиск по точному названию товара
   SELECT *
   FROM `remonline-sync.remonline_inventory.warehouse_goods_postings`
   WHERE product_title = 'Болт зливу оливи 523500'
   LIMIT 100

2. Поиск по части названия (содержит)
   SELECT *
   FROM `remonline-sync.remonline_inventory.warehouse_goods_postings`
   WHERE LOWER(product_title) LIKE LOWER('%болт%')
   LIMIT 100

3. Поиск по коду или артикулу товара
   SELECT *
   FROM `remonline-sync.remonline_inventory.warehouse_goods_postings`
   WHERE product_code = '523500' OR product_article = '523500'
   LIMIT 100

4. Поиск по ID товара
   SELECT *
   FROM `remonline-sync.remonline_inventory.warehouse_goods_postings`
   WHERE product_id = 51885793
   LIMIT 100

5. Поиск по складу и товару
   SELECT *
   FROM `remonline-sync.remonline_inventory.warehouse_goods_postings`
   WHERE warehouse_id = 2975737
   AND LOWER(product_title) LIKE LOWER('%болт%')
   LIMIT 100

6. Поиск по периоду времени
   SELECT *
   FROM `remonline-sync.remonline_inventory.warehouse_goods_postings`
   WHERE posting_created_at >= '2025-01-01'
   AND posting_created_at <= '2025-12-31'
   AND LOWER(product_title) LIKE LOWER('%болт%')
   LIMIT 100

7. Для больших таблиц - добавьте ORDER BY и LIMIT
   SELECT posting_id, product_title, warehouse_title, amount, posting_created_at
   FROM `remonline-sync.remonline_inventory.warehouse_goods_postings`
   WHERE LOWER(product_title) LIKE LOWER('%ваш*товар%')
   ORDER BY posting_created_at DESC
   LIMIT 50

8. Группировка для статистики
   SELECT
   product_title,
   COUNT(\*) as total_postings,
   SUM(amount) as total_amount,
   MAX(posting_created_at) as last_posting
   FROM `remonline-sync.remonline_inventory.warehouse_goods_postings`
   WHERE LOWER(product_title) LIKE LOWER('%болт%')
   GROUP BY product_title
   ORDER BY total_amount DESC
   LIMIT 20



