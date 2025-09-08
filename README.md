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
