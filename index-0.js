function displaySeparatedProductHistory(
  data,
  productTitle,
  warehouseTitle,
  filterWarehouseId = null
) {
  console.log("🎬 displaySeparatedProductHistory:", {
    productTitle,
    warehouseTitle,
    filterWarehouseId,
  });

  console.log("📊 Вхідні дані:", {
    postings: data.postings?.length || 0,
    moves: data.moves?.length || 0,
    outcomes: data.outcomes?.length || 0,
    sales: data.sales?.length || 0,
    goodsFlow: data.data?.length || 0,
  });

  const allOperations = [];

  // ============================================
  // 1. ЗБІР ОПЕРАЦІЙ
  // ============================================

  // Оприбуткування
  if (data.postings) {
    console.log(`📦 Обробка ${data.postings.length} оприбуткувань`);
    data.postings.forEach((item) => {
      allOperations.push({
        date: new Date(
          item.posting_created_at?.value || item.posting_created_at
        ),
        type: "posting",
        typeName: "📦 Оприбуткування",
        typeColor: "#059669",
        label: item.posting_label || "-",
        user: item.created_by_name || "-",
        warehouse: item.warehouse_title || warehouseTitle,
        warehouseId: item.warehouse_id || null,
        amount: item.amount || 0,
        description: item.posting_description || "-",
        delta: +item.amount,
      });
    });
  }

  // Переміщення
  if (data.moves) {
    console.log(`🔄 Обробка ${data.moves.length} переміщень`);
    data.moves.forEach((item, idx) => {
      const date = new Date(
        item.move_created_at?.value || item.move_created_at
      );

      // Очищаємо назви від префіксів
      let sourceTitle = item.source_warehouse_title || "";
      let targetTitle = item.target_warehouse_title || "";

      if (sourceTitle.includes(" > ")) {
        sourceTitle = sourceTitle.split(" > ")[1].trim();
      }
      if (targetTitle.includes(" > ")) {
        targetTitle = targetTitle.split(" > ")[1].trim();
      }

      if (idx === 0) {
        console.log("Перше переміщення:", {
          sourceTitle,
          targetTitle,
          warehouseTitle,
          source_match: sourceTitle.includes(warehouseTitle),
          target_match: targetTitle.includes(warehouseTitle),
        });
      }

      // Вихід зі складу
      if (sourceTitle.includes(warehouseTitle)) {
        allOperations.push({
          date,
          type: "move_out",
          typeName: "➡️ Переміщення (вихід)",
          typeColor: "#f59e0b",
          label: item.move_label || "-",
          user: item.created_by_name || "-",
          warehouse: `${sourceTitle} → ${targetTitle}`,
          warehouseId: null,
          sourceWarehouseTitle: sourceTitle,
          targetWarehouseTitle: targetTitle,
          amount: item.amount || 0,
          description: item.move_description || "-",
          delta: -(item.amount || 0),
        });
      }

      // Вхід на склад
      if (targetTitle.includes(warehouseTitle)) {
        allOperations.push({
          date,
          type: "move_in",
          typeName: "⬅️ Переміщення (вхід)",
          typeColor: "#3b82f6",
          label: item.move_label || "-",
          user: item.created_by_name || "-",
          warehouse: `${sourceTitle} → ${targetTitle}`,
          warehouseId: filterWarehouseId,
          sourceWarehouseTitle: sourceTitle,
          targetWarehouseTitle: targetTitle,
          amount: item.amount || 0,
          description: item.move_description || "-",
          delta: +(item.amount || 0),
        });
      }
    });
  }

  // Списання
  if (data.outcomes) {
    console.log(`🗑️ Обробка ${data.outcomes.length} списань`);
    data.outcomes.forEach((item) => {
      let outcomeWarehouse = item.source_warehouse_title || warehouseTitle;

      if (outcomeWarehouse.includes(" > ")) {
        outcomeWarehouse = outcomeWarehouse.split(" > ")[1].trim();
      }

      allOperations.push({
        date: new Date(
          item.outcome_created_at?.value || item.outcome_created_at
        ),
        type: "outcome",
        typeName: "🗑️ Списання",
        typeColor: "#ef4444",
        label: item.outcome_label || "-",
        user: item.created_by_name || "-",
        counterparty: outcomeWarehouse,
        warehouse: outcomeWarehouse,
        warehouseId: item.warehouse_id || null,
        amount: item.amount || 0,
        description: item.outcome_description || "-",
        delta: -(item.amount || 0),
      });
    });
  }

  // Продажі
  if (data.sales) {
    console.log(`💰 Обробка ${data.sales.length} продажів`);
    data.sales.forEach((item) => {
      allOperations.push({
        date: new Date(item.sale_created_at?.value || item.sale_created_at),
        type: "sale",
        typeName: "💰 Продаж",
        typeColor: "#8b5cf6",
        label: item.sale_label || "-",
        user: item.created_by_name || "-",
        warehouse: warehouseTitle,
        warehouseId: item.warehouse_id || null,
        amount: item.amount || 0,
        description: item.sale_description || "-",
        delta: -(item.amount || 0),
      });
    });
  }

  // Goods-flow (замовлення та повернення)
  if (data.data && Array.isArray(data.data)) {
    console.log(`🛒 Обробка ${data.data.length} goods-flow операцій`);
    data.data.forEach((item, idx) => {
      const typeInfo = item.type_info || { name: "Невідомо", color: "#6b7280" };

      const itemAmount = item.amount || item.quantity || 1;
      let delta = 0;
      let icon = "📋";
      let typeName = "Невідомо";

      switch (item.relation_type) {
        case 0: // Замовлення
          delta = -itemAmount;
          icon = "🛒";
          typeName = "Замовлення";
          break;
        case 7: // Повернення
          delta = +itemAmount;
          icon = "↩️";
          typeName = "Повернення";
          break;
        case 2: // Інше
          delta = 0;
          icon = "📋";
          typeName = typeInfo.name || "Інше";
          break;
        case 1: // Продаж (є в sales)
        case 3: // Оприбуткування (є в postings)
        case 4: // Списання (є в outcomes)
        case 5: // Переміщення (є в moves)
          return; // Пропускаємо
        default:
          delta = 0;
          icon = "📋";
          typeName = typeInfo.name || "Невідомо";
      }

      if (idx === 0) {
        console.log("📋 Перший goods-flow запис:", {
          employee_id: item.employee_id,
          relation_id_label: item.relation_id_label,
          amount: item.amount,
          quantity: item.quantity,
          itemAmount: itemAmount,
          delta: delta,
        });
      }

      allOperations.push({
        date: new Date(item.created_at?.value || item.created_at),
        type: `goods_flow_${item.relation_type}`,
        typeName: `${icon} ${typeName}`,
        typeColor: typeInfo.color || "#6b7280",
        label: item.relation_id_label || item.relation_label || item.id || "-",
        user: item.employee_id || "-",
        counterparty: "-",
        warehouseId: null,
        amount: Math.abs(itemAmount),
        description: item.comment || item.relation_label || "-",
        delta: delta,
      });
    });
  }

  console.log(`📊 Всього операцій зібрано: ${allOperations.length}`);

  // ============================================
  // 2. ФІЛЬТРАЦІЯ ПО СКЛАДУ
  // ============================================

  let filteredOperations = allOperations;

  if (filterWarehouseId) {
    console.log("🔍 Починаємо фільтрацію по складу:", {
      filterWarehouseId,
      warehouseTitle,
      totalOperations: allOperations.length,
    });

    // ✅ ДОДАЙ ДЕТАЛЬНИЙ ЛОГ КОЖНОЇ ОПЕРАЦІЇ
    console.log("📋 Операції ДО фільтрації:");
    allOperations.forEach((op, i) => {
      console.log(
        `  ${i + 1}. ${op.type} | warehouseId: ${
          op.warehouseId
        } | counterparty: ${op.counterparty || op.warehouse}`
      );
    });

    filteredOperations = allOperations.filter((op, index) => {
      // Переміщення - вже відфільтровані
      if (op.type === "move_in" || op.type === "move_out") {
        console.log(`✅ ${index + 1}. Залишаємо ${op.type}`);
        return true;
      }

      // Операції з warehouseId
      if (op.warehouseId !== null && op.warehouseId !== undefined) {
        const match = op.warehouseId == filterWarehouseId;
        console.log(
          `${match ? "✅" : "❌"} ${index + 1}. ${op.type} | warehouseId: ${
            op.warehouseId
          } ${match ? "==" : "!="} ${filterWarehouseId}`
        );
        return match;
      }

      // Goods-flow
      if (op.type && op.type.startsWith("goods_flow_")) {
        console.log(`✅ ${index + 1}. Залишаємо ${op.type}`);
        return true;
      }

      // Інші - по назві складу
      const match = op.counterparty && op.counterparty.includes(warehouseTitle);
      console.log(
        `${match ? "✅" : "❌"} ${index + 1}. ${op.type} | counterparty: ${
          op.counterparty
        } ${match ? "містить" : "НЕ містить"} ${warehouseTitle}`
      );
      return match;
    });

    console.log(
      `🔍 Фільтрація завершена: ${allOperations.length} → ${filteredOperations.length} операцій`
    );

    console.log("📋 Операції ПІСЛЯ фільтрації:");
    filteredOperations.forEach((op, i) => {
      console.log(
        `  ${i + 1}. ${op.typeName} | ${op.amount} шт | delta: ${op.delta}`
      );
    });
  }

  // ============================================
  // 3. РОЗРАХУНОК ЗАЛИШКІВ ТА ПОБУДОВА ТАБЛИЦІ
  // ============================================

  // ✅ Поточний залишок із BigQuery (останній відомий стан)
  const currentBalance = data.currentBalances?.[warehouseTitle] || 0;

  // Обчислюємо суму всіх змін (дельт) за всіма операціями
  const totalDelta = (
    operationsSorted ||
    operations ||
    allOperations ||
    []
  ).reduce((sum, op) => sum + (op.delta || 0), 0);

  // Початковий залишок = Поточний - сума всіх змін
  let initialBalance = currentBalance - totalDelta;

  // Стартуємо розрахунок з початкового залишку
  let runningBalance = initialBalance;

  console.log(`📊 Поточний залишок з data.currentBalances: ${currentBalance}`);
  console.log(`📊 Всі балансі:`, data.currentBalances);

  console.log("📊 Розрахунок залишків:");
  console.log("   Поточний баланс:", currentBalance);
  console.log("   Сума всіх змін:", totalDelta);
  console.log("   Обчислений початковий залишок:", initialBalance);
  console.log("📊 Початок побудови таблиці: runningBalance =", runningBalance);

  filteredOperations.forEach((op) => {
    initialBalance -= op.delta;
  });

  console.log(
    `📊 Розрахунок: Поточний ${currentBalance} - сума змін = ${initialBalance}`
  );
  console.log(
    `📊 Перевірка: початковий ${initialBalance} + зміни має дорівнювати ${currentBalance}`
  );

  // ✅ ВИПРАВЛЕНО: Сортуємо операції за датою
  const sortedOps = [...filteredOperations].sort((a, b) => a.date - b.date);

  console.log("📅 Сортовані операції (від старих до нових):");
  sortedOps.forEach((op, i) => {
    console.log(
      `  ${i + 1}. ${op.date.toLocaleDateString()} ${op.typeName} | delta: ${
        op.delta
      }`
    );
  });

  // Будуємо таблицю
  let tableRows = "";

  console.log(
    `📊 Початок побудови таблиці: runningBalance = ${runningBalance}`
  );

  sortedOps.forEach((op, idx) => {
    runningBalance += op.delta;

    if (idx < 3) {
      console.log(
        `  Крок ${idx + 1}: було ${runningBalance - op.delta}, зміна ${
          op.delta
        }, стало ${runningBalance}`
      );
    }

    const formattedDate = op.date.toLocaleString("uk-UA", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
    });

    const plusAmount = op.delta > 0 ? op.amount : "";
    const minusAmount = op.delta < 0 ? op.amount : "";

    // Додаємо рядок ЗВЕРХУ (щоб нові операції були вгорі)
    tableRows =
      `
        <tr style="border-bottom: 1px solid #e5e7eb;">
            <td style="padding: 12px 8px; white-space: nowrap;">${formattedDate}</td>
            <td style="padding: 12px 8px;">
                <span style="background: ${op.typeColor}; color: white; padding: 4px 8px; border-radius: 4px; font-size: 0.85rem; white-space: nowrap;">
                    ${op.typeName}
                </span>
            </td>
            <td style="padding: 12px 8px;">${op.label}</td>
            <td style="padding: 12px 8px;">${op.user}</td>
            <td style="padding: 12px 8px; font-size: 0.9rem; color: #6b7280;">${op.counterparty}</td>
            <td style="padding: 12px 8px; text-align: right; color: #059669; font-weight: 600;">${plusAmount}</td>
            <td style="padding: 12px 8px; text-align: right; color: #dc2626; font-weight: 600;">${minusAmount}</td>
            <td style="padding: 12px 8px; text-align: right; font-weight: 600;">${runningBalance}</td>
            <td style="padding: 12px 8px; font-size: 0.85rem; color: #6b7280;">${op.description}</td>
        </tr>
    ` + tableRows;
  });

  console.log(
    `✅ Таблиця побудована: ${sortedOps.length} рядків, кінцевий баланс: ${runningBalance}, очікуваний: ${currentBalance}`
  );

  // ✅ ПЕРЕВІРКА
  if (Math.abs(runningBalance - currentBalance) > 0.01) {
    console.error(
      `❌ ПОМИЛКА: Кінцевий баланс ${runningBalance} не дорівнює поточному ${currentBalance}!`
    );
  } else {
    console.log(`✅ Перевірка пройдена: балансі співпадають`);
  }

  // ============================================
  // 4. СТВОРЕННЯ МОДАЛЬНОГО ВІКНА
  // ============================================

  const historyHtml = `
        <div id="historyModal" style="position: fixed; top: 0; left: 0; right: 0; bottom: 0; background: rgba(0,0,0,0.5); z-index: 10000; display: flex; align-items: center; justify-content: center; padding: 20px;">
            <div style="background: white; border-radius: 8px; max-width: 95vw; max-height: 90vh; width: 100%; display: flex; flex-direction: column; box-shadow: 0 20px 25px -5px rgba(0,0,0,0.1);">
                <div style="padding: 20px; border-bottom: 1px solid #e5e7eb; display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <h3 style="margin: 0 0 8px 0; font-size: 1.5rem; font-weight: 600;">Історія операцій: ${productTitle}</h3>
                        <p style="margin: 0; color: #6b7280; font-size: 0.95rem;">Склад: ${warehouseTitle}</p>
                        <p style="margin: 4px 0 0 0; color: #059669; font-size: 0.9rem; font-weight: 600;">Поточний залишок: ${currentBalance} шт</p>
                    </div>
                    ${
                      filterWarehouseId
                        ? `
                        <button onclick="showFullProductHistory('${encodeURIComponent(
                          productTitle
                        )}')" 
                            style="padding: 8px 16px; background: #6b7280; color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 0.9rem; transition: background 0.2s;">
                            📊 Вся історія товару
                        </button>
                    `
                        : ""
                    }
                </div>

                <div style="overflow: auto; flex: 1; padding: 20px;">
                    <table style="width: 100%; border-collapse: collapse; font-size: 0.9rem;">
                        <thead style="background: #f3f4f6; position: sticky; top: 0; z-index: 1;">
                            <tr>
                                <th style="padding: 12px 8px; text-align: left; font-weight: 600;">Дата</th>
                                <th style="padding: 12px 8px; text-align: left; font-weight: 600;">Тип операції</th>
                                <th style="padding: 12px 8px; text-align: left; font-weight: 600;">Документ</th>
                                <th style="padding: 12px 8px; text-align: left; font-weight: 600;">Хто створив</th>
                                <th style="padding: 12px 8px; text-align: left; font-weight: 600;">Склад</th>
                                <th style="padding: 12px 8px; text-align: right; font-weight: 600;">Кількість</th>
                                <th style="padding: 12px 8px; text-align: right; font-weight: 600;">Зміна</th>
                                <th style="padding: 12px 8px; text-align: right; font-weight: 600;">Залишок</th>
                                <th style="padding: 12px 8px; text-align: left; font-weight: 600;">Опис</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${tableRows}
                        </tbody>
                    </table>
                </div>

                <div style="padding: 20px; border-top: 1px solid #e5e7eb; text-align: right;">
                    <button onclick="closeHistoryModal()" 
                        style="padding: 10px 24px; background: #ef4444; color: white; border: none; border-radius: 6px; cursor: pointer; font-size: 1rem; font-weight: 500; transition: background 0.2s;">
                        ✕ Закрити
                    </button>
                </div>
            </div>
        </div>

        <style>
            .positive { color: #059669; }
            .negative { color: #dc2626; }
            .neutral { color: #6b7280; }
            
            #historyModal table tbody tr:hover {
                background: #f9fafb;
            }
            
            #historyModal button:hover {
                opacity: 0.9;
                transform: translateY(-1px);
            }
        </style>
    `;

  document.body.insertAdjacentHTML("beforeend", historyHtml);
  console.log("✅ Modal відображено");
}
