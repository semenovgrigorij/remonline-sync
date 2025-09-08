// server.js - Синхронизация товаров Remonline с BigQuery (матричный формат)
/*------------------------------*/
console.log("🔍 Диагностика Google Cloud credentials...");

// Проверяем переменные окружения
console.log(
  "GOOGLE_APPLICATION_CREDENTIALS:",
  process.env.GOOGLE_APPLICATION_CREDENTIALS
);
console.log("NODE_ENV:", process.env.NODE_ENV);

// Проверяем файл credentials
const fs = require("fs");
const path = require("path");

try {
  const credentialsPath =
    process.env.GOOGLE_APPLICATION_CREDENTIALS || "./service-account-key.json";
  console.log("Путь к credentials:", credentialsPath);

  // Проверяем существование файла
  if (fs.existsSync(credentialsPath)) {
    console.log("✅ Файл credentials существует");

    // Читаем файл как buffer для проверки кодировки
    const buffer = fs.readFileSync(credentialsPath);
    console.log("Размер файла:", buffer.length, "байт");
    console.log("Первые 10 байт (hex):", buffer.slice(0, 10).toString("hex"));
    console.log("Первые 50 символов:", buffer.slice(0, 50).toString("utf8"));

    // Проверяем BOM (Byte Order Mark)
    if (buffer[0] === 0xef && buffer[1] === 0xbb && buffer[2] === 0xbf) {
      console.log("⚠️ Обнаружен UTF-8 BOM - это может вызывать проблемы");
    }

    // Пытаемся прочитать как UTF-8
    const content = fs.readFileSync(credentialsPath, "utf8");

    // Проверяем JSON
    const parsed = JSON.parse(content);
    console.log("✅ JSON валиден, project_id:", parsed.project_id);
  } else {
    console.log("❌ Файл credentials не найден по пути:", credentialsPath);
  }
} catch (error) {
  console.error("❌ Ошибка при проверке credentials:", error.message);
  console.error("Стек ошибки:", error.stack);
}
/*---------------------------*/
// Настройка для Render - создание credentials файла из base64
if (process.env.GOOGLE_APPLICATION_CREDENTIALS_BASE64) {
  const fs = require("fs");
  const path = require("path");

  const credentialsPath = path.join(__dirname, "service-account-key.json");
  const credentialsContent = Buffer.from(
    process.env.GOOGLE_APPLICATION_CREDENTIALS_BASE64,
    "base64"
  ).toString("utf8");

  fs.writeFileSync(credentialsPath, credentialsContent);
  process.env.GOOGLE_APPLICATION_CREDENTIALS = credentialsPath;
  console.log("✅ Google Cloud credentials настроены для Render");
}

const express = require("express");
const fetch = require("node-fetch");
const { BigQuery } = require("@google-cloud/bigquery");
const cron = require("node-cron");
const path = require("path");
require("dotenv").config();

class RemonlineMatrixSync {
  constructor() {
    this.app = express();
    this.bigquery = null;
    this.isRunning = false;
    this.lastSyncData = {
      timestamp: null,
      warehousesProcessed: 0,
      goodsFound: 0,
      uniqueProducts: 0,
      errors: [],
    };

    this.setupMiddleware();
    this.setupRoutes();
    this.initializeBigQuery();
    this.setupScheduledSync();
  }

  setupMiddleware() {
    this.app.use(express.json());
    this.app.use(express.static("public"));
    this.app.use((req, res, next) => {
      console.log(`${new Date().toISOString()} - ${req.method} ${req.url}`);
      next();
    });
  }

  setupRoutes() {
    // Главная страница
    this.app.get("/", (req, res) => {
      res.sendFile(path.join(__dirname, "public", "index.html"));
    });

    // Статус системы
    this.app.get("/api/status", (req, res) => {
      res.json({
        isRunning: this.isRunning,
        lastSync: this.lastSyncData,
        nextSync: this.getNextSyncTime(),
      });
    });

    // Тест подключения к API
    this.app.post("/api/test-connection", async (req, res) => {
      try {
        const warehouses = await this.fetchWarehouses();
        res.json({
          success: true,
          warehousesCount: warehouses.length,
          warehouses: warehouses.slice(0, 3),
        });
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // Запуск синхронизации
    this.app.post("/api/sync-now", async (req, res) => {
      try {
        const result = await this.performFullSync();
        res.json({
          success: true,
          result,
        });
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // Управление автосинхронизацией
    this.app.post("/api/start-auto-sync", (req, res) => {
      this.startAutoSync();
      res.json({ success: true, message: "Автосинхронизация запущена" });
    });

    this.app.post("/api/stop-auto-sync", (req, res) => {
      this.stopAutoSync();
      res.json({ success: true, message: "Автосинхронизация остановлена" });
    });

    this.app.post("/api/test-pagination", async (req, res) => {
      try {
        const warehouseId = req.body.warehouseId || 1751786; // ID первого склада по умолчанию
        const goods = await this.testPaginationForWarehouse(warehouseId);

        res.json({
          success: true,
          warehouseId,
          totalGoods: goods.length,
          uniqueProducts: new Set(goods.map((g) => g.title)).size,
          totalResidue: goods.reduce((sum, item) => sum + item.residue, 0),
          topGoods: goods
            .sort((a, b) => b.residue - a.residue)
            .slice(0, 5)
            .map((item) => ({
              title: item.title,
              residue: item.residue,
            })),
        });
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // Новый endpoint для пересоздания таблицы:
    this.app.post("/api/recreate-table", async (req, res) => {
      try {
        const success = await this.recreateBigQueryTable();
        if (success) {
          res.json({
            success: true,
            message:
              "Таблица пересоздана с правильной схемой (FLOAT для остатков)",
          });
        } else {
          res.status(500).json({
            success: false,
            error: "Не удалось пересоздать таблицу",
          });
        }
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // Получение данных для матрицы
    this.app.get("/api/preview-data", async (req, res) => {
      try {
        if (!this.bigquery) {
          return res.json({ data: [], message: "BigQuery не настроена" });
        }

        const limit = parseInt(req.query.limit) || 10000;

        const query = `
                    SELECT 
                        warehouse_title,
                        title,
                        residue,
                        updated_at
                    FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}\`
                    WHERE residue > 0
                    ORDER BY title, warehouse_title
                    LIMIT ${limit}
                `;

        const [rows] = await this.bigquery.query({ query, location: "EU" });

        res.json({
          data: rows,
          totalRecords: rows.length,
        });
      } catch (error) {
        console.error("Ошибка получения данных:", error);
        res.json({ data: [], error: error.message });
      }
    });

    // Статистика
    this.app.get("/api/statistics", async (req, res) => {
      try {
        if (!this.bigquery) {
          return res.json({ error: "BigQuery не настроена" });
        }

        const query = `
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT warehouse_title) as total_warehouses,
                        COUNT(DISTINCT title) as unique_products,
                        SUM(residue) as total_residues,
                        MAX(updated_at) as last_update
                    FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}\`
                    WHERE residue > 0
                `;

        const [rows] = await this.bigquery.query({ query, location: "EU" });
        res.json({ success: true, statistics: rows[0] || {} });
      } catch (error) {
        console.error("Ошибка получения статистики:", error);
        res.status(500).json({ error: error.message });
      }
    });
  }

  initializeBigQuery() {
    try {
      if (
        process.env.GOOGLE_APPLICATION_CREDENTIALS &&
        process.env.BIGQUERY_PROJECT_ID
      ) {
        this.bigquery = new BigQuery({
          projectId: process.env.BIGQUERY_PROJECT_ID,
        });
        console.log("✅ BigQuery инициализирована");
      } else {
        console.log(
          "⚠️ BigQuery не настроена (отсутствуют переменные окружения)"
        );
      }
    } catch (error) {
      console.error("❌ Ошибка инициализации BigQuery:", error.message);
    }
  }

  setupScheduledSync() {
    // Автосинхронизация каждый час
    cron.schedule("0 * * * *", async () => {
      if (this.isRunning) {
        console.log("🔄 Запуск запланированной синхронизации...");
        await this.performFullSync();
      }
    });

    console.log("⏰ Планировщик настроен (каждый час)");
  }

  async fetchWarehouses() {
    const options = {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
      },
    };

    const response = await fetch("https://api.roapp.io/warehouse/", options);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    return data.data || [];
  }

  async fetchWarehouseGoods(warehouseId) {
    const options = {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
      },
    };

    let allGoods = [];
    let page = 1;
    const perPage = 100; // Максимальный размер страницы
    let hasMore = true;

    console.log(`   📡 Получение товаров для склада ${warehouseId}...`);

    while (hasMore) {
      try {
        // Запрос с пагинацией и исключением товаров с нулевым остатком
        const url = `https://api.roapp.io/warehouse/goods/${warehouseId}?exclude_zero_residue=true&page=${page}&per_page=${perPage}`;

        const response = await fetch(url, options);

        if (!response.ok) {
          throw new Error(
            `HTTP ${response.status} для склада ${warehouseId}, страница ${page}`
          );
        }

        const data = await response.json();
        const goods = data.data || [];

        if (goods.length === 0) {
          hasMore = false;
        } else {
          allGoods = allGoods.concat(goods);
          console.log(
            `   📄 Страница ${page}: ${goods.length} товаров (всего: ${allGoods.length})`
          );

          // Если получили меньше товаров чем запрашивали, значит это последняя страница
          if (goods.length < perPage) {
            hasMore = false;
          } else {
            page++;
          }
        }

        // Небольшая задержка между запросами страниц
        await this.sleep(100);
      } catch (error) {
        console.error(
          `   ❌ Ошибка получения страницы ${page}: ${error.message}`
        );
        hasMore = false;
      }
    }

    console.log(`   ✅ Итого получено: ${allGoods.length} товаров в наличии`);
    return allGoods;
  }

  async performFullSync() {
    console.log("🔄 Начинается полная синхронизация товаров в наличии...");

    const syncStart = Date.now();
    const errors = [];
    let totalGoods = 0;
    let warehousesProcessed = 0;
    const uniqueProducts = new Set();

    try {
      const warehouses = await this.fetchWarehouses();
      console.log(`📍 Найдено ${warehouses.length} складов для обработки`);

      const allData = [];

      for (const warehouse of warehouses) {
        try {
          console.log(
            `\n📦 [${warehousesProcessed + 1}/${warehouses.length}] Склад: ${
              warehouse.title
            }`
          );

          const goodsInStock = await this.fetchWarehouseGoods(warehouse.id);

          if (goodsInStock.length > 0) {
            console.log(`   📊 Обработка ${goodsInStock.length} товаров...`);

            // Анализ уникальных товаров
            const warehouseUniqueProducts = new Set();

            goodsInStock.forEach((item) => {
              uniqueProducts.add(item.title);
              warehouseUniqueProducts.add(item.title);

              // Подготовка данных для BigQuery
              const processedItem = {
                warehouse_id: warehouse.id,
                warehouse_title: warehouse.title,
                warehouse_type: warehouse.type || "product",
                warehouse_is_global: warehouse.is_global || false,
                good_id: item.id,
                title: item.title,
                code: item.code || "",
                article: item.article || "",
                residue: item.residue,
                price_json: JSON.stringify(item.price || {}),
                category: item.category?.title || "",
                category_id: item.category?.id || null,
                description: item.description || "",
                uom_title: item.uom?.title || "",
                uom_description: item.uom?.description || "",
                image_url: Array.isArray(item.image)
                  ? item.image[0] || ""
                  : item.image || "",
                is_serial: item.is_serial || false,
                warranty: item.warranty || 0,
                warranty_period: item.warranty_period || 0,
                updated_at: new Date().toISOString(),
              };
              allData.push(processedItem);
            });

            totalGoods += goodsInStock.length;

            console.log(
              `   📈 Уникальных товаров на складе: ${warehouseUniqueProducts.size}`
            );
            console.log(
              `   📦 Общий остаток: ${goodsInStock.reduce(
                (sum, item) => sum + item.residue,
                0
              )}`
            );
          } else {
            console.log(`   ⚪ Нет товаров в наличии`);
          }

          warehousesProcessed++;

          // Промежуточная статистика каждые 10 складов
          if (warehousesProcessed % 10 === 0) {
            console.log(`\n📊 === ПРОМЕЖУТОЧНАЯ СТАТИСТИКА ===`);
            console.log(
              `Обработано складов: ${warehousesProcessed}/${warehouses.length}`
            );
            console.log(`Найдено товаров: ${totalGoods}`);
            console.log(`Уникальных товаров: ${uniqueProducts.size}`);
            console.log(
              `Прогресс: ${Math.round(
                (warehousesProcessed / warehouses.length) * 100
              )}%`
            );
          }

          // Задержка между складами
          await this.sleep(300);
        } catch (error) {
          const errorMsg = `Ошибка: ${warehouse.title} - ${error.message}`;
          console.error(`❌ ${errorMsg}`);
          errors.push(errorMsg);
          warehousesProcessed++;
        }
      }

      // Финальная статистика
      console.log(`\n📊 === ИТОГОВАЯ СТАТИСТИКА ===`);
      console.log(
        `Обработано складов: ${warehousesProcessed}/${warehouses.length}`
      );
      console.log(`Найдено товаров в наличии: ${totalGoods}`);
      console.log(`Уникальных товаров: ${uniqueProducts.size}`);
      console.log(`Ошибок: ${errors.length}`);
      console.log(
        `Время обработки: ${Math.round((Date.now() - syncStart) / 1000)} секунд`
      );

      // Сохранение в BigQuery
      if (allData.length > 0) {
        console.log(`\n💾 Сохранение ${allData.length} записей в BigQuery...`);
        await this.saveToBigQuery(allData);
        console.log(`✅ Данные сохранены в BigQuery`);
      } else {
        console.log(`⚠️ Нет данных для сохранения`);
      }

      this.lastSyncData = {
        timestamp: new Date().toISOString(),
        warehousesProcessed,
        goodsFound: totalGoods,
        uniqueProducts: uniqueProducts.size,
        errors,
        duration: Date.now() - syncStart,
      };

      console.log(
        `✅ Синхронизация завершена за ${Math.round(
          this.lastSyncData.duration / 1000
        )} секунд`
      );

      return this.lastSyncData;
    } catch (error) {
      const errorMsg = `Критическая ошибка синхронизации: ${error.message}`;
      console.error(`❌ ${errorMsg}`);

      this.lastSyncData = {
        timestamp: new Date().toISOString(),
        warehousesProcessed,
        goodsFound: totalGoods,
        uniqueProducts: uniqueProducts.size,
        errors: [...errors, errorMsg],
        duration: Date.now() - syncStart,
      };

      throw error;
    }
  }

  // Дополнительно: функция для тестирования пагинации на одном складе
  async testPaginationForWarehouse(warehouseId) {
    console.log(`🧪 Тестирование пагинации для склада ${warehouseId}:`);

    try {
      const goods = await this.fetchWarehouseGoods(warehouseId);

      console.log(`📊 Результаты тестирования:`);
      console.log(`- Всего товаров: ${goods.length}`);
      console.log(
        `- Уникальных названий: ${new Set(goods.map((g) => g.title)).size}`
      );
      console.log(
        `- Общий остаток: ${goods.reduce((sum, item) => sum + item.residue, 0)}`
      );

      // Показываем топ-10 товаров по остаткам
      const topGoods = goods.sort((a, b) => b.residue - a.residue).slice(0, 10);

      console.log(`📈 Топ-10 товаров по остаткам:`);
      topGoods.forEach((item, index) => {
        console.log(
          `   ${index + 1}. "${item.title}" - остаток: ${item.residue}`
        );
      });

      return goods;
    } catch (error) {
      console.error(`❌ Ошибка тестирования: ${error.message}`);
      return [];
    }
  }

  async createBigQueryTable() {
    if (!this.bigquery) {
      console.log("❌ BigQuery не инициализирована");
      return false;
    }

    try {
      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const [datasetExists] = await dataset.exists();

      if (!datasetExists) {
        console.log("📁 Создаем датасет...");
        await dataset.create({
          location: "EU",
          description: "Dataset for Remonline inventory matrix",
        });
        console.log("✅ Датасет создан");
      }

      const table = dataset.table(process.env.BIGQUERY_TABLE);
      const [tableExists] = await table.exists();

      if (tableExists) {
        console.log("✅ Таблица BigQuery уже существует");

        // Проверяем схему существующей таблицы
        const [metadata] = await table.getMetadata();
        const existingFields = metadata.schema.fields.map(
          (field) => field.name
        );
        console.log("📋 Существующие поля:", existingFields.join(", "));

        // Проверяем тип поля residue
        const residueField = metadata.schema.fields.find(
          (field) => field.name === "residue"
        );
        if (residueField && residueField.type === "INTEGER") {
          console.log(
            "⚠️ Поле residue имеет тип INTEGER, но нужен FLOAT для дробных остатков"
          );
          console.log(
            "💡 Рекомендация: пересоздайте таблицу или измените тип поля"
          );

          // Можно автоматически пересоздать таблицу
          if (process.env.AUTO_RECREATE_TABLE === "true") {
            console.log("🔄 Пересоздание таблицы с правильной схемой...");
            await table.delete();
            console.log("🗑️ Старая таблица удалена");
            // Продолжаем создание новой таблицы ниже
          } else {
            console.log(
              "❌ Невозможно вставить дробные остатки в INTEGER поле"
            );
            return false;
          }
        } else {
          return true;
        }
      }

      console.log("🔨 Создаем новую таблицу BigQuery с правильной схемой...");

      // ИСПРАВЛЕННАЯ схема с FLOAT для residue
      const schema = [
        { name: "warehouse_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "warehouse_title", type: "STRING", mode: "REQUIRED" },
        { name: "warehouse_type", type: "STRING", mode: "NULLABLE" },
        { name: "warehouse_is_global", type: "BOOLEAN", mode: "NULLABLE" },
        { name: "good_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "title", type: "STRING", mode: "REQUIRED" },
        { name: "code", type: "STRING", mode: "NULLABLE" },
        { name: "article", type: "STRING", mode: "NULLABLE" },
        { name: "residue", type: "FLOAT", mode: "REQUIRED" },
        { name: "price_json", type: "STRING", mode: "NULLABLE" },
        { name: "category", type: "STRING", mode: "NULLABLE" },
        { name: "category_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "description", type: "STRING", mode: "NULLABLE" },
        { name: "uom_title", type: "STRING", mode: "NULLABLE" },
        { name: "uom_description", type: "STRING", mode: "NULLABLE" },
        { name: "image_url", type: "STRING", mode: "NULLABLE" },
        { name: "is_serial", type: "BOOLEAN", mode: "NULLABLE" },
        { name: "warranty", type: "INTEGER", mode: "NULLABLE" },
        { name: "warranty_period", type: "INTEGER", mode: "NULLABLE" },
        { name: "updated_at", type: "TIMESTAMP", mode: "REQUIRED" },
        { name: "sync_id", type: "STRING", mode: "NULLABLE" },
      ];

      await table.create({ schema, location: "EU" });
      console.log("✅ Таблица BigQuery создана с FLOAT типом для остатков");
      return true;
    } catch (error) {
      console.error("❌ Ошибка создания таблицы BigQuery:", error.message);
      return false;
    }
  }

  // Функция для принудительного пересоздания таблицы:
  async recreateBigQueryTable() {
    if (!this.bigquery) {
      console.log("❌ BigQuery не инициализирована");
      return false;
    }

    try {
      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const table = dataset.table(process.env.BIGQUERY_TABLE);

      const [tableExists] = await table.exists();
      if (tableExists) {
        console.log("🗑️ Удаление существующей таблицы...");
        await table.delete();
        console.log("✅ Таблица удалена");
      }

      // Создаем новую таблицу с правильной схемой
      return await this.createBigQueryTable();
    } catch (error) {
      console.error("❌ Ошибка пересоздания таблицы:", error.message);
      return false;
    }
  }

  async saveToBigQuery(data) {
    if (!this.bigquery || !data.length) return;

    try {
      await this.createBigQueryTable();

      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const table = dataset.table(process.env.BIGQUERY_TABLE);

      // Получаем метаданные таблицы для проверки схемы
      const [metadata] = await table.getMetadata();
      const existingFields = metadata.schema.fields.map((field) => field.name);

      // Добавляем sync_id и пустые значения для категорий если их нет в данных
      const syncId = Date.now().toString();
      const enhancedData = data.map((item) => {
        const processedItem = {
          ...item,
          sync_id: syncId,
        };

        // Добавляем пустые категории если поля существуют в таблице
        if (existingFields.includes("category") && !processedItem.category) {
          processedItem.category = "";
        }
        if (
          existingFields.includes("category_id") &&
          !processedItem.category_id
        ) {
          processedItem.category_id = null;
        }

        return processedItem;
      });

      console.log("📊 Вставка данных в BigQuery...");
      console.log(
        `📋 Первая запись:`,
        JSON.stringify(enhancedData[0], null, 2)
      );

      // Вставляем данные меньшими батчами с обработкой ошибок
      const batchSize = 500; // Уменьшенный размер батча
      let insertedCount = 0;
      let failedCount = 0;

      for (let i = 0; i < enhancedData.length; i += batchSize) {
        const batch = enhancedData.slice(i, i + batchSize);

        try {
          await table.insert(batch);
          insertedCount += batch.length;
          console.log(
            `📊 Успешно вставлено ${insertedCount}/${enhancedData.length} записей`
          );
        } catch (error) {
          console.error(
            `❌ Ошибка вставки батча ${i}-${i + batch.length}:`,
            error.message
          );

          if (error.errors && error.errors.length > 0) {
            console.log(`🔍 Детали первых 3 ошибок:`);
            error.errors.slice(0, 3).forEach((err, index) => {
              console.log(
                `   ${index + 1}. Ошибка:`,
                err.errors[0]?.message || "Unknown"
              );
              console.log(`      Данные:`, JSON.stringify(err.row, null, 2));
            });
          }

          // Пытаемся вставить записи по одной для определения проблемных
          console.log(`🔄 Попытка вставки по одной записи...`);
          for (const record of batch) {
            try {
              await table.insert([record]);
              insertedCount++;
            } catch (singleError) {
              failedCount++;
              console.log(`❌ Не удалось вставить запись: ${record.title}`);
              if (failedCount <= 5) {
                console.log(`   Ошибка:`, singleError.message);
                console.log(`   Данные:`, JSON.stringify(record, null, 2));
              }
            }
          }
        }
      }

      console.log(
        `📊 Итого: ${insertedCount} успешно, ${failedCount} с ошибками`
      );

      // Удаляем старые данные через 30 секунд
      if (insertedCount > 0) {
        setTimeout(async () => {
          try {
            const deleteQuery = `
                        DELETE FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}\` 
                        WHERE sync_id != @current_sync_id OR sync_id IS NULL
                    `;

            const deleteOptions = {
              query: deleteQuery,
              params: { current_sync_id: syncId },
              types: { current_sync_id: "STRING" },
              location: "EU",
            };

            console.log("🗑️ Удаление старых данных...");
            const [deleteJob] = await this.bigquery.createQueryJob(
              deleteOptions
            );
            await deleteJob.getQueryResults();
            console.log("✅ Старые данные удалены");
          } catch (error) {
            console.log("⚠️ Не удалось удалить старые данные:", error.message);
          }
        }, 30000);
      }

      if (insertedCount > 0) {
        console.log(`✅ Успешно сохранено ${insertedCount} записей в BigQuery`);
      } else {
        throw new Error("Не удалось сохранить ни одной записи");
      }
    } catch (error) {
      console.error(
        "❌ Критическая ошибка сохранения в BigQuery:",
        error.message
      );
      throw error;
    }
  }

  startAutoSync() {
    this.isRunning = true;
    console.log("🚀 Автоматическая синхронизация запущена");
  }

  stopAutoSync() {
    this.isRunning = false;
    console.log("⏹️ Автоматическая синхронизация остановлена");
  }

  getNextSyncTime() {
    if (!this.isRunning) return null;

    const now = new Date();
    const nextHour = new Date(
      now.getFullYear(),
      now.getMonth(),
      now.getDate(),
      now.getHours() + 1,
      0,
      0
    );
    return nextHour.toISOString();
  }

  sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  start() {
    const PORT = process.env.PORT || 3000;
    this.app.listen(PORT, () => {
      console.log(`🚀 Сервер запущен на порту ${PORT}`);
      console.log(`📱 Откройте http://localhost:${PORT} в браузере`);
      console.log(`📊 Матричное отображение: товары × склады`);
    });
  }
}

// Запуск приложения
const syncApp = new RemonlineMatrixSync();
syncApp.start();
