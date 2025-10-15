// server.js - Синхронизация товаров Remonline с BigQuery (матричный формат)

const fs = require("fs");
const path = require("path");
const express = require("express");
const fetch = require("node-fetch");
const { BigQuery } = require("@google-cloud/bigquery");
const cron = require("node-cron");
require("dotenv").config();

if (process.env.NODE_ENV === "production") {
  process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = 0;
}
/*------------------------------*/
console.log("🔍 Диагностика Google Cloud credentials...");

// Проверяем переменные окружения
console.log(
  "GOOGLE_APPLICATION_CREDENTIALS:",
  process.env.GOOGLE_APPLICATION_CREDENTIALS
);
console.log("NODE_ENV:", process.env.NODE_ENV);

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
// Настройка для Render
if (process.env.GOOGLE_CREDENTIALS_JSON) {
  try {
    console.log("Настройка credentials из GOOGLE_CREDENTIALS_JSON...");
    const credentials = JSON.parse(process.env.GOOGLE_CREDENTIALS_JSON);

    const credentialsPath = path.join(__dirname, "service-account-key.json");
    fs.writeFileSync(credentialsPath, JSON.stringify(credentials, null, 2));
    process.env.GOOGLE_APPLICATION_CREDENTIALS = credentialsPath;

    console.log(
      "✅ Google Cloud credentials настроены, project_id:",
      credentials.project_id
    );
  } catch (error) {
    console.error("❌ Ошибка парсинга GOOGLE_CREDENTIALS_JSON:", error.message);
    process.exit(1);
  }
} else {
  console.log("❌ GOOGLE_CREDENTIALS_JSON не найдена в переменных окружения");
}

class RemonlineMatrixSync {
  constructor() {
    this.app = express();
    this.bigquery = null;
    this.isRunning = false;
    this.employeesCache = new Map();
    this.suppliersCache = new Map();
    this.movesCache = new Map();

    // СПИСОК ИСКЛЮЧЕННЫХ ЛОКАЦИЙ
    this.excludedBranchIds = [112954, 123343, 178097];
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
    this.browser = null;
    this.userCookies = new Map();
    // this.autoLogin();
    this.loginServiceUrl = process.env.LOGIN_SERVICE_URL;

    // Отримуємо cookies при старті (відкладено)
    setTimeout(() => {
      this.refreshCookiesAutomatically();
    }, 5000); // Через 5 секунд після старту
  }

  async autoLogin() {
    // НЕ логінимось при старті сервера, щоб не блокувати запуск
    console.log("⏳ Автологін буде виконано при першому запиті goods-flow");
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
    // Endpoint для отримання історії товару (замовлення + повернення)
    this.app.get(
      "/api/goods-flow-items/:productId/:warehouseId?",
      async (req, res) => {
        try {
          const productId = req.params.productId;
          const warehouseId = req.params.warehouseId
            ? parseInt(req.params.warehouseId)
            : null;

          console.log(
            `📡 Запит goods-flow для товару ${productId}${
              warehouseId ? `, склад ${warehouseId}` : ""
            }`
          );

          const cookies = await this.getCookies();
          if (!cookies) {
            return res.json({
              success: false,
              needManualUpdate: true,
              error:
                "Cookies відсутні або застарілі. Оновіть через адмін-панель.",
            });
          }

          const startDate = new Date("2022-05-01").toISOString();
          const endDate = new Date().toISOString();

          console.log(`📅 Період: ${startDate} - ${endDate}`);

          const flowItems = await this.fetchGoodsFlowForProduct(
            productId,
            startDate,
            endDate,
            cookies
          );

          console.log(`📊 Отримано з API: ${flowItems.length} операцій`);

          let filteredItems = flowItems;

          if (warehouseId) {
            const beforeFilter = flowItems.length;

            filteredItems = flowItems.filter((item) => {
              if (!item.warehouse_id) {
                return false;
              }
              return parseInt(item.warehouse_id) === parseInt(warehouseId);
            });

            console.log(
              `✅ Фільтрація: ${beforeFilter} → ${filteredItems.length} операцій для складу ${warehouseId}`
            );
          }

          res.json({
            success: true,
            productId: productId,
            warehouseId: warehouseId,
            data: filteredItems,
            totalRecords: filteredItems.length,
            totalBeforeFilter: flowItems.length,
          });
        } catch (error) {
          console.error("❌ Помилка отримання goods-flow:", error);

          // ✅ ВИПРАВЛЕНО: перевіряємо чи це помилка авторизації
          if (
            error.message.includes("401") ||
            error.message.includes("403") ||
            error.message.includes("500")
          ) {
            return res.json({
              success: false,
              needManualUpdate: true,
              error:
                "Cookies застарілі! Оновіть через адмін-панель (⚙️ → 🔐 Оновити Cookies)",
            });
          }

          res.status(500).json({
            success: false,
            error: error.message,
          });
        }
      }
    );
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
        const allWarehouses = await this.fetchWarehouses();

        // Фильтруем склады по названию
        const activeWarehouses = allWarehouses.filter((warehouse) => {
          const title = warehouse.title || "";
          return (
            !title.startsWith("001_") &&
            !title.startsWith("002_") &&
            !title.startsWith("003_")
          );
        });

        console.log(
          `Всего складов: ${allWarehouses.length}, активных: ${activeWarehouses.length}`
        );

        res.json({
          success: true,
          warehousesCount: activeWarehouses.length,
          warehouses: activeWarehouses.slice(0, 3),
        });
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    this.app.post("/api/temp-set-cookies", async (req, res) => {
      try {
        console.log("📥 Отримано запит на встановлення cookies");
        console.log("📦 Body:", req.body);

        const { cookies } = req.body;

        if (!cookies) {
          console.log("❌ Cookies відсутні в body");
          return res.status(400).json({
            success: false,
            error: "Cookies обов'язкові",
          });
        }

        this.userCookies.set("shared_user", cookies);
        console.log("✅ Cookies встановлено");
        console.log("📏 Довжина:", cookies.length);

        res.json({
          success: true,
          message: "Cookies успішно збережено",
          cookiesLength: cookies.length,
        });
      } catch (error) {
        console.error("❌ Помилка:", error);
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });
    // НОВЫЕ ЭНДПОИНТЫ ДЛЯ ВЫПАДАЮЩИХ СПИСКОВ

    // Получение складов для конкретной локации (branch_id)
    this.app.get("/api/branch-warehouses/:branchId", async (req, res) => {
      try {
        const branchId = parseInt(req.params.branchId);

        console.log(`📡 Получение складов для филиала ${branchId} через API`);
        const warehouses = await this.fetchWarehousesByBranch(branchId);

        res.json({
          success: true,
          branchId,
          data: warehouses,
          totalWarehouses: warehouses.length,
        });
      } catch (error) {
        console.error(
          `❌ Ошибка получения складов филиала ${branchId}:`,
          error
        );
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    this.app.get("/api/all-warehouses", async (req, res) => {
      try {
        if (!this.bigquery) {
          return res.json({
            success: false,
            error: "BigQuery не настроена",
            warehouses: [],
            totalWarehouses: 0,
          });
        }

        // ПРИБРАЛИ residue > 0
        const query = `
      SELECT DISTINCT 
          warehouse_id as id,
          warehouse_title as title
      FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
      ORDER BY warehouse_title
    `;

        const [rows] = await this.bigquery.query({ query, location: "EU" });

        console.log(`✅ Получено ${rows.length} складов из BigQuery`);

        res.json({
          success: true,
          warehouses: rows,
          totalWarehouses: rows.length,
        });
      } catch (error) {
        console.error("❌ Ошибка получения складов из BigQuery:", error);
        res.status(500).json({
          success: false,
          error: error.message,
          warehouses: [],
          totalWarehouses: 0,
        });
      }
    });
    // Получение товаров конкретного склада
    this.app.get(
      "/api/selected-warehouse-goods/:warehouseId",
      async (req, res) => {
        try {
          if (!this.bigquery) {
            return res.json({
              success: false,
              error: "BigQuery не настроена",
            });
          }

          const warehouseId = parseInt(req.params.warehouseId);

          const query = `
            SELECT 
                warehouse_title,
                title,
                residue,
                code,
                article,
                uom_title,
                updated_at
            FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
            WHERE warehouse_id = @warehouse_id AND residue > 0
            ORDER BY title
        `;

          const [rows] = await this.bigquery.query({
            query,
            location: "EU",
            params: { warehouse_id: warehouseId },
          });

          const warehouseTitle =
            rows.length > 0
              ? rows[0].warehouse_title
              : `Склад ID: ${warehouseId}`;

          res.json({
            success: true,
            warehouseId,
            warehouseTitle,
            data: rows.map((item) => ({
              title: item.title,
              residue: item.residue,
              code: item.code || "",
              article: item.article || "",
              uom_title: item.uom_title || "",
              updated_at: item.updated_at,
            })),
            totalItems: rows.length,
            totalQuantity: rows.reduce((sum, item) => sum + item.residue, 0),
          });
        } catch (error) {
          console.error("Ошибка получения товаров выбранного склада:", error);
          res.status(500).json({
            success: false,
            error: error.message,
          });
        }
      }
    );

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

    // Синхронизация истории оприходований
    this.app.post("/api/sync-postings", async (req, res) => {
      try {
        const result = await this.fetchPostings();
        res.json(result);
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    this.app.post("/api/sync-moves", async (req, res) => {
      try {
        const result = await this.fetchMoves();
        res.json(result);
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // Синхронізація списань
    this.app.post("/api/sync-outcomes", async (req, res) => {
      try {
        const result = await this.fetchOutcomes();
        res.json(result);
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // Синхронізація продажів
    this.app.post("/api/sync-sales", async (req, res) => {
      try {
        const result = await this.fetchSales();
        res.json(result);
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // Створює ендпоінт для створення view
    this.app.post("/api/create-stock-view", async (req, res) => {
      try {
        const success = await this.createStockCalculationView();
        res.json({
          success,
          message: success ? "View створено успішно" : "Помилка створення view",
        });
      } catch (error) {
        res.status(500).json({ success: false, error: error.message });
      }
    });

    //  Endpoint для логіну
    this.app.post("/api/login-remonline", async (req, res) => {
      try {
        const { email, password } = req.body;

        const cookies = await this.loginToRemOnline(email, password);

        // Зберігаємо cookies в пам'яті
        this.userCookies.set("main_user", cookies);

        res.json({
          success: true,
          message: "Успішний вхід",
        });
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    this.app.post("/api/sync-orders", async (req, res) => {
      try {
        const result = await this.syncOrders();
        res.json(result);
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
    FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
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
    FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
    WHERE residue > 0
`;

        const [rows] = await this.bigquery.query({ query, location: "EU" });
        res.json({ success: true, statistics: rows[0] || {} });
      } catch (error) {
        console.error("Ошибка получения статистики:", error);
        res.status(500).json({ error: error.message });
      }
    });

    // Товары по конкретному складу
    this.app.get("/api/warehouse-goods/:warehouseTitle", async (req, res) => {
      try {
        if (!this.bigquery) {
          return res.json({ data: [], message: "BigQuery не настроена" });
        }

        const warehouseTitle = decodeURIComponent(req.params.warehouseTitle);

        const query = `
    SELECT 
        title,
        residue,
        code,
        article,
        uom_title,
        updated_at
    FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
    WHERE warehouse_title = @warehouse_title AND residue > 0
    ORDER BY residue DESC, title
`;

        const [rows] = await this.bigquery.query({
          query,
          location: "EU",
          params: { warehouse_title: warehouseTitle },
          types: { warehouse_title: "STRING" },
        });

        res.json({
          success: true,
          warehouseTitle,
          data: rows,
          totalItems: rows.length,
          totalQuantity: rows.reduce((sum, item) => sum + item.residue, 0),
        });
      } catch (error) {
        console.error("Ошибка получения товаров склада:", error);
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // Склады с конкретным товаром
    this.app.get("/api/product-warehouses/:productTitle", async (req, res) => {
      try {
        if (!this.bigquery) {
          return res.json({ data: [], message: "BigQuery не настроена" });
        }

        const productTitle = decodeURIComponent(req.params.productTitle);

        const query = `
    SELECT 
        warehouse_title,
        residue,
        code,
        article,
        uom_title,
        updated_at
    FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
    WHERE title = @product_title AND residue > 0
    ORDER BY residue DESC, warehouse_title
`;

        const [rows] = await this.bigquery.query({
          query,
          location: "EU",
          params: { product_title: productTitle },
          types: { product_title: "STRING" },
        });

        res.json({
          success: true,
          productTitle,
          data: rows,
          totalWarehouses: rows.length,
          totalQuantity: rows.reduce((sum, item) => sum + item.residue, 0),
        });
      } catch (error) {
        console.error("Ошибка получения складов товара:", error);
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // Поиск товаров по названию

    this.app.get("/api/search-products/:searchTerm", async (req, res) => {
      try {
        if (!this.bigquery) {
          return res.json({ data: [], message: "BigQuery не настроена" });
        }

        const searchTerm = decodeURIComponent(req.params.searchTerm);

        // Запит БЕЗ category якщо його немає в таблиці
        const currentStockQuery = `
      SELECT 
          title,
          warehouse_title,
          SUM(residue) as residue,
          MAX(code) as code,
          MAX(article) as article,
          MAX(uom_title) as uom_title,
          MAX(updated_at) as updated_at
      FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
      WHERE LOWER(title) LIKE LOWER(@search_term)
      GROUP BY title, warehouse_title
      ORDER BY title, warehouse_title
    `;

        const [currentRows] = await this.bigquery.query({
          query: currentStockQuery,
          location: "EU",
          params: { search_term: `%${searchTerm}%` },
        });

        // Якщо знайшли в поточних остатках - повертаємо їх
        if (currentRows.length > 0) {
          console.log(
            `🔍 Пошук "${searchTerm}": знайдено ${currentRows.length} результатів (в наявності)`
          );

          return res.json({
            success: true,
            searchTerm,
            data: currentRows,
            totalResults: currentRows.length,
            totalQuantity: currentRows.reduce(
              (sum, item) => sum + (item.residue || 0),
              0
            ),
          });
        }

        // Якщо не знайшли - шукаємо в історії
        const historicalQuery = `
      SELECT DISTINCT
          product_title as title,
          warehouse_title,
          0 as residue,
          product_code as code,
          product_article as article,
          uom_title,
          posting_created_at as updated_at
      FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_postings\`
      WHERE LOWER(product_title) LIKE LOWER(@search_term)
      ORDER BY product_title, warehouse_title
      LIMIT 100
    `;

        const [historicalRows] = await this.bigquery.query({
          query: historicalQuery,
          location: "EU",
          params: { search_term: `%${searchTerm}%` },
        });

        console.log(
          `🔍 Пошук "${searchTerm}": знайдено ${historicalRows.length} результатів (історія)`
        );

        res.json({
          success: true,
          searchTerm,
          data: historicalRows,
          totalResults: historicalRows.length,
          totalQuantity: 0,
        });
      } catch (error) {
        console.error("Ошибка поиска товаров:", error);
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // 1. Получение всех локаций
    this.app.get("/api/locations", async (req, res) => {
      try {
        if (!this.bigquery) {
          return res.json({ data: [], message: "BigQuery не настроена" });
        }

        const query = `
      SELECT 
        REGEXP_EXTRACT(warehouse_title, r'^([^-]+)') as location_name,
        COUNT(DISTINCT warehouse_title) as warehouses_count,
        COUNT(DISTINCT title) as unique_products,
        SUM(residue) as total_residue,
        STRING_AGG(DISTINCT warehouse_title, ', ' ORDER BY warehouse_title) as warehouses_list
      FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
      WHERE residue > 0 
        AND warehouse_title IS NOT NULL 
        AND warehouse_title != ''
      GROUP BY location_name
      HAVING location_name IS NOT NULL AND location_name != ''
      ORDER BY location_name
    `;

        const [rows] = await this.bigquery.query({ query, location: "EU" });

        res.json({
          success: true,
          data: rows.map((row) => ({
            location_name: row.location_name.trim(),
            warehouses_count: row.warehouses_count,
            unique_products: row.unique_products,
            total_residue: row.total_residue,
            warehouses: row.warehouses_list.split(", "),
          })),
        });
      } catch (error) {
        console.error("Ошибка получения локаций:", error);
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // 2. Получение матрицы товаров для конкретной локации
    this.app.get("/api/location-matrix/:locationName", async (req, res) => {
      try {
        if (!this.bigquery) {
          return res.json({ data: [], message: "BigQuery не настроена" });
        }

        const locationName = decodeURIComponent(req.params.locationName);
        const limit = parseInt(req.query.limit) || 10000;

        const query = `
      SELECT 
        warehouse_title,
        title,
        residue,
        updated_at,
        code,
        article,
        category,
        uom_title
      FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
      WHERE residue > 0 
        AND REGEXP_EXTRACT(warehouse_title, r'^([^-]+)') = @location_name
      ORDER BY title, warehouse_title
      LIMIT ${limit}
    `;

        const [rows] = await this.bigquery.query({
          query,
          location: "EU",
          params: { location_name: locationName },
          types: { location_name: "STRING" },
        });

        // Статистика для локации
        const statsQuery = `
      SELECT 
        COUNT(DISTINCT warehouse_title) as warehouses_count,
        COUNT(DISTINCT title) as unique_products,
        SUM(residue) as total_residue,
        COUNT(*) as total_records
      FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
      WHERE residue > 0 
        AND REGEXP_EXTRACT(warehouse_title, r'^([^-]+)') = @location_name
    `;

        const [statsRows] = await this.bigquery.query({
          query: statsQuery,
          location: "EU",
          params: { location_name: locationName },
          types: { location_name: "STRING" },
        });

        res.json({
          success: true,
          locationName,
          data: rows,
          statistics: statsRows[0] || {},
          totalRecords: rows.length,
        });
      } catch (error) {
        console.error("Ошибка получения матрицы локации:", error);
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // 3. Получение складов в конкретной локации
    this.app.get("/api/location-warehouses/:locationName", async (req, res) => {
      try {
        if (!this.bigquery) {
          return res.json({ data: [], message: "BigQuery не настроена" });
        }

        const locationName = decodeURIComponent(req.params.locationName);

        const query = `
      SELECT 
        warehouse_title,
        COUNT(DISTINCT title) as unique_products,
        SUM(residue) as total_residue,
        MAX(updated_at) as last_updated
      FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
      WHERE residue > 0 
        AND REGEXP_EXTRACT(warehouse_title, r'^([^-]+)') = @location_name
      GROUP BY warehouse_title
      ORDER BY warehouse_title
    `;

        const [rows] = await this.bigquery.query({
          query,
          location: "EU",
          params: { location_name: locationName },
          types: { location_name: "STRING" },
        });

        res.json({
          success: true,
          locationName,
          warehouses: rows,
          totalWarehouses: rows.length,
        });
      } catch (error) {
        console.error("Ошибка получения складов локации:", error);
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // 4. Получение группированной матрицы с локациями в заголовках
    this.app.get("/api/grouped-matrix", async (req, res) => {
      try {
        if (!this.bigquery) {
          return res.json({ data: [], message: "BigQuery не настроена" });
        }

        const limit = parseInt(req.query.limit) || 10000;

        const query = `
      SELECT 
        warehouse_title,
        REGEXP_EXTRACT(warehouse_title, r'^([^-]+)') as location_name,
        title,
        residue,
        updated_at
      FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
      WHERE residue > 0
      ORDER BY location_name, warehouse_title, title
      LIMIT ${limit}
    `;

        const [rows] = await this.bigquery.query({ query, location: "EU" });

        // Группируем данные по локациям
        const locationGroups = {};
        const allWarehouses = new Set();
        const allProducts = new Set();

        rows.forEach((row) => {
          const location = row.location_name?.trim();
          const warehouse = row.warehouse_title;
          const product = row.title;

          if (location && warehouse && product) {
            if (!locationGroups[location]) {
              locationGroups[location] = {
                warehouses: new Set(),
                products: new Set(),
              };
            }

            locationGroups[location].warehouses.add(warehouse);
            allWarehouses.add(warehouse);
            allProducts.add(product);
          }
        });

        // Преобразуем Set в Array
        Object.keys(locationGroups).forEach((location) => {
          locationGroups[location].warehouses = Array.from(
            locationGroups[location].warehouses
          );
          locationGroups[location].products = Array.from(
            locationGroups[location].products
          );
        });

        res.json({
          success: true,
          data: rows,
          locationGroups,
          totalWarehouses: allWarehouses.size,
          totalProducts: allProducts.size,
          totalLocations: Object.keys(locationGroups).length,
        });
      } catch (error) {
        console.error("Ошибка получения группированной матрицы:", error);
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    this.app.get("/api/debug-warehouse/:warehouseId", async (req, res) => {
      try {
        const warehouseId = req.params.warehouseId;
        console.log(`🔍 ОТЛАДКА: Получение всех товаров склада ${warehouseId}`);

        const goods = await this.fetchWarehouseGoods(warehouseId);

        res.json({
          success: true,
          warehouseId,
          totalGoods: goods.length,
          uniqueProducts: new Set(goods.map((g) => g.title)).size,
          pagesLoaded: Math.ceil(goods.length / 100),
          sampleGoods: goods.slice(0, 5).map((item) => ({
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

    this.app.get("/api/employees", async (req, res) => {
      try {
        const employees = await this.fetchEmployees();
        res.json({
          success: true,
          data: Array.from(this.employeesCache.values()),
          totalEmployees: employees.length,
        });
      } catch (error) {
        console.error("Ошибка получения сотрудников:", error);
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    this.app.get("/api/employee-name/:employeeId", async (req, res) => {
      try {
        const employeeId = parseInt(req.params.employeeId);

        // Если кеш пуст, загружаем сотрудников
        if (this.employeesCache.size === 0) {
          await this.fetchEmployees();
        }

        const employeeName = this.getEmployeeName(employeeId);

        res.json({
          success: true,
          employeeId,
          employeeName,
          cacheSize: this.employeesCache.size,
        });
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    this.app.get("/api/warehouse-id/:warehouseTitle", async (req, res) => {
      try {
        const warehouseTitle = decodeURIComponent(req.params.warehouseTitle);
        const warehouses = await this.fetchWarehouses();

        const warehouse = warehouses.find((w) => w.title === warehouseTitle);

        if (warehouse) {
          res.json({
            success: true,
            warehouseId: warehouse.id,
            warehouseTitle: warehouse.title,
          });
        } else {
          res.status(404).json({
            success: false,
            error: `Склад "${warehouseTitle}" не найден`,
          });
        }
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    this.app.get("/api/suppliers", async (req, res) => {
      try {
        await this.fetchSuppliersFromPostings();
        res.json({
          success: true,
          data: Array.from(this.suppliersCache.values()),
          totalSuppliers: this.suppliersCache.size,
        });
      } catch (error) {
        console.error("Ошибка получения поставщиков:", error);
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    this.app.get("/api/supplier/:supplierId", async (req, res) => {
      try {
        const supplierId = parseInt(req.params.supplierId);
        let supplier = this.suppliersCache.get(supplierId);

        if (!supplier) {
          supplier = await this.fetchSupplierInfo(supplierId);
          if (supplier) {
            this.suppliersCache.set(supplierId, supplier);
          }
        }

        if (supplier) {
          res.json({
            success: true,
            supplier: supplier,
          });
        } else {
          res.status(404).json({
            success: false,
            error: `Поставщик ${supplierId} не найден`,
          });
        }
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    this.app.get("/api/supplier-ids", async (req, res) => {
      try {
        if (!this.bigquery) {
          return res.json({ error: "BigQuery не настроена" });
        }

        const query = `
            SELECT supplier_id, COUNT(*) as count
            FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_postings\`
            WHERE supplier_id IS NOT NULL
            GROUP BY supplier_id
            ORDER BY count DESC
            LIMIT 10
        `;

        const [rows] = await this.bigquery.query({ query, location: "EU" });

        res.json({
          success: true,
          supplierIds: rows,
        });
      } catch (error) {
        res.status(500).json({ error: error.message });
      }
    });

    // Получение объединенной истории товара (оприходования + перемещения)
    this.app.get(
      "/api/product-history/:warehouseId/:productTitle",
      async (req, res) => {
        try {
          if (!this.bigquery) {
            return res.json({ data: [], message: "BigQuery не настроена" });
          }

          const warehouseId = req.params.warehouseId;
          const productTitle = decodeURIComponent(req.params.productTitle);

          // Получаем product_id
          const productIdQuery = `
    SELECT DISTINCT product_id 
    FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
    WHERE title = @product_title
    LIMIT 1
`;

          const [productIdRows] = await this.bigquery.query({
            query: productIdQuery,
            location: "EU",
            params: {
              product_title: productTitle,
            },
          });

          if (productIdRows.length === 0) {
            return res.json({
              success: true,
              data: [],
              error: "Товар не найден",
            });
          }

          const productId = productIdRows[0].product_id;

          // Объединенный запрос оприходований и перемещений
          const historyQuery = `
            SELECT 
                'posting' as operation_type,
                posting_id as operation_id,
                posting_label as operation_label,
                posting_created_at as operation_date,
                created_by_name,
                warehouse_title as warehouse_info,
                '' as source_warehouse,
                '' as target_warehouse,
                amount,
                price,
                uom_title,
                posting_description as description
            FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_postings\`
            WHERE product_id = @product_id
            
            UNION ALL
            
            SELECT 
                'move' as operation_type,
                move_id as operation_id,
                move_label as operation_label,
                move_created_at as operation_date,
                created_by_name,
                '' as warehouse_info,
                source_warehouse_title as source_warehouse,
                target_warehouse_title as target_warehouse,
                amount,
                move_cost as price,
                uom_title,
                move_description as description
            FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_moves\`
            WHERE product_id = @product_id
            
            ORDER BY operation_date DESC
            LIMIT 200
        `;

          const [rows] = await this.bigquery.query({
            query: historyQuery,
            location: "EU",
            params: {
              product_id: parseInt(productId),
            },
          });

          res.json({
            success: true,
            productTitle,
            productId,
            data: rows,
            totalRecords: rows.length,
          });
        } catch (error) {
          console.error("❌ Ошибка получения истории товара:", error);
          res.status(500).json({
            success: false,
            error: error.message,
          });
        }
      }
    );

    this.app.get(
      "/api/product-warehouse-history/:warehouseId/:productTitle",
      async (req, res) => {
        try {
          console.log("\n🔍 === API ENDPOINT ВИКЛИКАНО ===");
          console.log("warehouseId:", req.params.warehouseId);
          console.log("productTitle:", req.params.productTitle);

          if (!this.bigquery) {
            console.log("❌ BigQuery не налаштована");
            return res.json({
              data: { postings: [], moves: [], outcomes: [], sales: [] },
              message: "BigQuery не настроена",
            });
          }

          const warehouseId = parseInt(req.params.warehouseId);
          const productTitle = decodeURIComponent(req.params.productTitle);

          console.log(
            `✅ Парсинг: warehouseId=${warehouseId}, productTitle="${productTitle}"`
          );

          // Пошук product_id
          let productId = null;

          const productFromPostingsQuery = `
        SELECT DISTINCT product_id
        FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_postings\`
        WHERE LOWER(product_title) = LOWER(@product_title)
        LIMIT 1
      `;

          console.log("🔍 Шукаємо product_id...");
          const [postingRows] = await this.bigquery.query({
            query: productFromPostingsQuery,
            location: "EU",
            params: { product_title: productTitle },
          });

          if (postingRows.length > 0) {
            productId = postingRows[0].product_id;
            console.log(`✅ Знайдено product_id: ${productId}`);
          } else {
            console.log(`❌ product_id НЕ знайдено для "${productTitle}"`);
            return res.json({
              success: true,
              data: { postings: [], moves: [], outcomes: [], sales: [] },
              error: "Товар не найден",
              productId: null,
            });
          }

          // ОПРИБУТКУВАННЯ
          console.log("📦 Запит оприбуткувань...");
          const postingsQuery = `
        SELECT DISTINCT
            posting_created_at,
            posting_label,
            created_by_name,
            supplier_id,
            supplier_name,
            amount,
            posting_description,
            price,
            warehouse_title,
            warehouse_id
        FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_postings\`
        WHERE product_id = @product_id
          AND warehouse_id = @warehouse_id
        ORDER BY posting_created_at DESC
      `;

          const [postingsData] = await this.bigquery.query({
            query: postingsQuery,
            location: "EU",
            params: {
              product_id: productId,
              warehouse_id: warehouseId,
            },
          });

          console.log(`✅ Оприбуткувань: ${postingsData.length}`);

          // ✅ ПЕРЕМІЩЕННЯ - БЕЗ ФІЛЬТРАЦІЇ (НОВИЙ КОД!)
          console.log("🔄 Запит переміщень...");
          const movesQuery = `
        SELECT DISTINCT
            m.move_id,
            m.move_label,
            m.move_created_at,
            m.created_by_name,
            m.source_warehouse_title,
            m.target_warehouse_title,
            m.amount,
            m.move_description,
            m.warehouse_id
        FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_moves\` m
        WHERE m.product_id = @product_id
        ORDER BY m.move_created_at DESC
      `;

          const [movesData] = await this.bigquery.query({
            query: movesQuery,
            location: "EU",
            params: { product_id: productId },
          });

          console.log(`✅ Переміщень (всього): ${movesData.length}`);

          // СПИСАННЯ
          console.log("🗑️ Запит списань...");
          const outcomesQuery = `
        SELECT DISTINCT
            o.outcome_created_at,
            o.outcome_label,
            o.created_by_name,
            o.source_warehouse_title,
            o.amount,
            o.outcome_description,
            o.outcome_cost
        FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_outcomes\` o
        WHERE o.product_id = @product_id
        ORDER BY o.outcome_created_at DESC
      `;

          const [outcomesData] = await this.bigquery.query({
            query: outcomesQuery,
            location: "EU",
            params: { product_id: productId },
          });

          console.log(`✅ Списань (всього): ${outcomesData.length}`);

          // ПРОДАЖІ
          console.log("💰 Запит продажів...");
          const salesQuery = `
        SELECT DISTINCT
            sale_created_at,
            sale_label,
            created_by_name,
            warehouse_id,
            amount,
            price,
            cost,
            discount_value,
            sale_description
        FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_sales\`
        WHERE LOWER(product_title) = LOWER(@product_title)
          AND warehouse_id = @warehouse_id
        ORDER BY sale_created_at DESC
      `;

          const [salesData] = await this.bigquery.query({
            query: salesQuery,
            location: "EU",
            params: {
              product_title: productTitle,
              warehouse_id: warehouseId,
            },
          });

          console.log(`✅ Продажів: ${salesData.length}`);

          console.log("\n📊 === ПІДСУМОК API ===");
          console.log(`Postings: ${postingsData.length}`);
          console.log(`Moves: ${movesData.length}`);
          console.log(`Outcomes: ${outcomesData.length}`);
          console.log(`Sales: ${salesData.length}`);

          res.json({
            success: true,
            productTitle,
            productId,
            warehouseId,
            data: {
              postings: postingsData,
              moves: movesData,
              outcomes: outcomesData,
              sales: salesData,
            },
            totalPostings: postingsData.length,
            totalMoves: movesData.length,
            totalOutcomes: outcomesData.length,
            totalSales: salesData.length,
          });
        } catch (error) {
          console.error("❌ КРИТИЧНА ПОМИЛКА API:", error);
          console.error("Stack:", error.stack);
          res.status(500).json({
            success: false,
            error: error.message,
          });
        }
      }
    );

    // Добавьте временный endpoint для отладки
    this.app.get("/api/debug-postings/:warehouseId", async (req, res) => {
      const warehouseId = req.params.warehouseId;
      const options = {
        method: "GET",
        headers: {
          accept: "application/json",
          authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
        },
      };

      let allPostings = [];
      let page = 1;
      const maxPages = 10; // проверим первые 10 страниц

      while (page <= maxPages) {
        const url = `https://api.roapp.io/warehouse/postings/?page=${page}&warehouse_ids[]=${warehouseId}&per_page=100`;
        const response = await fetch(url, options);
        const data = await response.json();
        const postings = data.data || [];

        allPostings.push(...postings);
        console.log(`Страница ${page}: ${postings.length} постингов`);

        if (postings.length === 0) break;
        page++;
      }

      res.json({
        total: allPostings.length,
        pages: page - 1,
        sample: allPostings.slice(0, 5),
      });
    });

    // ТИМЧАСОВО для тестування
    this.app.get("/api/set-cookies/:cookieString", async (req, res) => {
      try {
        const cookies = decodeURIComponent(req.params.cookieString);
        this.userCookies.set("shared_user", cookies);

        res.json({
          success: true,
          message: "Cookies встановлено",
          cookiesLength: cookies.length,
        });
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
      }
    });

    // endpoint для проверки cookies

    this.app.get("/api/check-cookies-status", async (req, res) => {
      try {
        const cookies = this.userCookies.get("shared_user");

        if (!cookies) {
          return res.json({
            success: false,
            hasCookies: false,
            message: "Cookies відсутні. Необхідно оновити через адмін-панель.",
            cookiesLength: 0,
          });
        }

        // Тестуємо чи працюють cookies
        try {
          const testUrl =
            "https://web.roapp.io/app/warehouse/get-goods-flow-items?page=1&pageSize=1&id=1&startDate=0&endDate=999999999999";

          const testResponse = await fetch(testUrl, {
            method: "GET",
            headers: {
              accept: "application/json",
              cookie: cookies,
              "User-Agent":
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            },
          });

          const isValid = testResponse.ok;

          return res.json({
            success: true,
            hasCookies: true,
            cookiesValid: isValid,
            cookiesLength: cookies.length,
            httpStatus: testResponse.status,
            message: isValid
              ? "✅ Cookies валідні, goods-flow доступний"
              : `❌ Cookies застарілі (HTTP ${testResponse.status}), потрібно оновити`,
          });
        } catch (testError) {
          return res.json({
            success: true,
            hasCookies: true,
            cookiesValid: false,
            cookiesLength: cookies.length,
            error: testError.message,
            message: "❌ Помилка перевірки cookies",
          });
        }
      } catch (error) {
        res.status(500).json({
          success: false,
          error: error.message,
        });
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
    // Остатки товарів - щогодини о 00 хвилині
    // cron.schedule("0 * * * *", async () => {
    //   if (this.isRunning) {
    //     console.log("🔄 Запуск запланированной синхронизации остатков...");
    //     await this.performFullSync();
    //   }
    // });

    // Оприбуткування - щогодини о 30 хвилині
    cron.schedule("30 * * * *", async () => {
      if (this.isRunning) {
        console.log("🔄 Запуск запланированной синхронизации оприходований...");
        await this.fetchPostings();
      }
    });

    // Переміщення - кожні 2 години о 15 хвилині
    cron.schedule("15 */2 * * *", async () => {
      if (this.isRunning) {
        console.log("🔄 Запуск запланированной синхронизации перемещений...");
        await this.fetchMoves();
      }
    });

    // Списання - кожні 4 години о 25 хвилині
    cron.schedule("25 */4 * * *", async () => {
      if (this.isRunning) {
        console.log("🔄 Запуск запланированной синхронизации списаний...");
        await this.fetchOutcomes();
      }
    });

    // Продажі - кожні 6 годин о 35 хвилині
    cron.schedule("35 */6 * * *", async () => {
      if (this.isRunning) {
        console.log("🔄 Запуск запланированной синхронизации продаж...");
        await this.fetchSales();
      }
    });

    // Оновлення cookies кожні 10 хвилин
    cron.schedule("*/10 * * * *", async () => {
      console.log("⏰ Планове оновлення cookies...");
      await this.refreshCookiesAutomatically();
    });

    console.log("   - Оновлення cookies: кожні 10 хвилин");
    console.log("⏰ Планировщик настроен:");
    console.log("   - Остатки: каждый час");
    console.log("   - Оприходования: каждый час (+30 мин)");
    console.log("   - Перемещения: каждые 2 часа");
    console.log("   - Списания: каждые 4 часа");
    console.log("   - Продажи: каждые 6 часов");
  }

  async createPostingsTable() {
    if (!this.bigquery) {
      console.log("❌ BigQuery не инициализирована");
      return false;
    }

    try {
      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const tableName = `${process.env.BIGQUERY_TABLE}_postings`;
      const table = dataset.table(tableName);

      const [tableExists] = await table.exists();

      if (tableExists) {
        console.log("✅ Таблица истории оприходований уже существует");
        return true;
      }

      console.log("🔨 Создаем таблицу истории оприходований...");

      const schema = [
        { name: "posting_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "posting_label", type: "STRING", mode: "NULLABLE" },
        { name: "posting_created_at", type: "TIMESTAMP", mode: "REQUIRED" },
        { name: "created_by_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "created_by_name", type: "STRING", mode: "NULLABLE" },
        { name: "supplier_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "supplier_name", type: "STRING", mode: "NULLABLE" },
        { name: "warehouse_id", type: "INTEGER", mode: "REQUIRED" }, // ✅ УЖЕ ЕСТЬ!
        { name: "warehouse_title", type: "STRING", mode: "NULLABLE" },
        { name: "product_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "product_title", type: "STRING", mode: "REQUIRED" },
        { name: "product_code", type: "STRING", mode: "NULLABLE" },
        { name: "product_article", type: "STRING", mode: "NULLABLE" },
        { name: "uom_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "uom_title", type: "STRING", mode: "NULLABLE" },
        { name: "uom_description", type: "STRING", mode: "NULLABLE" },
        { name: "amount", type: "FLOAT", mode: "REQUIRED" },
        { name: "price", type: "FLOAT", mode: "NULLABLE" },
        { name: "is_serial", type: "BOOLEAN", mode: "NULLABLE" },
        { name: "posting_description", type: "STRING", mode: "NULLABLE" },
        { name: "document_number", type: "STRING", mode: "NULLABLE" },
        { name: "sync_id", type: "STRING", mode: "NULLABLE" },
        { name: "updated_at", type: "TIMESTAMP", mode: "REQUIRED" },
      ];

      await table.create({ schema, location: "EU" });
      console.log("✅ Таблица истории оприходований создана");
      return true;
    } catch (error) {
      console.error("❌ Ошибка создания таблицы постинга:", error.message);
      return false;
    }
  }

  async fetchPostings() {
    console.log(
      "🔄 Начинается синхронизация истории оприходований с мая 2022..."
    );

    const syncStart = Date.now();
    const errors = [];
    let totalPostings = 0;
    let processedProducts = 0;

    try {
      await this.fetchEmployees();
      await this.fetchSuppliersFromPostings();

      const warehouses = await this.fetchWarehouses();
      console.log(
        `📍 Найдено ${warehouses.length} складів для отримання історії`
      );

      const allPostingsData = [];
      const startTime = 1651363200000;
      const endTime = Date.now();

      for (const warehouse of warehouses) {
        try {
          console.log(
            `\n📦 Обробка складу: ${warehouse.title} (ID: ${warehouse.id})`
          );

          const warehousePostings = await this.fetchWarehousePostings(
            warehouse.id,
            startTime,
            endTime
          );

          if (warehousePostings.length > 0) {
            for (const posting of warehousePostings) {
              for (const product of posting.products) {
                const postingItem = {
                  posting_id: posting.id,
                  posting_label: posting.id_label || "",
                  posting_created_at: new Date(
                    posting.created_at
                  ).toISOString(),
                  created_by_id: posting.created_by_id,
                  created_by_name: this.getEmployeeName(posting.created_by_id),
                  supplier_id: posting.supplier_id,
                  supplier_name: await this.getSupplierName(
                    posting.supplier_id
                  ),
                  warehouse_id: warehouse.id, // ✅ КРИТИЧНО - добавляем ID склада!
                  warehouse_title: warehouse.title,
                  product_id: product.id,
                  product_title: product.title,
                  product_code: product.code || "",
                  product_article: product.article || "",
                  uom_id: product.uom?.id || null,
                  uom_title: product.uom?.title || "",
                  uom_description: product.uom?.description || "",
                  amount: product.amount,
                  price: product.price || 0,
                  is_serial: product.is_serial || false,
                  posting_description: posting.description || "",
                  document_number: posting.document_number || "",
                  sync_id: Date.now().toString(),
                  updated_at: new Date().toISOString(),
                };

                allPostingsData.push(postingItem);
                processedProducts++;
              }
            }
            totalPostings += warehousePostings.length;
          }

          console.log(
            `✅ Склад оброблено: ${warehousePostings.length} оприбуткувань`
          );
          await this.sleep(100);
        } catch (error) {
          const errorMsg = `Помилка складу ${warehouse.title}: ${error.message}`;
          console.error(`❌ ${errorMsg}`);
          errors.push(errorMsg);
        }
      }

      console.log(`\n📊 === ПІДСУМОК ПО ІСТОРІЇ (травень 2022 - зараз) ===`);
      console.log(`Оброблено складів: ${warehouses.length}`);
      console.log(`Знайдено оприбуткувань: ${totalPostings}`);
      console.log(`Оброблено позицій товарів: ${processedProducts}`);

      if (allPostingsData.length > 0) {
        console.log(
          `\n💾 Збереження ${allPostingsData.length} записів історії в BigQuery...`
        );
        await this.savePostingsToBigQuery(allPostingsData);
      }

      return {
        success: true,
        totalPostings,
        processedProducts,
        errors,
        duration: Date.now() - syncStart,
      };
    } catch (error) {
      console.error(
        `❌ Критична помилка синхронізації історії: ${error.message}`
      );
      throw error;
    }
  }

  async fetchWarehousePostings(warehouseId, startTime, endTime) {
    const options = {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
      },
    };

    let allPostings = [];
    let page = 1;
    const perPage = 50;
    let hasMore = true;
    let consecutiveEmptyPages = 0;

    const startTimestamp = startTime || 1651363200000;
    const endTimestamp = endTime || Date.now();

    console.log(`   📡 Получение оприходований для склада ${warehouseId}`);
    console.log(
      `   📅 Период: ${new Date(startTimestamp).toISOString()} - ${new Date(
        endTimestamp
      ).toISOString()}`
    );

    while (hasMore && consecutiveEmptyPages < 3) {
      try {
        const url = `https://api.roapp.io/warehouse/postings/?page=${page}&warehouse_ids[]=${warehouseId}&per_page=${perPage}`;

        console.log(`   📄 Страница ${page}: ${url}`);

        const response = await fetch(url, options);

        if (!response.ok) {
          throw new Error(
            `HTTP ${response.status} для склада ${warehouseId}, страница ${page}`
          );
        }

        const data = await response.json();
        const postings = data.data || [];

        console.log(
          `   📊 Страница ${page}: получено ${postings.length} оприходований`
        );

        if (postings.length === 0) {
          consecutiveEmptyPages++;
          console.log(
            `   ⚠️ Пустая страница ${page}, счетчик: ${consecutiveEmptyPages}/3`
          );

          if (consecutiveEmptyPages >= 3) {
            console.log(`   ✅ Три пустые страницы подряд, завершаем`);
            hasMore = false;
          } else {
            page++;
          }
        } else {
          consecutiveEmptyPages = 0; // Сбрасываем счетчик
          allPostings = allPostings.concat(postings);

          console.log(
            `   📈 Всего загружено: ${allPostings.length} оприходований`
          );

          // Продолжаем пагинацию
          page++;
        }

        await this.sleep(100);
      } catch (error) {
        console.error(
          `   ❌ Ошибка получения страницы ${page}: ${error.message}`
        );
        hasMore = false;
      }
    }

    if (allPostings.length > 0) {
      const allDates = allPostings.map((p) => new Date(p.created_at));
      const earliestDate = new Date(Math.min(...allDates));
      const latestDate = new Date(Math.max(...allDates));

      console.log(`   ✅ ИТОГО для склада ${warehouseId}:`);
      console.log(`   - Всего оприходований: ${allPostings.length}`);
      console.log(
        `   - Фактический период: ${
          earliestDate.toISOString().split("T")[0]
        } - ${latestDate.toISOString().split("T")[0]}`
      );
    }

    return allPostings;
  }
  async savePostingsToBigQuery(data) {
    if (!this.bigquery || !data.length) return;

    try {
      await this.createPostingsTable();

      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const tableName = `${process.env.BIGQUERY_TABLE}_postings`;
      const table = dataset.table(tableName);

      const syncId = Date.now().toString();

      // ДЕДУПЛИКАЦИЯ: створюємо унікальний ключ для кожного запису
      const uniqueRecords = new Map();

      data.forEach((item) => {
        // Унікальний ключ: posting_id + product_id + warehouse_id
        const uniqueKey = `${item.posting_id}_${item.product_id}_${item.warehouse_id}`;

        // Якщо запис з таким ключем вже є, підсумовуємо кількість
        if (uniqueRecords.has(uniqueKey)) {
          const existing = uniqueRecords.get(uniqueKey);
          existing.amount += item.amount;
        } else {
          uniqueRecords.set(uniqueKey, {
            ...item,
            sync_id: syncId,
          });
        }
      });

      const enhancedData = Array.from(uniqueRecords.values());

      console.log(
        `📊 Вхідних записів: ${data.length}, унікальних: ${enhancedData.length}`
      );

      // СПОЧАТКУ видаляємо всі старі дані
      console.log("🗑️ Видалення всіх старих даних оприбуткувань...");
      try {
        const deleteQuery = `
          DELETE FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${tableName}\` 
          WHERE TRUE
        `;

        const [deleteJob] = await this.bigquery.createQueryJob({
          query: deleteQuery,
          location: "EU",
        });
        await deleteJob.getQueryResults();
        console.log("✅ Старі дані оприбуткувань видалено");
      } catch (deleteError) {
        console.log("⚠️ Не вдалося видалити старі дані:", deleteError.message);
      }

      console.log("📊 Вставка унікальних даних оприбуткувань в BigQuery...");

      const batchSize = 500;
      let insertedCount = 0;

      for (let i = 0; i < enhancedData.length; i += batchSize) {
        const batch = enhancedData.slice(i, i + batchSize);

        try {
          await table.insert(batch);
          insertedCount += batch.length;
          console.log(
            `📊 Вставлено ${insertedCount}/${enhancedData.length} записів`
          );
        } catch (error) {
          console.error(`❌ Помилка вставки батчу: ${error.message}`);
        }
      }

      if (insertedCount > 0) {
        console.log(
          `✅ Збережено ${insertedCount} унікальних записів оприбуткувань в BigQuery`
        );
      }
    } catch (error) {
      console.error(
        "❌ Критична помилка збереження оприбуткувань:",
        error.message
      );
      throw error;
    }
  }

  async fetchWarehouses() {
    const options = {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
      },
    };

    // Список нужных локаций
    const activeBranchIds = [
      134397, 137783, 170450, 198255, 171966, 189625, 147848, 186381, 185929,
      155210, 158504, 177207, 205571, 154905, 184657,
    ];

    const allWarehouses = [];

    // Запрашиваем склады для каждой локации
    for (const branchId of activeBranchIds) {
      try {
        const response = await fetch(
          `https://api.roapp.io/warehouse/?branch_id=${branchId}`,
          options
        );

        if (!response.ok) {
          console.error(`HTTP ${response.status} для локации ${branchId}`);
          continue;
        }

        const data = await response.json();
        const warehouses = data.data || [];

        console.log(
          `📍 Локация ${branchId}: найдено ${warehouses.length} складов`
        );
        allWarehouses.push(...warehouses);

        // Задержка между запросами
        // await this.sleep(100);
      } catch (error) {
        console.error(
          `Ошибка получения складов для локации ${branchId}:`,
          error.message
        );
      }
    }

    console.log(
      `✅ Всего загружено ${allWarehouses.length} складов из ${activeBranchIds.length} локаций`
    );
    return allWarehouses;
  }

  // НОВЫЙ МЕТОД: Фильтрация активных складов
  async fetchActiveWarehouses() {
    const allWarehouses = await this.fetchWarehouses();

    const activeWarehouses = allWarehouses.filter((warehouse) => {
      // Проверяем по branch_id (если есть)
      if (
        warehouse.branch_id &&
        this.excludedBranchIds.includes(warehouse.branch_id)
      ) {
        return false;
      }

      // Проверяем по началу названия склада
      const title = warehouse.title || "";
      return (
        !title.startsWith("001_G_CAR_UA") &&
        !title.startsWith("002_G_CAR_PL") &&
        !title.startsWith("003_INSURANCE CASES")
      );
    });

    console.log(
      `📍 Всего складов: ${allWarehouses.length}, активных: ${
        activeWarehouses.length
      }, исключено: ${allWarehouses.length - activeWarehouses.length}`
    );

    return activeWarehouses;
  }
  // НОВЫЙ МЕТОД: Получение складов по branch_id
  async fetchWarehousesByBranch(branchId) {
    const options = {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
      },
    };

    const url = `https://api.roapp.io/warehouse/?branch_id=${branchId}`;
    console.log(`📡 Запрос складов для филиала ${branchId}: ${url}`);

    const response = await fetch(url, options);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    const data = await response.json();
    console.log(
      `✅ Получено ${data.data?.length || 0} складов для филиала ${branchId}`
    );

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
    let hasMore = true;

    console.log(`📡 Получение товаров для склада ${warehouseId}...`);

    while (hasMore) {
      try {
        const url = `https://api.roapp.io/warehouse/goods/${warehouseId}?exclude_zero_residue=false&page=${page}`;

        const response = await fetch(url, options);

        if (!response.ok) {
          throw new Error(
            `HTTP ${response.status} для склада ${warehouseId}, страница ${page}`
          );
        }

        const data = await response.json();
        const goods = data.data || [];

        console.log(`📄 Страница ${page}: получено ${goods.length} товаров`);

        if (goods.length === 0) {
          console.log(`✅ Страница ${page} пустая, завершаем загрузку`);
          hasMore = false;
        } else {
          allGoods = allGoods.concat(goods);
          console.log(`📈 Всего загружено: ${allGoods.length} товаров`);

          // Якщо отримали менше 50 товарів - це остання сторінка
          if (goods.length < 50) {
            console.log(
              `✅ Получена последняя страница (${goods.length} товаров)`
            );
            hasMore = false;
          } else {
            page++;
          }
        }
      } catch (error) {
        console.error(`❌ Ошибка получения страницы ${page}:`, error.message);
        hasMore = false;
      }
    }

    console.log(
      `✅ Склад ${warehouseId}: всего получено ${allGoods.length} товаров`
    );
    return allGoods;
  }

  async fetchGoodsFlowForProduct(productId, startDate, endDate, cookies) {
    if (!cookies) {
      throw new Error("Потрібні cookies для доступу до goods-flow");
    }

    const options = {
      method: "GET",
      headers: {
        accept: "application/json",
        cookie: cookies,
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      },
    };

    let allItems = [];
    let page = 1;
    const pageSize = 100;
    let consecutiveErrors = 0;

    while (page <= 100 && consecutiveErrors < 3) {
      try {
        const url = `https://web.roapp.io/app/warehouse/get-goods-flow-items?page=${page}&pageSize=${pageSize}&id=${productId}&startDate=${startDate}&endDate=${endDate}`;

        console.log(`   📄 Goods-flow сторінка ${page}`);

        const response = await fetch(url, options);

        if (!response.ok) {
          // ✅ ДОДАНО: детальніше логування помилки
          console.error(`   ❌ HTTP ${response.status} на сторінці ${page}`);
          console.error(`   📋 URL: ${url}`);

          // Спробуємо прочитати тіло відповіді
          try {
            const errorText = await response.text();
            console.error(
              `   📋 Відповідь сервера:`,
              errorText.substring(0, 500)
            );
          } catch (e) {
            console.error(`   📋 Не вдалося прочитати тіло відповіді`);
          }

          if (response.status === 401 || response.status === 403) {
            throw new Error(
              `HTTP ${response.status}: Авторізація невалідна - ПОТРІБНО ОНОВИТИ COOKIES`
            );
          }

          if (response.status === 500) {
            throw new Error(
              `HTTP 500: Внутрішня помилка Remonline API - COOKIES МОЖЛИВО ЗАСТАРІЛІ`
            );
          }

          consecutiveErrors++;
          if (consecutiveErrors >= 3) {
            console.log(`   ⚠️ 3 помилки поспіль, припиняємо`);
            break;
          }
          continue;
        }

        const result = await response.json();
        const items = result.data || [];

        if (page === 1 && items.length > 0) {
          console.log("📋 Перший goods-flow запис від Remonline API:", {
            warehouse_id: items[0].warehouse_id,
            warehouse_title: items[0].warehouse_title,
            relation_id_label: items[0].relation_id_label,
            employee_id: items[0].employee_id,
            amount: items[0].amount,
          });
        }
        console.log(`   ✅ Отримано ${items.length} записів`);

        if (items.length === 0) break;

        allItems = allItems.concat(items);

        if (items.length < pageSize) break;

        page++;
        consecutiveErrors = 0;

        await this.sleep(300);
      } catch (error) {
        console.error(`   ❌ Помилка на сторінці ${page}:`, error.message);

        if (
          error.message.includes("401") ||
          error.message.includes("403") ||
          error.message.includes("500")
        ) {
          throw error; // Кидаємо далі щоб показати в UI
        }

        consecutiveErrors++;
        if (consecutiveErrors >= 3) break;
      }
    }

    console.log(`📊 Всього goods-flow операцій: ${allItems.length}`);
    return allItems;
  }

  async fetchEmployees() {
    const options = {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
      },
    };

    try {
      console.log("📡 Получение списка сотрудников...");
      const response = await fetch("https://api.roapp.io/employees/", options);

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const data = await response.json();
      const employees = data.data || [];

      console.log(
        `🔍 ОТЛАДКА: Получено ${employees.length} сотрудников от API`
      );
      if (employees.length > 0) {
        console.log(`🔍 ОТЛАДКА: Первый сотрудник:`, employees[0]);
      }

      // Кешируем сотрудников
      this.employeesCache.clear();
      employees.forEach((employee) => {
        const fullName = `${employee.first_name || ""} ${
          employee.last_name || ""
        }`.trim();

        console.log(
          `🔍 ОТЛАДКА: Обрабатываем сотрудника ID ${employee.id}: "${fullName}"`
        );

        this.employeesCache.set(employee.id, {
          id: employee.id,
          fullName: fullName || "Неизвестно",
          firstName: employee.first_name || "",
          lastName: employee.last_name || "",
          position: employee.position || "",
          email: employee.email || "",
        });
      });

      console.log(`✅ Загружено ${employees.length} сотрудников в кеш`);
      console.log(
        `🔍 ОТЛАДКА: Размер кеша после загрузки: ${this.employeesCache.size}`
      );

      return employees;
    } catch (error) {
      console.error("❌ Ошибка получения сотрудников:", error.message);
      return [];
    }
  }

  async getCookies() {
    const cookies = this.userCookies.get("shared_user");

    if (!cookies) {
      console.log("❌ Cookies відсутні в пам'яті");
      return null;
    }

    console.log(`✅ Cookies знайдено (${cookies.length} символів)`);
    return cookies;
  }
  getEmployeeName(employeeId) {
    console.log(
      `🔍 getEmployeeName вызван с ID: ${employeeId}, тип: ${typeof employeeId}`
    );

    if (!employeeId) {
      console.log(`🔍 employeeId пустой, возвращаем "Не указан"`);
      return "Не указан";
    }

    const employee = this.employeesCache.get(employeeId);
    console.log(`🔍 Найден сотрудник в кеше:`, employee);

    return employee ? employee.fullName : `ID: ${employeeId}`;
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

      console.log(
        `📍 Обробляємо ${warehouses.length} складів (виключено склад ${excludedWarehouseIds[0]})`
      );

      // const warehouses = await this.fetchWarehouses();
      console.log(`📍 Найдено ${warehouses.length} складов для обработки`);

      const batchSize = 20;

      for (let i = 0; i < warehouses.length; i += batchSize) {
        const warehouseBatch = warehouses.slice(i, i + batchSize);
        const batchData = [];

        for (const warehouse of warehouseBatch) {
          try {
            console.log(
              `\n📦 [${warehousesProcessed + 1}/${warehouses.length}] Склад: ${
                warehouse.title
              }`
            );

            const goodsInStock = await this.fetchWarehouseGoods(warehouse.id);

            if (goodsInStock.length > 0) {
              console.log(`   📊 Обработка ${goodsInStock.length} товаров...`);

              goodsInStock.forEach((item) => {
                uniqueProducts.add(item.title);

                const processedItem = {
                  warehouse_id: warehouse.id,
                  warehouse_title: warehouse.title || "Неизвестный склад",
                  warehouse_type: warehouse.type || "product",
                  warehouse_is_global: warehouse.is_global || false,
                  good_id: item.id,
                  product_id: item.product_id || item.id,
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
                batchData.push(processedItem);
              });

              totalGoods += goodsInStock.length;
            }

            warehousesProcessed++;
          } catch (error) {
            const errorMsg = `Ошибка: ${warehouse.title} - ${error.message}`;
            console.error(`❌ ${errorMsg}`);
            errors.push(errorMsg);
            warehousesProcessed++;
          }
        }

        if (batchData.length > 0) {
          console.log(
            `\n💾 Сохранение порции ${batchData.length} записей в BigQuery...`
          );
          await this.saveToBigQuery(batchData);

          if (global.gc) {
            global.gc();
          }
        }

        console.log(
          `📊 Прогресс: ${warehousesProcessed}/${warehouses.length} складов`
        );
      }

      this.lastSyncData = {
        timestamp: new Date().toISOString(),
        warehousesProcessed,
        goodsFound: totalGoods,
        uniqueProducts: uniqueProducts.size,
        errors,
        duration: Date.now() - syncStart,
      };

      return this.lastSyncData;
    } catch (error) {
      console.error(`❌ ${error.message}`);
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
        { name: "product_id", type: "INTEGER", mode: "REQUIRED" },
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

      const syncId = Date.now().toString();
      const enhancedData = data.map((item) => ({
        ...item,
        sync_id: syncId,
      }));

      // Вставляємо порцію без видалення старих даних
      console.log("📊 Вставка порції даних в BigQuery...");

      const batchSize = 500;
      let insertedCount = 0;

      for (let i = 0; i < enhancedData.length; i += batchSize) {
        const batch = enhancedData.slice(i, i + batchSize);

        try {
          await table.insert(batch);
          insertedCount += batch.length;
          console.log(
            `📊 Вставлено ${insertedCount}/${enhancedData.length} записей`
          );
        } catch (error) {
          console.error(`❌ Ошибка вставки батча:`, error.message);
        }
      }

      console.log(`✅ Сохранено ${insertedCount} записей`);
    } catch (error) {
      console.error("❌ Ошибка сохранения в BigQuery:", error.message);
      throw error;
    }
  }

  async clearPostingsTable() {
    if (!this.bigquery) return;

    try {
      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const table = dataset.table(`${process.env.BIGQUERY_TABLE}_postings`);

      // Проверяем существование таблицы перед попыткой очистки
      const [exists] = await table.exists();
      if (!exists) {
        console.log("ℹ️ Таблица постингов не существует, очистка не требуется");
        return;
      }

      const query = `
        DELETE FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_postings\`
        WHERE TRUE
      `;

      const [job] = await this.bigquery.createQueryJob({
        query,
        location: "EU",
      });
      await job.getQueryResults();

      console.log("✅ Старые данные истории удалены");
    } catch (error) {
      console.error("❌ Ошибка очистки таблицы истории:", error.message);
      // НЕ выбрасываем ошибку, чтобы процесс продолжился
    }
  }

  // Добавить методы для работы с поставщиками
  async fetchSupplierInfo(supplierId) {
    if (!supplierId) return null;

    const options = {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
      },
    };

    try {
      // Пробуем получить как организацию
      console.log(`📡 Запрос организации ID: ${supplierId}`);
      let response = await fetch(
        `https://api.roapp.io/contacts/organizations/${supplierId}`,
        options
      );

      console.log(
        `📊 Ответ API организации ${supplierId}: статус ${response.status}`
      );

      if (response.ok) {
        const data = await response.json();
        console.log(`✅ Найдена организация ${supplierId}:`, data);
        return {
          id: supplierId,
          name: data.name || "Неизвестная организация",
          address: data.address || "",
          type: "organization",
        };
      }

      // Пробуем как частное лицо
      console.log(`📡 Запрос частного лица ID: ${supplierId}`);
      response = await fetch(
        `https://api.roapp.io/contacts/people/${supplierId}`,
        options
      );

      console.log(
        `📊 Ответ API частного лица ${supplierId}: статус ${response.status}`
      );

      if (response.ok) {
        const data = await response.json();
        console.log(`✅ Найдено частное лицо ${supplierId}:`, data);
        const fullName = `${data.first_name || ""} ${
          data.last_name || ""
        }`.trim();
        return {
          id: supplierId,
          name: fullName || "Неизвестное лицо",
          address: "",
          type: "person",
        };
      }

      // Если не найден
      const orgError = await response.text();
      console.log(
        `❌ Поставщик ID ${supplierId} не найден. Ошибка: ${orgError}`
      );
      return null;
    } catch (error) {
      console.error(
        `❌ Ошибка сети при запросе поставщика ${supplierId}:`,
        error.message
      );
      return null;
    }
  }

  async fetchSuppliersFromPostings() {
    if (!this.bigquery) return;

    try {
      console.log(
        "📡 Получение уникальных поставщиков из истории оприходований..."
      );

      const query = `
            SELECT DISTINCT supplier_id
            FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_postings\`
            WHERE supplier_id IS NOT NULL
        `;

      const [rows] = await this.bigquery.query({ query, location: "EU" });
      const supplierIds = rows.map((row) => row.supplier_id);

      console.log(`📊 Найдено ${supplierIds.length} уникальных поставщиков`);

      // Загружаем информацию о каждом поставщике
      for (const supplierId of supplierIds) {
        if (!this.suppliersCache.has(supplierId)) {
          const supplierInfo = await this.fetchSupplierInfo(supplierId);
          if (supplierInfo) {
            this.suppliersCache.set(supplierId, supplierInfo);
          } else {
            // Сохраняем даже неудачные попытки, чтобы не повторять запросы
            this.suppliersCache.set(supplierId, {
              id: supplierId,
              name: `ID: ${supplierId}`,
              address: "",
              type: "unknown",
            });
          }

          // Задержка между запросами
          await this.sleep(100);
        }
      }

      console.log(`✅ Загружено ${this.suppliersCache.size} поставщиков в кеш`);
    } catch (error) {
      console.error("❌ Ошибка получения поставщиков:", error.message);
    }
  }

  async getSupplierName(supplierId) {
    console.log(
      `🔧 getSupplierName вызван с ID: ${supplierId}, тип: ${typeof supplierId}`
    );

    if (!supplierId) {
      console.log(`🔧 supplierId пустой, возвращаем "Не указан"`);
      return "Не указан";
    }

    // Проверяем кеш
    let supplier = this.suppliersCache.get(supplierId);

    // Если в кеше нет - загружаем прямо сейчас
    if (!supplier) {
      console.log(`🔧 Поставщик ${supplierId} не найден в кеше, загружаем...`);
      supplier = await this.fetchSupplierInfo(supplierId);

      if (supplier) {
        this.suppliersCache.set(supplierId, supplier);
        console.log(
          `🔧 Поставщик ${supplierId} загружен и добавлен в кеш: "${supplier.name}"`
        );
        return supplier.name;
      } else {
        // Помечаем как неизвестного
        const unknownSupplier = {
          id: supplierId,
          name: `ID: ${supplierId}`,
          type: "unknown",
        };
        this.suppliersCache.set(supplierId, unknownSupplier);
        console.log(
          `🔧 Поставщик ${supplierId} не найден в API, сохраняем как unknown`
        );
        return `ID: ${supplierId}`;
      }
    }

    console.log(`🔧 Поставщик ${supplierId} найден в кеше: "${supplier.name}"`);
    return supplier.name;
  }

  // Метод синхронизации перемещений
  async fetchMoves() {
    console.log("🔄 Начинается синхронизация истории перемещений...");

    const syncStart = Date.now();
    const errors = [];
    let totalMoves = 0;
    let processedProducts = 0;

    try {
      await this.fetchEmployees();

      // Используем массив branchIds из вашего кода
      const branchIds = [
        // { name: "001_G_CAR_UA", id: 112954 },
        // { name: "002_G_CAR_PL", id: 123343 },
        // { name: "003_INSURANCE CASES", id: 178097 },
        { name: "01.1_G_CAR_KY", id: 134397 },
        { name: "02.1_G_CAR_LV", id: 137783 },
        { name: "02.2_G_CAR_LV", id: 170450 },
        { name: "02.3_G_CAR_LV", id: 198255 },
        { name: "03_G_CAR_OD", id: 171966 },
        { name: "07_G_CAR_VN", id: 189625 },
        { name: "08_G_CAR_PLT", id: 147848 },
        { name: "09_G_CAR_IF", id: 186381 },
        { name: "15_G_CAR_CK", id: 185929 },
        { name: "16_G_CAR_CV", id: 155210 },
        { name: "18.1_G_CAR_LU", id: 158504 },
        { name: "18.2_G_CAR_LU", id: 177207 },
        { name: "18.3_G_CAR_LU", id: 205571 },
        { name: "19.1_G_CAR_RV", id: 154905 },
        { name: "19.2_G_CAR_RV", id: 184657 },
      ];

      console.log(
        `📍 Будем обрабатывать ${branchIds.length} локаций для получения перемещений`
      );

      const allMovesData = [];

      // Обрабатываем перемещения по локациям, а не по складам
      for (const branch of branchIds) {
        try {
          console.log(
            `\n📦 Обработка локации: ${branch.name} (ID: ${branch.id})`
          );

          const branchMoves = await this.fetchBranchMoves(branch.id);

          if (branchMoves.length > 0) {
            for (const move of branchMoves) {
              for (const product of move.products) {
                const moveItem = {
                  move_id: move.id,
                  move_label: move.id_label || "",
                  move_created_at: new Date(move.created_at).toISOString(),
                  created_by_id: move.created_by_id,
                  created_by_name: this.getEmployeeName(move.created_by_id),
                  warehouse_id: move.warehouse_id,
                  source_warehouse_title: move.source_warehouse_title || "",
                  target_warehouse_title: move.target_warehouse_title || "",
                  product_id: product.id,
                  product_title: product.title,
                  product_code: product.code || "",
                  product_article: product.article || "",
                  uom_id: product.uom?.id || null,
                  uom_title: product.uom?.title || "",
                  uom_description: product.uom?.description || "",
                  amount: product.amount,
                  is_serial: product.is_serial || false,
                  move_description: move.description || "",
                  move_cost: move.cost || 0,
                  sync_id: Date.now().toString(),
                  updated_at: new Date().toISOString(),
                };

                allMovesData.push(moveItem);
                processedProducts++;
              }
            }
            totalMoves += branchMoves.length;
          }

          await this.sleep(100);
        } catch (error) {
          const errorMsg = `Ошибка: ${branch.name} - ${error.message}`;
          console.error(`❌ ${errorMsg}`);
          errors.push(errorMsg);
        }
      }

      console.log(`\n📊 === ИТОГО ПО ПЕРЕМЕЩЕНИЯМ ===`);
      console.log(`Найдено перемещений: ${totalMoves}`);
      console.log(`Обработано позиций товаров: ${processedProducts}`);

      if (allMovesData.length > 0) {
        console.log(
          `\n💾 Сохранение ${allMovesData.length} записей перемещений в BigQuery...`
        );
        await this.saveMovesToBigQuery(allMovesData);
      }

      return {
        success: true,
        totalMoves,
        processedProducts,
        errors,
        duration: Date.now() - syncStart,
      };
    } catch (error) {
      console.error(
        `❌ Критическая ошибка синхронизации перемещений: ${error.message}`
      );
      throw error;
    }
  }

  // Новый метод для получения перемещений по branch_id
  async fetchBranchMoves(branchId, startTime, endTime) {
    const options = {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
      },
    };

    let allMoves = [];
    let page = 1;
    const perPage = 50;
    let hasMore = true;
    let consecutiveEmptyPages = 0;

    const startTimestamp = startTime || 1651363200000;
    const endTimestamp = endTime || Date.now();

    console.log(`   📡 Получение перемещений для локации ${branchId}`);

    while (hasMore && consecutiveEmptyPages < 3) {
      try {
        const url = `https://api.roapp.io/warehouse/moves/?branch_id=${branchId}&page=${page}&per_page=${perPage}`;
        console.log(`   📄 Страница ${page}: ${url}`);

        const response = await fetch(url, options);

        if (!response.ok) {
          throw new Error(
            `HTTP ${response.status} для локации ${branchId}, страница ${page}`
          );
        }

        const data = await response.json();
        const moves = data.data || [];

        console.log(
          `   📊 Страница ${page}: получено ${moves.length} перемещений`
        );

        if (moves.length === 0) {
          consecutiveEmptyPages++;
          console.log(
            `   ⚠️ Пустая страница ${page}, счетчик: ${consecutiveEmptyPages}/3`
          );

          if (consecutiveEmptyPages >= 3) {
            hasMore = false;
          } else {
            page++;
          }
        } else {
          consecutiveEmptyPages = 0;

          // Фильтруем по времени
          const filteredMoves = moves.filter((move) => {
            const moveTime = move.created_at;
            return moveTime >= startTimestamp && moveTime <= endTimestamp;
          });

          allMoves = allMoves.concat(filteredMoves);
          console.log(`   📈 Всего загружено: ${allMoves.length} перемещений`);

          page++;
        }

        await this.sleep(100);
      } catch (error) {
        console.error(
          `   ❌ Ошибка получения страницы ${page}: ${error.message}`
        );
        hasMore = false;
      }
    }

    console.log(
      `   ✅ ИТОГО перемещений для локации ${branchId}: ${allMoves.length}`
    );
    return allMoves;
  }
  async createMovesTable() {
    if (!this.bigquery) {
      console.log("❌ BigQuery не инициализирована");
      return false;
    }

    try {
      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const tableName = `${process.env.BIGQUERY_TABLE}_moves`;
      const table = dataset.table(tableName);

      const [tableExists] = await table.exists();
      if (tableExists) {
        console.log("✅ Таблица перемещений уже существует");
        return true;
      }

      console.log("🔨 Создаем таблицу перемещений...");

      const schema = [
        { name: "move_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "move_label", type: "STRING", mode: "NULLABLE" },
        { name: "move_created_at", type: "TIMESTAMP", mode: "REQUIRED" },
        { name: "created_by_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "created_by_name", type: "STRING", mode: "NULLABLE" },
        { name: "warehouse_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "source_warehouse_title", type: "STRING", mode: "NULLABLE" },
        { name: "target_warehouse_title", type: "STRING", mode: "NULLABLE" },
        { name: "product_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "product_title", type: "STRING", mode: "REQUIRED" },
        { name: "product_code", type: "STRING", mode: "NULLABLE" },
        { name: "product_article", type: "STRING", mode: "NULLABLE" },
        { name: "uom_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "uom_title", type: "STRING", mode: "NULLABLE" },
        { name: "uom_description", type: "STRING", mode: "NULLABLE" },
        { name: "amount", type: "FLOAT", mode: "REQUIRED" },
        { name: "is_serial", type: "BOOLEAN", mode: "NULLABLE" },
        { name: "move_description", type: "STRING", mode: "NULLABLE" },
        { name: "move_cost", type: "FLOAT", mode: "NULLABLE" },
        { name: "sync_id", type: "STRING", mode: "NULLABLE" },
        { name: "updated_at", type: "TIMESTAMP", mode: "REQUIRED" },
      ];

      await table.create({ schema, location: "EU" });
      console.log("✅ Таблица перемещений создана");
      return true;
    } catch (error) {
      console.error("❌ Ошибка создания таблицы перемещений:", error.message);
      return false;
    }
  }

  //  Метод створення таблиці

  async createOutcomesTable() {
    if (!this.bigquery) {
      console.log("❌ BigQuery не ініціалізована");
      return false;
    }

    try {
      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const tableName = `${process.env.BIGQUERY_TABLE}_outcomes`;
      const table = dataset.table(tableName);

      const [tableExists] = await table.exists();
      if (tableExists) {
        console.log("✅ Таблиця списань вже існує");
        return true;
      }

      console.log("🔨 Створюємо таблицю списань...");

      const schema = [
        { name: "outcome_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "outcome_label", type: "STRING", mode: "NULLABLE" },
        { name: "outcome_created_at", type: "TIMESTAMP", mode: "REQUIRED" },
        { name: "created_by_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "created_by_name", type: "STRING", mode: "NULLABLE" },
        { name: "source_warehouse_title", type: "STRING", mode: "NULLABLE" },
        { name: "product_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "product_title", type: "STRING", mode: "REQUIRED" },
        { name: "product_code", type: "STRING", mode: "NULLABLE" },
        { name: "product_article", type: "STRING", mode: "NULLABLE" },
        { name: "uom_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "uom_title", type: "STRING", mode: "NULLABLE" },
        { name: "uom_description", type: "STRING", mode: "NULLABLE" },
        { name: "amount", type: "FLOAT", mode: "REQUIRED" },
        { name: "is_serial", type: "BOOLEAN", mode: "NULLABLE" },
        { name: "outcome_description", type: "STRING", mode: "NULLABLE" },
        { name: "outcome_cost", type: "FLOAT", mode: "NULLABLE" },
        { name: "sync_id", type: "STRING", mode: "NULLABLE" },
        { name: "updated_at", type: "TIMESTAMP", mode: "REQUIRED" },
      ];

      await table.create({ schema, location: "EU" });
      console.log("✅ Таблиця списань створена");
      return true;
    } catch (error) {
      console.error("❌ Помилка створення таблиці списань:", error.message);
      return false;
    }
  }

  // Метод збереження списань
  async saveOutcomesToBigQuery(data) {
    if (!this.bigquery || !data.length) return;

    try {
      await this.createOutcomesTable();

      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const tableName = `${process.env.BIGQUERY_TABLE}_outcomes`;
      const table = dataset.table(tableName);

      const syncId = Date.now().toString();

      const uniqueRecords = new Map();

      data.forEach((item) => {
        const uniqueKey = `${item.outcome_id}_${item.product_id}`;

        if (uniqueRecords.has(uniqueKey)) {
          const existing = uniqueRecords.get(uniqueKey);
          existing.amount += item.amount;
        } else {
          uniqueRecords.set(uniqueKey, {
            ...item,
            sync_id: syncId,
          });
        }
      });

      const enhancedData = Array.from(uniqueRecords.values());

      console.log(
        `📊 Вхідних записів: ${data.length}, унікальних: ${enhancedData.length}`
      );

      console.log("🗑️ Видалення всіх старих даних списань...");
      try {
        const deleteQuery = `
          DELETE FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${tableName}\` 
          WHERE TRUE
        `;

        const [deleteJob] = await this.bigquery.createQueryJob({
          query: deleteQuery,
          location: "EU",
        });
        await deleteJob.getQueryResults();
        console.log("✅ Старі дані списань видалено");
      } catch (deleteError) {
        console.log("⚠️ Не вдалося видалити старі дані:", deleteError.message);
      }

      console.log("📊 Вставка унікальних даних списань в BigQuery...");

      const batchSize = 500;
      let insertedCount = 0;

      for (let i = 0; i < enhancedData.length; i += batchSize) {
        const batch = enhancedData.slice(i, i + batchSize);

        try {
          await table.insert(batch);
          insertedCount += batch.length;
          console.log(
            `📊 Вставлено ${insertedCount}/${enhancedData.length} записів`
          );
        } catch (error) {
          console.error(`❌ Помилка вставки батчу: ${error.message}`);
        }
      }

      if (insertedCount > 0) {
        console.log(
          `✅ Збережено ${insertedCount} унікальних записів списань в BigQuery`
        );
      }
    } catch (error) {
      console.error("❌ Критична помилка збереження списань:", error.message);
      throw error;
    }
  }
  async saveMovesToBigQuery(data) {
    if (!this.bigquery || !data.length) return;

    try {
      await this.createMovesTable();

      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const tableName = `${process.env.BIGQUERY_TABLE}_moves`;
      const table = dataset.table(tableName);

      const syncId = Date.now().toString();
      const enhancedData = data.map((item) => ({
        ...item,
        sync_id: syncId,
      }));

      // СНАЧАЛА удаляем старые данные
      console.log("🗑️ Удаление старых данных перемещений...");
      try {
        const deleteQuery = `
                DELETE FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_moves\` 
                WHERE sync_id != @current_sync_id OR sync_id IS NULL
            `;

        const [deleteJob] = await this.bigquery.createQueryJob({
          query: deleteQuery,
          params: { current_sync_id: syncId },
          types: { current_sync_id: "STRING" },
          location: "EU",
        });
        await deleteJob.getQueryResults();
        console.log("✅ Старые данные перемещений удалены");
      } catch (deleteError) {
        console.log(
          "⚠️ Не удалось удалить старые данные перемещений:",
          deleteError.message
        );
      }

      // ЗАТЕМ вставляем новые данные
      console.log("📊 Вставка данных перемещений в BigQuery...");

      const batchSize = 500;
      let insertedCount = 0;

      for (let i = 0; i < data.length; i += batchSize) {
        const batch = data.slice(i, i + batchSize);

        try {
          await table.insert(batch);
          insertedCount += batch.length;
          console.log(
            `📊 Успешно вставлено ${insertedCount}/${data.length} записей перемещений`
          );
        } catch (error) {
          console.error(`❌ Ошибка вставки батча: ${error.message}`);
        }
      }

      if (insertedCount > 0) {
        console.log(
          `✅ Успешно сохранено ${insertedCount} записей перемещений в BigQuery`
        );
      }
    } catch (error) {
      console.error(
        "❌ Критическая ошибка сохранения перемещений:",
        error.message
      );
      throw error;
    }
  }

  // Метод для отримання списань
  async fetchOutcomes() {
    console.log("🔄 Начинается синхронизация истории списаний...");

    const syncStart = Date.now();
    const errors = [];
    let totalOutcomes = 0;
    let processedProducts = 0;

    try {
      await this.fetchEmployees();

      const branchIds = [
        { name: "01.1_G_CAR_KY", id: 134397 },
        { name: "02.1_G_CAR_LV", id: 137783 },
        { name: "02.2_G_CAR_LV", id: 170450 },
        { name: "02.3_G_CAR_LV", id: 198255 },
        { name: "03_G_CAR_OD", id: 171966 },
        { name: "07_G_CAR_VN", id: 189625 },
        { name: "08_G_CAR_PLT", id: 147848 },
        { name: "09_G_CAR_IF", id: 186381 },
        { name: "15_G_CAR_CK", id: 185929 },
        { name: "16_G_CAR_CV", id: 155210 },
        { name: "18.1_G_CAR_LU", id: 158504 },
        { name: "18.2_G_CAR_LU", id: 177207 },
        { name: "18.3_G_CAR_LU", id: 205571 },
        { name: "19.1_G_CAR_RV", id: 154905 },
        { name: "19.2_G_CAR_RV", id: 184657 },
      ];

      console.log(
        `📍 Будемо обробляти ${branchIds.length} локацій для отримання списань`
      );

      const allOutcomesData = [];

      for (const branch of branchIds) {
        try {
          console.log(
            `\n📦 Обробка локації: ${branch.name} (ID: ${branch.id})`
          );

          const branchOutcomes = await this.fetchBranchOutcomes(branch.id);

          if (branchOutcomes.length > 0) {
            for (const outcome of branchOutcomes) {
              for (const product of outcome.products) {
                const outcomeItem = {
                  outcome_id: outcome.id,
                  outcome_label: outcome.id_label || "",
                  outcome_created_at: new Date(
                    outcome.created_at
                  ).toISOString(),
                  created_by_id: outcome.created_by_id,
                  created_by_name: this.getEmployeeName(outcome.created_by_id),
                  source_warehouse_title: outcome.source_warehouse_title || "",
                  product_id: product.id,
                  product_title: product.title,
                  product_code: product.code || "",
                  product_article: product.article || "",
                  uom_id: product.uom?.id || null,
                  uom_title: product.uom?.title || "",
                  uom_description: product.uom?.description || "",
                  amount: product.amount,
                  is_serial: product.is_serial || false,
                  outcome_description: outcome.description || "",
                  outcome_cost: outcome.cost || 0,
                  sync_id: Date.now().toString(),
                  updated_at: new Date().toISOString(),
                };

                allOutcomesData.push(outcomeItem);
                processedProducts++;
              }
            }
            totalOutcomes += branchOutcomes.length;
          }

          await this.sleep(100);
        } catch (error) {
          const errorMsg = `Помилка: ${branch.name} - ${error.message}`;
          console.error(`❌ ${errorMsg}`);
          errors.push(errorMsg);
        }
      }

      console.log(`\n📊 === ПІДСУМОК ПО СПИСАННЯМ ===`);
      console.log(`Знайдено списань: ${totalOutcomes}`);
      console.log(`Оброблено позицій товарів: ${processedProducts}`);

      if (allOutcomesData.length > 0) {
        console.log(
          `\n💾 Збереження ${allOutcomesData.length} записів списань в BigQuery...`
        );
        await this.saveOutcomesToBigQuery(allOutcomesData);
      }

      return {
        success: true,
        totalOutcomes,
        processedProducts,
        errors,
        duration: Date.now() - syncStart,
      };
    } catch (error) {
      console.error(
        `❌ Критична помилка синхронізації списань: ${error.message}`
      );
      throw error;
    }
  }

  async fetchBranchOutcomes(branchId, startTime, endTime) {
    const options = {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
      },
    };

    let allOutcomes = [];
    let page = 1;
    const perPage = 50;
    let hasMore = true;
    let consecutiveEmptyPages = 0;

    const startTimestamp = startTime || 1651363200000;
    const endTimestamp = endTime || Date.now();

    console.log(`   📡 Отримання списань для локації ${branchId}`);

    while (hasMore && consecutiveEmptyPages < 3) {
      try {
        const url = `https://api.roapp.io/warehouse/outcome-transactions/?branch_id=${branchId}&page=${page}&per_page=${perPage}`;
        console.log(`   📄 Сторінка ${page}: ${url}`);

        const response = await fetch(url, options);

        if (!response.ok) {
          throw new Error(
            `HTTP ${response.status} для локації ${branchId}, сторінка ${page}`
          );
        }

        const data = await response.json();
        const outcomes = data.data || [];

        console.log(
          `   📊 Сторінка ${page}: отримано ${outcomes.length} списань`
        );

        if (outcomes.length === 0) {
          consecutiveEmptyPages++;
          console.log(
            `   ⚠️ Пуста сторінка ${page}, лічильник: ${consecutiveEmptyPages}/3`
          );

          if (consecutiveEmptyPages >= 3) {
            hasMore = false;
          } else {
            page++;
          }
        } else {
          consecutiveEmptyPages = 0;

          const filteredOutcomes = outcomes.filter((outcome) => {
            const outcomeTime = outcome.created_at;
            return outcomeTime >= startTimestamp && outcomeTime <= endTimestamp;
          });

          allOutcomes = allOutcomes.concat(filteredOutcomes);
          console.log(
            `   📈 Всього завантажено: ${allOutcomes.length} списань`
          );

          page++;
        }

        await this.sleep(100);
      } catch (error) {
        console.error(
          `   ❌ Помилка отримання сторінки ${page}: ${error.message}`
        );
        hasMore = false;
      }
    }

    console.log(
      `   ✅ ПІДСУМОК списань для локації ${branchId}: ${allOutcomes.length}`
    );
    return allOutcomes;
  }

  // Mетоди для продажів
  async fetchSales() {
    console.log("🔄 Начинается синхронизация продаж товаров...");

    const syncStart = Date.now();
    const errors = [];
    let totalSales = 0;
    let processedProducts = 0;

    try {
      await this.fetchEmployees();

      console.log(`📍 Получение всех продаж из системы`);

      const allSalesData = [];
      const allSales = await this.fetchAllSales();

      if (allSales.length > 0) {
        for (const sale of allSales) {
          for (const product of sale.products) {
            // Пропускаємо послуги (service = true або type != 0)
            if (product.service || product.type !== 0) {
              continue;
            }

            const saleItem = {
              sale_id: sale.id,
              sale_label: sale.id_label || "",
              sale_created_at: new Date(sale.created_at).toISOString(),
              created_by_id: sale.created_by_id,
              created_by_name: this.getEmployeeName(sale.created_by_id),
              client_id: sale.client_id || null,
              warehouse_id: sale.warehouse_id,
              product_title: product.title,
              product_code: product.code || "",
              product_article: product.article || "",
              uom_id: product.uom?.id || null,
              uom_title: product.uom?.title || "",
              uom_description: product.uom?.description || "",
              amount: product.amount,
              price: product.price || 0,
              cost: product.cost || 0,
              discount_value: product.discount_value || 0,
              sale_description: sale.description || "",
              sync_id: Date.now().toString(),
              updated_at: new Date().toISOString(),
            };

            allSalesData.push(saleItem);
            processedProducts++;
          }
        }
        totalSales = allSales.length;
      }

      console.log(`\n📊 === ПІДСУМОК ПО ПРОДАЖАМ ===`);
      console.log(`Знайдено продаж: ${totalSales}`);
      console.log(`Оброблено позицій товарів: ${processedProducts}`);

      if (allSalesData.length > 0) {
        console.log(
          `\n💾 Збереження ${allSalesData.length} записів продажів в BigQuery...`
        );
        await this.saveSalesToBigQuery(allSalesData);
      }

      return {
        success: true,
        totalSales,
        processedProducts,
        errors,
        duration: Date.now() - syncStart,
      };
    } catch (error) {
      console.error(
        `❌ Критична помилка синхронізації продажів: ${error.message}`
      );
      throw error;
    }
  }

  // Mетоди для замовлень та повернень постачальнику
  async syncOrders() {
    console.log("🔄 Починається синхронізація замовлень та повернень...");

    const syncStart = Date.now();
    let totalOrders = 0;

    try {
      const cookies = this.userCookies.get("shared_user");

      if (!cookies) {
        throw new Error(
          "Cookies для goods-flow відсутні! Оновіть через адмін-панель."
        );
      }

      // ✅ ВИПРАВЛЕНО: Отримуємо тільки товари з поточними остатками
      const productsQuery = `
      SELECT DISTINCT 
        product_id,
        title as product_title
      FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_calculated_stock\`
      WHERE product_id IS NOT NULL AND residue > 0
      LIMIT 500
    `;

      const [products] = await this.bigquery.query({
        query: productsQuery,
        location: "EU",
      });

      console.log(`📦 Знайдено ${products.length} товарів для синхронізації`);

      const allOrdersData = [];
      const startDate = new Date("2022-05-01").getTime();
      const endDate = Date.now();

      let processedCount = 0;

      for (const product of products) {
        try {
          console.log(
            `📊 Обробка ${++processedCount}/${products.length}: ${
              product.product_title
            }`
          );

          const flowItems = await this.fetchGoodsFlowForProduct(
            product.product_id,
            startDate,
            endDate,
            cookies
          );

          // Фільтруємо тільки замовлення (0) та повернення (7)
          const ordersAndReturns = flowItems.filter(
            (item) => item.relation_type === 0 || item.relation_type === 7
          );

          console.log(
            `   ✅ Знайдено ${ordersAndReturns.length} замовлень/повернень`
          );

          for (const item of ordersAndReturns) {
            allOrdersData.push({
              order_id: item.id || 0,
              relation_type: item.relation_type,
              relation_label: item.relation_label || "",
              created_at: new Date(item.created_at).toISOString(),
              warehouse_id: null, // ⚠️ goods-flow НЕ містить warehouse_id
              product_id: product.product_id,
              product_title: item.product_title || product.product_title,
              amount: item.amount || 0,
              comment: item.comment || "",
              sync_id: Date.now().toString(),
              updated_at: new Date().toISOString(),
            });
          }

          totalOrders += ordersAndReturns.length;

          await this.sleep(300); // Затримка між запитами
        } catch (error) {
          console.error(
            `❌ Помилка товару ${product.product_id}:`,
            error.message
          );
          // Продовжуємо обробку інших товарів
        }
      }

      console.log(`\n📊 === ПІДСУМОК ПО ЗАМОВЛЕННЯМ ===`);
      console.log(`Оброблено товарів: ${processedCount}`);
      console.log(`Знайдено операцій: ${totalOrders}`);

      if (allOrdersData.length > 0) {
        console.log(`💾 Збереження ${allOrdersData.length} записів...`);
        await this.saveOrdersToBigQuery(allOrdersData);
      } else {
        console.log("⚠️ Замовлень не знайдено");
      }

      return {
        success: true,
        totalOrders,
        processedProducts: processedCount,
        duration: Date.now() - syncStart,
      };
    } catch (error) {
      console.error(`❌ Критична помилка:`, error.message);
      throw error;
    }
  }
  async fetchAllSales(startTime, endTime) {
    const options = {
      method: "GET",
      headers: {
        accept: "application/json",
        authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
      },
    };

    let allSales = [];
    let page = 1;
    const perPage = 50;
    let hasMore = true;
    let consecutiveEmptyPages = 0;

    const startTimestamp = startTime || 1651363200000;
    const endTimestamp = endTime || Date.now();

    console.log(`   📡 Отримання продажів`);

    while (hasMore && consecutiveEmptyPages < 3) {
      try {
        const url = `https://api.roapp.io/retail/sales/?page=${page}&per_page=${perPage}`;
        console.log(`   📄 Сторінка ${page}: ${url}`);

        const response = await fetch(url, options);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}, сторінка ${page}`);
        }

        const data = await response.json();
        const sales = data.data || [];

        console.log(
          `   📊 Сторінка ${page}: отримано ${sales.length} продажів`
        );

        if (sales.length === 0) {
          consecutiveEmptyPages++;
          console.log(
            `   ⚠️ Пуста сторінка ${page}, лічильник: ${consecutiveEmptyPages}/3`
          );

          if (consecutiveEmptyPages >= 3) {
            hasMore = false;
          } else {
            page++;
          }
        } else {
          consecutiveEmptyPages = 0;

          const filteredSales = sales.filter((sale) => {
            const saleTime = sale.created_at;
            return saleTime >= startTimestamp && saleTime <= endTimestamp;
          });

          allSales = allSales.concat(filteredSales);
          console.log(`   📈 Всього завантажено: ${allSales.length} продажів`);

          page++;
        }

        await this.sleep(100);
      } catch (error) {
        console.error(
          `   ❌ Помилка отримання сторінки ${page}: ${error.message}`
        );
        hasMore = false;
      }
    }

    console.log(`   ✅ ПІДСУМОК продажів: ${allSales.length}`);
    return allSales;
  }

  async createSalesTable() {
    if (!this.bigquery) {
      console.log("❌ BigQuery не ініціалізована");
      return false;
    }

    try {
      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const tableName = `${process.env.BIGQUERY_TABLE}_sales`;
      const table = dataset.table(tableName);

      const [tableExists] = await table.exists();
      if (tableExists) {
        console.log("✅ Таблиця продажів вже існує");
        return true;
      }

      console.log("🔨 Створюємо таблицю продажів...");

      const schema = [
        { name: "sale_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "sale_label", type: "STRING", mode: "NULLABLE" },
        { name: "sale_created_at", type: "TIMESTAMP", mode: "REQUIRED" },
        { name: "created_by_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "created_by_name", type: "STRING", mode: "NULLABLE" },
        { name: "client_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "warehouse_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "product_title", type: "STRING", mode: "REQUIRED" },
        { name: "product_code", type: "STRING", mode: "NULLABLE" },
        { name: "product_article", type: "STRING", mode: "NULLABLE" },
        { name: "uom_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "uom_title", type: "STRING", mode: "NULLABLE" },
        { name: "uom_description", type: "STRING", mode: "NULLABLE" },
        { name: "amount", type: "FLOAT", mode: "REQUIRED" },
        { name: "price", type: "FLOAT", mode: "NULLABLE" },
        { name: "cost", type: "FLOAT", mode: "NULLABLE" },
        { name: "discount_value", type: "FLOAT", mode: "NULLABLE" },
        { name: "sale_description", type: "STRING", mode: "NULLABLE" },
        { name: "sync_id", type: "STRING", mode: "NULLABLE" },
        { name: "updated_at", type: "TIMESTAMP", mode: "REQUIRED" },
      ];

      await table.create({ schema, location: "EU" });
      console.log("✅ Таблиця продажів створена");
      return true;
    } catch (error) {
      console.error("❌ Помилка створення таблиці продажів:", error.message);
      return false;
    }
  }

  async createOrdersTable() {
    if (!this.bigquery) {
      console.log("❌ BigQuery не інціалізована");
      return false;
    }

    try {
      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const tableName = `${process.env.BIGQUERY_TABLE}_orders`;
      const table = dataset.table(tableName);

      const [tableExists] = await table.exists();
      if (tableExists) {
        console.log("✅ Таблиця замовлень вже існує");
        return true;
      }

      console.log("🔨 Створюємо таблицю замовлень...");

      const schema = [
        { name: "order_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "relation_type", type: "INTEGER", mode: "REQUIRED" }, // 0 = замовлення, 7 = повернення
        { name: "relation_label", type: "STRING", mode: "NULLABLE" },
        { name: "created_at", type: "TIMESTAMP", mode: "REQUIRED" },
        { name: "warehouse_id", type: "INTEGER", mode: "NULLABLE" },
        { name: "product_id", type: "INTEGER", mode: "REQUIRED" },
        { name: "product_title", type: "STRING", mode: "REQUIRED" },
        { name: "amount", type: "FLOAT", mode: "REQUIRED" },
        { name: "comment", type: "STRING", mode: "NULLABLE" },
        { name: "sync_id", type: "STRING", mode: "NULLABLE" },
        { name: "updated_at", type: "TIMESTAMP", mode: "REQUIRED" },
      ];

      await table.create({ schema, location: "EU" });
      console.log("✅ Таблиця замовлень створена");
      return true;
    } catch (error) {
      console.error("❌ Помилка створення таблиці замовлень:", error.message);
      return false;
    }
  }
  async saveSalesToBigQuery(data) {
    if (!this.bigquery || !data.length) return;

    try {
      await this.createSalesTable();

      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const tableName = `${process.env.BIGQUERY_TABLE}_sales`;
      const table = dataset.table(tableName);

      const syncId = Date.now().toString();

      const uniqueRecords = new Map();

      data.forEach((item) => {
        const uniqueKey = `${item.sale_id}_${item.product_title}_${item.warehouse_id}`;

        if (uniqueRecords.has(uniqueKey)) {
          const existing = uniqueRecords.get(uniqueKey);
          existing.amount += item.amount;
        } else {
          uniqueRecords.set(uniqueKey, {
            ...item,
            sync_id: syncId,
          });
        }
      });

      const enhancedData = Array.from(uniqueRecords.values());

      console.log(
        `📊 Вхідних записів: ${data.length}, унікальних: ${enhancedData.length}`
      );

      console.log("🗑️ Видалення всіх старих даних продажів...");
      try {
        const deleteQuery = `
          DELETE FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${tableName}\` 
          WHERE TRUE
        `;

        const [deleteJob] = await this.bigquery.createQueryJob({
          query: deleteQuery,
          location: "EU",
        });
        await deleteJob.getQueryResults();
        console.log("✅ Старі дані продажів видалено");
      } catch (deleteError) {
        console.log("⚠️ Не вдалося видалити старі дані:", deleteError.message);
      }

      console.log("📊 Вставка унікальних даних продажів в BigQuery...");

      const batchSize = 500;
      let insertedCount = 0;

      for (let i = 0; i < enhancedData.length; i += batchSize) {
        const batch = enhancedData.slice(i, i + batchSize);

        try {
          await table.insert(batch);
          insertedCount += batch.length;
          console.log(
            `📊 Вставлено ${insertedCount}/${enhancedData.length} записів`
          );
        } catch (error) {
          console.error(`❌ Помилка вставки батчу: ${error.message}`);
        }
      }

      if (insertedCount > 0) {
        console.log(
          `✅ Збережено ${insertedCount} унікальних записів продажів в BigQuery`
        );
      }
    } catch (error) {
      console.error("❌ Критична помилка збереження продажів:", error.message);
      throw error;
    }
  }

  // Метод сохранения замовлень и повернення постачальнику
  async saveOrdersToBigQuery(data) {
    if (!this.bigquery || !data.length) return;

    try {
      await this.createOrdersTable();

      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const tableName = `${process.env.BIGQUERY_TABLE}_orders`;
      const table = dataset.table(tableName);

      console.log(`📊 Підготовка ${data.length} записів замовлень...`);

      // ✅ ВИКОРИСТОВУЄМО LOAD JOB (працює на Free Tier)
      const [job] = await table.load(data, {
        sourceFormat: "JSON",
        writeDisposition: "WRITE_TRUNCATE", // Перезаписує таблицю
        schema: {
          fields: [
            { name: "order_id", type: "INTEGER", mode: "REQUIRED" },
            { name: "relation_type", type: "INTEGER", mode: "REQUIRED" },
            { name: "relation_label", type: "STRING", mode: "NULLABLE" },
            { name: "created_at", type: "TIMESTAMP", mode: "REQUIRED" },
            { name: "warehouse_id", type: "INTEGER", mode: "NULLABLE" },
            { name: "product_id", type: "INTEGER", mode: "REQUIRED" },
            { name: "product_title", type: "STRING", mode: "REQUIRED" },
            { name: "amount", type: "FLOAT", mode: "REQUIRED" },
            { name: "comment", type: "STRING", mode: "NULLABLE" },
            { name: "sync_id", type: "STRING", mode: "NULLABLE" },
            { name: "updated_at", type: "TIMESTAMP", mode: "REQUIRED" },
          ],
        },
      });

      console.log(`⏳ Очікування завершення load job...`);
      await job.promise();

      console.log(`✅ Успішно завантажено ${data.length} замовлень`);
    } catch (error) {
      console.error("❌ Помилка збереження замовлень:", error.message);
      throw error;
    }
  }

  // Створює SQL view для розрахунку остатків
  async createStockCalculationView() {
    if (!this.bigquery) {
      console.log("❌ BigQuery не інціалізована");
      return false;
    }

    try {
      const dataset = this.bigquery.dataset(process.env.BIGQUERY_DATASET);
      const viewName = `${process.env.BIGQUERY_TABLE}_calculated_stock`;

      console.log(`🔨 Створення view ${viewName}...`);

      const [exists] = await dataset.table(viewName).exists();
      if (exists) {
        await dataset.table(viewName).delete();
        console.log(`🗑️ Старий view видалено`);
      }

      const viewQuery = `
      WITH initial_stock AS (
          -- Початкові остатки з повної синхронізації
          SELECT 
              warehouse_id,
              warehouse_title,
              product_id,
              title as product_title,
              code as product_code,
              article as product_article,
              uom_title,
              residue as movement,
              updated_at as operation_date
          FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}\`
          WHERE residue > 0
          
          UNION ALL
          
          -- Оприбуткування (+)
          SELECT 
              warehouse_id,
              warehouse_title,
              product_id,
              product_title,
              product_code,
              product_article,
              uom_title,
              amount as movement,
              posting_created_at as operation_date
          FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_postings\`
          
          UNION ALL
          
          -- Вхідні переміщення (+)
          SELECT 
              w.warehouse_id,
              m.target_warehouse_title as warehouse_title,
              m.product_id,
              m.product_title,
              m.product_code,
              m.product_article,
              m.uom_title,
              m.amount as movement,
              m.move_created_at as operation_date
          FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_moves\` m
          JOIN (
              SELECT DISTINCT warehouse_id, warehouse_title 
              FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}\`
          ) w ON m.target_warehouse_title = w.warehouse_title
          
          UNION ALL
          
          -- Вихідні переміщення (-)
          SELECT 
              w.warehouse_id,
              m.source_warehouse_title as warehouse_title,
              m.product_id,
              m.product_title,
              m.product_code,
              m.product_article,
              m.uom_title,
              -m.amount as movement,
              m.move_created_at as operation_date
          FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_moves\` m
          JOIN (
              SELECT DISTINCT warehouse_id, warehouse_title 
              FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}\`
          ) w ON m.source_warehouse_title = w.warehouse_title
          
          UNION ALL
          
          -- Списання (-)
          SELECT 
              w.warehouse_id,
              o.source_warehouse_title as warehouse_title,
              o.product_id,
              o.product_title,
              o.product_code,
              o.product_article,
              o.uom_title,
              -o.amount as movement,
              o.outcome_created_at as operation_date
          FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_outcomes\` o
          JOIN (
              SELECT DISTINCT warehouse_id, warehouse_title 
              FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}\`
          ) w ON o.source_warehouse_title = w.warehouse_title
          
          UNION ALL
          
          -- Продажі (-)
          SELECT 
              s.warehouse_id,
              w.warehouse_title,
              NULL as product_id,
              s.product_title,
              s.product_code,
              s.product_article,
              s.uom_title,
              -s.amount as movement,
              s.sale_created_at as operation_date
          FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}_sales\` s
          JOIN (
              SELECT DISTINCT warehouse_id, warehouse_title 
              FROM \`${process.env.BIGQUERY_PROJECT_ID}.${process.env.BIGQUERY_DATASET}.${process.env.BIGQUERY_TABLE}\`
          ) w ON s.warehouse_id = w.warehouse_id
      )
      
      SELECT 
          warehouse_id,
          MAX(warehouse_title) as warehouse_title,
          product_id,
          MAX(product_title) as title,
          MAX(product_code) as code,
          MAX(product_article) as article,
          MAX(uom_title) as uom_title,
          SUM(movement) as residue,
          MAX(operation_date) as updated_at
      FROM initial_stock
      WHERE warehouse_id IS NOT NULL
      GROUP BY warehouse_id, product_id
      HAVING SUM(movement) > 0
    `;

      const metadata = {
        view: {
          query: viewQuery,
          useLegacySql: false,
        },
        location: "EU",
      };

      await dataset.createTable(viewName, metadata);
      console.log(`✅ View ${viewName} успішно створено`);
      return true;
    } catch (error) {
      console.error("❌ Помилка створення view:", error.message);
      return false;
    }
  }

  async refreshCookiesAutomatically() {
    if (!this.loginServiceUrl) {
      console.log("⚠️ LOGIN_SERVICE_URL не налаштовано");
      return;
    }

    try {
      console.log("🔄 Запит нових cookies з login-service...");

      // ВИПРАВЛЕНО: Прибрано зайвий слеш
      const url = `${this.loginServiceUrl}/get-cookies`;
      console.log(`📡 URL: ${url}`);

      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email: process.env.REMONLINE_EMAIL,
          password: process.env.REMONLINE_PASSWORD,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const result = await response.json();

      if (result.success) {
        this.userCookies.set("shared_user", result.cookies);
        console.log(`✅ Cookies оновлено (cached: ${result.cached})`);
      } else {
        console.error("❌ Помилка оновлення:", result.error);
      }
    } catch (error) {
      console.error("❌ Помилка зв'язку з login-service:", error.message);
    }
  }

  // async initialize() {
  //   await this.refreshCookiesAutomatically();
  //   console.log("✅ Сервер повністю ініціалізовано");
  // }
  async initialize() {
    console.log("🚀 Ініціалізація сервера...");

    // Перевіряємо cookies
    const cookies = this.userCookies.get("shared_user");

    if (!cookies) {
      console.log("⚠️ ========================================");
      console.log("⚠️ УВАГА: Cookies для goods-flow ВІДСУТНІ!");
      console.log("⚠️ Замовлення та повернення НЕ БУДУТЬ відображатись");
      console.log("⚠️ Оновіть cookies через адмін-панель (⚙️ → 🔐)");
      console.log("⚠️ ========================================");
    } else {
      console.log(`✅ Cookies знайдено (${cookies.length} символів)`);

      // Перевіряємо валідність
      try {
        const testUrl =
          "https://web.roapp.io/app/warehouse/get-goods-flow-items?page=1&pageSize=1&id=1&startDate=0&endDate=999999999999";

        const testResponse = await fetch(testUrl, {
          method: "GET",
          headers: {
            accept: "application/json",
            cookie: cookies,
            "User-Agent":
              "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          },
        });

        if (testResponse.ok) {
          console.log("✅ Cookies ВАЛІДНІ, goods-flow доступний");
        } else {
          console.log("⚠️ ========================================");
          console.log(
            `⚠️ УВАГА: Cookies ЗАСТАРІЛІ (HTTP ${testResponse.status})`
          );
          console.log("⚠️ Замовлення та повернення НЕ БУДУТЬ відображатись");
          console.log("⚠️ Оновіть cookies через адмін-панель (⚙️ → 🔐)");
          console.log("⚠️ ========================================");
        }
      } catch (testError) {
        console.log(
          "⚠️ Не вдалося перевірити валідність cookies:",
          testError.message
        );
      }
    }

    console.log("✅ Сервер повністю ініціалізовано");
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

  async start() {
    try {
      // Викликаємо async ініціалізацію
      await this.initialize();

      const PORT = process.env.PORT || 3000;

      this.app.listen(PORT, () => {
        console.log(`🚀 Сервер запущен на порту ${PORT}`);
        console.log(`📱 Откройте http://localhost:${PORT} в браузере`);
        console.log(`📊 Матричное отображение: товары × склади`);
      });
    } catch (error) {
      console.error("❌ Помилка ініціалізації:", error);
      process.exit(1);
    }
  }
}

// Запуск приложения
const syncApp = new RemonlineMatrixSync();
syncApp.start();
