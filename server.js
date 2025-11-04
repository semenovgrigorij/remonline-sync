// === server.js ===
import express from "express";
import fetch from "node-fetch";
import cors from "cors";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

// === Конфигурация ===
const PORT = process.env.PORT || 3000;
const API_BASE = "https://api.roapp.io";
const LOGIN_SERVICE_URL = process.env.LOGIN_SERVICE_URL;

// === Универсальный запрос к RemOnline API ===
async function apiGet(endpoint) {
  const url = `${API_BASE}${endpoint}`;
  const res = await fetch(url, {
    headers: {
      accept: "application/json",
      authorization: `Bearer ${process.env.REMONLINE_API_TOKEN}`,
    },
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`API error ${res.status}: ${errText}`);
  }

  return res.json();
}

// === Универсальный запрос к Web API с cookies ===
async function webGet(endpoint, cookies) {
  const url = `https://web.roapp.io${endpoint}`;
  const res = await fetch(url, {
    headers: {
      cookie: cookies,
      accept: "application/json",
    },
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`WEB error ${res.status}: ${errText}`);
  }

  return res.json();
}

// === Получение cookies через login-service ===
// === Получение cookies через login-service ===
async function getCookies(forceRefresh = false) {
  if (!LOGIN_SERVICE_URL) {
    return null;
  }

  const username = process.env.REMONLINE_USERNAME;
  const password = process.env.REMONLINE_PASSWORD;

  if (!username || !password) {
    console.warn(
      "⚠️ REMONLINE_USERNAME або REMONLINE_PASSWORD не встановлені в .env"
    );
    return null;
  }

  try {
    const url = forceRefresh
      ? `${LOGIN_SERVICE_URL}/get-cookies?force=true`
      : `${LOGIN_SERVICE_URL}/get-cookies`;

    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        username: username,
        password: password,
      }),
    });

    const data = await res.json();

    if (!data?.success) {
      throw new Error(data?.error || "Login service error");
    }

    if (forceRefresh) {
      console.log(`✅ Cookies примусово оновлено`);
    } else {
      console.log(`✅ Cookies отримано${data.cached ? " (з кешу)" : ""}`);
    }

    return data.cookies;
  } catch (e) {
    console.warn("⚠️ getCookies failed:", e.message);
    return null;
  }
}

// === Универсальная функция постраничного получения данных ===
async function fetchAllPages(urlBase, useWeb = false, cookies = null) {
  let page = 1;
  let allData = [];

  while (true) {
    const url = `${urlBase}${urlBase.includes("?") ? "&" : "?"}page=${page}`;
    try {
      const res = useWeb ? await webGet(url, cookies) : await apiGet(url);

      if (!res?.data || res.data.length === 0) break;

      allData = allData.concat(res.data);
      page++;

      if (page > 100) break; // предохранитель от зацикливания
    } catch (err) {
      if (err.message.includes("404") || err.message.includes("no results"))
        break;
      console.warn(`⚠️ fetchAllPages error (${url}):`, err.message);
      break;
    }
  }

  return allData;
}

// === 1️⃣ Локации ===
app.get("/api/branches", (req, res) => {
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

  res.json(branchIds);
});

// === 2️⃣ Склады конкретной локации ===
app.get("/api/warehouses/:branchId", async (req, res) => {
  const { branchId } = req.params;

  try {
    const data = await apiGet(`/warehouse/?branch_id=${branchId}`);
    res.json({ success: true, data: data.data });
  } catch (err) {
    console.error("❌ /api/warehouses:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// === 3️⃣ Товары на складе (все страницы) ===
app.get("/api/warehouse-goods/:warehouseId", async (req, res) => {
  const { warehouseId } = req.params;

  try {
    const goods = await fetchAllPages(
      `/warehouse/goods/${warehouseId}?exclude_zero_residue=true`
    );
    res.json({ success: true, total: goods.length, data: goods });
  } catch (err) {
    console.error("❌ /api/warehouse-goods:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// === 4️⃣ Історія товару по складу ===
// === 4️⃣ Історія товару по складу (НОВА ВЕРСІЯ) ===
app.get("/api/goods-history/:productId/:warehouseId", async (req, res) => {
  const { productId, warehouseId } = req.params;
  const branchId = req.query.branch_id;

  if (!branchId) {
    return res
      .status(400)
      .json({ success: false, error: "branch_id є обов'язковим параметром" });
  }

  try {
    let cookies = await getCookies();

    if (!cookies) {
      return res.status(500).json({
        success: false,
        error: "Не вдалося отримати cookies для авторизації",
      });
    }

    const allOps = [];

    // 🆕 НОВИЙ ПІДХІД: Використовуємо універсальний ендпоінт get-goods-flow-items
    // Він повертає ВСЮ історію товару з усіма типами операцій

    // Маппінг типів операцій з API на зрозумілі назви
    const OPERATION_TYPES = {
      0: "Замовлення",
      1: "Продаж",
      3: "Оприходування",
      4: "Списання",
      5: "Переміщення",
      7: "Повернення постачальнику",
    };

    // Отримуємо поточний timestamp для endDate
    const endDate = Date.now();

    // Запит до нового ендпоінту
    let page = 1;
    let hasMorePages = true;

    console.log(
      `📊 Завантаження історії товару ${productId} на складі ${warehouseId}...`
    );

    while (hasMorePages && page <= 100) {
      const url = `/app/warehouse/get-goods-flow-items?page=${page}&pageSize=50&id=${productId}&startDate=0&endDate=${endDate}`;

      try {
        const data = await webGet(url, cookies);

        if (!data.data || data.data.length === 0) {
          hasMorePages = false;
          break;
        }

        console.log(
          `📄 Сторінка ${page}: знайдено ${data.data.length} операцій`
        );

        // Обробляємо кожну операцію
        for (const item of data.data) {
          // Фільтруємо тільки операції для потрібного складу
          if (String(item.warehouse_id) !== String(warehouseId)) {
            continue;
          }

          const operationType = OPERATION_TYPES[item.relation_type];

          // Якщо тип операції невідомий, пропускаємо
          if (!operationType) {
            console.warn(`⚠️ Невідомий тип операції: ${item.relation_type}`);
            continue;
          }

          // Визначаємо delta (income - приход, outcome - витрата)
          let delta = 0;
          let finalOperationType = operationType;

          if (item.income !== undefined && item.income !== null) {
            delta = +item.income; // Приход товару
            // Для переміщення уточнюємо напрямок
            if (item.relation_type === 5) {
              finalOperationType = "Переміщення (вхід)";
            }
          } else if (item.outcome !== undefined && item.outcome !== null) {
            delta = -item.outcome; // Витрата товару
            // Для переміщення уточнюємо напрямок
            if (item.relation_type === 5) {
              finalOperationType = "Переміщення (вихід)";
            }
          }

          allOps.push({
            type: finalOperationType,
            date: new Date(item.created_at),
            delta: delta,
            documentId: item.relation_id_label || item.relation_id,
            clientName: item.client_name || null,
            warehouseTitle: item.warehouse_title || null,
          });
        }

        page++;

        // Якщо отримали менше ніж pageSize, значить це остання сторінка
        if (data.data.length < 50) {
          hasMorePages = false;
        }
      } catch (err) {
        console.log(`🔍 DEBUG: err.message = "${err.message}"`);
        console.log(
          `🔍 DEBUG: includes('401') = ${err.message.includes("401")}`
        );
        console.log(`🔍 DEBUG: page = ${page}`);

        // Перевіряємо чи це помилка 401 (Unauthorized)
        if (err.message.includes("401") && page === 1) {
          console.warn(`⚠️ Помилка 401 - cookies застаріли, оновлюємо...`);

          // Оновлюємо cookies примусово
          cookies = await getCookies(true);

          if (cookies) {
            console.log(`🔄 Повторна спроба з новими cookies...`);
            // Повторюємо запит з новими cookies
            try {
              const data = await webGet(url, cookies);

              if (data.data && data.data.length > 0) {
                console.log(
                  `📄 Сторінка ${page}: знайдено ${data.data.length} операцій`
                );

                // Обробляємо операції (копіюємо логіку з основного блоку)
                for (const item of data.data) {
                  if (String(item.warehouse_id) !== String(warehouseId))
                    continue;

                  const operationType = OPERATION_TYPES[item.relation_type];
                  if (!operationType) continue;

                  let delta = 0;
                  let finalOperationType = operationType;

                  if (item.income !== undefined && item.income !== null) {
                    delta = +item.income;
                    if (item.relation_type === 5)
                      finalOperationType = "Переміщення (вхід)";
                  } else if (
                    item.outcome !== undefined &&
                    item.outcome !== null
                  ) {
                    delta = -item.outcome;
                    if (item.relation_type === 5)
                      finalOperationType = "Переміщення (вихід)";
                  }

                  allOps.push({
                    type: finalOperationType,
                    date: new Date(item.created_at),
                    delta: delta,
                    documentId: item.relation_id_label || item.relation_id,
                    clientName: item.client_name || null,
                    warehouseTitle: item.warehouse_title || null,
                  });
                }

                page++;
                if (data.data.length < 50) hasMorePages = false;
              } else {
                hasMorePages = false;
              }
            } catch (retryErr) {
              console.warn(`⚠️ Повторна спроба не вдалася:`, retryErr.message);
              hasMorePages = false;
            }
          } else {
            console.error(`❌ Не вдалося оновити cookies`);
            hasMorePages = false;
          }
        } else {
          console.warn(`⚠️ Помилка на сторінці ${page}:`, err.message);
          hasMorePages = false;
        }
      }
    }

    // Сортуємо за датою
    allOps.sort((a, b) => new Date(a.date) - new Date(b.date));

    console.log(`✅ Загалом знайдено операцій: ${allOps.length}`);

    // Групуємо статистику по типах
    const stats = {};
    allOps.forEach((op) => {
      stats[op.type] = (stats[op.type] || 0) + 1;
    });
    console.log(`📊 Статистика:`, stats);

    res.json({
      success: true,
      total: allOps.length,
      productId,
      warehouseId,
      branchId,
      history: allOps,
      stats,
    });
  } catch (err) {
    console.error("❌ /api/goods-history:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// === 5️⃣ Раздача фронтенда (если index.html в /public) ===
import path from "path";
import { fileURLToPath } from "url";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.static(path.join(__dirname, "public")));
app.get("/", (req, res) =>
  res.sendFile(path.join(__dirname, "public", "index.html"))
);

// === Запуск сервера ===
app.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
