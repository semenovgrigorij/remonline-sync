import express from "express";
import session from "express-session";
import https from "https";

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static("public"));
app.use(
  session({
    secret: "remonline-secret-key-2024",
    resave: false,
    saveUninitialized: false,
    cookie: { secure: false, httpOnly: true, maxAge: 24 * 60 * 60 * 1000 },
  })
);

function requireAuth(req, res, next) {
  if (!req.session.username || !req.session.cookies) {
    return res.status(401).json({ success: false, error: "Не авторизовано" });
  }
  next();
}

async function getCookiesFromLoginService(
  username,
  password,
  forceNew = false
) {
  return new Promise((resolve) => {
    const postData = JSON.stringify({ username, password, forceNew });
    const req = https.request(
      {
        // hostname: "remonline-login.fly.dev",
        hostname: "remonline-login-improved.fly.dev",
        port: 443,
        path: "/get-cookies",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Content-Length": Buffer.byteLength(postData),
        },
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          try {
            const result = JSON.parse(data);
            resolve(result.success && result.cookies ? result.cookies : null);
          } catch (err) {
            resolve(null);
          }
        });
      }
    );
    req.on("error", () => resolve(null));
    req.write(postData);
    req.end();
  });
}

function webGet(path, cookies) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: "web.roapp.io",
        port: 443,
        path: path,
        method: "GET",
        headers: { Cookie: cookies, Accept: "application/json" },
      },
      (res) => {
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => {
          console.log(
            "📡 webGet відповідь:",
            res.statusCode,
            "→",
            data.length,
            "байт"
          );
          if (res.statusCode === 401) {
            console.log("⚠️ 401 Unauthorized");
            reject(new Error("401"));
          } else {
            try {
              const parsed = JSON.parse(data);
              resolve(parsed);
            } catch (err) {
              console.error("❌ Помилка парсингу JSON:", err.message);
              reject(new Error("Invalid JSON"));
            }
          }
        });
      }
    );
    req.on("error", reject);
    req.end();
  });
}

function apiGet(path, token) {
  return new Promise((resolve, reject) => {
    const req = https.request(
      {
        hostname: "api.roapp.io",
        port: 443,
        path: path,
        method: "GET",
        headers: {
          Accept: "application/json",
          Authorization: "Bearer " + token,
        },
      },
      (res) => {
        res.setEncoding("utf8");
        let data = "";
        let chunkCount = 0;

        res.on("data", (chunk) => {
          data += chunk;
          chunkCount++;
        });

        res.on("end", () => {
          console.log(
            "📊 API відповідь:",
            chunkCount,
            "chunks,",
            data.length,
            "символів"
          );

          if (res.statusCode === 401) {
            reject(new Error("401"));
          } else {
            try {
              const parsed = JSON.parse(data);
              console.log(
                "📦 Розпарсено:",
                parsed.data ? parsed.data.length : 0,
                "записів"
              );
              resolve(parsed);
            } catch (err) {
              console.error("❌ Помилка парсингу:", err.message);
              console.error("📄 Довжина:", data.length);
              reject(new Error("Invalid JSON"));
            }
          }
        });
      }
    );
    req.on("error", reject);
    req.end();
  });
}

app.post("/api/login", async (req, res) => {
  const { username, password, apiToken } = req.body;
  if (!username || !password || !apiToken)
    return res
      .status(400)
      .json({ success: false, error: "Всі поля обов'язкові" });

  try {
    console.log("🔐 Вхід:", username);
    const cookies = await getCookiesFromLoginService(username, password, true);
    if (!cookies)
      return res.status(401).json({ success: false, error: "Невірний логін" });

    req.session.username = username;
    req.session.password = password;
    req.session.cookies = cookies;
    req.session.apiToken = apiToken;

    console.log("✅", username, "увійшов");
    res.json({ success: true, username: username });
  } catch (err) {
    console.error("Помилка:", err);
    res.status(500).json({ success: false, error: "Помилка сервера" });
  }
});

app.post("/api/logout", (req, res) => {
  req.session.destroy();
  res.json({ success: true });
});

app.get("/api/auth-status", (req, res) => {
  res.json(
    req.session.username && req.session.cookies
      ? { authenticated: true, username: req.session.username }
      : { authenticated: false }
  );
});

app.get("/api/warehouses", requireAuth, async (req, res) => {
  const { branch_id } = req.query;
  if (!branch_id)
    return res
      .status(400)
      .json({ success: false, error: "branch_id обов'язковий" });

  try {
    const apiToken = req.session.apiToken;
    if (!apiToken)
      return res
        .status(401)
        .json({ success: false, error: "API Token відсутній" });

    const data = await apiGet("/warehouse/?branch_id=" + branch_id, apiToken);
    console.log("✅ Складів:", data.data ? data.data.length : 0);
    res.json({ success: true, warehouses: data.data || [] });
  } catch (err) {
    console.error("❌ Помилка:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get(
  "/api/warehouse-all-goods/:warehouseId",
  requireAuth,
  async (req, res) => {
    const { warehouseId } = req.params;
    console.log("📦 /api/warehouse-all-goods:", warehouseId);

    try {
      const apiToken = req.session.apiToken;
      if (!apiToken)
        return res
          .status(401)
          .json({ success: false, error: "API Token відсутній" });

      const allGoods = [];
      let page = 1;
      let hasMorePages = true;

      while (hasMorePages && page <= 100) {
        const url =
          "/warehouse/goods/" +
          warehouseId +
          "?exclude_zero_residue=true&page=" +
          page +
          "&pageSize=50";
        console.log("📡 Запит сторінки", page + ":", url);

        const data = await apiGet(url, apiToken);

        if (!data.data || data.data.length === 0) {
          hasMorePages = false;
          break;
        }

        allGoods.push(...data.data);
        console.log(
          "✅ Отримано",
          data.data.length,
          "товарів (всього:",
          allGoods.length + ")"
        );

        // Якщо отримали менше 50, це остання сторінка
        if (data.data.length < 50) {
          hasMorePages = false;
        }

        page++;
      }

      console.log("🎯 Загалом завантажено товарів:", allGoods.length);

      if (allGoods.length > 0) {
        console.log("📊 Перший:", allGoods[0].title);
        console.log("📊 Останній:", allGoods[allGoods.length - 1].title);
      }

      res.json({ success: true, goods: allGoods });
    } catch (err) {
      console.error("❌ Помилка:", err.message);
      res.status(500).json({ success: false, error: err.message });
    }
  }
);

app.get("/api/search-goods", requireAuth, async (req, res) => {
  const { branch_id, warehouse_id, query } = req.query;
  if (!branch_id || !warehouse_id)
    return res
      .status(400)
      .json({ success: false, error: "branch_id та warehouse_id обов'язкові" });

  try {
    const apiToken = req.session.apiToken;
    if (!apiToken)
      return res
        .status(401)
        .json({ success: false, error: "API Token відсутній" });

    console.log("🔍 Пошук:", query || "(всі товари)");

    // Завантажуємо всі товари зі складу
    const allGoods = [];
    let page = 1;
    let hasMorePages = true;

    while (hasMorePages && page <= 100) {
      const url =
        "/warehouse/goods/" +
        warehouse_id +
        "?exclude_zero_residue=true&page=" +
        page +
        "&pageSize=50";

      const data = await apiGet(url, apiToken);

      if (!data.data || data.data.length === 0) {
        hasMorePages = false;
        break;
      }

      allGoods.push(...data.data);

      if (data.data.length < 50) {
        hasMorePages = false;
      }

      page++;
    }

    console.log("📦 Завантажено товарів:", allGoods.length);

    // Фільтруємо по запиту
    let filteredGoods = allGoods;

    if (query && query.trim()) {
      const searchTerm = query.toLowerCase().trim();
      filteredGoods = allGoods.filter((g) => {
        const title = (g.title || "").toLowerCase();
        const article = (g.article || "").toLowerCase();
        return title.includes(searchTerm) || article.includes(searchTerm);
      });
      console.log(
        "✅ Знайдено після фільтрації:",
        filteredGoods.length,
        "товарів"
      );
    } else {
      console.log("✅ Повернуто всі товари:", filteredGoods.length);
    }

    res.json({
      success: true,
      goods: filteredGoods,
      count: filteredGoods.length,
    });
  } catch (err) {
    console.error("❌ Помилка пошуку:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get(
  "/api/goods-history/:productId/:warehouseId",
  requireAuth,
  async (req, res) => {
    const { productId, warehouseId } = req.params;
    const { branch_id, startDate } = req.query;
    if (!branch_id)
      return res
        .status(400)
        .json({ success: false, error: "branch_id обов'язковий" });

    try {
      let cookies = req.session.cookies;
      const username = req.session.username;
      const password = req.session.password;

      if (!cookies)
        return res
          .status(401)
          .json({ success: false, error: "Сесія застаріла" });

      const allOps = [];
      const OPERATION_TYPES = {
        0: "Замовлення",
        1: "Продаж",
        3: "Оприходування",
        4: "Списання",
        5: "Переміщення",
        7: "Повернення",
      };
      const historyStartDate = startDate ? parseInt(startDate) : 0;
      const endDate = Date.now();

      let page = 1,
        hasMorePages = true;

      console.log(
        "📊 Запит історії для productId:",
        productId,
        "warehouseId:",
        warehouseId
      );

      while (hasMorePages && page <= 50) {
        const url =
          "/app/warehouse/get-goods-flow-items?page=" +
          page +
          "&pageSize=50&id=" +
          productId +
          "&startDate=" +
          historyStartDate +
          "&endDate=" +
          endDate;

        try {
          const data = await webGet(url, cookies);
          console.log(
            "📦 Сторінка",
            page + ":",
            data.data ? data.data.length : 0,
            "записів"
          );

          if (!data.data || data.data.length === 0) {
            hasMorePages = false;
            break;
          }

          for (const item of data.data) {
            const operationType = OPERATION_TYPES[item.relation_type];
            if (!operationType) continue;

            let delta = 0,
              finalType = operationType,
              clientName = item.client_name || item.client_title || "—";
            let targetWarehouseId = item.warehouse_id; // За замовчуванням - основний склад

            // Обробка переміщень (relation_type === 5)
            if (item.relation_type === 5) {
              if (item.income > 0) {
                // ВХІД: товар приходить на optional_warehouse (склад-отримувач)
                targetWarehouseId = item.optional_warehouse_id;
                delta = +item.income;
                finalType = "Переміщення (вхід)";
                clientName = item.warehouse_title || "—"; // Звідки прийшов товар
              } else if (item.outcome > 0) {
                // ВИХІД: товар йде з warehouse (склад-відправник)
                targetWarehouseId = item.warehouse_id;
                delta = -item.outcome;
                finalType = "Переміщення (вихід)";
                clientName = item.optional_warehouse_title || "—"; // Куди пішов товар
              }
            }
            // Інші типи операцій (не переміщення)
            else {
              targetWarehouseId = item.warehouse_id;

              if (item.income > 0) {
                delta = +item.income;
              }
              if (item.outcome > 0) {
                delta = -item.outcome;
              }
            }

            // Фільтруємо: залишаємо тільки операції для нашого складу
            if (String(targetWarehouseId) !== String(warehouseId)) continue;

            allOps.push({
              type: finalType,
              date: new Date(item.created_at),
              delta: delta,
              documentId: item.relation_id_label || "—",
              clientName: clientName,
              warehouseTitle: item.warehouse_title || "—",
              employeeId: item.employee_id || null, // ← Додаємо employee_id
            });
          }

          page++;
          if (data.data.length < 50) hasMorePages = false;
        } catch (err) {
          if (err.message.includes("401") && page === 1) {
            cookies = await getCookiesFromLoginService(
              username,
              password,
              true
            );
            if (cookies) {
              req.session.cookies = cookies;
              continue;
            }
          }
          throw err;
        }
      }

      allOps.sort((a, b) => new Date(a.date) - new Date(b.date));

      console.log("✅ Знайдено операцій:", allOps.length);

      const stats = {};
      allOps.forEach((op) => {
        stats[op.type] = (stats[op.type] || 0) + 1;
      });

      res.json({ success: true, operations: allOps, stats: stats });
    } catch (err) {
      console.error("❌ Помилка історії:", err.message);
      res.status(500).json({ success: false, error: err.message });
    }
  }
);

// Кеш для списку співробітників
let employeesCache = null;
let employeesCacheTime = 0;
const EMPLOYEES_CACHE_TTL = 30 * 60 * 1000;

// Функція для завантаження списку всіх співробітників
async function loadAllEmployees(apiToken) {
  const now = Date.now();

  // Перевіряємо кеш
  if (employeesCache && now - employeesCacheTime < EMPLOYEES_CACHE_TTL) {
    console.log("📦 Використовуємо кешований список співробітників");
    return employeesCache;
  }

  console.log("🔄 Завантажуємо список всіх співробітників...");
  const data = await apiGet("/employees/", apiToken);

  if (data && data.data && Array.isArray(data.data)) {
    // Створюємо Map для швидкого пошуку за ID
    const employeesMap = new Map();
    data.data.forEach((emp) => {
      if (!emp.deleted) {
        // Пропускаємо видалених
        const fullName =
          (emp.first_name || "").trim() +
          (emp.last_name ? " " + emp.last_name.trim() : "");
        employeesMap.set(emp.id, fullName || "—");
      }
    });

    employeesCache = employeesMap;
    employeesCacheTime = now;

    console.log(`✅ Завантажено ${employeesMap.size} співробітників`);
    return employeesMap;
  }

  console.log("⚠️ Не вдалося завантажити співробітників");
  return new Map();
}

// Ендпоінт для отримання імені співробітника
app.get("/api/employee/:employeeId", requireAuth, async (req, res) => {
  const { employeeId } = req.params;

  try {
    const apiToken = req.session.apiToken;
    if (!apiToken) {
      return res
        .status(401)
        .json({ success: false, error: "API Token відсутній" });
    }

    console.log("👤 Запит співробітника:", employeeId);

    // Завантажуємо список всіх співробітників (або беремо з кешу)
    const employeesMap = await loadAllEmployees(apiToken);

    // Шукаємо співробітника за ID
    const employeeName = employeesMap.get(parseInt(employeeId));

    if (employeeName) {
      console.log("✅ Знайдено:", employeeName);
      res.json({ success: true, name: employeeName });
    } else {
      console.log(
        "⚠️ Співробітник не знайдений або видалений (ID:",
        employeeId,
        ")"
      );
      res.json({ success: true, name: "—" });
    }
  } catch (err) {
    console.error("❌ Помилка отримання співробітника:", err.message);
    res.json({ success: true, name: "—" });
  }
});

// app.listen(PORT, () => {
//   console.log("🚀 RemOnline Sync v5.5.8 → http://localhost:" + PORT + "/");
// });

// ====================================
// ENDPOINT ДЛЯ HEALTH CHECK
// ====================================
app.get("/health", (req, res) => {
  res.json({ status: "ok", timestamp: Date.now() });
});

// ====================================
// ЗАПУСК СЕРВЕРА + ПІНГУВАННЯ
// ====================================
app.listen(PORT, () => {
  console.log("🚀 RemOnline Sync v5.5.8 → http://localhost:" + PORT + "/");
  console.log("🔔 Запущено пінгування Fly.io кожні 10 хвилин");

  // Перший пінг одразу (через 5 сек)
  setTimeout(async () => {
    try {
      const response = await fetch(
        "https://remonline-login-improved.fly.dev/health"
      );
      if (response.ok) {
        console.log("✅ Fly.io: перший пінг успішний");
      }
    } catch (e) {
      console.log("⚠️ Fly.io: перший пінг не вдався");
    }
  }, 5000);

  // Пінгування кожні 10 хвилин
  setInterval(async () => {
    try {
      const response = await fetch(
        "https://remonline-login-improved.fly.dev/health"
      );
      if (response.ok) {
        const now = new Date().toLocaleTimeString("uk-UA");
        console.log(`✅ [${now}] Fly.io pinged successfully`);
      } else {
        console.log(`⚠️ Fly.io ping failed: ${response.status}`);
      }
    } catch (e) {
      console.error("❌ Fly.io ping error:", e.message);
    }
  }, 10 * 60 * 1000); // 10 хвилин
});
