/**
 * server.js
 * - Убирает BigQuery, использует RemOnline (RO App) через cookies от remonline-login-service
 * - Эндпоинты:
 *    GET  /api/branches
 *    GET  /api/warehouses/:branchId
 *    GET  /api/realtime-warehouse-goods/:warehouseId
 *    GET  /api/goods-flow-items/:productId/:warehouseId?
 *
 * Настройки через .env:
 *  LOGIN_SERVICE_URL    - базовый URL твоего remonline-login-service (например https://your-login-service.fly.dev)
 *  REMONLINE_USERNAME   - логин для RemOnline (будет отправлен в login-service)
 *  REMONLINE_PASSWORD   - пароль
 *  REMONLINE_BASE_URL   - базовый URL RemOnline / RO App (по умолчанию https://web.roapp.io)
 *  GOODS_LIST_PATH      - путь для запроса товаров / товара по складу (по умолчанию: /api/v2/inventory/warehouse_goods)
 *  GOODS_FLOW_PATH      - путь для запроса goods-flow (по умолчанию: /api/v2/inventory/goods-flow)
 *  WAREHOUSES_PATH      - путь для получения складов (по умолчанию: /api/v2/warehouses)
 *
 * Примечание: если реальные endpoint-ы у тебя другие, поправь константы ниже.
 */

const express = require("express");
const fetch = require("node-fetch");
const path = require("path");
require("dotenv").config();

const app = express();
app.use(express.json());
app.use(express.static("public"));

// --- Конфигурация (можно переопределить через .env) ---
const LOGIN_SERVICE_URL =
  process.env.LOGIN_SERVICE_URL || "http://localhost:3000"; // remonline-login-service
const LOGIN_ENDPOINT = "/get-cookies"; // POST { username, password } -> { success, cookies }

const REMONLINE_BASE_URL =
  process.env.REMONLINE_BASE_URL || "https://web.roapp.io";

// ПУТИ к API RemOnline (если отличаются — подправь)
const GOODS_LIST_PATH =
  process.env.GOODS_LIST_PATH || "/api/v2/inventory/warehouse_goods"; // ?warehouse_id=...
const GOODS_FLOW_PATH =
  process.env.GOODS_FLOW_PATH || "/api/v2/inventory/goods-flow"; // ?product_id=...&from=...&to=...
const WAREHOUSES_PATH = process.env.WAREHOUSES_PATH || "/api/v2/warehouses"; // ?branch_id=...
const BRANCHES_PATH = process.env.BRANCHES_PATH || "/api/v2/branches"; // если есть

const REM_USERNAME = process.env.REMONLINE_USERNAME;
const REM_PASSWORD = process.env.REMONLINE_PASSWORD;

if (!REM_USERNAME || !REM_PASSWORD) {
  console.warn(
    "⚠️ REMONLINE_USERNAME or REMONLINE_PASSWORD not set in .env — getCookies() will fail until set."
  );
}

// --- Вспомогательный fetch с Cookie ---
async function getCookiesFromLoginService() {
  // Вызов твоего remonline-login-service
  try {
    const res = await fetch(`${LOGIN_SERVICE_URL}${LOGIN_ENDPOINT}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: REM_USERNAME,
        password: REM_PASSWORD,
      }),
      timeout: 45000,
    });

    const json = await res.json();
    if (!json.success || !json.cookies) {
      throw new Error(
        `Login service error: ${json.error || "no cookies returned"}`
      );
    }
    return json.cookies; // строка cookie, вроде "sid=..; other=.."
  } catch (err) {
    console.error("❌ getCookiesFromLoginService failed:", err.message);
    throw err;
  }
}

async function ensureCookiesCached() {
  // можно добавить кеширование при желании (здесь на каждый запрос получаем, но remonline-login-service сам кеширует)
  return await getCookiesFromLoginService();
}

function buildHeaders(cookieString) {
  const headers = {
    Accept: "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Node.js) RemOnline-integration",
    Cookie: cookieString,
  };
  return headers;
}

// Универсальный вызов к RemOnline (GET)
async function remonlineGet(pathWithQuery, cookieString) {
  const url = REMONLINE_BASE_URL + pathWithQuery;
  const res = await fetch(url, {
    method: "GET",
    headers: buildHeaders(cookieString),
  });

  // Если RemOnline отдаёт HTML на ошибку, пытаемся взять json если возможно
  const contentType = res.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return await res.json();
  } else {
    const text = await res.text();
    // Пробуем парсить JSON внутри body (иногда web UI возвращает обёртку)
    try {
      return JSON.parse(text);
    } catch (e) {
      // возврат текстового ответа
      return { raw: text, status: res.status };
    }
  }
}

// --- Эндпоинты API сервера ---

// 1) Список локаций/филиалов (branches)
app.get("/api/branches", async (req, res) => {
  try {
    const cookies = await ensureCookiesCached();
    // Попытка вызвать BRANCHES_PATH
    const data = await remonlineGet(`${BRANCHES_PATH}`, cookies);
    res.json({ success: true, data });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 2) Список складов для филиала
app.get("/api/warehouses/:branchId", async (req, res) => {
  try {
    const branchId = req.params.branchId;
    const cookies = await ensureCookiesCached();

    // По умолчанию пытаемся к WAREHOUSES_PATH?branch_id=...
    const data = await remonlineGet(
      `${WAREHOUSES_PATH}?branch_id=${encodeURIComponent(branchId)}`,
      cookies
    );
    res.json({ success: true, branchId, warehouses: data });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 3) real-time: список товаров склада + рассчитанный остаток (через goods-flow)
app.get("/api/realtime-warehouse-goods/:warehouseId", async (req, res) => {
  try {
    const warehouseId = req.params.warehouseId;
    const cookies = await ensureCookiesCached();

    // 1) Получаем список товаров/позиций на складе (goods list)
    // Пример: GOODS_LIST_PATH?warehouse_id=123&limit=1000
    const goodsResp = await remonlineGet(
      `${GOODS_LIST_PATH}?warehouse_id=${encodeURIComponent(
        warehouseId
      )}&limit=10000`,
      cookies
    );

    // Предполагаем, что goodsResp — массив объектов { id, title, code, article, uom_title, ... }
    const goodsList = Array.isArray(goodsResp)
      ? goodsResp
      : goodsResp.data || goodsResp.items || [];

    // 2) Для каждого товара делаем запрос к goods-flow и считаем остаток
    // Внимание: последовательные запросы могут быть медленными. Здесь реализована последовательная обработка.
    // При необходимости можно легко распараллелить через Promise.all с rate-limit.

    const startDate = new Date("2022-05-01").toISOString(); // начало периода (настрой)
    const endDate = new Date().toISOString();

    const result = [];

    for (const product of goodsList) {
      const productId =
        product.id || product.product_id || product.good_id || product.goods_id;

      if (!productId) {
        // Если нет id — пропускаем (или можно пробовать по коду/названию)
        continue;
      }

      // Формируем запрос goods-flow: GOODS_FLOW_PATH?product_id=...&from=...&to=...
      // Ожидаем, что ответ — массив операций с полями: date, warehouse_id, delta (число)
      const flowResp = await remonlineGet(
        `${GOODS_FLOW_PATH}?product_id=${encodeURIComponent(
          productId
        )}&from=${encodeURIComponent(startDate)}&to=${encodeURIComponent(
          endDate
        )}&limit=10000`,
        cookies
      );

      const flow = Array.isArray(flowResp)
        ? flowResp
        : flowResp.data || flowResp.items || [];

      // Фильтруем по warehouseId
      const filtered = flow.filter((f) => {
        // Возможные названия поля: warehouse_id, warehouseId, warehouse
        const wid = f.warehouse_id || f.warehouseId || f.warehouse;
        return String(wid) === String(warehouseId);
      });

      // Сортируем по дате (старые -> новые)
      filtered.sort(
        (a, b) =>
          new Date(a.date || a.operation_date || a.created_at) -
          new Date(b.date || b.operation_date || b.created_at)
      );

      // Суммируем delta (возможные поля: delta, amount, quantity)
      let residue = 0;
      filtered.forEach((op) => {
        const d = op.delta ?? op.amount ?? op.quantity ?? 0;
        residue += Number(d) || 0;
      });

      result.push({
        product_id: productId,
        title: product.title || product.name || product.product_title || "",
        code: product.code || "",
        article: product.article || "",
        uom_title: product.uom_title || product.unit || "",
        calculated_residue: residue,
        last_update_from_flow: filtered.length
          ? filtered[filtered.length - 1].date ||
            filtered[filtered.length - 1].operation_date ||
            filtered[filtered.length - 1].created_at
          : null,
        total_operations: filtered.length,
      });
    }

    res.json({
      success: true,
      warehouseId,
      totalProducts: result.length,
      data: result,
    });
  } catch (err) {
    console.error("❌ /api/realtime-warehouse-goods error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 4) История товара (goods-flow) — уже был у тебя, возвращаем операции
app.get("/api/goods-flow-items/:productId/:warehouseId?", async (req, res) => {
  try {
    const productId = req.params.productId;
    const warehouseId = req.params.warehouseId;
    const cookies = await ensureCookiesCached();

    const startDate = req.query.from || new Date("2022-05-01").toISOString();
    const endDate = req.query.to || new Date().toISOString();

    const flowResp = await remonlineGet(
      `${GOODS_FLOW_PATH}?product_id=${encodeURIComponent(
        productId
      )}&from=${encodeURIComponent(startDate)}&to=${encodeURIComponent(
        endDate
      )}&limit=10000`,
      cookies
    );
    const flow = Array.isArray(flowResp)
      ? flowResp
      : flowResp.data || flowResp.items || [];

    let filtered = flow;
    if (warehouseId) {
      filtered = flow.filter((f) => {
        const wid = f.warehouse_id || f.warehouseId || f.warehouse;
        return String(wid) === String(warehouseId);
      });
    }

    // Сортируем от старых к новым
    filtered.sort(
      (a, b) =>
        new Date(a.date || a.operation_date || a.created_at) -
        new Date(b.date || b.operation_date || b.created_at)
    );

    // Дополним информацию: delta, тип операции
    const normalized = filtered.map((op) => ({
      operation_type: op.type || op.operation_type || op.operation || "unknown",
      date: op.date || op.operation_date || op.created_at,
      delta: op.delta ?? op.amount ?? op.quantity ?? 0,
      doc: op.document || op.operation_label || op.label || "",
      employee_id: op.employee_id || op.employeeId || null,
      raw: op,
    }));

    res.json({
      success: true,
      productId,
      warehouseId: warehouseId || null,
      totalRecords: normalized.length,
      data: normalized,
    });
  } catch (err) {
    console.error("❌ /api/goods-flow-items error:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// Serve index.html for root (if exists in public)
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 Server started on port ${PORT}`);
  console.log(`🔗 Login service URL: ${LOGIN_SERVICE_URL}${LOGIN_ENDPOINT}`);
  console.log(`🔗 RemOnline base URL: ${REMONLINE_BASE_URL}`);
});
