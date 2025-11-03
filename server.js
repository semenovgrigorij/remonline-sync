/**
 * server.js — гибридная версия
 * Получает данные из API (Bearer) + Web (через cookies)
 * Считает реальные остатки товаров по складу
 */

import express from "express";
import fetch from "node-fetch";
import dotenv from "dotenv";
import path from "path";
import { fileURLToPath } from "url";

dotenv.config();
const app = express();
app.use(express.json());
app.use(express.static("public"));

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ==== Конфигурация ====
const API_BASE = process.env.REMONLINE_BASE_URL_API || "https://api.roapp.io";
const WEB_BASE = process.env.REMONLINE_BASE_URL_WEB || "https://web.roapp.io";
const TOKEN = process.env.REMONLINE_API_TOKEN;

const LOGIN_SERVICE_URL = process.env.LOGIN_SERVICE_URL;
const REMONLINE_USERNAME = process.env.REMONLINE_USERNAME;
const REMONLINE_PASSWORD = process.env.REMONLINE_PASSWORD;

const PORT = process.env.PORT || 3000;

// ==========================
// Вспомогательные функции
// ==========================

// --- Получение cookies от login-service ---
async function getCookies() {
  try {
    const res = await fetch(`${LOGIN_SERVICE_URL}/get-cookies`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: REMONLINE_USERNAME,
        password: REMONLINE_PASSWORD,
      }),
    });
    const data = await res.json();
    if (!data.success || !data.cookies) throw new Error("Login service error");
    return data.cookies;
  } catch (err) {
    console.error("⚠️ getCookies failed:", err.message);
    return null;
  }
}

// --- Универсальный GET-запрос к API (Bearer) ---
async function apiGet(endpoint) {
  const res = await fetch(`${API_BASE}${endpoint}`, {
    method: "GET",
    headers: {
      accept: "application/json",
      authorization: `Bearer ${TOKEN}`,
    },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API error ${res.status}: ${text}`);
  }
  return await res.json();
}

// --- Универсальный GET-запрос к web.roapp.io (cookies) ---
async function webGet(endpoint, cookies) {
  const res = await fetch(`${WEB_BASE}${endpoint}`, {
    method: "GET",
    headers: {
      accept: "application/json, text/plain, */*",
      cookie: cookies,
    },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`WEB error ${res.status}: ${text}`);
  }
  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

// ==========================
// Эндпоинты сервера
// ==========================

// 1️⃣ Локации
// 1️⃣ Локации (берём из жёстко заданного массива)
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

app.get("/api/branches", (req, res) => {
  try {
    res.json({ success: true, data: branchIds });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

// 2️⃣ Склады
app.get("/api/warehouses/:branchId", async (req, res) => {
  try {
    const { branchId } = req.params;
    const data = await apiGet(`/warehouse/?branch_id=${branchId}`);
    res.json({ success: true, warehouses: data.data || [] });
  } catch (err) {
    console.error("❌ /api/warehouses:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 3️⃣ История операций по товару
app.get("/api/goods-flow-items/:productId/:warehouseId", async (req, res) => {
  const { productId, warehouseId } = req.params;
  try {
    const cookies = await getCookies();

    // Получаем операции из API
    const apiResp = await apiGet(
      `/goods-flow/?product_id=${productId}&warehouse_id=${warehouseId}`
    );
    const apiFlow = apiResp.data || [];

    // Получаем операции из Web (если cookies есть)
    let webFlow = [];
    if (cookies) {
      try {
        const webResp = await webGet(
          `/api/v2/inventory/goods-flow?product_id=${productId}&warehouse_id=${warehouseId}`,
          cookies
        );
        webFlow = webResp.data || webResp.items || [];
      } catch (err) {
        console.warn("⚠️ Web flow fetch failed:", err.message);
      }
    }

    // Объединяем оба источника
    const combined = [...apiFlow, ...webFlow].sort(
      (a, b) =>
        new Date(a.date || a.created_at) - new Date(b.date || b.created_at)
    );

    res.json({
      success: true,
      productId,
      warehouseId,
      total: combined.length,
      data: combined,
    });
  } catch (err) {
    console.error("❌ /api/goods-flow-items:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// 4️⃣ Реальные остатки товаров по складу
app.get("/api/realtime-warehouse-goods/:warehouseId", async (req, res) => {
  const { warehouseId } = req.params;
  try {
    const cookies = await getCookies();
    const goodsResp = await apiGet(
      `/warehouse_goods/?warehouse_id=${warehouseId}`
    );
    const goodsList = goodsResp.data || [];
    const results = [];

    for (const item of goodsList) {
      const productId = item.id || item.product_id;
      if (!productId) continue;

      // Получаем потоки из API и Web
      const apiResp = await apiGet(
        `/goods-flow/?product_id=${productId}&warehouse_id=${warehouseId}`
      );
      const apiFlow = apiResp.data || [];

      let webFlow = [];
      if (cookies) {
        try {
          const webResp = await webGet(
            `/api/v2/inventory/goods-flow?product_id=${productId}&warehouse_id=${warehouseId}`,
            cookies
          );
          webFlow = webResp.data || webResp.items || [];
        } catch (err) {
          console.warn("⚠️ Web flow fetch failed for", productId, err.message);
        }
      }

      const allFlow = [...apiFlow, ...webFlow];
      allFlow.sort(
        (a, b) =>
          new Date(a.date || a.created_at) - new Date(b.date || b.created_at)
      );

      let residue = 0;
      allFlow.forEach((op) => {
        const delta = Number(op.delta ?? op.quantity ?? 0);
        residue += delta;
      });

      results.push({
        product_id: productId,
        title: item.title || item.name || "",
        article: item.article || "",
        code: item.code || "",
        uom_title: item.uom_title || "",
        calculated_residue: residue,
        total_operations: allFlow.length,
      });
    }

    res.json({
      success: true,
      warehouseId,
      totalProducts: results.length,
      data: results,
    });
  } catch (err) {
    console.error("❌ /api/realtime-warehouse-goods:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ------------------------
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

app.listen(PORT, () => {
  console.log(`🚀 Server running at http://localhost:${PORT}`);
  console.log(`🔗 RemOnline API: ${API_BASE}`);
  console.log(`🔗 Login-service: ${LOGIN_SERVICE_URL}`);
});
