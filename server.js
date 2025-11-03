// server.js — финальная версия RemOnline Sync без BigQuery
// =========================================================

import express from "express";
import fetch from "node-fetch";
import dotenv from "dotenv";
import cors from "cors";

dotenv.config();

const app = express();
app.use(express.json());
app.use(cors());

// ---------------------
// 🔧 Конфигурация
// ---------------------
const PORT = process.env.PORT || 3000;
const API_URL = "https://api.roapp.io";
const LOGIN_SERVICE_URL = process.env.LOGIN_SERVICE_URL;
const API_TOKEN = process.env.REMONLINE_API_TOKEN;

// ---------------------
// 🧰 Утилиты
// ---------------------
async function apiGet(path) {
  const url = `${API_URL}${path}`;
  const res = await fetch(url, {
    headers: {
      accept: "application/json",
      authorization: `Bearer ${API_TOKEN}`,
    },
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`API error ${res.status}: ${txt}`);
  }
  return await res.json();
}

async function webGet(path, cookies) {
  const res = await fetch(`https://web.roapp.io${path}`, {
    headers: {
      accept: "application/json",
      cookie: cookies,
    },
  });
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`WEB error ${res.status}: ${txt}`);
  }
  return await res.json();
}

// ---------------------
// 🔑 Получение cookies
// ---------------------
async function getCookies() {
  try {
    const res = await fetch(`${LOGIN_SERVICE_URL}/get-cookies`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        username: process.env.REMONLINE_EMAIL,
        password: process.env.REMONLINE_PASSWORD,
      }),
    });
    if (!res.ok) throw new Error(`Login service error ${res.status}`);
    const data = await res.json();
    if (data.success && data.cookies) {
      return data.cookies;
    } else {
      throw new Error("Login-service did not return cookies");
    }
  } catch (err) {
    console.warn("⚠️ getCookies failed:", err.message);
    return null;
  }
}

// ---------------------
// 📍 1. Список локаций
// ---------------------
app.get("/api/branches", async (req, res) => {
  try {
    const branches = [
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
    res.json(branches);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------------------
// 🏢 2. Список складов по локации
// ---------------------
app.get("/api/warehouses/:branchId", async (req, res) => {
  try {
    const { branchId } = req.params;
    const data = await apiGet(`/warehouse/?branch_id=${branchId}`);
    res.json(data.data || []);
  } catch (err) {
    console.error("❌ /api/warehouses:", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ---------------------
// 📦 3. Остатки по складу
// ---------------------
app.get("/api/realtime-warehouse-goods/:warehouseId", async (req, res) => {
  const { warehouseId } = req.params;
  try {
    const goodsResp = await apiGet(
      `/warehouse/goods/${warehouseId}?exclude_zero_residue=true`
    );

    const goodsList = goodsResp.data || [];
    const results = goodsList.map((item) => ({
      product_id: item.id,
      title: item.title,
      article: item.article || "",
      category: item.category?.title || "",
      uom_title: item.uom?.title || "",
      image: item.image?.[0] || "",
      residue: item.residue ?? 0,
    }));

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

// ---------------------
// 📈 4. История товара по складу
// ---------------------
app.get("/api/goods-history/:productId/:warehouseId", async (req, res) => {
  const { productId, warehouseId } = req.params;
  const branchId = req.query.branch_id;

  if (!branchId) {
    return res
      .status(400)
      .json({ success: false, error: "branch_id є обов'язковим параметром" });
  }

  try {
    const cookies = await getCookies();

    // 🔹 Основні операції через Bearer API
    const [postings, moves, outcomes, sales] = await Promise.all([
      apiGet(
        `/warehouse/postings/?warehouse_ids[]=${warehouseId}&branch_id=${branchId}`
      ),
      apiGet(
        `/warehouse/moves/?warehouse_id=${warehouseId}&branch_id=${branchId}`
      ),
      apiGet(
        `/warehouse/outcome-transactions/?warehouse_id=${warehouseId}&branch_id=${branchId}`
      ),
      apiGet(
        `/retail/sales/?branch_id=${branchId}&warehouse_id=${warehouseId}`
      ),
    ]);

    const allOps = [];

    const pushOps = (arr, type) => {
      if (!arr?.data) return;
      for (const item of arr.data) {
        for (const p of item.products || []) {
          if (String(p.id) === String(productId)) {
            const qty = p.quantity || p.qty || p.amount || 0;

            // определяем знак
            let delta = qty;
            if (
              [
                "Переміщення",
                "Списання",
                "Продаж",
                "Повернення постачальнику",
              ].includes(type)
            ) {
              delta = -Math.abs(qty);
            }

            allOps.push({
              type,
              date: new Date(item.created_at || item.date || Date.now()),
              delta,
            });
          }
        }
      }
    };

    pushOps(postings, "Оприходування");
    pushOps(moves, "Переміщення");
    pushOps(outcomes, "Списання");
    pushOps(sales, "Продаж");

    // 🔹 Додатково — Заказ і Повернення (через cookies)
    if (cookies) {
      try {
        const orders = await webGet(
          `/api/v2/warehouse/orders/?warehouse_id=${warehouseId}&branch_id=${branchId}`,
          cookies
        );
        pushOps(orders, "Замовлення");
      } catch (e) {
        console.warn("⚠️ Orders fetch failed:", e.message);
      }

      try {
        const returns = await webGet(
          `/api/v2/warehouse/returns/?warehouse_id=${warehouseId}&branch_id=${branchId}`,
          cookies
        );
        pushOps(returns, "Повернення постачальнику");
      } catch (e) {
        console.warn("⚠️ Returns fetch failed:", e.message);
      }
    }

    allOps.sort((a, b) => new Date(a.date) - new Date(b.date));

    res.json({
      success: true,
      total: allOps.length,
      productId,
      warehouseId,
      branchId,
      history: allOps,
    });
  } catch (err) {
    console.error("❌ /api/goods-history:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ✅ Обслуживаем всё содержимое /public (HTML, CSS, JS)
app.use(express.static(path.join(__dirname, "public")));

// ✅ Если запрашивается "/", возвращаем index.html
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "public", "index.html"));
});

// ---------------------
// 🚀 Старт сервера
// ---------------------
app.listen(PORT, () => {
  console.log(`🚀 Server running at http://localhost:${PORT}`);
  console.log(`🔗 RemOnline API: ${API_URL}`);
  console.log(`🔗 Login-service: ${LOGIN_SERVICE_URL}`);
});
