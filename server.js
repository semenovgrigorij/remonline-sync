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
  const url = `https://app.remonline.ua${endpoint}`;
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
async function getCookies() {
  try {
    const res = await fetch(`${LOGIN_SERVICE_URL}/get-cookies`, {
      method: "POST",
    });
    const data = await res.json();
    if (!data?.success) throw new Error("Login service error");
    return data.cookies;
  } catch (e) {
    console.warn("⚠️ getCookies failed:", e.message);
    return null; // не прерываем выполнение
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
    const allOps = [];

    // 🔸 Оприходування
    const postings = await fetchAllPages(
      `/warehouse/postings/?warehouse_ids[]=${warehouseId}&branch_id=${branchId}`
    );
    for (const item of postings)
      for (const p of item.products || [])
        if (String(p.id) === String(productId))
          allOps.push({
            type: "Оприходування",
            date: new Date(item.created_at),
            delta: +Math.abs(p.amount || 0),
          });

    // 🔸 Переміщення
    const moves = await fetchAllPages(
      `/warehouse/moves/?warehouse_id=${warehouseId}&branch_id=${branchId}`
    );
    for (const item of moves)
      for (const p of item.products || [])
        if (String(p.id) === String(productId))
          allOps.push({
            type: "Переміщення",
            date: new Date(item.created_at),
            delta: -Math.abs(p.amount || 0),
          });

    // 🔸 Списання
    const outcomes = await fetchAllPages(
      `/warehouse/outcome-transactions/?warehouse_id=${warehouseId}&branch_id=${branchId}`
    );
    for (const item of outcomes)
      for (const p of item.products || [])
        if (String(p.id) === String(productId))
          allOps.push({
            type: "Списання",
            date: new Date(item.created_at),
            delta: -Math.abs(p.amount || 0),
          });

    // 🔸 Продаж
    const sales = await fetchAllPages(
      `/retail/sales/?branch_id=${branchId}&warehouse_id=${warehouseId}`
    );
    for (const item of sales)
      for (const p of item.products || [])
        if (String(p.id) === String(productId))
          allOps.push({
            type: "Продаж",
            date: new Date(item.created_at),
            delta: -Math.abs(p.amount || 0),
          });

    // 🔹 Замовлення / Повернення постачальнику (через cookies)
    if (cookies) {
      try {
        const orders = await fetchAllPages(
          `/api/v2/warehouse/orders/?warehouse_id=${warehouseId}&branch_id=${branchId}`,
          true,
          cookies
        );
        for (const item of orders)
          for (const p of item.products || [])
            if (String(p.id) === String(productId))
              allOps.push({
                type: "Замовлення",
                date: new Date(item.created_at),
                delta: +Math.abs(p.amount || 0),
              });
      } catch (e) {
        console.warn("⚠️ Orders fetch failed:", e.message);
      }

      try {
        const returns = await fetchAllPages(
          `/api/v2/warehouse/returns/?warehouse_id=${warehouseId}&branch_id=${branchId}`,
          true,
          cookies
        );
        for (const item of returns)
          for (const p of item.products || [])
            if (String(p.id) === String(productId))
              allOps.push({
                type: "Повернення постачальнику",
                date: new Date(item.created_at),
                delta: -Math.abs(p.amount || 0),
              });
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
