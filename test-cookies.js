// === test-cookies.js ===
// Тестовий скрипт для перевірки cookies від login service

import fetch from "node-fetch";
import dotenv from "dotenv";

dotenv.config();

const LOGIN_SERVICE_URL = process.env.LOGIN_SERVICE_URL;
const USERNAME = process.env.REMONLINE_USERNAME;
const PASSWORD = process.env.REMONLINE_PASSWORD;

async function testCookies() {
  console.log("=== Тестування Login Service ===\n");

  // 1. Перевірка статусу login service
  console.log("1️⃣ Перевірка статусу login service...");
  try {
    const statusRes = await fetch(LOGIN_SERVICE_URL);
    const status = await statusRes.json();
    console.log("✅ Status:", status);
    console.log("");
  } catch (e) {
    console.error("❌ Помилка:", e.message);
    return;
  }

  // 2. Отримання cookies (з кешу або нових)
  console.log("2️⃣ Отримання cookies...");
  try {
    const cookiesRes = await fetch(`${LOGIN_SERVICE_URL}/get-cookies`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: USERNAME, password: PASSWORD }),
    });
    const cookiesData = await cookiesRes.json();

    if (cookiesData.success) {
      console.log("✅ Cookies отримано");
      console.log("📦 Cached:", cookiesData.cached);
      console.log("⏱️ Expires in:", cookiesData.expiresIn, "секунд");
      console.log("🍪 Cookies length:", cookiesData.cookies.length, "символів");
      console.log(
        "🍪 Cookies preview:",
        cookiesData.cookies.substring(0, 100) + "..."
      );
      console.log("");

      // 3. Тестування cookies на RemOnline API
      console.log("3️⃣ Тестування cookies на RemOnline API...");

      const testUrl =
        "https://web.roapp.io/app/warehouse/get-goods-flow-items?page=1&pageSize=1&id=46955809&startDate=0&endDate=" +
        Date.now();

      const apiRes = await fetch(testUrl, {
        headers: {
          cookie: cookiesData.cookies,
          accept: "application/json",
        },
      });

      console.log("📊 Status:", apiRes.status, apiRes.statusText);

      if (apiRes.status === 200) {
        const data = await apiRes.json();
        console.log("✅ API працює! Знайдено записів:", data.data?.length || 0);
      } else if (apiRes.status === 401) {
        console.log("❌ 401 Unauthorized - cookies НЕ ВАЛІДНІ!");
        const errorText = await apiRes.text();
        console.log("📄 Відповідь:", errorText.substring(0, 200));

        // 4. Спроба оновити cookies примусово
        console.log("\n4️⃣ Примусове оновлення cookies...");
        const freshRes = await fetch(
          `${LOGIN_SERVICE_URL}/get-cookies?force=true`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ username: USERNAME, password: PASSWORD }),
          }
        );
        const freshData = await freshRes.json();

        if (freshData.success) {
          console.log("✅ Нові cookies отримано");
          console.log("🍪 Нові cookies length:", freshData.cookies.length);

          // Тестуємо нові cookies
          const retryRes = await fetch(testUrl, {
            headers: {
              cookie: freshData.cookies,
              accept: "application/json",
            },
          });

          console.log("📊 Повторний тест - Status:", retryRes.status);

          if (retryRes.status === 200) {
            console.log("✅ Нові cookies ПРАЦЮЮТЬ!");
          } else {
            console.log("❌ Нові cookies ТЕАЖ НЕ ПРАЦЮЮТЬ!");
            console.log(
              "⚠️ Можливо проблема з акаунтом або доступом до складу"
            );
          }
        }
      } else {
        console.log("⚠️ Інший статус:", apiRes.status);
      }
    } else {
      console.error("❌ Помилка отримання cookies:", cookiesData.error);
    }
  } catch (e) {
    console.error("❌ Помилка:", e.message);
  }
}

testCookies();
