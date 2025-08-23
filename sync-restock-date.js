require("dotenv").config();

const API_VERSION = "2025-07";
const ENDPOINT = `https://${process.env.SHOPIFY_STORE}/admin/api/${API_VERSION}/graphql`; // /graphql（.jsonなし）

async function gql(query, variables = {}) {
  const res = await fetch(ENDPOINT, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Shopify-Access-Token": process.env.SHOPIFY_ADMIN_TOKEN,
    },
    body: JSON.stringify({ query, variables }),
  });

  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { errors: text };
  }

  if (!res.ok) {
    throw new Error(
      `HTTP ${res.status} ${res.statusText}\nendpoint: ${ENDPOINT}\nbody: ${typeof json === "object" ? JSON.stringify(json) : String(json)}`
    );
  }
  if (json.errors) {
    throw new Error(`GraphQL errors: ${JSON.stringify(json.errors, null, 2)}`);
  }
  return json.data;
}

// ざっくりCSVパーサ（カンマのみ想定・ダブルクォートなし想定）
function parseCsv(text) {
  return text
    .replace(/\r/g, "")
    .trim()
    .split("\n")
    .map((line) => line.split(","));
}

(async () => {
  // 1) 接続テスト
  try {
    console.log("***** テスト: 接続確認クエリ送信…");
    const shopData = await gql(`{ shop { name } }`);
    console.log("接続成功！Shop name:", shopData.shop.name);
  } catch (e) {
    console.error("GraphQL 接続テスト失敗：", e);
    process.exit(1);
  }

  // 2) CSV取得（A=SKU, B=在庫, C=再入荷日）
  const csvText = await (await fetch(process.env.SHEET_CSV_URL)).text();
  const rows = parseCsv(csvText);
  // ヘッダー行を除外して正規化
  const dataRows = rows
    .slice(1)
    .map((cols) => {
      const [sku = "", stock = "", restock = ""] = cols;
      return {
        sku: String(sku).trim(),
        stock: Number(String(stock).trim()),
        restock: String(restock).trim(),
      };
    })
    .filter((r) => r.sku);

  const targets = dataRows.filter((r) => r.stock === 0);
  console.log(`対象SKU: ${targets.map((r) => r.sku).join(", ") || "(なし)"}`);

  if (targets.length === 0) {
    console.log("在庫0の対象がないため、同期はスキップします。");
    return;
  }

  // 3) SKU → variant.id 解決
  const variantMap = new Map();
  const chunk = (arr, size) => {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
  };

  for (const group of chunk(targets, 20)) {
    const query = `
      query($q: String!) {
        productVariants(first: 100, query: $q) {
          nodes { id sku }
        }
      }
    `;
    const skusQuery = group.map((r) => `sku:${r.sku}`).join(" OR ");
    const data = await gql(query, { q: skusQuery });
    data.productVariants.nodes.forEach((v) => {
      if (v.sku) variantMap.set(v.sku, v.id);
    });
  }

  // 4) metafieldsSet で再入荷日を書き込み（variant の custom.restock_date）
  const toWrite = targets
    .filter((r) => variantMap.get(r.sku))
    .map((r) => ({
      ownerId: variantMap.get(r.sku),
      namespace: "custom",
      key: "restock_date",
      type: "single_line_text_field",
      value: r.restock || "",
    }));

  if (toWrite.length === 0) {
    console.warn(
      "SKU→Variant 解決結果が空です。SKUの綴り/一致を確認してください。"
    );
    return;
  }

  for (const group of chunk(toWrite, 25)) {
    // 25件/回が上限
    const mutation = `
      mutation($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) {
          metafields { key namespace value owner { __typename ... on ProductVariant { id } } }
          userErrors { field message }
        }
      }
    `;
    const res = await gql(mutation, { metafields: group });
    if (res.metafieldsSet.userErrors && res.metafieldsSet.userErrors.length) {
      console.error("UserErrors:", res.metafieldsSet.userErrors);
      throw new Error("metafieldsSet failed.");
    }
  }

  console.log("✅ restock_date synced.");
})();
