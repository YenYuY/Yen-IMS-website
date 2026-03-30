const state = {
  user: null,
  dashboard: null,
  products: [],
  movements: [],
  users: [],
  flash: null,
  currentView: "dashboard",
  productFilter: "all",
  movementFilter: "all",
  userFilter: "all",
  showChangePassword: false,
  showProductForm: false,
  showUserForm: false,
  lastActivityAt: Date.now(),
};

const app = document.querySelector("#app");
const INACTIVITY_LIMIT_MS = 30 * 60 * 1000;
const SESSION_REFRESH_MS = 5 * 60 * 1000;
const SUCCESS_FLASH_MS = 13 * 1000;
let inactivityTimerId = null;
let sessionRefreshTimerId = null;
let flashTimerId = null;

const roleLabels = {
  admin: "管理員",
  manager: "主管",
  operator: "作業人員",
  viewer: "檢視者",
};

const viewLabels = {
  dashboard: "系統總覽",
  products: "商品管理",
  stockIn: "入庫管理",
  stockOut: "出庫管理",
  movements: "異動紀錄",
  users: "使用者管理",
  roles: "角色權限",
};

function hasPermission(permission) {
  return state.user?.permissions?.includes(permission);
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function stopFlashTimer() {
  if (flashTimerId) {
    window.clearTimeout(flashTimerId);
    flashTimerId = null;
  }
}

function setFlash(type, text) {
  stopFlashTimer();
  state.flash = text ? { type, text } : null;
  if (type === "success" && text) {
    flashTimerId = window.setTimeout(() => {
      state.flash = null;
      flashTimerId = null;
      render();
    }, SUCCESS_FLASH_MS);
  }
  render();
}

function clearAuthState(message, type = "success") {
  state.user = null;
  state.dashboard = null;
  state.products = [];
  state.movements = [];
  state.users = [];
  state.showChangePassword = false;
  state.showProductForm = false;
  state.showUserForm = false;
  stopSessionTimers();
  setFlash(type, message);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });
  const payload = await response.json();
  if (response.status === 401 && state.user) {
    clearAuthState("登入已逾時，請重新登入", "error");
    throw new Error(payload.error || "登入已逾時");
  }
  if (!response.ok) {
    throw new Error(payload.error || "操作失敗");
  }
  return payload;
}

function markActivity() {
  state.lastActivityAt = Date.now();
  if (state.user) {
    resetInactivityTimer();
  }
}

function resetInactivityTimer() {
  if (inactivityTimerId) {
    window.clearTimeout(inactivityTimerId);
  }
  inactivityTimerId = window.setTimeout(async () => {
    try {
      await fetch("/api/logout", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      });
    } catch (error) {
      // Ignore network/logout errors during forced timeout.
    }
    clearAuthState("閒置超過 30 分鐘，系統已自動登出", "error");
  }, INACTIVITY_LIMIT_MS);
}

function startSessionTimers() {
  stopSessionTimers();
  markActivity();
  sessionRefreshTimerId = window.setInterval(async () => {
    if (!state.user) {
      return;
    }
    if (Date.now() - state.lastActivityAt >= INACTIVITY_LIMIT_MS) {
      return;
    }
    try {
      await api("/api/session", { headers: {} });
    } catch (error) {
      // api() handles expired sessions and updates UI.
    }
  }, SESSION_REFRESH_MS);
}

function stopSessionTimers() {
  if (inactivityTimerId) {
    window.clearTimeout(inactivityTimerId);
    inactivityTimerId = null;
  }
  if (sessionRefreshTimerId) {
    window.clearInterval(sessionRefreshTimerId);
    sessionRefreshTimerId = null;
  }
}

function bindGlobalActivityListeners() {
  ["mousedown", "keydown", "touchstart", "scroll"].forEach((eventName) => {
    window.addEventListener(eventName, markActivity, { passive: true });
  });
}

function formatDate(value) {
  if (!value) return "-";
  return new Date(value).toLocaleString("zh-TW", { hour12: false });
}

function formatRole(role) {
  return roleLabels[role] || role;
}

function getNavigationItems() {
  return [
    { key: "dashboard", label: "系統總覽", show: true },
    { key: "products", label: "商品管理", show: true },
    { key: "stockIn", label: "入庫管理", show: hasPermission("stock_in") },
    { key: "stockOut", label: "出庫管理", show: hasPermission("stock_out") },
    { key: "movements", label: "異動紀錄", show: true },
    { key: "users", label: "使用者管理", show: hasPermission("manage_users") },
    { key: "roles", label: "角色權限", show: true },
  ].filter((item) => item.show);
}

function ensureAccessibleView() {
  const available = getNavigationItems().map((item) => item.key);
  if (!available.includes(state.currentView)) {
    state.currentView = available[0] || "dashboard";
  }
}

function getFilteredProducts() {
  if (state.productFilter === "low") {
    return state.products.filter((product) => product.isLowStock);
  }
  return state.products;
}

function getFilteredMovements() {
  if (state.movementFilter === "in") {
    return state.movements.filter((item) => item.movementType === "in");
  }
  if (state.movementFilter === "out") {
    return state.movements.filter((item) => item.movementType === "out");
  }
  return state.movements;
}

function getFilteredUsers() {
  if (state.userFilter === "all") {
    return state.users;
  }
  return state.users.filter((user) => user.role === state.userFilter);
}

async function loadSession() {
  const data = await api("/api/session", { headers: {} });
  state.user = data.user;
}

async function loadDashboardData() {
  const [dashboard, products, movements] = await Promise.all([
    api("/api/dashboard"),
    api("/api/products"),
    api("/api/movements"),
  ]);
  state.dashboard = dashboard;
  state.user = dashboard.user;
  state.products = products.products;
  state.movements = movements.movements;
  if (hasPermission("manage_users")) {
    const users = await api("/api/users");
    state.users = users.users;
  } else {
    state.users = [];
  }
  ensureAccessibleView();
}

async function refreshData(message) {
  await loadDashboardData();
  setFlash(message ? "success" : null, message || null);
}

function renderFlash() {
  if (!state.flash) return "";
  return `<div class="message ${state.flash.type}">${escapeHtml(state.flash.text)}</div>`;
}

function renderLogin() {
  app.innerHTML = `
    <div class="login-shell">
      <div class="login-card">
        <span class="eyebrow">Inventory Management</span>
        <h1>庫存管理系統</h1>
        <p>先使用管理員帳號登入，再依角色分配權限與執行入庫、出庫作業。</p>
        ${renderFlash()}
        <form id="login-form" class="form-grid">
          <label>
            帳號
            <input name="username" autocomplete="username" placeholder="請輸入帳號" required />
          </label>
          <label>
            密碼
            <input name="password" type="password" autocomplete="current-password" placeholder="請輸入密碼" required />
          </label>
          <button class="primary-btn" type="submit">登入系統</button>
        </form>
        <div class="muted-box">
          預設管理員帳號：<strong>admin</strong><br />
          預設密碼：<strong>Admin@123456</strong>
        </div>
      </div>
    </div>
  `;

  document.querySelector("#login-form").addEventListener("submit", handleLogin);
}

function renderShell() {
  ensureAccessibleView();
  const navItems = getNavigationItems();
  const currentLabel = viewLabels[state.currentView] || "系統";

  app.innerHTML = `
    <div class="admin-layout">
      <aside class="sidebar">
        <div class="brand-block">
          <div class="brand-mark">IMS</div>
          <div>
            <strong>庫存 IMS</strong>
            <span>Inventory Manager</span>
          </div>
        </div>
        <nav class="sidebar-nav">
          ${navItems
            .map(
              (item) => `
                <button class="nav-item ${item.key === state.currentView ? "active" : ""}" data-view="${item.key}">
                  <span>${item.label}</span>
                </button>
              `
            )
            .join("")}
        </nav>
      </aside>

      <div class="workspace">
        <header class="topbar">
          <div>
            <div class="topbar-caption">${escapeHtml(currentLabel)}</div>
            <h1>${escapeHtml(currentLabel)}</h1>
          </div>
          <div class="topbar-actions">
            <div class="user-pill">
              <strong>${escapeHtml(state.user.fullName)}</strong>
              <span>${escapeHtml(formatRole(state.user.role))}</span>
            </div>
            <button id="refresh-btn" class="topbar-btn">重新整理</button>
            <button id="change-password-btn" class="topbar-btn">修改密碼</button>
            <button id="logout-btn" class="topbar-btn topbar-btn-danger">登出</button>
          </div>
        </header>

        <main class="workspace-body">
          ${renderFlash()}
          ${renderView()}
        </main>
      </div>
      ${renderChangePasswordModal()}
    </div>
  `;

  bindShellEvents();
}

function renderView() {
  switch (state.currentView) {
    case "products":
      return renderProductsView();
    case "stockIn":
      return renderMovementView("in");
    case "stockOut":
      return renderMovementView("out");
    case "movements":
      return renderMovementsView();
    case "users":
      return renderUsersView();
    case "roles":
      return renderRolesView();
    case "dashboard":
    default:
      return renderOverviewView();
  }
}

function renderOverviewView() {
  const stats = state.dashboard?.stats || {};
  const lowStockList = state.products.filter((item) => item.isLowStock).slice(0, 5);
  const recentMovements = state.movements.slice(0, 5);

  return `
    <section class="page-panel">
      <div class="page-header">
        <div>
          <h2>營運概況</h2>
          <p>集中查看商品、庫存異動與帳號授權概況。</p>
        </div>
      </div>
      <div class="dashboard-metrics">
        <article class="metric-card">
          <span>商品數量</span>
          <strong>${stats.productCount || 0}</strong>
        </article>
        <article class="metric-card">
          <span>系統使用者</span>
          <strong>${stats.userCount || 0}</strong>
        </article>
        <article class="metric-card">
          <span>低庫存品項</span>
          <strong>${stats.lowStockCount || 0}</strong>
        </article>
        <article class="metric-card">
          <span>總庫存單位數</span>
          <strong>${stats.totalUnits || 0}</strong>
        </article>
      </div>
      <div class="split-layout">
        <section class="subpanel">
          <div class="subpanel-head">
            <h3>低庫存提醒</h3>
            <button class="link-btn" data-view-jump="products">前往商品管理</button>
          </div>
          ${
            lowStockList.length
              ? `
                <div class="list-stack">
                  ${lowStockList
                    .map(
                      (item) => `
                      <article class="list-row">
                        <div>
                          <strong>${escapeHtml(item.name)}</strong>
                          <span>${escapeHtml(item.sku)}</span>
                        </div>
                        <div class="align-right">
                          <strong>${item.quantity}</strong>
                          <span>安全庫存 ${item.minQuantity}</span>
                        </div>
                      </article>
                    `
                    )
                    .join("")}
                </div>
              `
              : `<div class="empty-state">目前沒有低庫存品項。</div>`
          }
        </section>
        <section class="subpanel">
          <div class="subpanel-head">
            <h3>最近異動</h3>
            <button class="link-btn" data-view-jump="movements">查看全部</button>
          </div>
          ${
            recentMovements.length
              ? `
                <div class="list-stack">
                  ${recentMovements
                    .map(
                      (item) => `
                      <article class="list-row">
                        <div>
                          <strong>${escapeHtml(item.productName)}</strong>
                          <span>${item.movementType === "in" ? "入庫" : "出庫"}｜${escapeHtml(item.createdBy)}</span>
                        </div>
                        <div class="align-right">
                          <strong>${item.quantity}</strong>
                          <span>${formatDate(item.createdAt)}</span>
                        </div>
                      </article>
                    `
                    )
                    .join("")}
                </div>
              `
              : `<div class="empty-state">目前沒有異動紀錄。</div>`
          }
        </section>
      </div>
    </section>
  `;
}

function renderProductsView() {
  const products = getFilteredProducts();

  return `
    <section class="page-panel">
      <div class="page-header page-header-spread">
        <div>
          <h2>商品管理</h2>
          <p>維護商品主檔與目前庫存狀態。</p>
        </div>
        ${
          hasPermission("manage_products")
            ? `<button class="action-btn" data-toggle-form="product-form-wrap">新增商品</button>`
            : ""
        }
      </div>

      ${
        hasPermission("manage_products")
          ? `
            <section id="product-form-wrap" class="form-card ${state.showProductForm ? "" : "collapsed"}">
              ${renderProductForm()}
            </section>
          `
          : ""
      }

      <div class="toolbar">
        <div class="tab-row">
          <button class="tab-btn ${state.productFilter === "all" ? "active" : ""}" data-product-filter="all">全部</button>
          <button class="tab-btn ${state.productFilter === "low" ? "active" : ""}" data-product-filter="low">低庫存</button>
        </div>
        <div class="toolbar-note">共 ${products.length} 筆商品</div>
      </div>

      ${renderProductsTable(products)}
    </section>
  `;
}

function renderMovementView(type) {
  const isIn = type === "in";
  const title = isIn ? "入庫管理" : "出庫管理";
  const description = isIn ? "登錄採購入庫、盤點補入等動作。" : "登錄出貨、領料、報廢等動作。";
  const filtered = state.movements.filter((item) => item.movementType === type).slice(0, 10);

  return `
    <section class="page-panel">
      <div class="page-header">
        <div>
          <h2>${title}</h2>
          <p>${description}</p>
        </div>
      </div>

      <div class="split-layout">
        <section class="subpanel">
          <div class="subpanel-head">
            <h3>${title}表單</h3>
          </div>
          ${renderMovementForm(type)}
        </section>
        <section class="subpanel">
          <div class="subpanel-head">
            <h3>最近${isIn ? "入庫" : "出庫"}紀錄</h3>
          </div>
          ${
            filtered.length
              ? `
                <div class="list-stack">
                  ${filtered
                    .map(
                      (item) => `
                      <article class="list-row">
                        <div>
                          <strong>${escapeHtml(item.productName)}</strong>
                          <span>${escapeHtml(item.referenceNo || "未填單號")}</span>
                        </div>
                        <div class="align-right">
                          <strong>${item.quantity}</strong>
                          <span>${formatDate(item.createdAt)}</span>
                        </div>
                      </article>
                    `
                    )
                    .join("")}
                </div>
              `
              : `<div class="empty-state">目前還沒有${isIn ? "入庫" : "出庫"}紀錄。</div>`
          }
        </section>
      </div>
    </section>
  `;
}

function renderMovementsView() {
  const movements = getFilteredMovements();

  return `
    <section class="page-panel">
      <div class="page-header">
        <div>
          <h2>異動紀錄</h2>
          <p>查看所有庫存入出庫歷程與操作人。</p>
        </div>
      </div>

      <div class="toolbar">
        <div class="tab-row">
          <button class="tab-btn ${state.movementFilter === "all" ? "active" : ""}" data-movement-filter="all">全部</button>
          <button class="tab-btn ${state.movementFilter === "in" ? "active" : ""}" data-movement-filter="in">入庫</button>
          <button class="tab-btn ${state.movementFilter === "out" ? "active" : ""}" data-movement-filter="out">出庫</button>
        </div>
        <div class="toolbar-note">最近 ${movements.length} 筆</div>
      </div>

      ${renderMovementsTable(movements)}
    </section>
  `;
}

function renderUsersView() {
  const users = getFilteredUsers();

  return `
    <section class="page-panel">
      <div class="page-header page-header-spread">
        <div>
          <h2>使用者管理</h2>
          <p>由管理員建立帳號並指定系統角色。</p>
        </div>
        <button class="action-btn" data-toggle-form="user-form-wrap">新增使用者</button>
      </div>

      <section id="user-form-wrap" class="form-card ${state.showUserForm ? "" : "collapsed"}">
        ${renderUserForm()}
      </section>

      <div class="toolbar">
        <div class="tab-row">
          <button class="tab-btn ${state.userFilter === "all" ? "active" : ""}" data-user-filter="all">全部</button>
          <button class="tab-btn ${state.userFilter === "admin" ? "active" : ""}" data-user-filter="admin">管理員</button>
          <button class="tab-btn ${state.userFilter === "manager" ? "active" : ""}" data-user-filter="manager">主管</button>
          <button class="tab-btn ${state.userFilter === "operator" ? "active" : ""}" data-user-filter="operator">作業人員</button>
          <button class="tab-btn ${state.userFilter === "viewer" ? "active" : ""}" data-user-filter="viewer">檢視者</button>
        </div>
        <div class="toolbar-note">共 ${users.length} 位使用者</div>
      </div>

      ${renderUsersTable(users)}
    </section>
  `;
}

function renderRolesView() {
  const roles = state.dashboard?.roles || [];

  return `
    <section class="page-panel">
      <div class="page-header">
        <div>
          <h2>角色權限</h2>
          <p>目前先採固定角色，依角色決定可操作功能。</p>
        </div>
      </div>
      <div class="role-grid">
        ${roles
          .map(
            (item) => `
            <article class="role-card">
              <h3>${escapeHtml(formatRole(item.role))}</h3>
              <div class="badge-stack">
                ${item.permissions.map((permission) => `<span class="badge">${escapeHtml(permission)}</span>`).join("")}
              </div>
            </article>
          `
          )
          .join("")}
      </div>
    </section>
  `;
}

function renderProductForm() {
  return `
    <form id="product-form" class="inline-form columns-2">
      <label>
        商品編號 SKU
        <input name="sku" placeholder="例如：ITEM-001" required />
      </label>
      <label>
        商品名稱
        <input name="name" placeholder="例如：筆記型電腦" required />
      </label>
      <label>
        單位
        <input name="unit" placeholder="pcs / 箱 / 台" value="pcs" />
      </label>
      <label>
        安全庫存
        <input name="minQuantity" type="number" min="0" value="0" />
      </label>
      <label class="span-2">
        商品說明
        <textarea name="description" placeholder="可填品牌、規格、儲位等資訊"></textarea>
      </label>
      <div>
        <button class="primary-btn" type="submit">建立商品</button>
      </div>
    </form>
  `;
}

function renderMovementForm(type) {
  const options = state.products
    .map((product) => `<option value="${product.id}">${escapeHtml(product.sku)} ｜ ${escapeHtml(product.name)}</option>`)
    .join("");

  return `
    <form id="movement-form" data-movement-type="${type}" class="inline-form columns-2">
      <label>
        商品
        <select name="productId" required>
          <option value="">請選擇商品</option>
          ${options}
        </select>
      </label>
      <label>
        數量
        <input name="quantity" type="number" min="1" value="1" required />
      </label>
      <label>
        單號 / 參考編號
        <input name="referenceNo" placeholder="例如：${type === "in" ? "PO-20260330-001" : "SO-20260330-001"}" />
      </label>
      <label>
        操作摘要
        <input name="note" placeholder="例如：${type === "in" ? "採購入庫" : "客戶出貨"}" />
      </label>
      <div>
        <button class="primary-btn" type="submit">${type === "in" ? "送出入庫" : "送出出庫"}</button>
      </div>
    </form>
  `;
}

function renderProductsTable(products) {
  if (!products.length) {
    return `<div class="empty-state">目前沒有符合條件的商品資料。</div>`;
  }

  return `
    <div class="table-card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>商品</th>
              <th>SKU</th>
              <th>庫存</th>
              <th>安全庫存</th>
              <th>說明</th>
              <th>更新時間</th>
            </tr>
          </thead>
          <tbody>
            ${products
              .map(
                (product) => `
                <tr>
                  <td>
                    <strong>${escapeHtml(product.name)}</strong><br />
                    <span>${escapeHtml(product.unit)}</span>
                  </td>
                  <td>${escapeHtml(product.sku)}</td>
                  <td>
                    ${product.quantity}
                    ${product.isLowStock ? '<span class="badge low">低庫存</span>' : ""}
                  </td>
                  <td>${product.minQuantity}</td>
                  <td>${escapeHtml(product.description || "-")}</td>
                  <td>${formatDate(product.updatedAt)}</td>
                </tr>
              `
              )
              .join("")}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

function renderMovementsTable(movements) {
  if (!movements.length) {
    return `<div class="empty-state">目前沒有符合條件的異動紀錄。</div>`;
  }

  return `
    <div class="table-card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>時間</th>
              <th>類型</th>
              <th>商品</th>
              <th>數量</th>
              <th>單號</th>
              <th>操作人</th>
              <th>備註</th>
            </tr>
          </thead>
          <tbody>
            ${movements
              .map(
                (movement) => `
                <tr>
                  <td>${formatDate(movement.createdAt)}</td>
                  <td>${movement.movementType === "in" ? "入庫" : "出庫"}</td>
                  <td><strong>${escapeHtml(movement.productName)}</strong><br />${escapeHtml(movement.productSku)}</td>
                  <td>${movement.quantity}</td>
                  <td>${escapeHtml(movement.referenceNo || "-")}</td>
                  <td>${escapeHtml(movement.createdBy)}</td>
                  <td>${escapeHtml(movement.note || "-")}</td>
                </tr>
              `
              )
              .join("")}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

function renderUserForm() {
  return `
    <form id="user-form" class="inline-form columns-2">
      <label>
        帳號
        <input name="username" placeholder="例如：amy.lin" required />
      </label>
      <label>
        姓名
        <input name="fullName" placeholder="例如：林小美" required />
      </label>
      <label>
        密碼
        <input name="password" type="password" minlength="8" placeholder="至少 8 碼" required />
      </label>
      <label>
        角色
        <select name="role">
          <option value="manager">主管</option>
          <option value="operator">作業人員</option>
          <option value="viewer">檢視者</option>
          <option value="admin">管理員</option>
        </select>
      </label>
      <div>
        <button class="primary-btn" type="submit">建立使用者</button>
      </div>
    </form>
  `;
}

function renderUsersTable(users) {
  return `
    <div class="table-card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>帳號</th>
              <th>姓名</th>
              <th>角色</th>
              <th>建立時間</th>
            </tr>
          </thead>
          <tbody>
            ${users
              .map(
                (user) => `
                <tr>
                  <td>${escapeHtml(user.username)}</td>
                  <td>${escapeHtml(user.fullName)}</td>
                  <td>${escapeHtml(formatRole(user.role))}</td>
                  <td>${formatDate(user.createdAt)}</td>
                </tr>
              `
              )
              .join("")}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

function renderChangePasswordModal() {
  if (!state.showChangePassword) {
    return "";
  }

  return `
    <div class="modal-backdrop">
      <div class="modal-card">
        <div class="subpanel-head">
          <h3>修改密碼</h3>
          <button id="close-password-modal" class="ghost-btn" type="button">關閉</button>
        </div>
        <form id="password-form" class="form-grid">
          <label>
            舊密碼
            <input name="oldPassword" type="password" required />
          </label>
          <label>
            新密碼
            <input name="newPassword" type="password" minlength="8" required />
          </label>
          <label>
            確認新密碼
            <input name="confirmPassword" type="password" minlength="8" required />
          </label>
          <button class="primary-btn" type="submit">更新密碼</button>
        </form>
      </div>
    </div>
  `;
}

function bindShellEvents() {
  document.querySelectorAll("[data-view]").forEach((button) => {
    button.addEventListener("click", () => {
      state.currentView = button.dataset.view;
      render();
    });
  });

  document.querySelectorAll("[data-view-jump]").forEach((button) => {
    button.addEventListener("click", () => {
      state.currentView = button.dataset.viewJump;
      render();
    });
  });

  document.querySelectorAll("[data-toggle-form]").forEach((button) => {
    button.addEventListener("click", () => {
      if (button.dataset.toggleForm === "product-form-wrap") {
        state.showProductForm = !state.showProductForm;
        render();
        return;
      }
      if (button.dataset.toggleForm === "user-form-wrap") {
        state.showUserForm = !state.showUserForm;
        render();
      }
    });
  });

  document.querySelectorAll("[data-product-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      state.productFilter = button.dataset.productFilter;
      render();
    });
  });

  document.querySelectorAll("[data-movement-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      state.movementFilter = button.dataset.movementFilter;
      render();
    });
  });

  document.querySelectorAll("[data-user-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      state.userFilter = button.dataset.userFilter;
      render();
    });
  });

  document.querySelector("#refresh-btn")?.addEventListener("click", async () => {
    try {
      await refreshData("資料已更新");
    } catch (error) {
      setFlash("error", error.message);
    }
  });

  document.querySelector("#change-password-btn")?.addEventListener("click", () => {
    state.showChangePassword = true;
    render();
  });

  document.querySelector("#close-password-modal")?.addEventListener("click", () => {
    state.showChangePassword = false;
    render();
  });

  document.querySelector("#logout-btn")?.addEventListener("click", handleLogout);

  document.querySelector("#product-form")?.addEventListener("submit", handleCreateProduct);
  document.querySelector("#movement-form")?.addEventListener("submit", handleCreateMovement);
  document.querySelector("#user-form")?.addEventListener("submit", handleCreateUser);
  document.querySelector("#password-form")?.addEventListener("submit", handleChangePassword);
}

async function handleLogin(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const formData = new FormData(form);

  try {
    stopFlashTimer();
    state.flash = null;
    await api("/api/login", {
      method: "POST",
      body: JSON.stringify({
        username: formData.get("username"),
        password: formData.get("password"),
      }),
    });
    state.currentView = "dashboard";
    state.showProductForm = false;
    await refreshData("登入成功");
    startSessionTimers();
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleLogout() {
  try {
    await api("/api/logout", { method: "POST", body: "{}" });
    clearAuthState("已登出");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleCreateProduct(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const data = new FormData(form);

  try {
    await api("/api/products", {
      method: "POST",
      body: JSON.stringify({
        sku: data.get("sku"),
        name: data.get("name"),
        unit: data.get("unit"),
        minQuantity: data.get("minQuantity"),
        description: data.get("description"),
      }),
    });
    form.reset();
    form.querySelector('[name="unit"]').value = "pcs";
    form.querySelector('[name="minQuantity"]').value = "0";
    state.showProductForm = false;
    await refreshData("商品已建立");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleCreateMovement(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const data = new FormData(form);
  const movementType = form.dataset.movementType;

  try {
    await api(movementType === "in" ? "/api/stock-in" : "/api/stock-out", {
      method: "POST",
      body: JSON.stringify({
        productId: data.get("productId"),
        quantity: data.get("quantity"),
        referenceNo: data.get("referenceNo"),
        note: data.get("note"),
      }),
    });
    form.reset();
    form.querySelector('[name="quantity"]').value = "1";
    await refreshData(movementType === "in" ? "入庫完成" : "出庫完成");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleCreateUser(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const data = new FormData(form);

  try {
    await api("/api/users", {
      method: "POST",
      body: JSON.stringify({
        username: data.get("username"),
        fullName: data.get("fullName"),
        password: data.get("password"),
        role: data.get("role"),
      }),
    });
    form.reset();
    await refreshData("使用者已建立");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleChangePassword(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const data = new FormData(form);

  try {
    await api("/api/change-password", {
      method: "POST",
      body: JSON.stringify({
        oldPassword: data.get("oldPassword"),
        newPassword: data.get("newPassword"),
        confirmPassword: data.get("confirmPassword"),
      }),
    });
    clearAuthState("密碼已更新，請重新登入");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function bootstrap() {
  try {
    await loadSession();
    if (state.user) {
      await loadDashboardData();
      startSessionTimers();
      renderShell();
    } else {
      renderLogin();
    }
  } catch (error) {
    setFlash("error", error.message);
    renderLogin();
  }
}

function render() {
  if (state.user) {
    renderShell();
  } else {
    renderLogin();
  }
}

bindGlobalActivityListeners();
bootstrap();
