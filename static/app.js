const state = {
  user: null,
  dashboard: null,
  products: [],
  partners: [],
  warehouses: [],
  locations: [],
  stockLevels: [],
  movements: [],
  documents: [],
  users: [],
  auditLogs: [],
  flash: null,
  currentView: "dashboard",
  showChangePassword: false,
  showProductForm: false,
  showPartnerForm: false,
  showWarehouseForm: false,
  showLocationForm: false,
  showDocumentForm: false,
  showUserForm: false,
  lastActivityAt: Date.now(),
  partnerFilter: "all",
  documentFilter: "all",
  documentStatusFilter: "all",
  stocktakeFilter: "all",
  productDraft: null,
  partnerDraft: null,
  warehouseDraft: null,
  locationDraft: null,
  userDraft: null,
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

const permissionLabels = {
  manage_users: "使用者管理",
  reset_passwords: "重設密碼",
  manage_products: "商品管理",
  delete_products: "刪除商品",
  manage_partners: "往來對象管理",
  manage_warehouses: "倉庫與儲位管理",
  manage_documents: "單據建立",
  complete_documents: "單據完成",
  manage_stocktakes: "盤點管理",
  manage_manual_stock: "手動庫存異動",
  stock_in: "入庫作業",
  stock_out: "出庫作業",
  view_reports: "報表檢視",
  export_reports: "報表匯出",
  view_audit: "稽核紀錄",
};

function blankProductDraft() {
  return {
    id: "",
    sku: "",
    name: "",
    description: "",
    unit: "pcs",
    minQuantity: 0,
    supplierId: "",
    barcode: "",
    qrCode: "",
    active: true,
    trackBatch: false,
    trackSerial: false,
    trackExpiry: false,
  };
}

function blankPartnerDraft() {
  return {
    id: "",
    partnerType: "supplier",
    name: "",
    contactName: "",
    phone: "",
    email: "",
    taxId: "",
    address: "",
    status: "active",
  };
}

function blankWarehouseDraft() {
  return {
    id: "",
    code: "",
    name: "",
    address: "",
    status: "active",
  };
}

function blankLocationDraft() {
  return {
    id: "",
    warehouseId: "",
    code: "",
    name: "",
    status: "active",
  };
}

function blankUserDraft() {
  return {
    id: "",
    username: "",
    fullName: "",
    password: "",
    role: "manager",
    status: "active",
    extraPermissions: [],
  };
}

state.productDraft = blankProductDraft();
state.partnerDraft = blankPartnerDraft();
state.warehouseDraft = blankWarehouseDraft();
state.locationDraft = blankLocationDraft();
state.userDraft = blankUserDraft();

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

function resetDrafts() {
  state.productDraft = blankProductDraft();
  state.partnerDraft = blankPartnerDraft();
  state.warehouseDraft = blankWarehouseDraft();
  state.locationDraft = blankLocationDraft();
  state.userDraft = blankUserDraft();
}

function clearAuthState(message, type = "success") {
  state.user = null;
  state.dashboard = null;
  state.products = [];
  state.partners = [];
  state.warehouses = [];
  state.locations = [];
  state.stockLevels = [];
  state.movements = [];
  state.documents = [];
  state.users = [];
  state.auditLogs = [];
  state.showChangePassword = false;
  state.showProductForm = false;
  state.showPartnerForm = false;
  state.showWarehouseForm = false;
  state.showLocationForm = false;
  state.showDocumentForm = false;
  state.showUserForm = false;
  stopSessionTimers();
  resetDrafts();
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

  const contentType = response.headers.get("Content-Type") || "";
  const payload = contentType.includes("application/json")
    ? await response.json()
    : await response.text();

  if (response.status === 401 && state.user) {
    clearAuthState("登入已逾時，請重新登入", "error");
    throw new Error(typeof payload === "object" ? payload.error || "登入已逾時" : "登入已逾時");
  }
  if (!response.ok) {
    throw new Error(typeof payload === "object" ? payload.error || "操作失敗" : "操作失敗");
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
      // ignore
    }
    clearAuthState("閒置超過 30 分鐘，系統已自動登出", "error");
  }, INACTIVITY_LIMIT_MS);
}

function startSessionTimers() {
  stopSessionTimers();
  markActivity();
  sessionRefreshTimerId = window.setInterval(async () => {
    if (!state.user) return;
    if (Date.now() - state.lastActivityAt >= INACTIVITY_LIMIT_MS) return;
    try {
      await api("/api/session", { headers: {} });
    } catch (error) {
      // api handles timeout
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

function renderFlash() {
  if (!state.flash) return "";
  return `<div class="message ${state.flash.type}">${escapeHtml(state.flash.text)}</div>`;
}

function getNavigationItems() {
  return [
    { key: "dashboard", label: "系統總覽", show: true },
    { key: "inventory", label: "商品管理", show: true },
    { key: "stock", label: "庫存與倉位", show: hasPermission("manage_manual_stock") || hasPermission("manage_warehouses") },
    { key: "partners", label: "供應商 / 客戶", show: hasPermission("manage_partners") || hasPermission("view_reports") },
    { key: "documents", label: "採購 / 出貨 / 退貨", show: hasPermission("manage_documents") || hasPermission("view_reports") },
    { key: "stocktakes", label: "盤點調整", show: hasPermission("manage_stocktakes") || hasPermission("view_reports") },
    { key: "users", label: "使用者 / 權限", show: hasPermission("manage_users") },
    { key: "audit", label: "操作稽核", show: hasPermission("view_audit") },
    { key: "reports", label: "報表匯出", show: hasPermission("view_reports") },
  ].filter((item) => item.show);
}

function ensureAccessibleView() {
  const available = getNavigationItems().map((item) => item.key);
  if (!available.includes(state.currentView)) {
    state.currentView = available[0] || "dashboard";
  }
}

async function loadSession() {
  const data = await api("/api/session", { headers: {} });
  state.user = data.user;
}

async function loadAllData() {
  const requests = [
    api("/api/dashboard"),
    api("/api/products"),
    api("/api/partners"),
    api("/api/warehouses"),
    api("/api/locations"),
    api("/api/stock-levels"),
    api("/api/movements"),
    api("/api/documents"),
  ];
  if (hasPermission("manage_users")) {
    requests.push(api("/api/users"));
  }
  if (hasPermission("view_audit")) {
    requests.push(api("/api/audit-logs"));
  }

  const results = await Promise.all(requests);
  const [
    dashboard,
    products,
    partners,
    warehouses,
    locations,
    stockLevels,
    movements,
    documents,
    users = { users: [] },
    auditLogs = { auditLogs: [] },
  ] = results;

  state.dashboard = dashboard;
  state.user = dashboard.user;
  state.products = products.products;
  state.partners = partners.partners;
  state.warehouses = warehouses.warehouses;
  state.locations = locations.locations;
  state.stockLevels = stockLevels.stockLevels;
  state.movements = movements.movements;
  state.documents = documents.documents;
  state.users = users.users || [];
  state.auditLogs = auditLogs.auditLogs || [];
  ensureAccessibleView();
}

async function refreshData(message) {
  await loadAllData();
  setFlash(message ? "success" : null, message || null);
}

function getSupplierOptions() {
  return state.partners.filter((item) => item.partnerType === "supplier" && item.status === "active");
}

function getCustomerOptions() {
  return state.partners.filter((item) => item.partnerType === "customer" && item.status === "active");
}

function getPermissionCatalog() {
  return state.dashboard?.permissionCatalog || [];
}

function getPartnerName(partnerId) {
  return state.partners.find((item) => String(item.id) === String(partnerId))?.name || "";
}

function getWarehouseLocations(warehouseId) {
  return state.locations.filter((item) => String(item.warehouseId) === String(warehouseId));
}

function getFilteredPartners() {
  if (state.partnerFilter === "all") return state.partners;
  return state.partners.filter((item) => item.partnerType === state.partnerFilter);
}

function getFilteredDocuments() {
  return state.documents.filter((doc) => {
    const typeMatch = state.documentFilter === "all" || doc.docType === state.documentFilter;
    const statusMatch = state.documentStatusFilter === "all" || doc.status === state.documentStatusFilter;
    return typeMatch && statusMatch;
  });
}

function getFilteredStocktakes() {
  return state.documents.filter((doc) => doc.docType === "stocktake");
}

function renderLogin() {
  app.innerHTML = `
    <div class="login-shell">
      <div class="login-card">
        <span class="eyebrow">Inventory Management</span>
        <h1>庫存營運管理系統</h1>
        <p>登入後可管理商品、倉庫、夥伴資料、單據、盤點與報表。</p>
        ${renderFlash()}
        <form id="login-form" class="form-grid">
          <label>帳號<input name="username" autocomplete="username" required /></label>
          <label>密碼<input name="password" type="password" autocomplete="current-password" required /></label>
          <button class="primary-btn" type="submit">登入系統</button>
        </form>
      </div>
    </div>
  `;
  document.querySelector("#login-form").addEventListener("submit", handleLogin);
}

function renderShell() {
  ensureAccessibleView();
  const currentLabel = getNavigationItems().find((item) => item.key === state.currentView)?.label || "系統";

  app.innerHTML = `
    <div class="admin-layout">
      <aside class="sidebar">
        <div class="brand-block">
          <div class="brand-mark">IMS</div>
          <div>
            <strong>Yen IMS</strong>
            <span>Operations Console</span>
          </div>
        </div>
        <nav class="sidebar-nav">
          ${getNavigationItems()
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
            <div class="topbar-caption">Integrated Operations</div>
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
    case "inventory":
      return renderInventoryView();
    case "stock":
      return renderStockView();
    case "partners":
      return renderPartnersView();
    case "documents":
      return renderDocumentsView();
    case "stocktakes":
      return renderStocktakesView();
    case "users":
      return renderUsersView();
    case "audit":
      return renderAuditView();
    case "reports":
      return renderReportsView();
    case "dashboard":
    default:
      return renderDashboardView();
  }
}

function renderDashboardView() {
  const stats = state.dashboard?.stats || {};
  const recentDocs = state.documents.slice(0, 6);

  return `
    <section class="page-panel">
      <div class="page-header">
        <div>
          <h2>營運概況</h2>
          <p>集中查看商品、單據、夥伴、倉庫與稽核數量。</p>
        </div>
      </div>

      <div class="dashboard-metrics metrics-5">
        <article class="metric-card"><span>商品數量</span><strong>${stats.productCount || 0}</strong></article>
        <article class="metric-card"><span>供應商 / 客戶</span><strong>${stats.partnerCount || 0}</strong></article>
        <article class="metric-card"><span>倉庫 / 儲位</span><strong>${stats.warehouseCount || 0} / ${stats.locationCount || 0}</strong></article>
        <article class="metric-card"><span>待完成單據</span><strong>${stats.pendingDocumentCount || 0}</strong></article>
        <article class="metric-card"><span>低庫存品項</span><strong>${stats.lowStockCount || 0}</strong></article>
      </div>

      <div class="split-layout">
        <section class="subpanel">
          <div class="subpanel-head"><h3>最近單據</h3></div>
          ${
            recentDocs.length
              ? `
                <div class="list-stack">
                  ${recentDocs
                    .map(
                      (doc) => `
                      <article class="list-row">
                        <div>
                          <strong>${escapeHtml(doc.docNo)}</strong>
                          <span>${escapeHtml(doc.docType)}｜${escapeHtml(doc.productName)}</span>
                        </div>
                        <div class="align-right">
                          <strong>${escapeHtml(doc.status)}</strong>
                          <span>${formatDate(doc.createdAt)}</span>
                        </div>
                      </article>
                    `
                    )
                    .join("")}
                </div>
              `
              : `<div class="empty-state">目前尚無單據資料。</div>`
          }
        </section>
        <section class="subpanel">
          <div class="subpanel-head"><h3>目前權限</h3></div>
          <div class="badge-stack">
            ${state.user.permissions.map((permission) => `<span class="badge">${escapeHtml(permissionLabels[permission] || permission)}</span>`).join("")}
          </div>
          ${
            state.user.mustResetPassword
              ? `<div class="message error" style="margin-top:16px;">目前帳號已被要求重設密碼，請先完成修改。</div>`
              : ""
          }
        </section>
      </div>
    </section>
  `;
}

function renderInventoryView() {
  return `
    <section class="page-panel">
      <div class="page-header page-header-spread">
        <div>
          <h2>商品主檔</h2>
          <p>管理商品、條碼 / QR、效期 / 批號 / 序號追蹤設定。</p>
        </div>
        ${
          hasPermission("manage_products")
            ? `<button class="action-btn" data-toggle-form="product-form-wrap">${state.productDraft.id ? "編輯商品" : "新增商品"}</button>`
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

      ${renderProductsTable()}
    </section>
  `;
}

function renderStockView() {
  return `
    <section class="page-panel">
      <div class="page-header">
        <div>
          <h2>庫存 / 倉庫 / 儲位</h2>
          <p>管理多倉庫、多儲位，以及手動庫存異動。</p>
        </div>
      </div>
      <div class="triple-layout">
        ${
          hasPermission("manage_warehouses")
            ? `
              <section class="subpanel">
                <div class="subpanel-head"><h3>倉庫管理</h3></div>
                ${renderWarehouseForm()}
                ${renderWarehousesTable()}
              </section>
              <section class="subpanel">
                <div class="subpanel-head"><h3>儲位管理</h3></div>
                ${renderLocationForm()}
                ${renderLocationsTable()}
              </section>
            `
            : ""
        }
        ${
          hasPermission("manage_manual_stock")
            ? `
              <section class="subpanel">
                <div class="subpanel-head"><h3>手動庫存異動</h3></div>
                ${renderManualMovementForm()}
              </section>
            `
            : ""
        }
      </div>
      <section class="subpanel" style="margin-top:20px;">
        <div class="subpanel-head"><h3>現有庫存層級</h3></div>
        ${renderStockLevelsTable()}
      </section>
    </section>
  `;
}

function renderPartnersView() {
  return `
    <section class="page-panel">
      <div class="page-header page-header-spread">
        <div>
          <h2>供應商 / 客戶</h2>
          <p>管理供應商與客戶基本資料，供採購、出貨與退貨單使用。</p>
        </div>
        ${
          hasPermission("manage_partners")
            ? `<button class="action-btn" data-toggle-form="partner-form-wrap">${state.partnerDraft.id ? "編輯對象" : "新增對象"}</button>`
            : ""
        }
      </div>
      ${
        hasPermission("manage_partners")
          ? `
            <section id="partner-form-wrap" class="form-card ${state.showPartnerForm ? "" : "collapsed"}">
              ${renderPartnerForm()}
            </section>
          `
          : ""
      }
      <div class="toolbar">
        <div class="tab-row">
          <button class="tab-btn ${state.partnerFilter === "all" ? "active" : ""}" data-partner-filter="all">全部</button>
          <button class="tab-btn ${state.partnerFilter === "supplier" ? "active" : ""}" data-partner-filter="supplier">供應商</button>
          <button class="tab-btn ${state.partnerFilter === "customer" ? "active" : ""}" data-partner-filter="customer">客戶</button>
        </div>
      </div>
      ${renderPartnersTable()}
    </section>
  `;
}

function renderDocumentsView() {
  return `
    <section class="page-panel">
      <div class="page-header page-header-spread">
        <div>
          <h2>採購 / 出貨 / 退貨單</h2>
          <p>建立單據，完成後會自動寫入庫存交易。</p>
        </div>
        ${
          hasPermission("manage_documents")
            ? `<button class="action-btn" data-toggle-form="document-form-wrap">新增單據</button>`
            : ""
        }
      </div>
      ${
        hasPermission("manage_documents")
          ? `
            <section id="document-form-wrap" class="form-card ${state.showDocumentForm ? "" : "collapsed"}">
              ${renderDocumentForm()}
            </section>
          `
          : ""
      }
      <div class="toolbar">
        <div class="tab-row">
          <button class="tab-btn ${state.documentFilter === "all" ? "active" : ""}" data-document-filter="all">全部</button>
          <button class="tab-btn ${state.documentFilter === "purchase" ? "active" : ""}" data-document-filter="purchase">採購</button>
          <button class="tab-btn ${state.documentFilter === "sales" ? "active" : ""}" data-document-filter="sales">出貨</button>
          <button class="tab-btn ${state.documentFilter === "return" ? "active" : ""}" data-document-filter="return">退貨</button>
        </div>
        <div class="tab-row">
          <button class="tab-btn ${state.documentStatusFilter === "all" ? "active" : ""}" data-document-status-filter="all">全部狀態</button>
          <button class="tab-btn ${state.documentStatusFilter === "draft" ? "active" : ""}" data-document-status-filter="draft">草稿</button>
          <button class="tab-btn ${state.documentStatusFilter === "completed" ? "active" : ""}" data-document-status-filter="completed">已完成</button>
        </div>
      </div>
      ${renderDocumentsTable()}
    </section>
  `;
}

function renderStocktakesView() {
  return `
    <section class="page-panel">
      <div class="page-header">
        <div>
          <h2>盤點作業與差異調整</h2>
          <p>輸入實際盤點數量，系統會自動產生差異調整交易。</p>
        </div>
      </div>
      ${
        hasPermission("manage_stocktakes")
          ? `
            <section class="form-card">
              ${renderStocktakeForm()}
            </section>
          `
          : ""
      }
      <section class="subpanel" style="margin-top:20px;">
        <div class="subpanel-head"><h3>盤點紀錄</h3></div>
        ${renderStocktakesTable()}
      </section>
    </section>
  `;
}

function renderUsersView() {
  return `
    <section class="page-panel">
      <div class="page-header page-header-spread">
        <div>
          <h2>使用者 / 權限 / 密碼重設</h2>
          <p>支援角色基礎權限加額外權限，管理員可強制重設密碼。</p>
        </div>
        <button class="action-btn" data-toggle-form="user-form-wrap">${state.userDraft.id ? "編輯帳號" : "新增帳號"}</button>
      </div>
      <section id="user-form-wrap" class="form-card ${state.showUserForm ? "" : "collapsed"}">
        ${renderUserForm()}
      </section>
      <div class="split-layout" style="margin-top:20px;">
        <section class="subpanel">
          <div class="subpanel-head"><h3>帳號清單</h3></div>
          ${renderUsersTable()}
        </section>
        ${
          hasPermission("reset_passwords")
            ? `
              <section class="subpanel">
                <div class="subpanel-head"><h3>管理員重設密碼</h3></div>
                ${renderResetPasswordForm()}
              </section>
            `
            : ""
        }
      </div>
    </section>
  `;
}

function renderAuditView() {
  return `
    <section class="page-panel">
      <div class="page-header">
        <div>
          <h2>操作稽核紀錄</h2>
          <p>記錄建立、修改、刪除、完成單據與重設密碼等操作。</p>
        </div>
      </div>
      ${renderAuditTable()}
    </section>
  `;
}

function renderReportsView() {
  return `
    <section class="page-panel">
      <div class="page-header">
        <div>
          <h2>匯出報表</h2>
          <p>目前提供 CSV 匯出，Excel 可直接開啟。</p>
        </div>
      </div>
      <div class="card-grid">
        <button class="action-btn" data-export-type="products">匯出商品報表</button>
        <button class="action-btn" data-export-type="movements">匯出庫存交易</button>
        <button class="action-btn" data-export-type="documents">匯出單據報表</button>
        <button class="action-btn" data-export-type="partners">匯出供應商 / 客戶</button>
      </div>
      <section class="subpanel" style="margin-top:20px;">
        <div class="subpanel-head"><h3>最近庫存交易</h3></div>
        ${renderMovementsTable(state.movements.slice(0, 20))}
      </section>
    </section>
  `;
}

function renderProductForm() {
  const suppliers = getSupplierOptions();
  const draft = state.productDraft;
  return `
    <form id="product-form" class="inline-form columns-2">
      <input type="hidden" name="id" value="${escapeHtml(draft.id)}" />
      <label>商品編號 SKU<input name="sku" value="${escapeHtml(draft.sku)}" required /></label>
      <label>商品名稱<input name="name" value="${escapeHtml(draft.name)}" required /></label>
      <label>單位<input name="unit" value="${escapeHtml(draft.unit)}" /></label>
      <label>安全庫存<input name="minQuantity" type="number" min="0" value="${escapeHtml(draft.minQuantity)}" /></label>
      <label>條碼 Barcode<input name="barcode" value="${escapeHtml(draft.barcode)}" /></label>
      <label>QR Code<input name="qrCode" value="${escapeHtml(draft.qrCode)}" /></label>
      <label>預設供應商
        <select name="supplierId">
          <option value="">未指定</option>
          ${suppliers
            .map(
              (item) => `<option value="${item.id}" ${String(item.id) === String(draft.supplierId) ? "selected" : ""}>${escapeHtml(item.name)}</option>`
            )
            .join("")}
        </select>
      </label>
      <label>狀態
        <select name="active">
          <option value="true" ${draft.active ? "selected" : ""}>啟用</option>
          <option value="false" ${!draft.active ? "selected" : ""}>停用</option>
        </select>
      </label>
      <label class="span-2">商品說明<textarea name="description">${escapeHtml(draft.description)}</textarea></label>
      <div class="span-2 checkbox-grid">
        <label class="check-item"><input type="checkbox" name="trackBatch" ${draft.trackBatch ? "checked" : ""} />啟用批號管理</label>
        <label class="check-item"><input type="checkbox" name="trackSerial" ${draft.trackSerial ? "checked" : ""} />啟用序號管理</label>
        <label class="check-item"><input type="checkbox" name="trackExpiry" ${draft.trackExpiry ? "checked" : ""} />啟用到期日管理</label>
      </div>
      <div class="button-row">
        <button class="primary-btn" type="submit">${draft.id ? "更新商品" : "建立商品"}</button>
        <button class="ghost-btn" type="button" id="reset-product-form">清空表單</button>
      </div>
    </form>
  `;
}

function renderProductsTable() {
  if (!state.products.length) {
    return `<div class="empty-state">目前還沒有商品資料。</div>`;
  }
  return `
    <div class="table-card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>商品</th>
              <th>條碼 / QR</th>
              <th>供應商</th>
              <th>庫存</th>
              <th>追蹤</th>
              <th>狀態</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            ${state.products
              .map(
                (item) => `
                <tr>
                  <td><strong>${escapeHtml(item.name)}</strong><br />${escapeHtml(item.sku)}</td>
                  <td>${escapeHtml(item.barcode || "-")}<br />${escapeHtml(item.qrCode || "-")}</td>
                  <td>${escapeHtml(item.supplierName || "-")}</td>
                  <td>${item.quantity} ${item.isLowStock ? '<span class="badge low">低庫存</span>' : ""}</td>
                  <td>${[
                    item.trackBatch ? "批號" : "",
                    item.trackSerial ? "序號" : "",
                    item.trackExpiry ? "效期" : "",
                  ]
                    .filter(Boolean)
                    .join(" / ") || "-"}</td>
                  <td>${item.active ? '<span class="badge">啟用</span>' : '<span class="badge low">停用</span>'}</td>
                  <td class="table-actions">
                    ${hasPermission("manage_products") ? `<button class="ghost-btn" data-edit-product="${item.id}">編輯</button>` : ""}
                    ${hasPermission("manage_products") ? `<button class="ghost-btn" data-toggle-product="${item.id}" data-next-active="${item.active ? "false" : "true"}">${item.active ? "停用" : "啟用"}</button>` : ""}
                    ${hasPermission("delete_products") ? `<button class="ghost-btn danger-text" data-delete-product="${item.id}">刪除</button>` : ""}
                  </td>
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

function renderWarehouseForm() {
  const draft = state.warehouseDraft;
  return `
    <form id="warehouse-form" class="inline-form">
      <input type="hidden" name="id" value="${escapeHtml(draft.id)}" />
      <label>倉庫代碼<input name="code" value="${escapeHtml(draft.code)}" required /></label>
      <label>倉庫名稱<input name="name" value="${escapeHtml(draft.name)}" required /></label>
      <label>地址<input name="address" value="${escapeHtml(draft.address)}" /></label>
      <label>狀態
        <select name="status">
          <option value="active" ${draft.status === "active" ? "selected" : ""}>啟用</option>
          <option value="inactive" ${draft.status === "inactive" ? "selected" : ""}>停用</option>
        </select>
      </label>
      <div class="button-row">
        <button class="primary-btn" type="submit">${draft.id ? "更新倉庫" : "建立倉庫"}</button>
        <button class="ghost-btn" type="button" id="reset-warehouse-form">清空</button>
      </div>
    </form>
  `;
}

function renderWarehousesTable() {
  return `
    <div class="table-card compact-table">
      <div class="table-wrap">
        <table>
          <thead><tr><th>代碼</th><th>名稱</th><th>狀態</th><th>操作</th></tr></thead>
          <tbody>
            ${state.warehouses
              .map(
                (item) => `
                  <tr>
                    <td>${escapeHtml(item.code)}</td>
                    <td>${escapeHtml(item.name)}</td>
                    <td>${escapeHtml(item.status)}</td>
                    <td class="table-actions">
                      <button class="ghost-btn" data-edit-warehouse="${item.id}">編輯</button>
                      <button class="ghost-btn" data-toggle-warehouse="${item.id}" data-next-status="${item.status === "active" ? "inactive" : "active"}">${item.status === "active" ? "停用" : "啟用"}</button>
                    </td>
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

function renderLocationForm() {
  const draft = state.locationDraft;
  return `
    <form id="location-form" class="inline-form">
      <input type="hidden" name="id" value="${escapeHtml(draft.id)}" />
      <label>所屬倉庫
        <select name="warehouseId" required>
          <option value="">請選擇</option>
          ${state.warehouses.map((item) => `<option value="${item.id}" ${String(item.id) === String(draft.warehouseId) ? "selected" : ""}>${escapeHtml(item.code)}｜${escapeHtml(item.name)}</option>`).join("")}
        </select>
      </label>
      <label>儲位代碼<input name="code" value="${escapeHtml(draft.code)}" required /></label>
      <label>儲位名稱<input name="name" value="${escapeHtml(draft.name)}" required /></label>
      <label>狀態
        <select name="status">
          <option value="active" ${draft.status === "active" ? "selected" : ""}>啟用</option>
          <option value="inactive" ${draft.status === "inactive" ? "selected" : ""}>停用</option>
        </select>
      </label>
      <div class="button-row">
        <button class="primary-btn" type="submit">${draft.id ? "更新儲位" : "建立儲位"}</button>
        <button class="ghost-btn" type="button" id="reset-location-form">清空</button>
      </div>
    </form>
  `;
}

function renderLocationsTable() {
  return `
    <div class="table-card compact-table">
      <div class="table-wrap">
        <table>
          <thead><tr><th>倉庫</th><th>代碼</th><th>名稱</th><th>狀態</th><th>操作</th></tr></thead>
          <tbody>
            ${state.locations
              .map(
                (item) => `
                  <tr>
                    <td>${escapeHtml(item.warehouseCode)}</td>
                    <td>${escapeHtml(item.code)}</td>
                    <td>${escapeHtml(item.name)}</td>
                    <td>${escapeHtml(item.status)}</td>
                    <td class="table-actions">
                      <button class="ghost-btn" data-edit-location="${item.id}">編輯</button>
                      <button class="ghost-btn" data-toggle-location="${item.id}" data-next-status="${item.status === "active" ? "inactive" : "active"}">${item.status === "active" ? "停用" : "啟用"}</button>
                    </td>
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

function renderManualMovementForm() {
  return `
    <form id="manual-movement-form" class="inline-form columns-2">
      <label>方向
        <select name="direction">
          <option value="in">入庫</option>
          <option value="out">出庫</option>
        </select>
      </label>
      <label>交易類型<input name="transactionType" value="manual" /></label>
      <label>商品
        <select name="productId" required>${renderProductOptions()}</select>
      </label>
      <label>數量<input name="quantity" type="number" min="1" value="1" required /></label>
      <label>倉庫
        <select name="warehouseId" required>${renderWarehouseOptions()}</select>
      </label>
      <label>儲位
        <select name="locationId" required>${renderLocationOptions()}</select>
      </label>
      <label>參考編號<input name="referenceNo" /></label>
      <label>備註<input name="note" /></label>
      <label>批號<input name="batchNo" /></label>
      <label>序號<input name="serialNo" /></label>
      <label>到期日<input name="expiryDate" type="date" /></label>
      <div class="button-row">
        <button class="primary-btn" type="submit">送出異動</button>
      </div>
    </form>
  `;
}

function renderStockLevelsTable() {
  if (!state.stockLevels.length) return `<div class="empty-state">目前尚無庫存層級資料。</div>`;
  return `
    <div class="table-card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>商品</th>
              <th>倉庫 / 儲位</th>
              <th>批號</th>
              <th>序號</th>
              <th>到期日</th>
              <th>數量</th>
              <th>更新時間</th>
            </tr>
          </thead>
          <tbody>
            ${state.stockLevels
              .map(
                (item) => `
                  <tr>
                    <td><strong>${escapeHtml(item.productName)}</strong><br />${escapeHtml(item.productSku)}</td>
                    <td>${escapeHtml(item.warehouseName)}<br />${escapeHtml(item.locationName)}</td>
                    <td>${escapeHtml(item.batchNo || "-")}</td>
                    <td>${escapeHtml(item.serialNo || "-")}</td>
                    <td>${escapeHtml(item.expiryDate || "-")}</td>
                    <td>${item.quantity}</td>
                    <td>${formatDate(item.updatedAt)}</td>
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

function renderPartnerForm() {
  const draft = state.partnerDraft;
  return `
    <form id="partner-form" class="inline-form columns-2">
      <input type="hidden" name="id" value="${escapeHtml(draft.id)}" />
      <label>類型
        <select name="partnerType">
          <option value="supplier" ${draft.partnerType === "supplier" ? "selected" : ""}>供應商</option>
          <option value="customer" ${draft.partnerType === "customer" ? "selected" : ""}>客戶</option>
        </select>
      </label>
      <label>名稱<input name="name" value="${escapeHtml(draft.name)}" required /></label>
      <label>聯絡人<input name="contactName" value="${escapeHtml(draft.contactName)}" /></label>
      <label>電話<input name="phone" value="${escapeHtml(draft.phone)}" /></label>
      <label>Email<input name="email" value="${escapeHtml(draft.email)}" /></label>
      <label>統編<input name="taxId" value="${escapeHtml(draft.taxId)}" /></label>
      <label class="span-2">地址<input name="address" value="${escapeHtml(draft.address)}" /></label>
      <label>狀態
        <select name="status">
          <option value="active" ${draft.status === "active" ? "selected" : ""}>啟用</option>
          <option value="inactive" ${draft.status === "inactive" ? "selected" : ""}>停用</option>
        </select>
      </label>
      <div class="button-row">
        <button class="primary-btn" type="submit">${draft.id ? "更新對象" : "建立對象"}</button>
        <button class="ghost-btn" type="button" id="reset-partner-form">清空</button>
      </div>
    </form>
  `;
}

function renderPartnersTable() {
  const items = getFilteredPartners();
  if (!items.length) return `<div class="empty-state">目前沒有符合條件的往來對象。</div>`;
  return `
    <div class="table-card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>類型</th>
              <th>名稱</th>
              <th>聯絡方式</th>
              <th>統編</th>
              <th>狀態</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            ${items
              .map(
                (item) => `
                  <tr>
                    <td>${item.partnerType === "supplier" ? "供應商" : "客戶"}</td>
                    <td><strong>${escapeHtml(item.name)}</strong><br />${escapeHtml(item.contactName || "-")}</td>
                    <td>${escapeHtml(item.phone || "-")}<br />${escapeHtml(item.email || "-")}</td>
                    <td>${escapeHtml(item.taxId || "-")}</td>
                    <td>${item.status === "active" ? '<span class="badge">啟用</span>' : '<span class="badge low">停用</span>'}</td>
                    <td class="table-actions">
                      ${hasPermission("manage_partners") ? `<button class="ghost-btn" data-edit-partner="${item.id}">編輯</button>` : ""}
                      ${hasPermission("manage_partners") ? `<button class="ghost-btn" data-toggle-partner="${item.id}" data-next-status="${item.status === "active" ? "inactive" : "active"}">${item.status === "active" ? "停用" : "啟用"}</button>` : ""}
                    </td>
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

function renderDocumentForm() {
  return `
    <form id="document-form" class="inline-form columns-2">
      <label>單據類型
        <select name="docType">
          <option value="purchase">採購單</option>
          <option value="sales">出貨單</option>
          <option value="return">退貨單</option>
        </select>
      </label>
      <label>單號<input name="docNo" placeholder="留空自動編號" /></label>
      <label>對象
        <select name="partnerId">
          <option value="">未指定</option>
          ${state.partners.map((item) => `<option value="${item.id}">${item.partnerType === "supplier" ? "供" : "客"}｜${escapeHtml(item.name)}</option>`).join("")}
        </select>
      </label>
      <label>商品
        <select name="productId" required>${renderProductOptions()}</select>
      </label>
      <label>倉庫
        <select name="warehouseId" required>${renderWarehouseOptions()}</select>
      </label>
      <label>儲位
        <select name="locationId" required>${renderLocationOptions()}</select>
      </label>
      <label>數量<input name="quantity" type="number" min="1" value="1" required /></label>
      <label>單價<input name="unitPrice" type="number" min="0" step="0.01" value="0" /></label>
      <label>批號<input name="batchNo" /></label>
      <label>序號<input name="serialNo" /></label>
      <label>到期日<input name="expiryDate" type="date" /></label>
      <label>備註<input name="note" /></label>
      <div class="span-2 checkbox-grid">
        <label class="check-item"><input name="completeNow" type="checkbox" checked />建立後立即完成並同步庫存</label>
      </div>
      <div class="button-row">
        <button class="primary-btn" type="submit">建立單據</button>
      </div>
    </form>
  `;
}

function renderDocumentsTable() {
  const items = getFilteredDocuments();
  if (!items.length) return `<div class="empty-state">目前沒有符合條件的單據。</div>`;
  return `
    <div class="table-card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>單號</th>
              <th>類型</th>
              <th>對象</th>
              <th>商品</th>
              <th>倉位</th>
              <th>數量</th>
              <th>狀態</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            ${items
              .map(
                (item) => `
                  <tr>
                    <td><strong>${escapeHtml(item.docNo)}</strong><br />${formatDate(item.createdAt)}</td>
                    <td>${escapeHtml(item.docType)}</td>
                    <td>${escapeHtml(item.partnerName || "-")}</td>
                    <td><strong>${escapeHtml(item.productName)}</strong><br />${escapeHtml(item.productSku)}</td>
                    <td>${escapeHtml(item.warehouseName)}<br />${escapeHtml(item.locationName)}</td>
                    <td>${item.docType === "stocktake" ? escapeHtml(item.countedQuantity) : escapeHtml(item.quantity)}</td>
                    <td>${item.status === "completed" ? '<span class="badge">已完成</span>' : '<span class="badge low">草稿</span>'}</td>
                    <td class="table-actions">
                      ${item.status === "draft" && hasPermission("complete_documents") ? `<button class="ghost-btn" data-complete-document="${item.id}">完成</button>` : "-"}
                    </td>
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

function renderStocktakeForm() {
  return `
    <form id="stocktake-form" class="inline-form columns-2">
      <label>商品
        <select name="productId" required>${renderProductOptions()}</select>
      </label>
      <label>盤點數量<input name="countedQuantity" type="number" min="0" value="0" required /></label>
      <label>倉庫
        <select name="warehouseId" required>${renderWarehouseOptions()}</select>
      </label>
      <label>儲位
        <select name="locationId" required>${renderLocationOptions()}</select>
      </label>
      <label>批號<input name="batchNo" /></label>
      <label>序號<input name="serialNo" /></label>
      <label>到期日<input name="expiryDate" type="date" /></label>
      <label>備註<input name="note" placeholder="盤點原因 / 差異說明" /></label>
      <div class="button-row">
        <button class="primary-btn" type="submit">送出盤點差異</button>
      </div>
    </form>
  `;
}

function renderStocktakesTable() {
  const items = getFilteredStocktakes();
  if (!items.length) return `<div class="empty-state">目前尚無盤點紀錄。</div>`;
  return renderDocumentsTableFor(items);
}

function renderDocumentsTableFor(items) {
  return `
    <div class="table-card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>單號</th>
              <th>商品</th>
              <th>倉位</th>
              <th>盤點數量</th>
              <th>狀態</th>
              <th>完成時間</th>
            </tr>
          </thead>
          <tbody>
            ${items
              .map(
                (item) => `
                  <tr>
                    <td>${escapeHtml(item.docNo)}</td>
                    <td><strong>${escapeHtml(item.productName)}</strong><br />${escapeHtml(item.productSku)}</td>
                    <td>${escapeHtml(item.warehouseName)}<br />${escapeHtml(item.locationName)}</td>
                    <td>${item.countedQuantity}</td>
                    <td>${item.status === "completed" ? '<span class="badge">已完成</span>' : '<span class="badge low">草稿</span>'}</td>
                    <td>${formatDate(item.completedAt)}</td>
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
  const draft = state.userDraft;
  const permissionCatalog = getPermissionCatalog();
  return `
    <form id="user-form" class="inline-form columns-2">
      <input type="hidden" name="id" value="${escapeHtml(draft.id)}" />
      <label>帳號<input name="username" value="${escapeHtml(draft.username)}" required /></label>
      <label>姓名<input name="fullName" value="${escapeHtml(draft.fullName)}" required /></label>
      <label>角色
        <select name="role">
          ${Object.keys(roleLabels).map((role) => `<option value="${role}" ${draft.role === role ? "selected" : ""}>${escapeHtml(roleLabels[role])}</option>`).join("")}
        </select>
      </label>
      <label>狀態
        <select name="status">
          <option value="active" ${draft.status === "active" ? "selected" : ""}>啟用</option>
          <option value="inactive" ${draft.status === "inactive" ? "selected" : ""}>停用</option>
        </select>
      </label>
      <label class="span-2">密碼${draft.id ? "（不改可留空）" : ""}
        <input name="password" type="password" ${draft.id ? "" : "required"} />
      </label>
      <div class="span-2 checkbox-grid">
        ${permissionCatalog
          .map(
            (permission) => `
              <label class="check-item">
                <input
                  type="checkbox"
                  name="extraPermissions"
                  value="${permission}"
                  ${draft.extraPermissions.includes(permission) ? "checked" : ""}
                />
                ${escapeHtml(permissionLabels[permission] || permission)}
              </label>
            `
          )
          .join("")}
      </div>
      <div class="button-row">
        <button class="primary-btn" type="submit">${draft.id ? "更新帳號" : "建立帳號"}</button>
        <button class="ghost-btn" type="button" id="reset-user-form">清空</button>
      </div>
    </form>
  `;
}

function renderUsersTable() {
  if (!state.users.length) return `<div class="empty-state">目前沒有使用者資料。</div>`;
  return `
    <div class="table-card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>帳號</th>
              <th>角色</th>
              <th>狀態</th>
              <th>額外權限</th>
              <th>密碼狀態</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>
            ${state.users
              .map(
                (item) => `
                  <tr>
                    <td><strong>${escapeHtml(item.fullName)}</strong><br />${escapeHtml(item.username)}</td>
                    <td>${escapeHtml(formatRole(item.role))}</td>
                    <td>${item.status === "active" ? '<span class="badge">啟用</span>' : '<span class="badge low">停用</span>'}</td>
                    <td>${item.extraPermissions.map((permission) => escapeHtml(permissionLabels[permission] || permission)).join("、") || "-"}</td>
                    <td>${item.mustResetPassword ? '<span class="badge low">需重設</span>' : "-"}</td>
                    <td class="table-actions">
                      <button class="ghost-btn" data-edit-user="${item.id}">編輯</button>
                    </td>
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

function renderResetPasswordForm() {
  return `
    <form id="reset-password-form" class="inline-form">
      <label>目標帳號
        <select name="userId" required>
          <option value="">請選擇</option>
          ${state.users.map((item) => `<option value="${item.id}">${escapeHtml(item.fullName)}｜${escapeHtml(item.username)}</option>`).join("")}
        </select>
      </label>
      <label>暫時密碼<input name="tempPassword" type="password" minlength="8" required /></label>
      <button class="primary-btn" type="submit">重設密碼並要求下次修改</button>
    </form>
  `;
}

function renderAuditTable() {
  if (!state.auditLogs.length) return `<div class="empty-state">目前尚無稽核紀錄。</div>`;
  return `
    <div class="table-card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>時間</th>
              <th>動作</th>
              <th>對象</th>
              <th>內容</th>
              <th>操作人</th>
            </tr>
          </thead>
          <tbody>
            ${state.auditLogs
              .map(
                (item) => `
                  <tr>
                    <td>${formatDate(item.createdAt)}</td>
                    <td>${escapeHtml(item.action)}</td>
                    <td>${escapeHtml(item.entityType)} #${item.entityId}</td>
                    <td>${escapeHtml(item.detail)}</td>
                    <td>${escapeHtml(item.createdBy || "-")}</td>
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

function renderMovementsTable(items) {
  if (!items.length) return `<div class="empty-state">目前沒有庫存交易紀錄。</div>`;
  return `
    <div class="table-card">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>時間</th>
              <th>類型</th>
              <th>商品</th>
              <th>倉位</th>
              <th>方向</th>
              <th>數量</th>
              <th>單號</th>
            </tr>
          </thead>
          <tbody>
            ${items
              .map(
                (item) => `
                  <tr>
                    <td>${formatDate(item.createdAt)}</td>
                    <td>${escapeHtml(item.movementType)}</td>
                    <td><strong>${escapeHtml(item.productName)}</strong><br />${escapeHtml(item.productSku)}</td>
                    <td>${escapeHtml(item.warehouseName || "-")}<br />${escapeHtml(item.locationName || "-")}</td>
                    <td>${escapeHtml(item.direction)}</td>
                    <td>${item.quantity}</td>
                    <td>${escapeHtml(item.referenceNo || "-")}</td>
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

function renderProductOptions() {
  return `<option value="">請選擇商品</option>${state.products
    .filter((item) => item.active)
    .map((item) => `<option value="${item.id}">${escapeHtml(item.sku)}｜${escapeHtml(item.name)}</option>`)
    .join("")}`;
}

function renderWarehouseOptions() {
  return `<option value="">請選擇倉庫</option>${state.warehouses
    .filter((item) => item.status === "active")
    .map((item) => `<option value="${item.id}">${escapeHtml(item.code)}｜${escapeHtml(item.name)}</option>`)
    .join("")}`;
}

function renderLocationOptions() {
  return `<option value="">請選擇儲位</option>${state.locations
    .filter((item) => item.status === "active")
    .map((item) => `<option value="${item.id}">${escapeHtml(item.warehouseCode)}｜${escapeHtml(item.code)}｜${escapeHtml(item.name)}</option>`)
    .join("")}`;
}

function renderChangePasswordModal() {
  if (!state.showChangePassword) return "";
  return `
    <div class="modal-backdrop">
      <div class="modal-card">
        <div class="subpanel-head">
          <h3>修改密碼</h3>
          <button id="close-password-modal" class="ghost-btn" type="button">關閉</button>
        </div>
        <form id="password-form" class="form-grid">
          <label>舊密碼<input name="oldPassword" type="password" required /></label>
          <label>新密碼<input name="newPassword" type="password" minlength="8" required /></label>
          <label>確認新密碼<input name="confirmPassword" type="password" minlength="8" required /></label>
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

  document.querySelectorAll("[data-toggle-form]").forEach((button) => {
    button.addEventListener("click", () => {
      const key = button.dataset.toggleForm;
      if (key === "product-form-wrap") state.showProductForm = !state.showProductForm;
      if (key === "partner-form-wrap") state.showPartnerForm = !state.showPartnerForm;
      if (key === "warehouse-form-wrap") state.showWarehouseForm = !state.showWarehouseForm;
      if (key === "location-form-wrap") state.showLocationForm = !state.showLocationForm;
      if (key === "document-form-wrap") state.showDocumentForm = !state.showDocumentForm;
      if (key === "user-form-wrap") state.showUserForm = !state.showUserForm;
      render();
    });
  });

  document.querySelectorAll("[data-partner-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      state.partnerFilter = button.dataset.partnerFilter;
      render();
    });
  });
  document.querySelectorAll("[data-document-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      state.documentFilter = button.dataset.documentFilter;
      render();
    });
  });
  document.querySelectorAll("[data-document-status-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      state.documentStatusFilter = button.dataset.documentStatusFilter;
      render();
    });
  });

  document.querySelector("#product-form")?.addEventListener("submit", handleProductSubmit);
  document.querySelector("#partner-form")?.addEventListener("submit", handlePartnerSubmit);
  document.querySelector("#warehouse-form")?.addEventListener("submit", handleWarehouseSubmit);
  document.querySelector("#location-form")?.addEventListener("submit", handleLocationSubmit);
  document.querySelector("#manual-movement-form")?.addEventListener("submit", handleManualMovementSubmit);
  document.querySelector("#document-form")?.addEventListener("submit", handleDocumentSubmit);
  document.querySelector("#stocktake-form")?.addEventListener("submit", handleStocktakeSubmit);
  document.querySelector("#user-form")?.addEventListener("submit", handleUserSubmit);
  document.querySelector("#reset-password-form")?.addEventListener("submit", handleResetPasswordSubmit);
  document.querySelector("#password-form")?.addEventListener("submit", handleChangePassword);

  document.querySelector("#reset-product-form")?.addEventListener("click", () => {
    state.productDraft = blankProductDraft();
    render();
  });
  document.querySelector("#reset-partner-form")?.addEventListener("click", () => {
    state.partnerDraft = blankPartnerDraft();
    render();
  });
  document.querySelector("#reset-warehouse-form")?.addEventListener("click", () => {
    state.warehouseDraft = blankWarehouseDraft();
    render();
  });
  document.querySelector("#reset-location-form")?.addEventListener("click", () => {
    state.locationDraft = blankLocationDraft();
    render();
  });
  document.querySelector("#reset-user-form")?.addEventListener("click", () => {
    state.userDraft = blankUserDraft();
    render();
  });

  document.querySelectorAll("[data-edit-product]").forEach((button) => {
    button.addEventListener("click", () => {
      const item = state.products.find((product) => String(product.id) === button.dataset.editProduct);
      if (!item) return;
      state.productDraft = {
        ...blankProductDraft(),
        ...item,
      };
      state.showProductForm = true;
      render();
    });
  });
  document.querySelectorAll("[data-toggle-product]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        await api("/api/products/toggle-status", {
          method: "POST",
          body: JSON.stringify({
            productId: button.dataset.toggleProduct,
            active: button.dataset.nextActive,
          }),
        });
        await refreshData("商品狀態已更新");
      } catch (error) {
        setFlash("error", error.message);
      }
    });
  });
  document.querySelectorAll("[data-delete-product]").forEach((button) => {
    button.addEventListener("click", async () => {
      if (!window.confirm("確定要刪除此商品嗎？")) return;
      try {
        await api("/api/products/delete", {
          method: "POST",
          body: JSON.stringify({ productId: button.dataset.deleteProduct }),
        });
        await refreshData("商品已刪除");
      } catch (error) {
        setFlash("error", error.message);
      }
    });
  });

  document.querySelectorAll("[data-edit-partner]").forEach((button) => {
    button.addEventListener("click", () => {
      const item = state.partners.find((partner) => String(partner.id) === button.dataset.editPartner);
      if (!item) return;
      state.partnerDraft = { ...blankPartnerDraft(), ...item };
      state.showPartnerForm = true;
      render();
    });
  });
  document.querySelectorAll("[data-toggle-partner]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        await api("/api/partners/toggle-status", {
          method: "POST",
          body: JSON.stringify({ partnerId: button.dataset.togglePartner, status: button.dataset.nextStatus }),
        });
        await refreshData("往來對象狀態已更新");
      } catch (error) {
        setFlash("error", error.message);
      }
    });
  });

  document.querySelectorAll("[data-edit-warehouse]").forEach((button) => {
    button.addEventListener("click", () => {
      const item = state.warehouses.find((warehouse) => String(warehouse.id) === button.dataset.editWarehouse);
      if (!item) return;
      state.warehouseDraft = { ...blankWarehouseDraft(), ...item };
      render();
    });
  });
  document.querySelectorAll("[data-toggle-warehouse]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        await api("/api/warehouses/toggle-status", {
          method: "POST",
          body: JSON.stringify({ warehouseId: button.dataset.toggleWarehouse, status: button.dataset.nextStatus }),
        });
        await refreshData("倉庫狀態已更新");
      } catch (error) {
        setFlash("error", error.message);
      }
    });
  });
  document.querySelectorAll("[data-edit-location]").forEach((button) => {
    button.addEventListener("click", () => {
      const item = state.locations.find((location) => String(location.id) === button.dataset.editLocation);
      if (!item) return;
      state.locationDraft = { ...blankLocationDraft(), ...item };
      render();
    });
  });
  document.querySelectorAll("[data-toggle-location]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        await api("/api/locations/toggle-status", {
          method: "POST",
          body: JSON.stringify({ locationId: button.dataset.toggleLocation, status: button.dataset.nextStatus }),
        });
        await refreshData("儲位狀態已更新");
      } catch (error) {
        setFlash("error", error.message);
      }
    });
  });

  document.querySelectorAll("[data-complete-document]").forEach((button) => {
    button.addEventListener("click", async () => {
      try {
        await api("/api/documents/complete", {
          method: "POST",
          body: JSON.stringify({ documentId: button.dataset.completeDocument }),
        });
        await refreshData("單據已完成");
      } catch (error) {
        setFlash("error", error.message);
      }
    });
  });

  document.querySelectorAll("[data-edit-user]").forEach((button) => {
    button.addEventListener("click", () => {
      const item = state.users.find((user) => String(user.id) === button.dataset.editUser);
      if (!item) return;
      state.userDraft = {
        id: item.id,
        username: item.username,
        fullName: item.fullName,
        password: "",
        role: item.role,
        status: item.status,
        extraPermissions: item.extraPermissions || [],
      };
      state.showUserForm = true;
      render();
    });
  });

  document.querySelectorAll("[data-export-type]").forEach((button) => {
    button.addEventListener("click", () => handleExport(button.dataset.exportType));
  });
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
    await refreshData("登入成功");
    if (state.user?.mustResetPassword) {
      state.showChangePassword = true;
      setFlash("error", "管理員已重設此帳號密碼，請先修改新密碼");
    }
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

async function handleProductSubmit(event) {
  event.preventDefault();
  const data = new FormData(event.currentTarget);
  try {
    await api("/api/products", {
      method: "POST",
      body: JSON.stringify({
        id: data.get("id"),
        sku: data.get("sku"),
        name: data.get("name"),
        description: data.get("description"),
        unit: data.get("unit"),
        minQuantity: data.get("minQuantity"),
        supplierId: data.get("supplierId"),
        barcode: data.get("barcode"),
        qrCode: data.get("qrCode"),
        active: data.get("active"),
        trackBatch: data.get("trackBatch") === "on",
        trackSerial: data.get("trackSerial") === "on",
        trackExpiry: data.get("trackExpiry") === "on",
      }),
    });
    state.productDraft = blankProductDraft();
    state.showProductForm = false;
    await refreshData("商品資料已儲存");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handlePartnerSubmit(event) {
  event.preventDefault();
  const data = new FormData(event.currentTarget);
  try {
    await api("/api/partners", {
      method: "POST",
      body: JSON.stringify({
        id: data.get("id"),
        partnerType: data.get("partnerType"),
        name: data.get("name"),
        contactName: data.get("contactName"),
        phone: data.get("phone"),
        email: data.get("email"),
        taxId: data.get("taxId"),
        address: data.get("address"),
        status: data.get("status"),
      }),
    });
    state.partnerDraft = blankPartnerDraft();
    state.showPartnerForm = false;
    await refreshData("往來對象資料已儲存");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleWarehouseSubmit(event) {
  event.preventDefault();
  const data = new FormData(event.currentTarget);
  try {
    await api("/api/warehouses", {
      method: "POST",
      body: JSON.stringify({
        id: data.get("id"),
        code: data.get("code"),
        name: data.get("name"),
        address: data.get("address"),
        status: data.get("status"),
      }),
    });
    state.warehouseDraft = blankWarehouseDraft();
    await refreshData("倉庫資料已儲存");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleLocationSubmit(event) {
  event.preventDefault();
  const data = new FormData(event.currentTarget);
  try {
    await api("/api/locations", {
      method: "POST",
      body: JSON.stringify({
        id: data.get("id"),
        warehouseId: data.get("warehouseId"),
        code: data.get("code"),
        name: data.get("name"),
        status: data.get("status"),
      }),
    });
    state.locationDraft = blankLocationDraft();
    await refreshData("儲位資料已儲存");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleManualMovementSubmit(event) {
  event.preventDefault();
  const data = new FormData(event.currentTarget);
  try {
    await api("/api/manual-movement", {
      method: "POST",
      body: JSON.stringify({
        direction: data.get("direction"),
        transactionType: data.get("transactionType"),
        productId: data.get("productId"),
        quantity: data.get("quantity"),
        warehouseId: data.get("warehouseId"),
        locationId: data.get("locationId"),
        referenceNo: data.get("referenceNo"),
        note: data.get("note"),
        batchNo: data.get("batchNo"),
        serialNo: data.get("serialNo"),
        expiryDate: data.get("expiryDate"),
      }),
    });
    event.currentTarget.reset();
    await refreshData("庫存異動已完成");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleDocumentSubmit(event) {
  event.preventDefault();
  const data = new FormData(event.currentTarget);
  try {
    await api("/api/documents", {
      method: "POST",
      body: JSON.stringify({
        docType: data.get("docType"),
        docNo: data.get("docNo"),
        partnerId: data.get("partnerId"),
        productId: data.get("productId"),
        warehouseId: data.get("warehouseId"),
        locationId: data.get("locationId"),
        quantity: data.get("quantity"),
        unitPrice: data.get("unitPrice"),
        batchNo: data.get("batchNo"),
        serialNo: data.get("serialNo"),
        expiryDate: data.get("expiryDate"),
        note: data.get("note"),
        completeNow: data.get("completeNow") === "on",
      }),
    });
    state.showDocumentForm = false;
    event.currentTarget.reset();
    await refreshData("單據已建立");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleStocktakeSubmit(event) {
  event.preventDefault();
  const data = new FormData(event.currentTarget);
  try {
    await api("/api/stocktakes/adjust", {
      method: "POST",
      body: JSON.stringify({
        productId: data.get("productId"),
        countedQuantity: data.get("countedQuantity"),
        warehouseId: data.get("warehouseId"),
        locationId: data.get("locationId"),
        batchNo: data.get("batchNo"),
        serialNo: data.get("serialNo"),
        expiryDate: data.get("expiryDate"),
        note: data.get("note"),
      }),
    });
    event.currentTarget.reset();
    await refreshData("盤點差異調整已完成");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleUserSubmit(event) {
  event.preventDefault();
  const data = new FormData(event.currentTarget);
  try {
    await api("/api/users", {
      method: "POST",
      body: JSON.stringify({
        id: data.get("id"),
        username: data.get("username"),
        fullName: data.get("fullName"),
        password: data.get("password"),
        role: data.get("role"),
        status: data.get("status"),
        extraPermissions: data.getAll("extraPermissions"),
      }),
    });
    state.userDraft = blankUserDraft();
    state.showUserForm = false;
    await refreshData("使用者資料已儲存");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleResetPasswordSubmit(event) {
  event.preventDefault();
  const data = new FormData(event.currentTarget);
  try {
    await api("/api/users/reset-password", {
      method: "POST",
      body: JSON.stringify({
        userId: data.get("userId"),
        tempPassword: data.get("tempPassword"),
      }),
    });
    event.currentTarget.reset();
    await refreshData("使用者密碼已重設");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function handleChangePassword(event) {
  event.preventDefault();
  const data = new FormData(event.currentTarget);
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

async function handleExport(type) {
  try {
    const response = await fetch(`/api/export?type=${encodeURIComponent(type)}`, {
      credentials: "same-origin",
    });
    if (response.status === 401 && state.user) {
      clearAuthState("登入已逾時，請重新登入", "error");
      return;
    }
    if (!response.ok) {
      const payload = await response.json();
      throw new Error(payload.error || "匯出失敗");
    }
    const blob = await response.blob();
    const url = window.URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${type}.csv`;
    anchor.click();
    window.URL.revokeObjectURL(url);
    setFlash("success", "報表已開始下載");
  } catch (error) {
    setFlash("error", error.message);
  }
}

async function bootstrap() {
  try {
    await loadSession();
    if (state.user) {
      await loadAllData();
      if (state.user.mustResetPassword) {
        state.showChangePassword = true;
      }
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
