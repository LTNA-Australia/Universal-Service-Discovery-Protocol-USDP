const state = {
  runtimeConfig: null,
  services: [],
  selectedServiceId: null,
  page: 1,
  pageSize: 25,
  total: 0,
  totalPages: 1,
};

const elements = {
  errorBanner: document.getElementById("errorBanner"),
  protocolBadge: document.getElementById("protocolBadge"),
  adminBadge: document.getElementById("adminBadge"),
  serviceList: document.getElementById("serviceList"),
  serviceCount: document.getElementById("serviceCount"),
  detailView: document.getElementById("detailView"),
  detailStatus: document.getElementById("detailStatus"),
  healthStatus: document.getElementById("healthStatus"),
  healthMeta: document.getElementById("healthMeta"),
  healthBreakdown: document.getElementById("healthBreakdown"),
  operationsSummary: document.getElementById("operationsSummary"),
  operationsStatus: document.getElementById("operationsStatus"),
  auditList: document.getElementById("auditList"),
  auditStatus: document.getElementById("auditStatus"),
  purgeButton: document.getElementById("purgeButton"),
  nameFilter: document.getElementById("nameFilter"),
  typeFilter: document.getElementById("typeFilter"),
  statusFilter: document.getElementById("statusFilter"),
  siteFilter: document.getElementById("siteFilter"),
  areaFilter: document.getElementById("areaFilter"),
  tagFilter: document.getElementById("tagFilter"),
  capabilityFieldFilter: document.getElementById("capabilityFieldFilter"),
  capabilityValueFilter: document.getElementById("capabilityValueFilter"),
  criteriaFilter: document.getElementById("criteriaFilter"),
  inactiveFilter: document.getElementById("inactiveFilter"),
  pageSizeFilter: document.getElementById("pageSizeFilter"),
  prevPageButton: document.getElementById("prevPageButton"),
  nextPageButton: document.getElementById("nextPageButton"),
  pageIndicator: document.getElementById("pageIndicator"),
};

document.getElementById("refreshButton").addEventListener("click", refreshAll);
elements.purgeButton.addEventListener("click", purgeDueRecords);
for (const control of [
  elements.nameFilter,
  elements.siteFilter,
  elements.areaFilter,
  elements.tagFilter,
  elements.capabilityFieldFilter,
  elements.capabilityValueFilter,
  elements.criteriaFilter,
]) {
  control.addEventListener("input", debounce(resetPageAndRefresh, 220));
}
for (const control of [
  elements.typeFilter,
  elements.statusFilter,
  elements.inactiveFilter,
]) {
  control.addEventListener("change", resetPageAndRefresh);
}
elements.pageSizeFilter.addEventListener("change", () => {
  state.pageSize = Number(elements.pageSizeFilter.value) || 25;
  state.page = 1;
  refreshServices().catch(showRuntimeError);
});
elements.prevPageButton.addEventListener("click", () => changePage(-1));
elements.nextPageButton.addEventListener("click", () => changePage(1));

async function refreshAll() {
  clearRuntimeError();
  await loadRuntimeConfig();
  await Promise.all([refreshHealth(), refreshServices(), refreshAdmin()]);
}

async function loadRuntimeConfig() {
  if (state.runtimeConfig) {
    return state.runtimeConfig;
  }
  state.runtimeConfig = await requestJson("/api/config");
  elements.protocolBadge.textContent = `Protocol ${state.runtimeConfig.protocol_version}`;
  elements.adminBadge.textContent = state.runtimeConfig.admin_enabled ? "Admin enabled" : "Admin disabled";
  elements.adminBadge.dataset.active = state.runtimeConfig.admin_enabled ? "true" : "false";
  return state.runtimeConfig;
}

async function refreshHealth() {
  try {
    const payload = await requestJson("/api/health");
    const health = payload.data;
    elements.healthStatus.textContent = health.status.toUpperCase();
    const supportedVersions = health.supported_protocol_versions?.join(", ") || health.protocol_version;
    elements.healthMeta.textContent = `${health.registry_id || "registry"} | protocols ${supportedVersions}`;
    const breakdownValues = [
      health.active_services,
      health.stale_services,
      health.withdrawn_services ?? 0,
      health.federated_services ?? 0,
    ];
    [...elements.healthBreakdown.querySelectorAll("dd")].forEach((node, index) => {
      node.textContent = String(breakdownValues[index] ?? "-");
    });
  } catch (error) {
    elements.healthStatus.textContent = "Error";
    elements.healthMeta.textContent = error.message;
    [...elements.healthBreakdown.querySelectorAll("dd")].forEach((node) => {
      node.textContent = "-";
    });
    throw error;
  }
}

async function refreshServices() {
  try {
    const payload = await requestJson("/api/query", {
      method: "POST",
      body: JSON.stringify(buildQueryPayload()),
    });
    const data = payload.data;
    state.services = data.items || [];
    state.total = data.total || 0;
    state.totalPages = Math.max(1, Math.ceil(state.total / state.pageSize));
    state.page = Math.min(state.page, state.totalPages);

    elements.serviceCount.textContent = `${state.total} results`;
    elements.pageIndicator.textContent = `Page ${state.page} of ${state.totalPages}`;
    elements.prevPageButton.disabled = state.page <= 1;
    elements.nextPageButton.disabled = state.page >= state.totalPages;

    renderServiceList();
    syncDetailSelection();
    clearRuntimeError();
  } catch (error) {
    state.services = [];
    state.total = 0;
    state.totalPages = 1;
    elements.serviceCount.textContent = "0 results";
    elements.pageIndicator.textContent = "Page 1 of 1";
    elements.prevPageButton.disabled = true;
    elements.nextPageButton.disabled = true;
    renderServiceList();
    showEmptyDetails("No service selected", "The dashboard could not load services from the registry.");
    throw error;
  }
}

async function refreshAdmin() {
  if (!state.runtimeConfig?.admin_enabled) {
    elements.operationsStatus.textContent = "Admin views unavailable";
    elements.auditStatus.textContent = "No audit data";
    elements.purgeButton.disabled = true;
    return;
  }

  try {
    const [metrics, retention, audit] = await Promise.all([
      requestJson("/api/metrics"),
      requestJson("/api/admin/retention"),
      requestJson("/api/admin/audit?limit=10"),
    ]);
    renderOperations(metrics.data, retention.data);
    renderAudit(audit.data.items || []);
    elements.operationsStatus.textContent = "Admin views active";
    elements.auditStatus.textContent = `${audit.data.count || 0} events loaded`;
    elements.purgeButton.disabled = false;
  } catch (error) {
    elements.operationsStatus.textContent = error.message;
    elements.auditStatus.textContent = "Audit unavailable";
    elements.purgeButton.disabled = true;
    throw error;
  }
}

async function purgeDueRecords() {
  try {
    const payload = await requestJson("/api/admin/purge", {
      method: "POST",
      body: JSON.stringify({ protocol_version: state.runtimeConfig.protocol_version }),
    });
    const purgedCount = payload.data?.count || 0;
    elements.operationsStatus.textContent = `Purge complete: ${purgedCount} record(s) removed`;
    await Promise.all([refreshHealth(), refreshServices(), refreshAdmin()]);
  } catch (error) {
    showRuntimeError(error);
  }
}

function buildQueryPayload() {
  const version = state.runtimeConfig?.protocol_version || "2.0";
  const query = {
    protocol_version: version,
    page: state.page,
    page_size: state.pageSize,
    sort: [{ field: "name", direction: "asc" }],
    include_inactive: elements.inactiveFilter.checked,
  };

  if (version.startsWith("2")) {
    const criteriaText = elements.criteriaFilter.value.trim();
    if (criteriaText) {
      query.criteria = JSON.parse(criteriaText);
      return query;
    }

    const criteria = [];
    addCriterion(criteria, "name", "contains", elements.nameFilter.value.trim());
    addCriterion(criteria, "service_type", "eq", elements.typeFilter.value);
    addCriterion(criteria, "status", "eq", elements.statusFilter.value);
    addCriterion(criteria, "location.site", "eq", elements.siteFilter.value.trim());
    addCriterion(criteria, "location.area", "eq", elements.areaFilter.value.trim());
    addCriterion(criteria, "tags", "contains", elements.tagFilter.value.trim());

    const capabilityField = elements.capabilityFieldFilter.value.trim();
    const capabilityValue = elements.capabilityValueFilter.value.trim();
    if (capabilityField && capabilityValue) {
      addCriterion(criteria, capabilityField, "eq", parseScalar(capabilityValue));
    }

    if (criteria.length === 1) {
      query.criteria = criteria[0];
    } else if (criteria.length > 1) {
      query.criteria = { all: criteria };
    }
    return query;
  }

  const filters = {};
  if (elements.nameFilter.value.trim()) {
    filters.name_contains = elements.nameFilter.value.trim();
  }
  if (elements.typeFilter.value) {
    filters.service_type = elements.typeFilter.value;
  }
  if (elements.statusFilter.value) {
    filters.status = elements.statusFilter.value;
  }
  if (elements.siteFilter.value.trim() || elements.areaFilter.value.trim()) {
    filters.location = {};
    if (elements.siteFilter.value.trim()) {
      filters.location.site = elements.siteFilter.value.trim();
    }
    if (elements.areaFilter.value.trim()) {
      filters.location.area = elements.areaFilter.value.trim();
    }
  }
  if (elements.tagFilter.value.trim()) {
    filters.tags_all = [elements.tagFilter.value.trim()];
  }
  if (Object.keys(filters).length > 0) {
    query.filters = filters;
  }
  return query;
}

function addCriterion(criteria, field, op, rawValue) {
  if (rawValue == null || rawValue === "") {
    return;
  }
  criteria.push({ field, op, value: rawValue });
}

function renderServiceList() {
  elements.serviceList.innerHTML = "";
  const template = document.getElementById("serviceItemTemplate");

  if (state.services.length === 0) {
    const empty = document.createElement("div");
    empty.className = "detail-view empty";
    empty.textContent = "No services matched the current filters.";
    elements.serviceList.appendChild(empty);
    return;
  }

  for (const service of state.services) {
    const node = template.content.firstElementChild.cloneNode(true);
    node.querySelector(".service-name").textContent = service.name;
    node.querySelector(".service-type").textContent = formatServiceType(service.service_type);
    node.querySelector(".service-location").textContent = formatLocation(service.location);
    const endpointText = primaryEndpointLabel(service) || "No endpoint";
    const endpointNode = node.querySelector(".service-endpoint");
    endpointNode.textContent = endpointText;
    endpointNode.title = endpointText;

    const statusNode = node.querySelector(".service-status");
    statusNode.textContent = humanizeToken(service.status);
    statusNode.dataset.status = service.status;

    const tagsNode = node.querySelector(".service-tags");
    if ((service.tags || []).length > 0) {
      tagsNode.textContent = `Tags: ${(service.tags || []).join(", ")}`;
    }

    if (service.service_id === state.selectedServiceId) {
      node.classList.add("active");
    }

    node.addEventListener("click", () => {
      state.selectedServiceId = service.service_id;
      renderServiceList();
      renderDetails(service);
    });

    elements.serviceList.appendChild(node);
  }
}

function syncDetailSelection() {
  if (state.selectedServiceId) {
    const selected = state.services.find((item) => item.service_id === state.selectedServiceId);
    if (selected) {
      renderDetails(selected);
      return;
    }
  }

  if (state.services.length > 0) {
    state.selectedServiceId = state.services[0].service_id;
    renderDetails(state.services[0]);
    return;
  }

  state.selectedServiceId = null;
  showEmptyDetails("No service selected", "No services matched the current filters.");
}

function renderDetails(service) {
  elements.detailStatus.textContent = `${formatServiceType(service.service_type)} | ${humanizeToken(service.status)}`;
  elements.detailView.className = "detail-view";
  elements.detailView.innerHTML = "";

  appendGridSection("Overview", {
    Name: service.name,
    "Service ID": service.service_id,
    Type: service.service_type,
    Status: service.status,
    "TTL (seconds)": service.heartbeat_ttl_seconds,
  });
  appendGridSection("Type Summary", buildTypeSummary(service));
  appendKeyValueSection("Publisher", service.publisher || {});
  appendKeyValueSection("Publisher Identity", service.publisher_identity || {});
  appendKeyValueSection("Provenance", service.provenance || {});
  appendKeyValueSection("Location", service.location || {});
  appendJsonSection("Endpoints", service.endpoints || []);
  appendKeyValueSection("Capabilities", service.capabilities || {});
  appendKeyValueSection("Auth", service.auth || {});
  appendKeyValueSection("Metadata", service.metadata || {});
  appendKeyValueSection("Extensions", service.extensions || {});
  appendKeyValueSection("Timestamps", service.timestamps || {});
  appendJsonSection("Raw JSON", service);
}

function buildTypeSummary(service) {
  const capabilities = service.capabilities || {};
  switch (service.service_type) {
    case "database":
      return {
        Engine: capabilities.engine,
        Version: capabilities.version,
        Role: capabilities.role,
        "Read only": capabilities.read_only,
        "TLS": capabilities.supports_tls,
      };
    case "sensor":
      return {
        Kind: capabilities.sensor_kind,
        Measurements: (capabilities.measurement_types || []).join(", "),
        Units: capabilities.units,
        "Sampling ms": capabilities.sampling_interval_ms,
        Battery: capabilities.battery_powered,
      };
    case "ai_model_endpoint":
      return {
        Model: capabilities.model_name,
        Family: capabilities.model_family,
        Modalities: (capabilities.modalities || []).join(", "),
        Streaming: capabilities.supports_streaming,
        Context: capabilities.context_window,
      };
    default:
      return {
        Endpoint: primaryEndpointLabel(service),
        Tags: (service.tags || []).join(", "),
        Publisher: service.publisher?.publisher_name,
      };
  }
}

function renderOperations(metricsData, retentionData) {
  const health = metricsData.health || {};
  const runtime = metricsData.runtime_metrics || {};
  const counters = runtime.counters || {};
  elements.operationsSummary.innerHTML = "";

  const cards = [
    ["Registry", metricsData.registry_id],
    ["Query count", counters.requests_total || 0],
    ["Rate limited", counters.rate_limited_total || 0],
    ["Auth failures", counters.auth_failures_total || 0],
    ["Audit events", health.audit_event_count || 0],
    ["Retention", `${retentionData.retention.stale_retention_seconds}s stale / ${retentionData.retention.withdrawn_retention_seconds}s withdrawn`],
  ];

  for (const [label, value] of cards) {
    const card = document.createElement("div");
    card.className = "detail-card";
    const labelNode = document.createElement("span");
    labelNode.className = "label";
    labelNode.textContent = label;
    const valueNode = document.createElement("span");
    valueNode.className = "value";
    valueNode.textContent = String(value);
    card.appendChild(labelNode);
    card.appendChild(valueNode);
    elements.operationsSummary.appendChild(card);
  }
}

function renderAudit(items) {
  if (!items.length) {
    elements.auditList.className = "audit-list empty";
    elements.auditList.textContent = "No audit events returned.";
    return;
  }

  elements.auditList.className = "audit-list";
  elements.auditList.innerHTML = "";
  for (const item of items) {
    const actorName = item.actor?.actor_name || item.actor_name || item.actor?.actor_role || item.actor_role || "system";
    const row = document.createElement("div");
    row.className = "audit-item";
    row.innerHTML = `
      <div class="audit-top">
        <strong>${humanizeToken(item.action)}</strong>
        <span class="audit-time">${item.occurred_at}</span>
      </div>
      <div class="audit-meta">
        <span>${actorName}</span>
        <span>${item.service_id || "no service"}</span>
      </div>
    `;
    elements.auditList.appendChild(row);
  }
}

function appendGridSection(title, items) {
  const block = document.createElement("section");
  block.className = "detail-block";

  const heading = document.createElement("h3");
  heading.textContent = title;

  const grid = document.createElement("div");
  grid.className = "detail-grid";

  for (const [label, value] of Object.entries(items)) {
    const card = document.createElement("div");
    card.className = "detail-card";

    const labelNode = document.createElement("span");
    labelNode.className = "label";
    labelNode.textContent = label;

    const valueNode = document.createElement("span");
    valueNode.className = "value";
    valueNode.textContent = value == null || value === "" ? "-" : String(value);

    card.appendChild(labelNode);
    card.appendChild(valueNode);
    grid.appendChild(card);
  }

  block.appendChild(heading);
  block.appendChild(grid);
  elements.detailView.appendChild(block);
}

function appendKeyValueSection(title, value) {
  const block = document.createElement("section");
  block.className = "detail-block";

  const heading = document.createElement("h3");
  heading.textContent = title;

  if (!value || Object.keys(value).length === 0) {
    const empty = document.createElement("div");
    empty.className = "kv-list";
    empty.textContent = "No data";
    block.appendChild(heading);
    block.appendChild(empty);
    elements.detailView.appendChild(block);
    return;
  }

  const list = document.createElement("div");
  list.className = "kv-list";

  for (const [label, itemValue] of Object.entries(value)) {
    const row = document.createElement("div");
    row.className = "kv-item";

    const labelNode = document.createElement("span");
    labelNode.className = "label";
    labelNode.textContent = label;

    const valueNode = document.createElement("span");
    valueNode.className = "value";
    valueNode.textContent = formatValue(itemValue);

    row.appendChild(labelNode);
    row.appendChild(valueNode);
    list.appendChild(row);
  }

  block.appendChild(heading);
  block.appendChild(list);
  elements.detailView.appendChild(block);
}

function appendJsonSection(title, value) {
  const block = document.createElement("section");
  block.className = "detail-block";

  const heading = document.createElement("h3");
  heading.textContent = title;

  const pre = document.createElement("pre");
  pre.textContent = JSON.stringify(value, null, 2);

  block.appendChild(heading);
  block.appendChild(pre);
  elements.detailView.appendChild(block);
}

function showEmptyDetails(status, message) {
  elements.detailStatus.textContent = status;
  elements.detailView.className = "detail-view empty";
  elements.detailView.textContent = message;
}

function primaryEndpointLabel(service) {
  const endpoint = service.endpoints?.[0];
  if (!endpoint) {
    return "";
  }
  if (endpoint.url) {
    return endpoint.url;
  }
  const address = endpoint.address || "address";
  const port = endpoint.port ? `:${endpoint.port}` : "";
  const path = endpoint.path || "";
  return `${endpoint.protocol}://${address}${port}${path}`;
}

function formatValue(value) {
  if (value == null) {
    return "-";
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return JSON.stringify(value, null, 2);
}

function formatServiceType(value) {
  return humanizeToken(value).replace(/\bApi\b/g, "API").replace(/\bAi\b/g, "AI");
}

function formatLocation(location) {
  if (!location) {
    return "No location";
  }
  return [location.site, location.area].filter(Boolean).join(" / ") || "No location";
}

function humanizeToken(value) {
  return String(value)
    .replace(/_/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

function parseScalar(value) {
  if (value === "true") {
    return true;
  }
  if (value === "false") {
    return false;
  }
  if (!Number.isNaN(Number(value)) && value.trim() !== "") {
    return Number(value);
  }
  return value;
}

function changePage(delta) {
  const nextPage = state.page + delta;
  if (nextPage < 1 || nextPage > state.totalPages) {
    return;
  }
  state.page = nextPage;
  refreshServices().catch(showRuntimeError);
}

function resetPageAndRefresh() {
  state.page = 1;
  refreshServices().catch(showRuntimeError);
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, {
    headers: { Accept: "application/json", ...(options.headers || {}) },
    ...options,
  });
  const payload = await response.json();
  if (!response.ok) {
    const message = payload.errors?.[0]?.message || `Request failed with HTTP ${response.status}`;
    throw new Error(message);
  }
  if (payload.success === false) {
    const message = payload.errors?.[0]?.message || "Request failed";
    throw new Error(message);
  }
  return payload;
}

function showRuntimeError(error) {
  elements.errorBanner.textContent = error.message;
  elements.errorBanner.classList.remove("hidden");
}

function clearRuntimeError() {
  elements.errorBanner.textContent = "";
  elements.errorBanner.classList.add("hidden");
}

function debounce(fn, delay) {
  let timeoutId = null;
  return (...args) => {
    window.clearTimeout(timeoutId);
    timeoutId = window.setTimeout(() => fn(...args), delay);
  };
}

refreshAll().catch(showRuntimeError);
