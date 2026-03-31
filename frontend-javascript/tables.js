(() => {
  const API = String(window.API_BASE || "http://localhost:8000").replace(/\/$/, "");

  const tableSelect = document.getElementById("tableSelect");
  const btnReload = document.getElementById("btnReload");
  const btnDeleteTable = document.getElementById("btnDeleteTable");
  const pageError = document.getElementById("pageError");
  const recordsInfo = document.getElementById("recordsInfo");
  const recordsTable = document.getElementById("recordsTable");

  const state = {
    tableName: "",
    tables: [],
    columns: [],
    records: [],
    loading: false,
  };

  function setError(msg) {
    if (!msg) {
      pageError.classList.add("hidden");
      pageError.textContent = "";
      return;
    }
    pageError.classList.remove("hidden");
    pageError.textContent = msg;
  }

  function setLoading(loading) {
    state.loading = !!loading;
    btnReload.disabled = state.loading || !state.tableName;
    if (btnDeleteTable) {
      btnDeleteTable.disabled = state.loading || !state.tableName;
    }
    tableSelect.disabled = state.loading;
  }

  async function requestJson(path, method = "GET", payload = null) {
    const options = { method };
    if (payload != null) {
      options.headers = { "Content-Type": "application/json" };
      options.body = JSON.stringify(payload);
    }
    const res = await fetch(`${API}${path}`, options);
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(text || `Request failed: ${res.status}`);
    }
    return res.json();
  }

  function renderSelect() {
    tableSelect.innerHTML = "";
    if (!state.tables.length) {
      state.tableName = "";
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "No tables found";
      tableSelect.appendChild(option);
      tableSelect.disabled = true;
      return;
    }

    for (const item of state.tables) {
      const option = document.createElement("option");
      option.value = item.table;
      option.textContent = `${item.table} (${item.row_count})`;
      tableSelect.appendChild(option);
    }

    if (!state.tableName || !state.tables.some((t) => t.table === state.tableName)) {
      state.tableName = state.tables[0].table;
    }
    tableSelect.value = state.tableName;
    tableSelect.disabled = state.loading;
  }

  function renderTable() {
    recordsTable.innerHTML = "";
    if (!state.tableName) {
      recordsInfo.textContent = "";
      return;
    }

    recordsInfo.textContent = `${state.records.length} records in ${state.tableName}`;

    if (!state.columns.length) {
      const caption = document.createElement("caption");
      caption.className = "empty";
      caption.textContent = "No records found.";
      recordsTable.appendChild(caption);
      return;
    }

    const thead = document.createElement("thead");
    const headRow = document.createElement("tr");
    for (const col of state.columns) {
      const th = document.createElement("th");
      th.textContent = col;
      headRow.appendChild(th);
    }
    const actionHead = document.createElement("th");
    actionHead.className = "action-col";
    actionHead.textContent = "Action";
    headRow.appendChild(actionHead);
    thead.appendChild(headRow);

    const tbody = document.createElement("tbody");
    for (const row of state.records) {
      const tr = document.createElement("tr");
      for (const col of state.columns) {
        const td = document.createElement("td");
        td.textContent = String(row[col] ?? "");
        tr.appendChild(td);
      }

      const actionCell = document.createElement("td");
      actionCell.className = "action-col";
      const delBtn = document.createElement("button");
      delBtn.type = "button";
      delBtn.className = "button button-danger";
      delBtn.textContent = "Delete";
      delBtn.disabled = state.loading;
      delBtn.addEventListener("click", async () => {
        const rowId = row.__row_id;
        if (!rowId) return;
        const ok = window.confirm("Delete this record?");
        if (!ok) return;
        await deleteRecord(rowId);
      });
      actionCell.appendChild(delBtn);
      tr.appendChild(actionCell);
      tbody.appendChild(tr);
    }

    if (!state.records.length) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      td.colSpan = state.columns.length + 1;
      td.className = "empty";
      td.textContent = "No records found.";
      tr.appendChild(td);
      tbody.appendChild(tr);
    }

    recordsTable.appendChild(thead);
    recordsTable.appendChild(tbody);
  }

  async function loadTables() {
    const data = await requestJson("/api/table-browser/tables");
    state.tables = Array.isArray(data.tables) ? data.tables : [];
    renderSelect();
  }

  async function loadRows() {
    if (!state.tableName) {
      state.columns = [];
      state.records = [];
      renderTable();
      return;
    }
    const tableName = encodeURIComponent(state.tableName);
    const data = await requestJson(`/api/table-browser/rows?table_name=${tableName}`);
    state.columns = Array.isArray(data.columns) ? data.columns : [];
    state.records = Array.isArray(data.records) ? data.records : [];
    renderTable();
  }

  async function deleteRecord(rowId) {
    setError(null);
    setLoading(true);
    try {
      await requestJson("/api/table-browser/record", "DELETE", {
        table_name: state.tableName,
        row_id: rowId,
      });
      await loadTables();
      await loadRows();
    } catch (err) {
      setError(err && err.message ? err.message : "Delete failed");
    } finally {
      setLoading(false);
      renderTable();
    }
  }

  async function deleteCurrentTable() {
    if (!state.tableName) return;
    const selectedTable = state.tableName;
    const ok = window.confirm(`Delete table "${selectedTable}"? This cannot be undone.`);
    if (!ok) return;

    setError(null);
    setLoading(true);
    try {
      await requestJson("/api/table-browser/table", "DELETE", { table_name: selectedTable });
      state.tableName = "";
      await loadTables();
      await loadRows();
    } catch (err) {
      setError(err && err.message ? err.message : "Failed to delete table");
    } finally {
      setLoading(false);
      renderTable();
    }
  }

  async function reloadPage() {
    setError(null);
    setLoading(true);
    try {
      await loadTables();
      await loadRows();
    } catch (err) {
      setError(err && err.message ? err.message : "Failed to load tables");
    } finally {
      setLoading(false);
      renderTable();
    }
  }

  tableSelect.addEventListener("change", async () => {
    state.tableName = tableSelect.value || "";
    await reloadPage();
  });

  btnReload.addEventListener("click", () => {
    reloadPage();
  });
  if (btnDeleteTable) {
    btnDeleteTable.addEventListener("click", () => {
      deleteCurrentTable();
    });
  }

  setError(null);
  reloadPage();
})();
