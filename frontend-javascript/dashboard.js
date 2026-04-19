(() => {
    const API = String(window.API_BASE || 'http://localhost:8000').replace(/\/$/, '');
    const DEFAULT_PAGE_SIZE = Number(window.DEFAULT_PAGE_SIZE) || 50;
    const ICON_REFRESH = '↻';
    const ICON_UNPIN = '✕';
    const GRID_CELL_PX = 80;
    const CHART_MIN_W = 3;
    const CHART_MIN_H = 3;
    const CHART_DEFAULT_W = 4;
    const CHART_DEFAULT_H_PX = 320;
    const TABLE_MIN_W = 6;
    const TABLE_MIN_H = 4;
    const TABLE_DEFAULT_W = 12;
    const TABLE_DEFAULT_H_PX = 400;

    const btnRefreshAll = document.getElementById('btnRefreshAll');
    const totalCharts = document.getElementById('totalCharts');
    const totalTables = document.getElementById('totalTables');
    const lastReload = document.getElementById('lastReload');
    const dashboardError = document.getElementById('dashboardError');
    const dashboardGrid = document.getElementById('dashboardGrid');

    const state = {
        charts: [],
        tables: [],
        draggingId: null,
    };

    function setError(msg) {
        if (!msg) {
            dashboardError.classList.add('hidden');
            dashboardError.textContent = '';
            return;
        }
        dashboardError.classList.remove('hidden');
        dashboardError.textContent = msg;
    }

    async function getJson(path) {
        const res = await fetch(`${API}${path}`);
        if (!res.ok) {
            const txt = await res.text().catch(() => '');
            throw new Error(txt || `Request failed: ${res.status}`);
        }
        return res.json();
    }

    async function postForm(path, fields) {
        const form = new FormData();
        for (const [k, v] of Object.entries(fields || {})) {
            form.append(k, v);
        }
        const res = await fetch(`${API}${path}`, { method: 'POST', body: form });
        if (!res.ok) {
            const txt = await res.text().catch(() => '');
            throw new Error(txt || `Request failed: ${res.status}`);
        }
        return res.json();
    }

    async function postJson(path, payload) {
        const res = await fetch(`${API}${path}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload || {}),
        });
        if (!res.ok) {
            const txt = await res.text().catch(() => '');
            throw new Error(txt || `Request failed: ${res.status}`);
        }
        return res.json();
    }

    async function requestJson(path, method, payload) {
        const res = await fetch(`${API}${path}`, {
            method,
            headers: { 'Content-Type': 'application/json' },
            body: payload ? JSON.stringify(payload) : undefined,
        });
        if (!res.ok) {
            const txt = await res.text().catch(() => '');
            throw new Error(txt || `Request failed: ${res.status}`);
        }
        return res.json();
    }

    function setSummary() {
        totalCharts.textContent = String(state.charts.length);
        if (totalTables) totalTables.textContent = String(state.tables.length);
        lastReload.textContent = new Date().toLocaleString();
    }

    // ---------------------------------------------------------------
    // Chart card helpers
    // ---------------------------------------------------------------
    async function refreshChart(card, chart) {
        const refreshBtn = card.querySelector('.btn-refresh');
        const chartBox = card.querySelector('.chart-box');
        refreshBtn.disabled = true;
        refreshBtn.textContent = '…';

        try {
            const data = await postForm(`/api/charts/${chart.id}/refresh`, {});
            chartBox.innerHTML = '';
            const chartEl = document.createElement('plotly-chart');
            chartEl.config = data.chart_config || data.plotly || null;
            chartBox.appendChild(chartEl);
            setTimeout(() => window.dispatchEvent(new Event("resize")), 120);
        } catch (err) {
            chartBox.innerHTML = '';
            const msg = document.createElement('div');
            msg.className = 'error';
            msg.textContent = err && err.message ? err.message : 'Refresh failed';
            chartBox.appendChild(msg);
        } finally {
            refreshBtn.disabled = false;
            refreshBtn.textContent = ICON_REFRESH;
        }
    }

    // ---------------------------------------------------------------
    // Pinned table card helpers
    // ---------------------------------------------------------------
    function buildDashboardTable(columns, rows, meta, pinnedId) {
        const wrapper = document.createElement('div');
        wrapper.className = 'dash-table-wrapper';
        const allColumns = Array.isArray(columns) ? columns : [];
        let allRows = Array.isArray(rows) ? rows : [];

        const tblState = {
            sortBy: null,
            sortDir: null,
            query: '',
            visible: new Set(allColumns),
            page: (meta && meta.page) || 1,
            pageSize: (meta && meta.page_size) || DEFAULT_PAGE_SIZE,
            totalCount: (meta && meta.total_count) || allRows.length,
            totalPages: (meta && meta.total_pages) || 1,
            loading: false,
        };

        const controls = document.createElement('div');
        controls.className = 'dash-table-controls';
        const search = document.createElement('input');
        search.className = 'dash-table-search';
        search.type = 'search';
        search.placeholder = 'Search...';
        controls.appendChild(search);

        const colToggle = document.createElement('details');
        colToggle.className = 'dash-col-toggle';
        const colToggleSummary = document.createElement('summary');
        colToggleSummary.textContent = 'Columns';
        colToggle.appendChild(colToggleSummary);
        const colToggleBody = document.createElement('div');
        colToggleBody.className = 'dash-col-toggle-body';
        colToggle.appendChild(colToggleBody);
        controls.appendChild(colToggle);

        function renderColumnToggles() {
            colToggleBody.innerHTML = '';
            for (const col of allColumns) {
                const lbl = document.createElement('label');
                lbl.className = 'dash-col-toggle-item';
                const cb = document.createElement('input');
                cb.type = 'checkbox';
                cb.checked = tblState.visible.has(col);
                cb.addEventListener('change', () => {
                    if (cb.checked) { tblState.visible.add(col); }
                    else if (tblState.visible.size > 1) { tblState.visible.delete(col); }
                    else { cb.checked = true; return; }
                    if (tblState.sortBy && !tblState.visible.has(tblState.sortBy)) {
                        tblState.sortBy = null; tblState.sortDir = null;
                    }
                    renderTable();
                });
                const span = document.createElement('span');
                span.textContent = col;
                lbl.appendChild(cb);
                lbl.appendChild(span);
                colToggleBody.appendChild(lbl);
            }
        }

        const table = document.createElement('table');
        table.className = 'dash-table';
        const thead = document.createElement('thead');
        const tbody = document.createElement('tbody');

        const pagination = document.createElement('div');
        pagination.className = 'dash-table-pagination';

        const info = document.createElement('div');
        info.className = 'dash-table-info';

        async function fetchData(page, sortCol, sortDir) {
            if (tblState.loading) return;
            tblState.loading = true;
            wrapper.classList.add('dash-table-loading');
            try {
                const body = { page, page_size: tblState.pageSize };
                if (sortCol) { body.sort_column = sortCol; body.sort_direction = sortDir || 'asc'; }
                if (tblState.query) body.search = tblState.query;
                const data = await postJson(`/api/tables/${pinnedId}/data`, body);
                allRows = Array.isArray(data.rows) ? data.rows : [];
                tblState.page = data.meta.page;
                tblState.totalCount = data.meta.total_count;
                tblState.totalPages = data.meta.total_pages;
            } catch (err) {
                console.error('Dashboard table fetch error:', err);
            } finally {
                tblState.loading = false;
                wrapper.classList.remove('dash-table-loading');
                renderAll();
            }
        }

        function renderTable() {
            thead.innerHTML = '';
            tbody.innerHTML = '';
            const visibleCols = allColumns.filter(c => tblState.visible.has(c));
            const trh = document.createElement('tr');
            for (const col of visibleCols) {
                const th = document.createElement('th');
                const btn = document.createElement('button');
                btn.type = 'button';
                btn.className = 'dash-sort-btn';
                const isActive = tblState.sortBy === col;
                btn.textContent = `${col}${isActive ? (tblState.sortDir === 'asc' ? ' ↑' : ' ↓') : ''}`;
                btn.addEventListener('click', () => {
                    if (tblState.sortBy !== col) { tblState.sortBy = col; tblState.sortDir = 'asc'; }
                    else if (tblState.sortDir === 'asc') { tblState.sortDir = 'desc'; }
                    else { tblState.sortBy = null; tblState.sortDir = null; }
                    tblState.page = 1;
                    fetchData(1, tblState.sortBy, tblState.sortDir);
                });
                th.appendChild(btn);
                trh.appendChild(th);
            }
            thead.appendChild(trh);
            for (const row of allRows) {
                const tr = document.createElement('tr');
                for (const col of visibleCols) {
                    const td = document.createElement('td');
                    td.textContent = String(row[col] ?? '');
                    tr.appendChild(td);
                }
                tbody.appendChild(tr);
            }
        }

        function renderPagination() {
            pagination.innerHTML = '';
            if (tblState.totalPages <= 1) return;
            const makeBtn = (label, pg, disabled) => {
                const b = document.createElement('button');
                b.type = 'button';
                b.className = 'dash-page-btn';
                b.textContent = label;
                b.disabled = disabled || tblState.loading;
                if (!disabled) b.addEventListener('click', () => fetchData(pg, tblState.sortBy, tblState.sortDir));
                return b;
            };
            pagination.appendChild(makeBtn('«', 1, tblState.page <= 1));
            pagination.appendChild(makeBtn('‹', tblState.page - 1, tblState.page <= 1));
            const maxVis = 5;
            let s = Math.max(1, tblState.page - Math.floor(maxVis / 2));
            let e = Math.min(tblState.totalPages, s + maxVis - 1);
            if (e - s + 1 < maxVis) s = Math.max(1, e - maxVis + 1);
            for (let p = s; p <= e; p++) {
                const b = makeBtn(String(p), p, false);
                if (p === tblState.page) b.classList.add('dash-page-btn-active');
                pagination.appendChild(b);
            }
            pagination.appendChild(makeBtn('›', tblState.page + 1, tblState.page >= tblState.totalPages));
            pagination.appendChild(makeBtn('»', tblState.totalPages, tblState.page >= tblState.totalPages));
            const pi = document.createElement('span');
            pi.className = 'dash-page-info';
            pi.textContent = `${tblState.page}/${tblState.totalPages}`;
            pagination.appendChild(pi);
        }

        function renderInfo() {
            info.textContent = `${allRows.length} of ${tblState.totalCount} rows`;
        }

        function renderAll() { renderColumnToggles(); renderTable(); renderPagination(); renderInfo(); }

        let _timer = null;
        search.addEventListener('input', () => {
            tblState.query = search.value || '';
            clearTimeout(_timer);
            _timer = setTimeout(() => { tblState.page = 1; fetchData(1, tblState.sortBy, tblState.sortDir); }, 400);
        });

        renderAll();
        table.appendChild(thead);
        table.appendChild(tbody);
        wrapper.appendChild(info);
        wrapper.appendChild(controls);
        wrapper.appendChild(table);
        wrapper.appendChild(pagination);
        return wrapper;
    }

    async function refreshTableCard(card, tbl) {
        const refreshBtn = card.querySelector('.btn-refresh');
        const bodyEl = card.querySelector('.table-card-body');
        refreshBtn.disabled = true;
        refreshBtn.textContent = '…';
        try {
            const data = await postJson(`/api/tables/${tbl.id}/refresh`, {});
            bodyEl.innerHTML = '';
            bodyEl.appendChild(buildDashboardTable(data.columns, data.rows, data.meta, tbl.id));
        } catch (err) {
            bodyEl.innerHTML = '';
            const msg = document.createElement('div');
            msg.className = 'error';
            msg.textContent = err && err.message ? err.message : 'Refresh failed';
            bodyEl.appendChild(msg);
        } finally {
            refreshBtn.disabled = false;
            refreshBtn.textContent = ICON_REFRESH;
        }
    }

    // ---------------------------------------------------------------
    // Layout persistence
    // ---------------------------------------------------------------
    async function persistLayout(itemType, item) {
        const basePath = itemType === 'table' ? '/api/tables' : '/api/charts';
        await requestJson(`${basePath}/${item.id}/layout`, 'PATCH', {
            sort_order: Number.isFinite(item.sort_order) ? item.sort_order : null,
            width_units: Number.isFinite(item.width_units) ? item.width_units : (itemType === 'table' ? TABLE_DEFAULT_W : CHART_DEFAULT_W),
            height_px: Number.isFinite(item.height_px) ? item.height_px : (itemType === 'table' ? TABLE_DEFAULT_H_PX : CHART_DEFAULT_H_PX),
        });
    }

    function getAllItems() {
        return [
            ...state.charts.map(c => ({ ...c, _type: 'chart' })),
            ...state.tables.map(t => ({ ...t, _type: 'table' })),
        ].sort((a, b) => {
            const ao = Number.isFinite(a.sort_order) ? a.sort_order : Infinity;
            const bo = Number.isFinite(b.sort_order) ? b.sort_order : Infinity;
            return ao !== bo ? ao - bo : String(a.id).localeCompare(String(b.id));
        });
    }

    function getItemById(id) {
        return state.charts.find(c => c.id === id) || state.tables.find(t => t.id === id) || null;
    }
    function getItemType(id) {
        if (state.charts.find(c => c.id === id)) return 'chart';
        if (state.tables.find(t => t.id === id)) return 'table';
        return null;
    }

    async function persistFromGrid() {
        if (!grid) return;
        const nodes = (grid.engine && Array.isArray(grid.engine.nodes)) ? grid.engine.nodes : [];
        const ordered = [...nodes].sort((a, b) => (a.y - b.y) || (a.x - b.x));
        const updates = [];
        for (let idx = 0; idx < ordered.length; idx++) {
            const n = ordered[idx];
            if (!n || !n.el) continue;
            const itemId = n.el.dataset && n.el.dataset.id ? n.el.dataset.id : null;
            if (!itemId) continue;
            const item = getItemById(itemId);
            if (!item) continue;
            item.sort_order = idx;
            item.width_units = n.w;
            item.height_px = n.h * GRID_CELL_PX;
            updates.push({ item, type: getItemType(itemId) });
        }
        await Promise.all(updates.map(u => persistLayout(u.type, u.item)));
    }

    // ---------------------------------------------------------------
    // Fullscreen
    // ---------------------------------------------------------------
    function openFullscreenChart(chartConfig) {
        const overlay = document.createElement("div");
        overlay.className = "chart-fullscreen";
        const content = document.createElement("div");
        content.className = "chart-fullscreen-content";
        const closeBtn = document.createElement("button");
        closeBtn.className = "chart-fullscreen-close";
        closeBtn.innerText = "✕";
        const chart = document.createElement("plotly-chart");
        chart.config = chartConfig;
        content.appendChild(closeBtn);
        content.appendChild(chart);
        overlay.appendChild(content);
        document.body.appendChild(overlay);
        closeBtn.onclick = () => overlay.remove();
        overlay.onclick = (e) => { if (e.target === overlay) overlay.remove(); };
        setTimeout(() => window.dispatchEvent(new Event("resize")), 100);
    }

    // ---------------------------------------------------------------
    // Render
    // ---------------------------------------------------------------
    let grid;

    function render() {
        if (grid) { grid.removeAll(); } else { dashboardGrid.innerHTML = ""; }

        if (!grid) {
            grid = GridStack.init({
                column: 12, cellHeight: GRID_CELL_PX, margin: 12,
                animate: true, float: false, minRow: 1,
                columnOpts: {
                    breakpoints: [
                        { w: 700, c: 1 },
                        { w: 1100, c: 6 },
                        { w: 1400, c: 12 },
                    ],
                },
                draggable: { handle: ".chart-card-header, .table-card-header" },
                resizable: { handles: "se" },
            }, "#dashboardGrid");

            grid.on("change", async () => { await persistFromGrid(); });
            grid.on("resize", (event, el) => {
                const c = el.querySelector("plotly-chart");
                if (c) { c.style.width = "100%"; c.style.height = "100%"; }
            });
            grid.on("resizestop", (event, el) => {
                setTimeout(() => window.dispatchEvent(new Event("resize")), 60);
            });
        }

        const allItems = getAllItems();
        if (!allItems.length) {
            const empty = document.createElement("div");
            empty.className = "empty";
            empty.textContent = "No pinned charts or tables.";
            dashboardGrid.appendChild(empty);
            return;
        }

        allItems.forEach((item) => {
            if (item._type === 'chart') {
                renderChartCard(item);
            } else {
                renderTableCard(item);
            }
        });

        setTimeout(() => {
            if (!grid) return;
            window.dispatchEvent(new Event("resize"));
        }, 400);
    }

    function renderChartCard(chart) {
        const card = document.createElement("div");
        card.className = "chart-card";
        card.dataset.id = chart.id;
        card.innerHTML = `
        <div class="grid-stack-item-content">
            <div class="chart-card-header">
                <div>
                    <h3 class="chart-title">${chart.title || "Chart"}</h3>
                    <p class="chart-meta">${chart.chart_type || ""}</p>
                </div>
                <div class="chart-actions">
                    <button class="icon-action-btn btn-refresh">↻</button>
                    <button class="icon-action-btn btn-followup" title="Follow up in chat">↩</button>
                    <button class="icon-action-btn btn-fullscreen" title="Fullscreen">⛶</button>
                    <button class="icon-action-btn btn-unpin">✕</button>
                </div>
            </div>
            <div class="chart-box"></div>
        </div>`;

        const configuredW = Number.isFinite(chart.width_units) ? chart.width_units : CHART_DEFAULT_W;
        const w = Math.min(12, Math.max(CHART_MIN_W, configuredW));
        const heightPx = Number.isFinite(chart.height_px) ? chart.height_px : CHART_DEFAULT_H_PX;
        const h = Math.max(CHART_MIN_H, Math.round(heightPx / GRID_CELL_PX));
        grid.addWidget(card, { w, h, minW: CHART_MIN_W, minH: CHART_MIN_H });

        const chartBox = card.querySelector(".chart-box");
        const chartEl = document.createElement("plotly-chart");
        chartEl.style.width = "100%";
        chartEl.style.height = "100%";
        chartBox.appendChild(chartEl);

        refreshChart(card, chart);
        card.querySelector(".chart-box").ondblclick = () => {
            const ce = card.querySelector("plotly-chart");
            if (ce) openFullscreenChart(ce.config);
        };
        card.querySelector(".btn-fullscreen").onclick = () => {
            const ce = card.querySelector("plotly-chart");
            if (ce) openFullscreenChart(ce.config);
        };
        card.querySelector(".btn-refresh").onclick = () => refreshChart(card, chart);
        card.querySelector(".btn-followup").onclick = () => {
            const url = `./index.html?followup=chart&id=${encodeURIComponent(chart.id)}`;
            window.open(url, "_blank", "noopener,noreferrer");
        };
        card.querySelector(".btn-unpin").onclick = async () => {
            await requestJson(`/api/charts/${chart.id}`, "DELETE");
            state.charts = state.charts.filter(c => c.id !== chart.id);
            render();
            setSummary();
        };
    }

    function renderTableCard(tbl) {
        const card = document.createElement("div");
        card.className = "table-card";
        card.dataset.id = tbl.id;
        card.innerHTML = `
        <div class="grid-stack-item-content">
            <div class="table-card-header">
                <div>
                    <h3 class="chart-title">${tbl.title || "Table"}</h3>
                    <p class="chart-meta">table</p>
                </div>
                <div class="chart-actions">
                    <button class="icon-action-btn btn-refresh">↻</button>
                    <button class="icon-action-btn btn-followup" title="Follow up in chat">↩</button>
                    <button class="icon-action-btn btn-unpin">✕</button>
                </div>
            </div>
            <div class="table-card-body"></div>
        </div>`;

        const configuredW = Number.isFinite(tbl.width_units) ? tbl.width_units : TABLE_DEFAULT_W;
        const w = Math.min(12, Math.max(TABLE_MIN_W, configuredW));
        const heightPx = Number.isFinite(tbl.height_px) ? tbl.height_px : TABLE_DEFAULT_H_PX;
        const h = Math.max(TABLE_MIN_H, Math.round(heightPx / GRID_CELL_PX));
        grid.addWidget(card, { w, h, minW: TABLE_MIN_W, minH: TABLE_MIN_H });

        refreshTableCard(card, tbl);
        card.querySelector(".btn-refresh").onclick = () => refreshTableCard(card, tbl);
        card.querySelector(".btn-followup").onclick = () => {
            const url = `./index.html?followup=table&id=${encodeURIComponent(tbl.id)}`;
            window.open(url, "_blank", "noopener,noreferrer");
        };
        card.querySelector(".btn-unpin").onclick = async () => {
            await requestJson(`/api/tables/${tbl.id}`, "DELETE");
            state.tables = state.tables.filter(t => t.id !== tbl.id);
            render();
            setSummary();
        };
    }

    // ---------------------------------------------------------------
    // Refresh all
    // ---------------------------------------------------------------
    async function refreshAllCards() {
        const cards = Array.from(dashboardGrid.querySelectorAll('.chart-card, .table-card'));
        await Promise.all(cards.map((card) => {
            const itemId = card.dataset.id;
            const chart = state.charts.find(c => c.id === itemId);
            if (chart) return refreshChart(card, chart);
            const tbl = state.tables.find(t => t.id === itemId);
            if (tbl) return refreshTableCard(card, tbl);
            return Promise.resolve();
        }));
    }

    // ---------------------------------------------------------------
    // Load
    // ---------------------------------------------------------------
    async function loadAll() {
        const [chartsData, tablesData] = await Promise.all([
            getJson('/api/charts'),
            getJson('/api/tables'),
        ]);
        state.charts = Array.isArray(chartsData.items) ? chartsData.items : [];
        state.tables = Array.isArray(tablesData.items) ? tablesData.items : [];
        render();
        setSummary();
        setTimeout(() => window.dispatchEvent(new Event("resize")), 300);
    }

    btnRefreshAll.addEventListener('click', async () => {
        setError(null);
        btnRefreshAll.disabled = true;
        const prev = btnRefreshAll.textContent;
        btnRefreshAll.textContent = 'Refreshing…';
        try {
            await refreshAllCards();
            setSummary();
            setTimeout(() => window.dispatchEvent(new Event("resize")), 120);
        } catch (err) {
            setError(err && err.message ? err.message : 'Failed to refresh dashboard');
        } finally {
            btnRefreshAll.disabled = false;
            btnRefreshAll.textContent = prev || 'Refresh All';
        }
    });

    setError(null);
    loadAll()
        .then(() => setTimeout(() => window.dispatchEvent(new Event("resize")), 100))
        .catch((err) => setError(err && err.message ? err.message : 'Failed to load dashboard'));
})();
