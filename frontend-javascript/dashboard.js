(() => {
  const API = String(window.API_BASE || 'http://localhost:8000').replace(/\/$/, '');
  const ICON_REFRESH = '↻';
  const ICON_UNPIN = '✕';

  const btnRefreshAll = document.getElementById('btnRefreshAll');
  const totalCharts = document.getElementById('totalCharts');
  const lastReload = document.getElementById('lastReload');
  const dashboardError = document.getElementById('dashboardError');
  const dashboardGrid = document.getElementById('dashboardGrid');

  const state = {
    charts: [],
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
    lastReload.textContent = new Date().toLocaleString();
  }

  async function refreshChart(card, chart) {
    const refreshBtn = card.querySelector('.btn-refresh');
    const chartBox = card.querySelector('.chart-box');
    refreshBtn.disabled = true;
    refreshBtn.textContent = '…';

    try {
      const data = await postForm(`/api/charts/${chart.id}/refresh`, {});
      chartBox.innerHTML = '';
      const chartEl = document.createElement('plotly-chart');
      chartEl.config = data.plotly || null;
      chartBox.appendChild(chartEl);
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

  async function persistLayout(chart) {
    await requestJson(`/api/charts/${chart.id}/layout`, 'PATCH', {
      sort_order: Number.isFinite(chart.sort_order) ? chart.sort_order : null,
      width_units: Number.isFinite(chart.width_units) ? chart.width_units : 1,
      height_px: Number.isFinite(chart.height_px) ? chart.height_px : 320,
    });
  }

  async function persistOrder() {
    const updates = state.charts.map((c, idx) => ({ ...c, sort_order: idx }));
    state.charts = updates;
    await Promise.all(updates.map((c) => persistLayout(c)));
  }

  function moveChart(dragId, targetId) {
    const from = state.charts.findIndex((c) => c.id === dragId);
    const to = state.charts.findIndex((c) => c.id === targetId);
    if (from < 0 || to < 0 || from === to) return;
    const [item] = state.charts.splice(from, 1);
    state.charts.splice(to, 0, item);
  }

  function render() {
    dashboardGrid.innerHTML = '';
    if (!state.charts.length) {
      const empty = document.createElement('div');
      empty.className = 'empty';
      empty.textContent = 'No pinned charts found. Pin from chat page to build your dashboard.';
      dashboardGrid.appendChild(empty);
      return;
    }

    for (const chart of state.charts) {
      const card = document.createElement('article');
      const widthUnits = Number.isFinite(chart.width_units) ? chart.width_units : 1;
      const heightPx = Number.isFinite(chart.height_px) ? chart.height_px : 320;
      card.className = `chart-card${widthUnits >= 2 ? ' span-2' : ''}`;
      card.style.setProperty('--chart-height', `${heightPx}px`);
      card.dataset.id = chart.id;
      card.innerHTML = [
        '<div class="chart-card-header">',
        '  <div>',
        '    <h3 class="chart-title"></h3>',
        '    <p class="chart-meta"></p>',
        '  </div>',
        '  <div class="drag-handle" title="Drag to reorder" aria-hidden="true">⋮⋮</div>',
        '  <div class="chart-actions">',
          '    <button type="button" class="icon-action-btn btn-refresh" title="Refresh chart" aria-label="Refresh chart"></button>',
          '    <button type="button" class="icon-action-btn btn-unpin" title="Unpin chart" aria-label="Unpin chart"></button>',
        '  </div>',
        '</div>',
        '<div class="chart-box"></div>',
      ].join('');

      card.querySelector('.chart-title').textContent = chart.title || 'Pinned chart';
      card.querySelector('.chart-meta').textContent = `${chart.chart_type || 'bar'} • ${chart.created_at || ''}`;

      const btn = card.querySelector('.btn-refresh');
      btn.textContent = ICON_REFRESH;
      btn.addEventListener('click', () => {
        refreshChart(card, chart);
      });

      const btnUnpin = card.querySelector('.btn-unpin');
      btnUnpin.textContent = ICON_UNPIN;
      btnUnpin.addEventListener('click', async () => {
        try {
          await requestJson(`/api/charts/${chart.id}`, 'DELETE');
          state.charts = state.charts.filter((c) => c.id !== chart.id);
          render();
          setSummary();
          await refreshRenderedCards();
        } catch (err) {
          setError(err && err.message ? err.message : 'Failed to unpin chart');
        }
      });

      const header = card.querySelector('.chart-card-header');
      header.draggable = true;
      header.addEventListener('dragstart', () => {
        state.draggingId = chart.id;
        card.classList.add('dragging');
      });
      header.addEventListener('dragend', () => {
        state.draggingId = null;
        card.classList.remove('dragging');
      });
      card.addEventListener('dragover', (e) => {
        e.preventDefault();
      });
      card.addEventListener('drop', async (e) => {
        e.preventDefault();
        if (!state.draggingId || state.draggingId === chart.id) return;
        moveChart(state.draggingId, chart.id);
        render();
        try {
          await persistOrder();
          await refreshRenderedCards();
        } catch (err) {
          setError(err && err.message ? err.message : 'Failed to save chart order');
        }
      });

      dashboardGrid.appendChild(card);
    }
  }

  async function refreshRenderedCards() {
    const cards = Array.from(dashboardGrid.querySelectorAll('.chart-card'));
    await Promise.all(
      cards.map((card) => {
        const chartId = card.dataset.id;
        const chart = state.charts.find((c) => c.id === chartId);
        if (!chart) return Promise.resolve();
        return refreshChart(card, chart);
      })
    );
  }

  async function loadCharts() {
    const data = await getJson('/api/charts');
    state.charts = Array.isArray(data.items) ? data.items : [];
    render();
    setSummary();
    await refreshRenderedCards();
  }

  btnRefreshAll.addEventListener('click', () => {
    loadCharts().catch((err) => {
      setError(err && err.message ? err.message : 'Failed to refresh dashboard');
    });
  });

  setError(null);
  loadCharts().catch((err) => {
    setError(err && err.message ? err.message : 'Failed to load dashboard');
  });
})();
