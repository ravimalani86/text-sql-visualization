(() => {
  const API = String(window.API_BASE || 'http://localhost:8000').replace(/\/$/, '');

  const elPrompt = document.getElementById('prompt');
  const elError = document.getElementById('error');
  const btnAnalyze = document.getElementById('btnAnalyze');
  const btnNewConversation = document.getElementById('btnNewConversation');
  const btnImportData = document.getElementById('btnImportData');
  const importCsvInput = document.getElementById('importCsvInput');
  const importMessage = document.getElementById('importMessage');
  const historyList = document.getElementById('historyList');
  const chatTimeline = document.getElementById('chatTimeline');
  const sidebar = document.querySelector('.sidebar');
  const btnToggleSidebar = document.getElementById('btnToggleSidebar');
  const content = document.querySelector('.content');

  function setImportMessage(msg, isError) {
    if (!importMessage) return;
    if (!msg) {
      importMessage.classList.add('hidden');
      importMessage.textContent = '';
      importMessage.classList.remove('import-message-success', 'import-message-error');
      return;
    }
    importMessage.textContent = msg;
    importMessage.classList.remove('hidden');
    importMessage.classList.toggle('import-message-error', !!isError);
    importMessage.classList.toggle('import-message-success', !isError);
    window.clearTimeout(importMessage._hideTimer);
    importMessage._hideTimer = window.setTimeout(() => setImportMessage(''), 5000);
  }

  if (btnImportData && importCsvInput) {
    btnImportData.addEventListener('click', () => importCsvInput.click());
    importCsvInput.addEventListener('change', async () => {
      const file = importCsvInput.files && importCsvInput.files[0];
      importCsvInput.value = '';
      if (!file) return;
      if (!file.name.toLowerCase().endsWith('.csv')) {
        setImportMessage('Please select a .csv file.', true);
        return;
      }
      const prevText = btnImportData.textContent;
      btnImportData.disabled = true;
      btnImportData.textContent = 'Uploading…';
      setImportMessage('');
      try {
        const form = new FormData();
        form.append('file', file);
        const res = await fetch(`${API}/upload-csv/`, { method: 'POST', body: form });
        if (!res.ok) {
          const errBody = await res.json().catch(() => ({}));
          const detail = errBody.detail || await res.text().catch(() => '') || `Upload failed: ${res.status}`;
          setImportMessage(typeof detail === 'string' ? detail : JSON.stringify(detail), true);
          return;
        }
        const data = await res.json();
        const tableName = data.table || file.name.replace(/\.csv$/i, '').toLowerCase();
        setImportMessage(`Imported as table: ${tableName}`, false);
      } catch (err) {
        setImportMessage(err && err.message ? err.message : 'Upload failed', true);
      } finally {
        btnImportData.disabled = false;
        btnImportData.textContent = prevText;
      }
    });
  }

  if (btnToggleSidebar && sidebar) {
    btnToggleSidebar.addEventListener('click', (e) => {
      e.stopPropagation();
      sidebar.classList.toggle('show');
    });
  }

  if (content && sidebar) {
    content.addEventListener('click', () => {
      if (window.innerWidth <= 1024) {
        sidebar.classList.remove('show');
      }
    });
  }

  const state = {
    conversationId: null,
    conversations: [],
    turns: [],
    pinnedKeys: new Set(),
  };

  function setError(msg) {
    if (!msg) {
      elError.classList.add('hidden');
      elError.textContent = '';
      return;
    }
    elError.textContent = msg;
    elError.classList.remove('hidden');
  }

  function setLoading(btn, loading, textWhenIdle, textWhenLoading) {
    btn.disabled = !!loading;
    btn.textContent = loading ? textWhenLoading : textWhenIdle;
  }

  async function postForm(path, fields) {
    const form = new FormData();
    for (const [k, v] of Object.entries(fields)) {
      form.append(k, v);
    }
    const res = await fetch(`${API}${path}`, { method: 'POST', body: form });
    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      throw new Error(txt || `Request failed: ${res.status}`);
    }
    return res.json();
  }

  async function getJson(path) {
    const res = await fetch(`${API}${path}`);
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

  function makePinnedKey(sql, chartType, xField, yField) {
    return [String(sql || '').trim(), String(chartType || '').trim(), String(xField || ''), String(yField || '')].join('|');
  }

  async function loadPinnedKeys() {
    const data = await getJson('/api/charts');
    const items = Array.isArray(data.items) ? data.items : [];
    const next = new Set();
    for (const item of items) {
      next.add(makePinnedKey(item.sql_query, item.chart_type, item.x_field, item.y_field));
    }
    state.pinnedKeys = next;
  }

  function renderHistory() {
    historyList.innerHTML = '';
    if (!state.conversations.length) {
      const empty = document.createElement('div');
      empty.className = 'history-empty';
      empty.textContent = 'No conversations yet';
      historyList.appendChild(empty);
      return;
    }

    for (const conv of state.conversations) {
      const item = document.createElement('button');
      item.className = `history-item${conv.id === state.conversationId ? ' active' : ''}`;
      item.type = 'button';
      item.dataset.id = conv.id;

      const title = document.createElement('div');
      title.className = 'history-item-title';
      title.textContent = conv.title || conv.last_prompt || 'Untitled conversation';

      const meta = document.createElement('div');
      meta.className = 'history-item-meta';
      meta.textContent = `${conv.turn_count || 0} turn${conv.turn_count === 1 ? '' : 's'}`;

      item.appendChild(title);
      item.appendChild(meta);
      item.addEventListener('click', () => {
        loadConversation(conv.id).catch((err) => {
          setError(err && err.message ? err.message : 'Failed to load history');
        });
      });
      historyList.appendChild(item);
    }
  }

  function createCodeBlock(sql) {
    const pre = document.createElement('pre');
    pre.className = 'sql-block';
    pre.textContent = sql || '';
    return pre;
  }

  function createTableBlock(columns, rows, meta) {
    const wrapper = document.createElement('div');
    wrapper.className = 'table-wrapper';

    const table = document.createElement('table');
    table.className = 'table';

    const thead = document.createElement('thead');
    const trh = document.createElement('tr');
    for (const col of columns || []) {
      const th = document.createElement('th');
      th.textContent = col;
      trh.appendChild(th);
    }
    thead.appendChild(trh);

    const tbody = document.createElement('tbody');
    for (const row of rows || []) {
      const tr = document.createElement('tr');
      for (const col of columns || []) {
        const td = document.createElement('td');
        const v = row && Object.prototype.hasOwnProperty.call(row, col) ? row[col] : '';
        td.textContent = String(v);
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }

    table.appendChild(thead);
    table.appendChild(tbody);
    wrapper.appendChild(table);

    if (meta && typeof meta.row_count === 'number') {
      const info = document.createElement('div');
      info.className = 'block-meta';
      const shown = typeof meta.shown_rows === 'number' ? meta.shown_rows : rows.length;
      info.textContent = `Rows: ${shown}/${meta.row_count}`;
      wrapper.prepend(info);
    }
    return wrapper;
  }

  function createChartBlock(plotly, chartType, turn) {
    const box = document.createElement('div');
    box.className = 'chart-box';

    const toolbar = document.createElement('div');
    toolbar.className = 'chart-toolbar';

    const title = document.createElement('div');
    title.className = 'block-meta';
    title.textContent = chartType ? `Chart: ${chartType}` : 'Chart';
    toolbar.appendChild(title);

    if (turn && turn.sql) {
      const intent = turn.chart_intent || {};
      const key = makePinnedKey(turn.sql, chartType || intent.chart_type, intent.x, intent.y);
      const isPinned = state.pinnedKeys.has(key);

      const pinBtn = document.createElement('button');
      pinBtn.className = 'icon-button';
      pinBtn.type = 'button';
      pinBtn.title = isPinned ? 'Already pinned' : 'Pin chart to dashboard';
      pinBtn.textContent = isPinned ? 'Pinned' : '📌';
      pinBtn.disabled = isPinned;
      pinBtn.addEventListener('click', async () => {
        try {
          await pinChartFromTurn(turn, chartType);
          pinBtn.textContent = 'Pinned';
          pinBtn.disabled = true;
          pinBtn.title = 'Already pinned';
        } catch (err) {
          setError(err && err.message ? err.message : 'Failed to pin chart');
        }
      });
      toolbar.appendChild(pinBtn);
    }

    box.appendChild(toolbar);

    const chartEl = document.createElement('plotly-chart');
    chartEl.className = 'plotly-embedded';
    chartEl.config = plotly;
    box.appendChild(chartEl);
    return box;
  }

  function normalizeBlocks(turn) {
    const blocks = Array.isArray(turn.response_blocks) ? turn.response_blocks : [];
    if (blocks.length) {
      return blocks;
    }

    const fallback = [];
    if (turn.assistant_text) {
      fallback.push({ type: 'text', content: turn.assistant_text });
    }
    if (turn.sql) {
      fallback.push({ type: 'sql', sql: turn.sql });
    }
    if (Array.isArray(turn.columns) && Array.isArray(turn.data) && turn.columns.length) {
      fallback.push({ type: 'table', columns: turn.columns, rows: turn.data });
    }
    if (turn.plotly) {
      fallback.push({ type: 'chart', chart_type: turn.chart_intent && turn.chart_intent.chart_type, plotly: turn.plotly });
    }
    return fallback;
  }

  function renderConversation() {
    chatTimeline.innerHTML = '';
    if (!state.turns.length) {
      const empty = document.createElement('div');
      empty.className = 'chat-empty';
      empty.textContent = 'Start by asking a question about your data.';
      chatTimeline.appendChild(empty);
      return;
    }

    for (const turn of state.turns) {
      const userMsg = document.createElement('article');
      userMsg.className = 'message user';
      userMsg.innerHTML = `<div class="message-role">You</div><div class="message-text"></div>`;
      userMsg.querySelector('.message-text').textContent = turn.prompt || '';
      chatTimeline.appendChild(userMsg);

      const assistantMsg = document.createElement('article');
      assistantMsg.className = 'message assistant';
      assistantMsg.innerHTML = `<div class="message-role">AI</div><div class="message-body"></div>`;
      const body = assistantMsg.querySelector('.message-body');
      const blocks = normalizeBlocks(turn);

      for (const block of blocks) {
        if (block.type === 'text') {
          const p = document.createElement('p');
          p.className = 'message-text';
          p.textContent = block.content || '';
          body.appendChild(p);
        } else if (block.type === 'sql') {
          body.appendChild(createCodeBlock(block.sql));
        } else if (block.type === 'table') {
          body.appendChild(createTableBlock(block.columns || [], block.rows || [], block.meta || null));
        } else if (block.type === 'chart' && block.plotly) {
          body.appendChild(createChartBlock(block.plotly, block.chart_type, turn));
        }
      }

      if (turn.status === 'failed' && turn.error) {
        const err = document.createElement('div');
        err.className = 'error-inline';
        err.textContent = turn.error;
        body.appendChild(err);
      }

      chatTimeline.appendChild(assistantMsg);
    }
    chatTimeline.scrollTop = chatTimeline.scrollHeight;
  }

  async function loadHistoryList() {
    const data = await getJson('/history/conversations');
    state.conversations = Array.isArray(data.items) ? data.items : [];
    renderHistory();
  }

  async function loadConversation(conversationId) {
    const data = await getJson(`/history/${conversationId}`);
    state.conversationId = conversationId;
    state.turns = Array.isArray(data.turns) ? data.turns : [];
    renderHistory();
    renderConversation();
  }

  async function pinChartFromTurn(turn, chartType) {
    const intent = turn.chart_intent || {};
    const payload = {
      title: (turn.prompt || 'Pinned chart').slice(0, 120),
      sql: turn.sql,
      chart_type: chartType || intent.chart_type || 'bar',
      x_field: intent.x || null,
      y_field: intent.y || null,
    };
    await postJson('/api/charts/pin', payload);
    await loadPinnedKeys();
  }

  async function onAnalyze() {
    const prompt = elPrompt.value.trim();
    if (!prompt) {
      setError('Please enter a prompt');
      return;
    }

    setError(null);
    setLoading(btnAnalyze, true, 'Send', 'Thinking...');

    try {
      const fields = { prompt };
      if (state.conversationId) {
        fields.conversation_id = state.conversationId;
      }
      const data = await postForm('/analyze/', fields);

      state.conversationId = data.conversation_id;
      const turn = {
        id: data.turn_id,
        prompt: data.prompt,
        sql: data.sql,
        columns: Array.isArray(data.columns) ? data.columns : [],
        data: Array.isArray(data.data) ? data.data : [],
        chart_intent: data.chart_intent || null,
        plotly: data.plotly || null,
        assistant_text: data.assistant_text || '',
        response_blocks: Array.isArray(data.response_blocks) ? data.response_blocks : [],
        status: data.status || 'success',
        error: null,
        created_at: data.created_at || null,
      };
      state.turns.push(turn);
      renderConversation();
      elPrompt.value = '';
      await loadHistoryList();
    } catch (e) {
      setError(e && e.message ? e.message : 'Something went wrong');
      await loadHistoryList().catch(() => {});
    } finally {
      setLoading(btnAnalyze, false, 'Send', 'Thinking...');
    }
  }

  function onNewConversation() {
    state.conversationId = null;
    state.turns = [];
    elPrompt.value = '';
    setError(null);
    renderConversation();
    renderHistory();
  }

  btnAnalyze.addEventListener('click', onAnalyze);
  elPrompt.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      onAnalyze();
    }
  });
  btnNewConversation.addEventListener('click', onNewConversation);

  // Initial state.
  setError(null);
  loadPinnedKeys()
    .then(() => {
      renderConversation();
    })
    .catch(() => {
      renderConversation();
    });
  loadHistoryList().catch(() => {});
})();

