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

  async function postFormStream(path, fields, onEvent) {
    const form = new FormData();
    for (const [k, v] of Object.entries(fields)) {
      form.append(k, v);
    }

    const res = await fetch(`${API}${path}`, { method: 'POST', body: form });
    if (!res.ok) {
      const txt = await res.text().catch(() => '');
      throw new Error(txt || `Request failed: ${res.status}`);
    }
    if (!res.body) {
      throw new Error('Streaming is not supported by this browser');
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      let nlIdx = buffer.indexOf('\n');
      while (nlIdx !== -1) {
        const line = buffer.slice(0, nlIdx).trim();
        buffer = buffer.slice(nlIdx + 1);
        if (line) {
          onEvent(JSON.parse(line));
        }
        nlIdx = buffer.indexOf('\n');
      }
    }

    const rest = buffer.trim();
    if (rest) {
      onEvent(JSON.parse(rest));
    }
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
    const allColumns = Array.isArray(columns) ? columns : [];
    const allRows = Array.isArray(rows) ? rows : [];
    const state = {
      query: '',
      sortBy: null,
      sortDir: null,
      visible: new Set(allColumns),
    };

    const controls = document.createElement('div');
    controls.className = 'table-controls';

    const search = document.createElement('input');
    search.className = 'table-search';
    search.type = 'search';
    search.placeholder = 'Filter rows...';
    controls.appendChild(search);

    const colToggle = document.createElement('details');
    colToggle.className = 'table-column-toggle';
    const colToggleSummary = document.createElement('summary');
    colToggleSummary.textContent = 'Columns';
    colToggle.appendChild(colToggleSummary);
    const colToggleBody = document.createElement('div');
    colToggleBody.className = 'table-column-toggle-body';
    colToggle.appendChild(colToggleBody);
    controls.appendChild(colToggle);

    const table = document.createElement('table');
    table.className = 'table';
    const thead = document.createElement('thead');
    const tbody = document.createElement('tbody');

    const getCellValue = (row, col) => {
      const hasValue = row && Object.prototype.hasOwnProperty.call(row, col);
      return hasValue ? row[col] : '';
    };
    const isNumberLike = (val) => {
      if (typeof val === 'number') return Number.isFinite(val);
      if (typeof val !== 'string') return false;
      const n = Number(val.replace(/,/g, '').trim());
      return Number.isFinite(n);
    };
    const toNumber = (val) => {
      if (typeof val === 'number') return val;
      if (typeof val !== 'string') return NaN;
      return Number(val.replace(/,/g, '').trim());
    };

    const getVisibleColumns = () => allColumns.filter((c) => state.visible.has(c));
    const getFilteredRows = (visibleCols) => {
      const q = state.query.trim().toLowerCase();
      if (!q) return allRows.slice();
      return allRows.filter((row) =>
        visibleCols.some((col) => String(getCellValue(row, col) ?? '').toLowerCase().includes(q))
      );
    };
    const getSortedRows = (inputRows) => {
      if (!state.sortBy || !state.sortDir) return inputRows;
      const dir = state.sortDir === 'asc' ? 1 : -1;
      const col = state.sortBy;
      const output = inputRows.slice();
      output.sort((a, b) => {
        const va = getCellValue(a, col);
        const vb = getCellValue(b, col);
        if (va == null && vb == null) return 0;
        if (va == null) return 1;
        if (vb == null) return -1;
        if (isNumberLike(va) && isNumberLike(vb)) {
          return (toNumber(va) - toNumber(vb)) * dir;
        }
        return String(va).localeCompare(String(vb), undefined, { sensitivity: 'base', numeric: true }) * dir;
      });
      return output;
    };

    const renderColumnToggles = () => {
      colToggleBody.innerHTML = '';
      for (const col of allColumns) {
        const item = document.createElement('label');
        item.className = 'table-column-toggle-item';
        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.checked = state.visible.has(col);
        checkbox.addEventListener('change', () => {
          if (checkbox.checked) {
            state.visible.add(col);
          } else if (state.visible.size > 1) {
            state.visible.delete(col);
          } else {
            checkbox.checked = true;
            return;
          }
          if (state.sortBy && !state.visible.has(state.sortBy)) {
            state.sortBy = null;
            state.sortDir = null;
          }
          renderTable();
        });
        const labelText = document.createElement('span');
        labelText.textContent = col;
        item.appendChild(checkbox);
        item.appendChild(labelText);
        colToggleBody.appendChild(item);
      }
    };

    const renderTable = () => {
      const visibleCols = getVisibleColumns();
      const filteredRows = getFilteredRows(visibleCols);
      const finalRows = getSortedRows(filteredRows);

      thead.innerHTML = '';
      tbody.innerHTML = '';

      const trh = document.createElement('tr');
      for (const col of visibleCols) {
        const th = document.createElement('th');
        th.className = 'table-th-sortable';
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'table-sort-btn';
        const isActive = state.sortBy === col;
        const icon = isActive ? (state.sortDir === 'asc' ? ' ↑' : ' ↓') : '';
        btn.textContent = `${col}${icon}`;
        btn.addEventListener('click', () => {
          if (state.sortBy !== col) {
            state.sortBy = col;
            state.sortDir = 'asc';
          } else if (state.sortDir === 'asc') {
            state.sortDir = 'desc';
          } else {
            state.sortBy = null;
            state.sortDir = null;
          }
          renderTable();
        });
        th.appendChild(btn);
        trh.appendChild(th);
      }
      thead.appendChild(trh);

      for (const row of finalRows) {
        const tr = document.createElement('tr');
        for (const col of visibleCols) {
          const td = document.createElement('td');
          const v = getCellValue(row, col);
          td.textContent = String(v);
          tr.appendChild(td);
        }
        tbody.appendChild(tr);
      }
    };

    search.addEventListener('input', () => {
      state.query = search.value || '';
      renderTable();
    });

    renderColumnToggles();
    renderTable();

    table.appendChild(thead);
    table.appendChild(tbody);
    wrapper.appendChild(controls);
    wrapper.appendChild(table);

    if (meta && typeof meta.row_count === 'number') {
      const info = document.createElement('div');
      info.className = 'block-meta';
      const shown = typeof meta.shown_rows === 'number' ? meta.shown_rows : allRows.length;
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
      const hasChartBlock = blocks.some((b) => b && b.type === 'chart' && b.plotly);
      if (!hasChartBlock && turn.plotly) {
        blocks.push({ type: 'chart', chart_type: turn.chart_intent && turn.chart_intent.chart_type, plotly: turn.plotly });
      }
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
      assistantMsg.innerHTML = `<div class="message-role">NIA</div><div class="message-body"></div>`;
      const body = assistantMsg.querySelector('.message-body');
      const blocks = normalizeBlocks(turn);

      for (const block of blocks) {
        if (block.type === 'text' || block.type === 'status') {
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
      series_field: intent.series || null,
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

      const turn = {
        id: null,
        prompt,
        sql: null,
        columns: [],
        data: [],
        chart_intent: null,
        plotly: null,
        assistant_text: '',
        response_blocks: [{ type: 'text', content: 'Thinking...' }],
        status: 'streaming',
        error: null,
        created_at: null,
      };
      state.turns.push(turn);
      renderConversation();

      const upsertBlock = (type, block) => {
        const idx = turn.response_blocks.findIndex((b) => b && b.type === type);
        if (idx >= 0) {
          turn.response_blocks[idx] = block;
        } else {
          turn.response_blocks.push(block);
        }
      };
      const upsertBottomBlock = (type, block) => {
        turn.response_blocks = turn.response_blocks.filter((b) => !(b && b.type === type));
        turn.response_blocks.push(block);
      };
      const stageLabelByName = {
        intent_classified: 'Thinking...',
        reused_previous_result: 'Using previous query result...',
        prompt_cache_hit: 'Using saved result from history...',
        sql_generated: 'SQL generated. Running query...',
        query_executed: 'Query complete. Preparing table...',
        chart_intent_ready: 'Analyzing chart intent...',
        chart_intent_reused: 'Using previous chart intent...',
        chart_reused: 'Using existing chart...',
        chart_ready: 'Chart is ready.',
        assistant_ready: 'Preparing final response...',
      };

      const mapFinalTurn = (data) => ({
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
      });

      await postFormStream('/analyze/stream', fields, (evt) => {
        if (!evt || typeof evt !== 'object') return;
        console.log(evt.type, evt.name)
        if (evt.type === 'meta' && evt.conversation_id) {
          state.conversationId = evt.conversation_id;
        } else if (evt.type === 'stage') {
          const stageText = stageLabelByName[evt.name] || `Processing: ${String(evt.name || 'working')}`;
          upsertBottomBlock('status', { type: 'status', content: stageText });
          if (evt.name === 'sql_generated' && evt.sql) {
            turn.sql = evt.sql;
            upsertBlock('sql', { type: 'sql', sql: evt.sql });
          } else if (evt.name === 'query_executed') {
            const cols = Array.isArray(evt.columns) ? evt.columns : [];
            const rows = Array.isArray(evt.preview_rows) ? evt.preview_rows : [];
            turn.columns = cols;
            turn.data = rows;
            upsertBlock('table', {
              type: 'table',
              columns: cols,
              rows,
              meta: {
                row_count: typeof evt.row_count === 'number' ? evt.row_count : rows.length,
                shown_rows: rows.length,
              },
            });
          } else if (evt.name === 'chart_intent_ready' && evt.chart_intent) {
            turn.chart_intent = evt.chart_intent;
          } else if (evt.name === 'chart_ready' && evt.plotly) {
            turn.plotly = evt.plotly;
            upsertBlock('chart', {
              type: 'chart',
              chart_type: turn.chart_intent && turn.chart_intent.chart_type,
              plotly: evt.plotly,
            });
          } else if (evt.name === 'assistant_ready') {
            const text = evt.assistant_text || '';
            turn.assistant_text = text;
            upsertBottomBlock('status', { type: 'status', content: text || 'Preparing final response...' });
          }
          renderConversation();
        } else if (evt.type === 'final' && evt.data) {
          const finalTurn = mapFinalTurn(evt.data);
          Object.assign(turn, finalTurn);
          state.conversationId = evt.data.conversation_id || state.conversationId;
          renderConversation();
        } else if (evt.type === 'error') {
          turn.status = 'failed';
          turn.error = (evt.detail && String(evt.detail)) || 'Something went wrong';
          if (!turn.response_blocks.length) {
            turn.response_blocks.push({ type: 'text', content: 'Request failed.' });
          }
          renderConversation();
          throw new Error(turn.error);
        }
      });

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

