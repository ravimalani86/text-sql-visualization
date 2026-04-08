(() => {
    const API = String(window.API_BASE || 'http://localhost:8000').replace(/\/$/, '');
    const DEFAULT_PAGE_SIZE = Number(window.DEFAULT_PAGE_SIZE) || 10;

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
        pinnedTableKeys: new Set(),
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

    function makePinnedTableKey(sql) {
        return String(sql || '').trim();
    }

    async function loadPinnedTableKeys() {
        const data = await getJson('/api/tables');
        const items = Array.isArray(data.items) ? data.items : [];
        const next = new Set();
        for (const item of items) {
            next.add(makePinnedTableKey(item.sql_query));
        }
        state.pinnedTableKeys = next;
    }

    async function pinTableFromTurn(turn) {
        const payload = {
            title: (turn.prompt || 'Pinned table').slice(0, 120),
            sql: turn.sql,
            columns: Array.isArray(turn.columns) ? turn.columns : null,
        };
        await postJson('/api/tables/pin', payload);
        await loadPinnedTableKeys();
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

    function createTableBlock(columns, rows, meta, turn) {
        const wrapper = document.createElement('div');
        wrapper.className = 'table-wrapper';
        const allColumns = Array.isArray(columns) ? columns : [];
        let allRows = Array.isArray(rows) ? rows : [];
        const hasPagination = meta && typeof meta.total_count === 'number' && meta.total_count > (meta.page_size || DEFAULT_PAGE_SIZE);
        const turnId = (turn && turn.id) || null;

        const tblState = {
            query: '',
            sortBy: null,
            sortDir: null,
            visible: new Set(allColumns),
            page: (meta && meta.page) || 1,
            pageSize: (meta && meta.page_size) || DEFAULT_PAGE_SIZE,
            totalCount: (meta && meta.total_count) || allRows.length,
            totalPages: (meta && meta.total_pages) || 1,
            loading: false,
        };

        const controls = document.createElement('div');
        controls.className = 'table-controls';

        const search = document.createElement('input');
        search.className = 'table-search';
        search.type = 'search';
        search.placeholder = 'Search all rows...';
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

        const exportMenu = document.createElement('details');
        exportMenu.className = 'table-export';
        const exportSummary = document.createElement('summary');
        exportSummary.textContent = 'Export';
        exportMenu.appendChild(exportSummary);
        const exportBody = document.createElement('div');
        exportBody.className = 'table-export-body';
        exportMenu.appendChild(exportBody);

        const downloadExport = async (fmt) => {
            if (!turnId) {
                setError('Export is not available until the result is saved.');
                return;
            }
            try {
                exportSummary.textContent = 'Exporting…';
                const url = `${API}/api/export?turn_id=${encodeURIComponent(turnId)}&format=${encodeURIComponent(fmt)}`;
                const res = await fetch(url);
                if (!res.ok) {
                    const txt = await res.text().catch(() => '');
                    throw new Error(txt || `Export failed: ${res.status}`);
                }
                const blob = await res.blob();
                const cd = res.headers.get('content-disposition') || '';
                const match = cd.match(/filename="([^"]+)"/i);
                const filename = (match && match[1]) || `export.${fmt}`;
                const objectUrl = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = objectUrl;
                a.download = filename;
                document.body.appendChild(a);
                a.click();
                a.remove();
                URL.revokeObjectURL(objectUrl);
                exportMenu.open = false;
            } catch (err) {
                setError(err && err.message ? err.message : 'Export failed');
            } finally {
                exportSummary.textContent = 'Export';
            }
        };

        const makeExportItem = (label, fmt) => {
            const b = document.createElement('button');
            b.type = 'button';
            b.className = 'table-export-item';
            b.textContent = label;
            b.disabled = !turnId;
            b.addEventListener('click', () => downloadExport(fmt));
            return b;
        };
        exportBody.appendChild(makeExportItem('CSV', 'csv'));
        exportBody.appendChild(makeExportItem('Excel (.xlsx)', 'xlsx'));
        exportBody.appendChild(makeExportItem('PDF', 'pdf'));
        controls.appendChild(exportMenu);

        const table = document.createElement('table');
        table.className = 'table';
        const thead = document.createElement('thead');
        const tbody = document.createElement('tbody');

        const paginationBar = document.createElement('div');
        paginationBar.className = 'table-pagination';

        const infoBar = document.createElement('div');
        infoBar.className = 'block-meta table-info';

        const getCellValue = (row, col) => {
            const hasValue = row && Object.prototype.hasOwnProperty.call(row, col);
            return hasValue ? row[col] : '';
        };

        const getVisibleColumns = () => allColumns.filter((c) => tblState.visible.has(c));

        async function fetchPage(page, sortCol, sortDir) {
            if (!turnId || tblState.loading) return;
            tblState.loading = true;
            wrapper.classList.add('table-loading');
            try {
                const body = {
                    turn_id: turnId,
                    page: page,
                    page_size: tblState.pageSize,
                };
                if (sortCol) {
                    body.sort_column = sortCol;
                    body.sort_direction = sortDir || 'asc';
                }
                if (tblState.query) {
                    body.search = tblState.query;
                }
                const res = await fetch(`${API}/api/table-data`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body),
                });
                if (!res.ok) throw new Error(`Request failed: ${res.status}`);
                const data = await res.json();
                allRows = Array.isArray(data.rows) ? data.rows : [];
                tblState.page = data.meta.page;
                tblState.totalCount = data.meta.total_count;
                tblState.totalPages = data.meta.total_pages;
            } catch (err) {
                console.error('Pagination fetch error:', err);
            } finally {
                tblState.loading = false;
                wrapper.classList.remove('table-loading');
                renderTable();
                renderPagination();
                renderInfo();
            }
        }

        const renderColumnToggles = () => {
            colToggleBody.innerHTML = '';
            for (const col of allColumns) {
                const item = document.createElement('label');
                item.className = 'table-column-toggle-item';
                const checkbox = document.createElement('input');
                checkbox.type = 'checkbox';
                checkbox.checked = tblState.visible.has(col);
                checkbox.addEventListener('change', () => {
                    if (checkbox.checked) {
                        tblState.visible.add(col);
                    } else if (tblState.visible.size > 1) {
                        tblState.visible.delete(col);
                    } else {
                        checkbox.checked = true;
                        return;
                    }
                    if (tblState.sortBy && !tblState.visible.has(tblState.sortBy)) {
                        tblState.sortBy = null;
                        tblState.sortDir = null;
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
            const finalRows = allRows.slice(0, tblState.pageSize);

            thead.innerHTML = '';
            tbody.innerHTML = '';

            const trh = document.createElement('tr');
            for (const col of visibleCols) {
                const th = document.createElement('th');
                th.className = 'table-th-sortable';
                const btn = document.createElement('button');
                btn.type = 'button';
                btn.className = 'table-sort-btn';
                const isActive = tblState.sortBy === col;
                const icon = isActive ? (tblState.sortDir === 'asc' ? ' ↑' : ' ↓') : '';
                btn.textContent = `${col}${icon}`;
                btn.addEventListener('click', () => {
                    if (tblState.sortBy !== col) {
                        tblState.sortBy = col;
                        tblState.sortDir = 'asc';
                    } else if (tblState.sortDir === 'asc') {
                        tblState.sortDir = 'desc';
                    } else {
                        tblState.sortBy = null;
                        tblState.sortDir = null;
                    }
                    if (turnId && hasPagination) {
                        tblState.page = 1;
                        fetchPage(1, tblState.sortBy, tblState.sortDir);
                    } else {
                        renderTable();
                    }
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

        const renderPagination = () => {
            paginationBar.innerHTML = '';
            if (!hasPagination && tblState.totalPages <= 1) return;

            const makeBtn = (label, page, disabled) => {
                const b = document.createElement('button');
                b.type = 'button';
                b.className = 'page-btn';
                b.textContent = label;
                b.disabled = disabled || tblState.loading;
                if (!disabled) {
                    b.addEventListener('click', () => fetchPage(page, tblState.sortBy, tblState.sortDir));
                }
                return b;
            };

            paginationBar.appendChild(makeBtn('« First', 1, tblState.page <= 1));
            paginationBar.appendChild(makeBtn('‹ Prev', tblState.page - 1, tblState.page <= 1));

            const maxVisible = 5;
            let startPage = Math.max(1, tblState.page - Math.floor(maxVisible / 2));
            let endPage = Math.min(tblState.totalPages, startPage + maxVisible - 1);
            if (endPage - startPage + 1 < maxVisible) {
                startPage = Math.max(1, endPage - maxVisible + 1);
            }

            if (startPage > 1) {
                const dots = document.createElement('span');
                dots.className = 'page-dots';
                dots.textContent = '...';
                paginationBar.appendChild(dots);
            }

            for (let p = startPage; p <= endPage; p++) {
                const b = makeBtn(String(p), p, false);
                if (p === tblState.page) b.classList.add('page-btn-active');
                paginationBar.appendChild(b);
            }

            if (endPage < tblState.totalPages) {
                const dots = document.createElement('span');
                dots.className = 'page-dots';
                dots.textContent = '...';
                paginationBar.appendChild(dots);
            }

            paginationBar.appendChild(makeBtn('Next ›', tblState.page + 1, tblState.page >= tblState.totalPages));
            paginationBar.appendChild(makeBtn('Last »', tblState.totalPages, tblState.page >= tblState.totalPages));

            const pageInfo = document.createElement('span');
            pageInfo.className = 'page-info';
            pageInfo.textContent = `Page ${tblState.page} of ${tblState.totalPages}`;
            paginationBar.appendChild(pageInfo);
        };

        const renderInfo = () => {
            const shown = allRows.length;
            infoBar.textContent = `Showing ${shown} of ${tblState.totalCount} rows`;
        };

        let _searchTimer = null;
        search.addEventListener('input', () => {
            tblState.query = search.value || '';
            clearTimeout(_searchTimer);
            if (turnId && hasPagination) {
                _searchTimer = setTimeout(() => {
                    tblState.page = 1;
                    fetchPage(1, tblState.sortBy, tblState.sortDir);
                }, 400);
            }
        });

        renderColumnToggles();
        renderTable();
        renderPagination();
        renderInfo();

        if (turn && turn.sql) {
            const key = makePinnedTableKey(turn.sql);
            const isPinned = state.pinnedTableKeys.has(key);
            const pinBtn = document.createElement('button');
            pinBtn.className = 'icon-button table-pin-btn';
            pinBtn.type = 'button';
            pinBtn.title = isPinned ? 'Already pinned' : 'Pin table to dashboard';
            pinBtn.textContent = isPinned ? 'Pinned' : '📌';
            pinBtn.disabled = isPinned;
            pinBtn.addEventListener('click', async () => {
                try {
                    await pinTableFromTurn(turn);
                    pinBtn.textContent = 'Pinned';
                    pinBtn.disabled = true;
                    pinBtn.title = 'Already pinned';
                } catch (err) {
                    setError(err && err.message ? err.message : 'Failed to pin table');
                }
            });
            controls.appendChild(pinBtn);
        }

        table.appendChild(thead);
        table.appendChild(tbody);
        wrapper.appendChild(infoBar);
        wrapper.appendChild(controls);
        wrapper.appendChild(table);
        wrapper.appendChild(paginationBar);
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
        const blocks = Array.isArray(turn.response_blocks) ? [...turn.response_blocks] : [];
        if (blocks.length) {
            const suppressAuto = !!(turn && (turn.suppress_auto_blocks || turn.hide_user));
            const hasChartBlock = blocks.some((b) => b && b.type === 'chart' && b.plotly);
            if (!suppressAuto && !hasChartBlock && turn.plotly) {
                blocks.push({ type: 'chart', chart_type: turn.chart_intent && turn.chart_intent.chart_type, plotly: turn.plotly });
            }
            const nonStatusBlocks = [];
            let latestStatus = null;
            for (const block of blocks) {
                if (block && block.type === 'status') {
                    latestStatus = block;
                } else {
                    nonStatusBlocks.push(block);
                }
            }
            if (latestStatus) {
                nonStatusBlocks.push(latestStatus);
            }
            return nonStatusBlocks;
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
        if (turn.status === 'streaming') {
            fallback.push({ type: 'status', content: turn.assistant_text || 'Thinking' });
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
            const blocks = Array.isArray(turn.response_blocks) ? turn.response_blocks : [];
            const isFollowupBanner =
                blocks.length === 1
                && blocks[0]
                && blocks[0].type === 'text'
                && typeof blocks[0].content === 'string'
                && blocks[0].content.startsWith('↩ Follow-up started');
            if (!(turn.hide_user || isFollowupBanner)) {
                const userMsg = document.createElement('article');
                userMsg.className = 'message user';
                userMsg.innerHTML = `<div class="message-role">You</div><div class="message-text"></div>`;
                userMsg.querySelector('.message-text').textContent = turn.prompt || '';
                chatTimeline.appendChild(userMsg);
            }

            const assistantMsg = document.createElement('article');
            assistantMsg.className = 'message assistant';
            assistantMsg.innerHTML = `<div class="message-role">NIA</div><div class="message-body"></div>`;
            const body = assistantMsg.querySelector('.message-body');
            const normBlocks = normalizeBlocks(turn);

            for (const block of normBlocks) {
                if (block.type === 'text') {
                    const p = document.createElement('p');
                    p.className = 'message-text';
                    p.textContent = block.content || '';
                    body.appendChild(p);
                } else if (block.type === 'sql') {
                    body.appendChild(createCodeBlock(block.sql));
                } else if (block.type === 'table') {
                    body.appendChild(createTableBlock(block.columns || [], block.rows || [], block.meta || null, turn));
                } else if (block.type === 'chart' && block.plotly) {
                    body.appendChild(createChartBlock(block.plotly, block.chart_type, turn));
                } else if (block.type === 'status') {
                    const status = document.createElement('div');
                    status.className = 'message-status';
                    if (turn.status === 'streaming') {
                        status.classList.add('is-streaming');
                    }
                    const text = document.createElement('span');
                    text.className = 'message-status-text';
                    text.textContent = block.content || '';
                    status.appendChild(text);
                    if (turn.status === 'streaming') {
                        const dots = document.createElement('span');
                        dots.className = 'message-status-dots';
                        dots.textContent = '...';
                        status.appendChild(dots);
                    }
                    body.appendChild(status);
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
        setLoading(btnAnalyze, true, 'Send', 'Thinking');

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
                total_count: null,
                chart_intent: null,
                plotly: null,
                assistant_text: '',
                response_blocks: [{ type: 'status', content: 'Thinking' }],
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
            const withoutStatusBlocks = (blocks) => (
                Array.isArray(blocks) ? blocks.filter((b) => !(b && b.type === 'status')) : []
            );
            const stageLabelByName = {
                intent_classified: 'Thinking',
                reused_previous_result: 'Using previous query result',
                prompt_cache_hit: 'Reusing saved SQL; refreshing results from database',
                sql_generated: 'SQL generated. Running query',
                query_executed: 'Query complete. Table ready',
                chart_intent_ready: 'Analyzing chart intent',
                chart_intent_reused: 'Using previous chart intent',
                chart_reused: 'Using existing chart',
                chart_ready: 'Chart is ready',
                assistant_ready: 'Preparing final response',
            };

            const mapFinalTurn = (data) => ({
                id: data.turn_id,
                prompt: data.prompt,
                sql: data.sql,
                columns: Array.isArray(data.columns) ? data.columns : [],
                data: Array.isArray(data.data) ? data.data : [],
                total_count: data.total_count || null,
                chart_intent: data.chart_intent || null,
                plotly: data.plotly || null,
                assistant_text: data.assistant_text || '',
                response_blocks: Array.isArray(data.response_blocks) ? data.response_blocks : [],
                status: data.status || 'success',
                error: null,
                created_at: data.created_at || null,
            });

            elPrompt.value = '';
            await postFormStream('/analyze/stream', fields, (evt) => {
                if (!evt || typeof evt !== 'object') return;
                if (evt.type === 'meta' && evt.conversation_id) {
                    state.conversationId = evt.conversation_id;
                } else if (evt.type === 'stage') {
                    const stageText = stageLabelByName[evt.name] || `Processing: ${String(evt.name || 'working')}`;
                    upsertBlock('status', { type: 'status', content: stageText });
                    if (evt.name === 'sql_generated' && evt.sql) {
                        turn.sql = evt.sql;
                        upsertBlock('sql', { type: 'sql', sql: evt.sql });
                    } else if (evt.name === 'prompt_cache_hit' && evt.sql) {
                        turn.sql = evt.sql;
                        upsertBlock('sql', { type: 'sql', sql: evt.sql });
                    } else if (evt.name === 'query_executed' || evt.name === 'reused_previous_result') {
                        const cols = Array.isArray(evt.columns) ? evt.columns : [];
                        const rows = Array.isArray(evt.preview_rows) ? evt.preview_rows : [];
                        const totalCount = typeof evt.total_count === 'number' ? evt.total_count : (typeof evt.row_count === 'number' ? evt.row_count : rows.length);
                        if (evt.sql) turn.sql = evt.sql;
                        turn.columns = cols;
                        turn.data = rows;
                        turn.total_count = totalCount;
                        upsertBlock('table', {
                            type: 'table',
                            columns: cols,
                            rows,
                            meta: {
                                total_count: totalCount,
                                row_count: totalCount,
                                shown_rows: rows.length,
                                page: evt.page || 1,
                                page_size: evt.page_size || DEFAULT_PAGE_SIZE,
                                total_pages: Math.ceil(totalCount / (evt.page_size || DEFAULT_PAGE_SIZE)),
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
                        upsertBlock('status', { type: 'status', content: text || 'Preparing final response...' });
                    }
                    renderConversation();
                } else if (evt.type === 'final' && evt.data) {
                    const prevBlocks = Array.isArray(turn.response_blocks) ? [...turn.response_blocks] : [];
                    const prevColumns = Array.isArray(turn.columns) ? [...turn.columns] : [];
                    const prevRows = Array.isArray(turn.data) ? [...turn.data] : [];
                    const prevSql = turn.sql;
                    const prevTotalCount = turn.total_count;
                    const prevChartIntent = turn.chart_intent;
                    const prevPlotly = turn.plotly;
                    const prevAssistantText = turn.assistant_text;
                    const finalTurn = mapFinalTurn(evt.data);
                    const hasFinalBlocks = Array.isArray(finalTurn.response_blocks) && finalTurn.response_blocks.length > 0;
                    Object.assign(turn, finalTurn);
                    if (!hasFinalBlocks && prevBlocks.length) {
                        turn.response_blocks = withoutStatusBlocks(prevBlocks);
                    }
                    if (!turn.sql && prevSql) {
                        turn.sql = prevSql;
                    }
                    if ((!Array.isArray(turn.columns) || !turn.columns.length) && prevColumns.length) {
                        turn.columns = prevColumns;
                    }
                    if ((!Array.isArray(turn.data) || !turn.data.length) && prevRows.length) {
                        turn.data = prevRows;
                    }
                    if ((turn.total_count == null) && (prevTotalCount != null)) {
                        turn.total_count = prevTotalCount;
                    }
                    if (!turn.chart_intent && prevChartIntent) {
                        turn.chart_intent = prevChartIntent;
                    }
                    if (!turn.plotly && prevPlotly) {
                        turn.plotly = prevPlotly;
                    }
                    if (!turn.assistant_text && prevAssistantText) {
                        turn.assistant_text = prevAssistantText;
                    }
                    if (turn.status !== 'streaming' && Array.isArray(turn.response_blocks)) {
                        turn.response_blocks = withoutStatusBlocks(turn.response_blocks);
                    }
                    state.conversationId = evt.data.conversation_id || state.conversationId;
                    renderConversation();
                } else if (evt.type === 'error') {
                    turn.status = 'failed';
                    turn.error = (evt.detail && String(evt.detail)) || 'Something went wrong';
                    if (Array.isArray(turn.response_blocks)) {
                        turn.response_blocks = withoutStatusBlocks(turn.response_blocks);
                    }
                    if (!turn.response_blocks.length) {
                        turn.response_blocks.push({ type: 'text', content: 'Request failed.' });
                    }
                    renderConversation();
                    throw new Error(turn.error);
                }
            });
            await loadHistoryList();
        } catch (e) {
            setError(e && e.message ? e.message : 'Something went wrong');
            await loadHistoryList().catch(() => { });
        } finally {
            setLoading(btnAnalyze, false, 'Send', 'Thinking');
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

    async function seedFollowupFromUrl() {
        try {
            const params = new URLSearchParams(window.location.search || '');
            const followup = params.get('followup');
            const id = params.get('id');
            if (!followup || !id) return false;
            if (followup !== 'chart' && followup !== 'table') return false;

            setError(null);
            const resp = await postJson('/api/followup/seed', { type: followup, id });
            const convId = resp && resp.conversation_id;
            if (!convId) return false;

            const nextUrl = `${window.location.pathname}?conversation_id=${encodeURIComponent(convId)}`;
            window.history.replaceState({}, document.title, nextUrl);
            await loadConversation(convId);
            await loadHistoryList().catch(() => { });
            elPrompt && elPrompt.focus && elPrompt.focus();
            return true;
        } catch (err) {
            setError(err && err.message ? err.message : 'Failed to open follow-up');
            return false;
        }
    }

    async function loadConversationFromUrlIfPresent() {
        const params = new URLSearchParams(window.location.search || '');
        const convId = params.get('conversation_id');
        if (!convId) return false;
        await loadConversation(convId);
        return true;
    }

    // Initial state.
    setError(null);
    loadConversationFromUrlIfPresent()
        .then((loaded) => {
            if (loaded) return true;
            return seedFollowupFromUrl();
        })
        .then((handled) => {
            if (handled) return;
            return Promise.all([loadPinnedKeys(), loadPinnedTableKeys()])
                .then(() => renderConversation())
                .catch(() => renderConversation());
        })
        .catch(() => {
            Promise.all([loadPinnedKeys(), loadPinnedTableKeys()])
                .then(() => renderConversation())
                .catch(() => renderConversation());
        });
    loadHistoryList().catch(() => { });
})();

