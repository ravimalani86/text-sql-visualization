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
            setTimeout(() => {
                window.dispatchEvent(new Event("resize"))
            }, 120)
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

    let grid

    function render() {
        if (grid) {
            grid.removeAll()
        } else {
            dashboardGrid.innerHTML = ""
        }

        if (!grid) {
            grid = GridStack.init({
                column: 12,
                cellHeight: 90,
                margin: 12,
                animate: true,
                float: false,
                minRow: 1,
                draggable: {
                    handle: ".chart-card-header"
                },
                resizable: {
                    handles: "se"
                }
            }, "#dashboardGrid")

            // FIX: event handler grid init pachhi
            grid.on("change", async function (event, items) {
                items.forEach(item => {
                    const chart = state.charts[item.y]
                    if (!chart) return

                    chart.width_units = item.w
                    chart.height_px = item.h * 80
                })

                await persistOrder()
            })
            grid.on("resize", function (event, el) {
                const chart = el.querySelector("plotly-chart")
                if (!chart) return

                chart.style.width = "100%"
                chart.style.height = "100%"

            })

            grid.on("resizestop", function (event, el) {
                const chart = el.querySelector("plotly-chart")
                if (!chart) return

                setTimeout(() => {
                    window.dispatchEvent(new Event("resize"))
                }, 60)

            })
        }

        if (!state.charts.length) {
            const empty = document.createElement("div")
            empty.className = "empty"
            empty.textContent = "No pinned charts found."
            dashboardGrid.appendChild(empty)
            return
        }

        state.charts.forEach((chart, index) => {

            const card = document.createElement("div")

            card.innerHTML = `
            <div class="grid-stack-item-content">
                <div class="chart-card-header">
                    <div>
                        <h3 class="chart-title">${chart.title || "Chart"}</h3>
                        <p class="chart-meta">${chart.chart_type || ""}</p>
                    </div>
                    <div class="chart-actions">
                        <button class="icon-action-btn btn-refresh">↻</button>
                        <button class="icon-action-btn btn-fullscreen" title="Fullscreen">⛶</button>
                        <button class="icon-action-btn btn-unpin">✕</button>
                    </div>
                </div>
                <div class="chart-box"></div>
            </div>`

            const node = {
                w: 4,
                h: 4
            }

            grid.addWidget(card, node)

            const chartBox = card.querySelector(".chart-box")
            const chartEl = document.createElement("plotly-chart")
            chartEl.style.width = "100%"
            chartEl.style.height = "100%"
            chartBox.appendChild(chartEl)

            refreshChart(card, chart)
            card.querySelector(".chart-box").ondblclick = () => {
                const chartElement = card.querySelector("plotly-chart")
                if (!chartElement) return

                openFullscreenChart(chartElement.config)
            }
            card.querySelector(".btn-fullscreen").onclick = () => {
                const chartElement = card.querySelector("plotly-chart")
                if (!chartElement) return

                openFullscreenChart(chartElement.config)
            }
            card.querySelector(".btn-refresh").onclick = () => {
                refreshChart(card, chart)
            }
            card.querySelector(".btn-unpin").onclick = async () => {
                await requestJson(`/api/charts/${chart.id}`, "DELETE")
                state.charts = state.charts.filter(c => c.id !== chart.id)
                render()
                setSummary()
            }
        })
        setTimeout(() => {
            if (!grid) return
            grid.engine.nodes.forEach(n => {
                if (!n.el) return

                const chart = n.el.querySelector("plotly-chart")
                if (chart) {
                    window.dispatchEvent(new Event("resize"))
                }
            })
        }, 400)
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

    function openFullscreenChart(chartConfig) {

        const overlay = document.createElement("div")
        overlay.className = "chart-fullscreen"

        const content = document.createElement("div")
        content.className = "chart-fullscreen-content"

        const closeBtn = document.createElement("button")
        closeBtn.className = "chart-fullscreen-close"
        closeBtn.innerText = "✕"

        const chart = document.createElement("plotly-chart")
        chart.config = chartConfig

        content.appendChild(closeBtn)
        content.appendChild(chart)
        overlay.appendChild(content)

        document.body.appendChild(overlay)

        closeBtn.onclick = () => overlay.remove()
        overlay.onclick = (e) => {
            if (e.target === overlay) overlay.remove()
        }

        setTimeout(() => {
            window.dispatchEvent(new Event("resize"))
        }, 100)
    }

    async function loadCharts() {
        const data = await getJson('/api/charts');
        state.charts = Array.isArray(data.items) ? data.items : [];
        render();
        setSummary();
        setTimeout(() => {
            window.dispatchEvent(new Event("resize"))
        }, 300)
        await refreshRenderedCards();
    }

    btnRefreshAll.addEventListener('click', () => {
        loadCharts()
            .then(() => {
                setTimeout(() => {
                    if (grid) {
                        grid.compact();
                    }
                    window.dispatchEvent(new Event("resize"));
                }, 100);
            })
            .catch((err) => {
                setError(err && err.message ? err.message : 'Failed to refresh dashboard');
            });
    });

    setError(null);

    loadCharts()
        .then(() => {
            setTimeout(() => {
                if (grid) {
                    grid.compact();
                }
                window.dispatchEvent(new Event("resize"));
            }, 100);
        })
        .catch((err) => {
            setError(err && err.message ? err.message : 'Failed to load dashboard');
        });

})();
