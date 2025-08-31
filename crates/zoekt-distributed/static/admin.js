document.addEventListener('DOMContentLoaded', function () {
    // Theme: default to dark, allow user to switch to light
    const THEME_KEY = 'dzr_theme';
    function applyTheme(theme) {
        if (theme === 'light') document.documentElement.classList.add('light-mode');
        else document.documentElement.classList.remove('light-mode');
    }
    const saved = localStorage.getItem(THEME_KEY) || 'dark';
    applyTheme(saved);
    // wire up all theme toggle buttons (nav + login)
    // Show the target theme (what will happen when the user clicks)
    function setToggleAppearance(btn, theme) {
        const icon = btn.querySelector('.icon');
        const label = btn.querySelector('.label');
        if (!icon || !label) return;
        if (theme === 'light') {
            // currently light -> clicking will switch to dark (target)
            icon.textContent = '🌙';
            label.textContent = 'Dark mode';
        } else {
            // currently dark -> clicking will switch to light (target)
            icon.textContent = '☀️';
            label.textContent = 'Light mode';
        }
    }
    const toggles = Array.from(document.querySelectorAll('.theme-toggle'));
    toggles.forEach(btn => {
        // ensure markup
        if (!btn.querySelector('.icon')) {
            const ic = document.createElement('span'); ic.className = 'icon'; ic.textContent = '🌙';
            const lb = document.createElement('span'); lb.className = 'label'; lb.textContent = 'Dark';
            btn.textContent = ''; btn.appendChild(ic); btn.appendChild(document.createTextNode(' ')); btn.appendChild(lb);
        }
        setToggleAppearance(btn, saved);
        btn.addEventListener('click', function () {
            const now = document.documentElement.classList.contains('light-mode') ? 'dark' : 'light';
            applyTheme(now);
            localStorage.setItem(THEME_KEY, now);
            toggles.forEach(t => setToggleAppearance(t, now));
            // small visual pulse
            document.documentElement.animate([{ opacity: 0.98 }, { opacity: 1 }], { duration: 220, easing: 'ease' });
        });
    });
    const createForm = document.getElementById('create-form');
    const repoTableBody = document.getElementById('repo-table-body');
    const exportBtn = document.getElementById('export-csv');
    // remember current sort state so dynamic updates can reapply it
    const TABLE_SORT_KEY = 'dzr_table_sort';
    // Load saved sort state from localStorage if present
    let tableSortState = { idx: -1, asc: true };
    try {
        const savedSort = localStorage.getItem(TABLE_SORT_KEY);
        if (savedSort) {
            const p = JSON.parse(savedSort);
            if (p && typeof p.idx === 'number' && typeof p.asc === 'boolean') tableSortState = p;
        }
    } catch (e) { /* ignore parse errors */ }

    function makeRow(name, url, csrf, freq, lastIndexed, lastDurMs, leased) {
        const tr = document.createElement('tr');
        // ensure a stable key on the row so client-side diffs can match server-rendered rows
        tr.dataset.name = name;
        // lastDurMs maps to fifth column, memory (formatted) will be sixth, leased seventh
        const memRaw = (lastDurMs && typeof lastDurMs === 'object' && lastDurMs.memory_bytes !== undefined) ? String(lastDurMs.memory_bytes) : '';
        const memDisplay = (lastDurMs && typeof lastDurMs === 'object' && lastDurMs.memory_display) ? String(lastDurMs.memory_display) : '';
        // Fallback when server supplied scalar values for last_duration or memory
        const lastDurVal = (typeof lastDurMs === 'number' || typeof lastDurMs === 'string') ? lastDurMs : '';
        tr.innerHTML = `<td>${escapeHtml(name)}</td><td>${escapeHtml(url)}</td><td>${escapeHtml(String(freq || ''))}</td><td>${escapeHtml(String(lastIndexed || ''))}</td><td>${escapeHtml(String(lastDurVal || ''))}</td><td class="numeric memory" data-bytes="${escapeHtml(memRaw)}" title="${escapeHtml(memRaw)}">${escapeHtml(memDisplay || '')}</td><td>${escapeHtml(String(leased || ''))}</td><td><form class="delete-form" data-name="${escapeHtml(name)}"><input type="hidden" name="name" value="${escapeHtml(name)}"/><input type="hidden" name="csrf" value="${escapeHtml(csrf)}"/><button type="submit">Delete</button></form></td>`;
        return tr;
    }

    function escapeHtml(s) { return String(s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": "&#39;" }[c]; }); }

    if (createForm) {
        createForm.addEventListener('submit', function (e) {
            e.preventDefault();
            const formData = new URLSearchParams(new FormData(createForm));
            fetch('/create', {
                method: 'POST',
                body: formData,
                credentials: 'same-origin',
                headers: {
                    'X-Requested-With': 'XMLHttpRequest',
                    'Accept': 'application/json'
                }
            }).then(r => {
                if (r.ok) {
                    return r.json();
                }
                // Try to parse JSON error body to give a clearer message to the user
                return r.json().then(j => {
                    let msg = 'Create failed';
                    if (j && j.error === 'conflict' && j.reason) {
                        if (j.reason === 'name_exists') msg = `Create failed: name already exists (${j.name || ''})`;
                        else if (j.reason === 'url_exists') msg = `Create failed: url already exists (${j.url || ''})`;
                        else msg = `Create failed: ${j.reason}`;
                    } else if (j && j.error) {
                        msg = `Create failed: ${j.error}`;
                    } else {
                        msg = `Create failed: ${r.status} ${r.statusText}`;
                    }
                    throw new Error(msg);
                }).catch(() => {
                    // Non-JSON or parse error
                    throw new Error(`Create failed: ${r.status} ${r.statusText}`);
                });
            }).then(data => {
                // append row (guard in case table body missing)
                if (repoTableBody) {
                    repoTableBody.appendChild(makeRow(data.name, data.url, data.csrf, data.frequency, data.last_indexed, data.last_duration_ms, data.leased_node));
                    // reapply current sort if any
                    applyCurrentSort();
                }
                createForm.reset();
            }).catch(err => alert(err && err.message ? err.message : String(err)));
        });
    }

    if (exportBtn) {
        exportBtn.addEventListener('click', function () {
            // Fetch CSV using same-origin credentials; browser will include cookies for session or basic auth header will be used by the browser when set.
            fetch('/export.csv', { credentials: 'same-origin' }).then(async r => {
                if (!r.ok) {
                    // try to read JSON error body
                    try {
                        const j = await r.json();
                        alert(j && j.error ? `Export failed: ${j.error}` : `Export failed: ${r.status}`);
                    } catch (e) {
                        alert(`Export failed: ${r.status} ${r.statusText}`);
                    }
                    return;
                }
                const blob = await r.blob();
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                // prefer server-provided filename via Content-Disposition if possible; fallback otherwise
                let filename = 'zoekt_repos.csv';
                const cd = r.headers.get('content-disposition');
                if (cd) {
                    const m = /filename=(?:"([^"]+)"|([^;\n\r]+))/i.exec(cd);
                    if (m) filename = m[1] || m[2];
                }
                a.download = filename;
                document.body.appendChild(a);
                a.click();
                a.remove();
                URL.revokeObjectURL(url);
            }).catch(e => {
                alert(`Export failed: ${e && e.message ? e.message : String(e)}`);
            });
        });
    }

    if (repoTableBody) {
        repoTableBody.addEventListener('submit', function (e) {
            const f = e.target;
            if (f && f.classList && f.classList.contains('delete-form')) {
                e.preventDefault();
                if (!confirm('Delete repo ' + (f.dataset.name || '') + '?')) return;
                const formData = new URLSearchParams(new FormData(f));
                fetch('/delete', {
                    method: 'POST',
                    body: formData,
                    credentials: 'same-origin',
                    headers: {
                        'X-Requested-With': 'XMLHttpRequest',
                        'Accept': 'application/json'
                    }
                }).then(r => {
                    if (r.ok) return r.json();
                    throw new Error('delete failed');
                }).then(data => {
                    // remove row
                    const row = f.closest('tr'); if (row) { row.remove(); applyCurrentSort(); }
                }).catch(err => alert(err));
            }
        });
        // After initial page load, format any existing memory cells (server-rendered)
        (function formatMemoryCells() {
            const cells = Array.from(document.querySelectorAll('td.memory'));
            cells.forEach(c => {
                const raw = c.getAttribute('data-bytes');
                if (raw && raw !== '') {
                    const n = Number(raw);
                    if (!Number.isNaN(n)) {
                        c.textContent = humanReadableBytes(n);
                        c.setAttribute('title', raw);
                    }
                }
            });
        })();
    }

    // Client-side human readable bytes formatting (matching server)
    function humanReadableBytes(bytes) {
        if (bytes < 0 || Number.isNaN(bytes)) return 'N/A';
        const KB = 1024;
        if (bytes < KB) return bytes + ' B';
        let v = bytes / KB;
        if (v < KB) return v.toFixed(1) + ' KB';
        v = v / KB;
        if (v < KB) return v.toFixed(1) + ' MB';
        v = v / KB;
        return v.toFixed(2) + ' GB';
    }
    // Table sorting: add click handlers to sortable headers to sort tbody rows
    (function enableTableSorting() {
        const table = document.querySelector('table');
        if (!table) return;
        const thead = table.tHead;
        const tbody = table.tBodies && table.tBodies[0];
        if (!thead || !tbody) return;
        const headerCells = Array.from(thead.querySelectorAll('th'));

        function sortTable(colIndex, asc) {
            const type = headerCells[colIndex] && headerCells[colIndex].getAttribute('data-type') || 'text';
            const rows = Array.from(tbody.querySelectorAll('tr'));
            rows.sort((a, b) => {
                const aCell = a.children[colIndex];
                const bCell = b.children[colIndex];
                if (!aCell || !bCell) return 0;
                if (aCell.classList.contains('memory') || bCell.classList.contains('memory')) {
                    const av = Number(aCell.getAttribute('data-bytes') || aCell.textContent || '');
                    const bv = Number(bCell.getAttribute('data-bytes') || bCell.textContent || '');
                    if (isNaN(av) && isNaN(bv)) return 0;
                    if (isNaN(av)) return -1;
                    if (isNaN(bv)) return 1;
                    return av - bv;
                }
                if (type === 'number') {
                    const av = Number(aCell.textContent.replace(/[^0-9.-]/g, '') || '');
                    const bv = Number(bCell.textContent.replace(/[^0-9.-]/g, '') || '');
                    return (isNaN(av) ? 0 : av) - (isNaN(bv) ? 0 : bv);
                }
                if (type === 'date') {
                    const av = Date.parse(aCell.textContent) || 0;
                    const bv = Date.parse(bCell.textContent) || 0;
                    return av - bv;
                }
                return aCell.textContent.localeCompare(bCell.textContent);
            });
            if (!asc) rows.reverse();
            rows.forEach(r => tbody.appendChild(r));
            // update indicators
            headerCells.forEach(h => {
                const ind = h.querySelector('.sort-indicator');
                if (ind) ind.textContent = '';
            });
            const myInd = headerCells[colIndex] && headerCells[colIndex].querySelector('.sort-indicator');
            if (myInd) myInd.textContent = asc ? '▲' : '▼';
        }

        headerCells.forEach((th, colIndex) => {
            if (!th.classList.contains('sortable')) return;
            th.addEventListener('click', () => {
                if (tableSortState.idx === colIndex) tableSortState.asc = !tableSortState.asc;
                else { tableSortState.idx = colIndex; tableSortState.asc = true; }
                // persist user's chosen sort
                try { localStorage.setItem(TABLE_SORT_KEY, JSON.stringify(tableSortState)); } catch (e) { }
                sortTable(tableSortState.idx, tableSortState.asc);
            });
        });

        // expose helper to reapply current sort after dynamic updates
        window.applyCurrentSort = function () {
            if (tableSortState.idx >= 0) sortTable(tableSortState.idx, tableSortState.asc);
        };
        // if a saved sort exists, apply it now so indicators are correct on load
        if (tableSortState.idx >= 0) {
            // ensure idx is within bounds
            if (tableSortState.idx < headerCells.length) sortTable(tableSortState.idx, tableSortState.asc);
            else tableSortState = { idx: -1, asc: true };
        }
    })();

    // Polling: fetch /api/repos every 10s and refresh table
    (function enablePolling() {
        const POLL_MS = 10000;
        // delta-update the table: add new rows, update changed rows, remove deleted rows
        // Compare two table rows according to current tableSortState and header data-types.
        function compareRowsForSort(a, b) {
            const table = document.querySelector('table');
            const thead = table && table.tHead;
            const headerCells = thead ? Array.from(thead.querySelectorAll('th')) : [];
            const colIndex = tableSortState.idx;
            const asc = tableSortState.asc;
            if (colIndex < 0 || !headerCells[colIndex]) {
                // default: compare by name (col 0)
                const an = (a.dataset && a.dataset.name) ? a.dataset.name : (a.children[0] && a.children[0].textContent) || '';
                const bn = (b.dataset && b.dataset.name) ? b.dataset.name : (b.children[0] && b.children[0].textContent) || '';
                return an.localeCompare(bn) * (asc ? 1 : -1);
            }
            const type = headerCells[colIndex] && headerCells[colIndex].getAttribute('data-type') || 'text';
            const aCell = a.children[colIndex];
            const bCell = b.children[colIndex];
            if (!aCell || !bCell) return 0;
            let cmp = 0;
            if (aCell.classList.contains('memory') || bCell.classList.contains('memory')) {
                const av = Number(aCell.getAttribute('data-bytes') || aCell.textContent || '');
                const bv = Number(bCell.getAttribute('data-bytes') || bCell.textContent || '');
                if (isNaN(av) && isNaN(bv)) cmp = 0;
                else if (isNaN(av)) cmp = -1;
                else if (isNaN(bv)) cmp = 1;
                else cmp = av - bv;
            } else if (type === 'number') {
                const av = Number(aCell.textContent.replace(/[^0-9.-]/g, '') || '');
                const bv = Number(bCell.textContent.replace(/[^0-9.-]/g, '') || '');
                cmp = (isNaN(av) ? 0 : av) - (isNaN(bv) ? 0 : bv);
            } else if (type === 'date') {
                const av = Date.parse(aCell.textContent) || 0;
                const bv = Date.parse(bCell.textContent) || 0;
                cmp = av - bv;
            } else {
                cmp = aCell.textContent.localeCompare(bCell.textContent);
            }
            return cmp * (asc ? 1 : -1);
        }

        function updateTableFromData(data) {
            const tbody = document.getElementById('repo-table-body');
            if (!tbody) return;
            // build a map of incoming rows by name for quick lookup
            const incoming = new Map();
            data.forEach(r => incoming.set(r.name, r));

            // existing rows map by repo name (assume first td contains name)
            const existingRows = Array.from(tbody.querySelectorAll('tr'));
            const seen = new Set();
            for (const tr of existingRows) {
                const nameCell = tr.children[0];
                if (!nameCell) continue;
                // prefer a dataset key when available (server emits data-name on its rows)
                const name = (tr.dataset && tr.dataset.name) ? tr.dataset.name : nameCell.textContent.trim();
                if (!incoming.has(name)) {
                    // removed on server
                    tr.remove();
                    continue;
                }
                // update fields if changed
                const row = incoming.get(name);
                seen.add(name);
                // cells: 0=name,1=url,2=freq,3=lastIndexed,4=lastDuration,5=memory,6=leased,7=actions
                const urlCell = tr.children[1];
                const freqCell = tr.children[2];
                const lastIndexedCell = tr.children[3];
                const lastDurCell = tr.children[4];
                const memCell = tr.children[5];
                const leasedCell = tr.children[6];

                if (urlCell && row.url !== undefined && urlCell.textContent !== String(row.url)) urlCell.textContent = String(row.url);
                if (freqCell) {
                    const want = row.frequency == null ? '' : String(row.frequency);
                    if (freqCell.textContent !== want) freqCell.textContent = want;
                }
                if (lastIndexedCell) {
                    const want = row.last_indexed == null ? '' : String(row.last_indexed);
                    if (lastIndexedCell.textContent !== want) lastIndexedCell.textContent = want;
                }
                if (lastDurCell) {
                    const want = row.last_duration_ms == null ? '' : String(row.last_duration_ms);
                    if (lastDurCell.textContent !== want) lastDurCell.textContent = want;
                }
                if (memCell) {
                    const raw = row.memory_bytes == null ? '' : String(row.memory_bytes);
                    const display = row.memory_display == null ? (row.memory_bytes == null ? '' : humanReadableBytes(Number(row.memory_bytes))) : String(row.memory_display);
                    if (memCell.getAttribute('data-bytes') !== raw) memCell.setAttribute('data-bytes', raw);
                    if (memCell.textContent !== display) memCell.textContent = display;
                    if (raw) memCell.setAttribute('title', raw); else memCell.removeAttribute('title');
                }
                if (leasedCell) {
                    const want = row.leased_node == null ? '' : String(row.leased_node);
                    if (leasedCell.textContent !== want) leasedCell.textContent = want;
                }
            }

            // add any new rows at the end
            for (const [name, row] of incoming) {
                if (seen.has(name)) continue;
                const memObj = { memory_bytes: row.memory_bytes, memory_display: row.memory_display };
                const newRow = makeRow(row.name, row.url, document.querySelector('input[name="csrf"]').value || '', row.frequency, row.last_indexed, memObj, row.leased_node);
                // if a sort is active, insert in sorted position; otherwise append
                if (tableSortState.idx >= 0) {
                    // find first existing row that should come after newRow
                    const existingRows = Array.from(tbody.querySelectorAll('tr'));
                    let inserted = false;
                    for (const ex of existingRows) {
                        // skip rows that were removed in this pass
                        if (!ex.parentNode) continue;
                        // compare newRow vs ex
                        const cmp = compareRowsForSort(newRow, ex);
                        if (cmp < 0) {
                            tbody.insertBefore(newRow, ex);
                            inserted = true;
                            break;
                        }
                    }
                    if (!inserted) tbody.appendChild(newRow);
                } else {
                    tbody.appendChild(newRow);
                }
            }
        }

        async function fetchAndRefresh() {
            try {
                const r = await fetch('/api/repos', { headers: { Accept: 'application/json' }, credentials: 'same-origin' });
                if (!r.ok) return;
                const data = await r.json();
                updateTableFromData(data);
                // keep user's current sort
                if (window.applyCurrentSort) window.applyCurrentSort();
            } catch (e) { console.warn('poll error', e); }
        }
        // initial fetch + interval
        fetchAndRefresh();
        setInterval(fetchAndRefresh, POLL_MS);
    })();
});
