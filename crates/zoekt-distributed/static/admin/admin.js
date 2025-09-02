document.addEventListener('DOMContentLoaded', function () {
    // Theme switching is now handled by /static/common/theme.js

    // Use common utilities
    const escapeHtml = window.ZoektCommon?.escapeHtml || function (s) { return String(s).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": "&#39;" }[c]; }); };
    const humanReadableBytes = window.ZoektCommon?.humanReadableBytes || function (bytes) {
        if (bytes < 0 || Number.isNaN(bytes)) return 'N/A';
        const KB = 1024;
        if (bytes < KB) return bytes + ' B';
        let v = bytes / KB;
        if (v < KB) return v.toFixed(1) + ' KB';
        v = v / KB;
        if (v < KB) return v.toFixed(1) + ' MB';
        v = v / KB;
        return v.toFixed(2) + ' GB';
    };

    // Helper function to format UTC timestamp to local time
    function formatTimestamp(utcTimestamp) {
        if (!utcTimestamp) return '';
        try {
            // Parse UTC timestamp (ISO format: 2025-09-02T14:44:00.000Z)
            const date = new Date(utcTimestamp);
            if (isNaN(date.getTime())) return utcTimestamp; // fallback to original if parsing fails
            return date.toLocaleString();
        } catch (e) {
            return utcTimestamp; // fallback to original on error
        }
    }
    const createForm = document.getElementById('create-form');
    const repoTableBody = document.getElementById('repo-table-body');
    const exportBtn = document.getElementById('export-csv');
    const tabReposBtn = document.getElementById('tab-repos');
    const tabBranchesBtn = document.getElementById('tab-branches');
    const reposTab = document.getElementById('repos-tab');
    const branchesTab = document.getElementById('branches-tab');
    const branchTableBody = document.getElementById('branch-table-body');
    const tabIndexersBtn = document.getElementById('tab-indexers');
    const indexersTab = document.getElementById('indexers-tab');
    const indexerTableBody = document.getElementById('indexer-table-body');
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

    function makeRow(name, url, csrf, freq, branches, branchDetails) {
        const tr = document.createElement('tr');
        // ensure a stable key on the row so client-side diffs can match server-rendered rows
        tr.dataset.name = name;
        // attach branch details JSON for later use by the expander
        if (branchDetails) tr.dataset.branchDetails = JSON.stringify(branchDetails);
        const branchesVal = (typeof branches === 'string') ? branches : '';
        // add an expander cell at the start
        // columns: name, url, branches, frequency, actions
        tr.innerHTML = `<td><span class="expander">▶</span>${escapeHtml(name)}</td><td>${escapeHtml(url)}</td><td>${escapeHtml(branchesVal)}</td><td>${escapeHtml(String(freq || ''))}</td><td><form class="delete-form" data-name="${escapeHtml(name)}"><input type="hidden" name="name" value="${escapeHtml(name)}"/><input type="hidden" name="csrf" value="${escapeHtml(csrf)}"/><button type="submit">Delete</button></form></td>`;
        return tr;
    }

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
                    const branches = formData.get('branches') || 'main';
                    // server response does not include branch details for a freshly created repo
                    repoTableBody.appendChild(makeRow(data.name, data.url, data.csrf, data.frequency, branches));
                    // reapply current sort if any
                    applyCurrentSort();
                }
                createForm.reset();
            }).catch(err => alert(err && err.message ? err.message : String(err)));
        });
    }

    const bulkImportForm = document.getElementById('bulk-import-form');
    if (bulkImportForm) {
        bulkImportForm.addEventListener('submit', function (e) {
            e.preventDefault();
            const formData = new FormData(bulkImportForm);
            fetch('/bulk-import', {
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
                // Try to parse JSON error body
                return r.json().then(j => {
                    let msg = 'Bulk import failed';
                    if (j && j.error) {
                        msg = `Bulk import failed: ${j.error}`;
                    } else if (j && j.details) {
                        msg = `Bulk import completed with issues:\n${j.details.join('\n')}`;
                    } else {
                        msg = `Bulk import failed: ${r.status} ${r.statusText}`;
                    }
                    throw new Error(msg);
                }).catch(() => {
                    throw new Error(`Bulk import failed: ${r.status} ${r.statusText}`);
                });
            }).then(data => {
                let msg = `Bulk import completed successfully!\n`;
                if (data.created && data.created.length > 0) {
                    msg += `Created: ${data.created.length} repositories\n`;
                }
                if (data.errors && data.errors.length > 0) {
                    msg += `Errors: ${data.errors.length} repositories failed\n`;
                    msg += `Details: ${data.errors.join(', ')}`;
                }
                alert(msg);
                // Refresh the table to show new repositories
                if (window.location) {
                    window.location.reload();
                }
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
        // No initial memory cell formatting needed: runtime columns moved to branch-details.
        // Add click handler for expand/collapse
        repoTableBody.addEventListener('click', function (e) {
            const exp = e.target.closest('.expander');
            if (!exp) return;
            const tr = exp.closest('tr');
            if (!tr) return;
            toggleBranchRow(tr, exp);
        });
        // when rows change, also refresh aggregate branch listing if branches tab visible
        const observer = new MutationObserver(() => {
            if (branchesTab && branchesTab.style.display !== 'none') refreshBranchesTable();
        });
        observer.observe(repoTableBody, { childList: true, subtree: false });
    }

    function toggleBranchRow(tr, expanderEl) {
        const next = tr.nextElementSibling;
        // if next is a branch-row for this repo, remove it (collapse)
        if (next && next.classList && next.classList.contains('branch-row')) {
            next.remove();
            expanderEl.textContent = '▶';
            return;
        }
        // otherwise build and insert the branch details row
        const raw = tr.dataset.branchDetails;
        let details = null;
        if (raw) {
            try { details = JSON.parse(raw); } catch (e) { details = null; }
        }
        const br = document.createElement('tr');
        br.className = 'branch-row';
        const td = document.createElement('td');
        td.colSpan = tr.children.length;
        if (!details || !Array.isArray(details) || details.length === 0) {
            td.innerHTML = '<em class="muted">No branch details available</em>';
        } else {
            // build a small table showing branch details
            const tbl = document.createElement('table');
            tbl.style.width = '100%';
            tbl.style.borderCollapse = 'collapse';
            const h = document.createElement('thead');
            h.innerHTML = '<tr><th>Branch</th><th>Last indexed</th><th class="numeric">Duration (ms)</th><th class="numeric">Memory</th><th>Leased node</th></tr>';
            tbl.appendChild(h);
            const bbody = document.createElement('tbody');
            details.forEach(d => {
                const r = document.createElement('tr');
                const mem = (d.memory_display) ? d.memory_display : (d.memory_bytes ? humanReadableBytes(Number(d.memory_bytes)) : '');
                const formattedTime = formatTimestamp(d.last_indexed);
                r.innerHTML = `<td>${escapeHtml(d.branch || '')}</td><td>${escapeHtml(formattedTime)}</td><td class="numeric">${escapeHtml(String(d.last_duration_ms || ''))}</td><td class="numeric">${escapeHtml(mem)}</td><td>${escapeHtml(d.leased_node || '')}</td>`;
                bbody.appendChild(r);
            });
            tbl.appendChild(bbody);
            td.appendChild(tbl);
        }
        br.appendChild(td);
        tr.parentNode.insertBefore(br, tr.nextSibling);
        expanderEl.textContent = '▼';
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

    // Branches tab handling: aggregate per-row branch-details into a flat table
    function refreshBranchesTable() {
        if (!branchTableBody || !repoTableBody) return;
        // gather branch-details from each repo row
        const branchRows = [];
        const repoRows = Array.from(repoTableBody.querySelectorAll('tr'));
        for (const r of repoRows) {
            const repoName = r.dataset && r.dataset.name ? r.dataset.name : (r.children[0] && r.children[0].textContent) || '';
            const raw = r.dataset && r.dataset.branchDetails ? r.dataset.branchDetails : null;
            if (!raw) continue;
            let details = null;
            try { details = JSON.parse(raw); } catch (e) { details = null; }
            if (!details || !Array.isArray(details)) continue;
            for (const d of details) {
                branchRows.push({ repo: repoName, branch: d.branch || '', last_indexed: d.last_indexed || '', last_duration_ms: d.last_duration_ms || '', memory_display: d.memory_display || (d.memory_bytes ? humanReadableBytes(Number(d.memory_bytes)) : ''), memory_bytes: d.memory_bytes || null, leased_node: d.leased_node || '' });
            }
        }
        // clear and repopulate
        branchTableBody.innerHTML = '';
        for (const br of branchRows) {
            const tr = document.createElement('tr');
            const formattedTime = formatTimestamp(br.last_indexed);
            tr.innerHTML = `<td>${escapeHtml(br.repo)}</td><td>${escapeHtml(br.branch)}</td><td>${escapeHtml(formattedTime)}</td><td class="numeric">${escapeHtml(String(br.last_duration_ms || ''))}</td><td class="numeric">${escapeHtml(br.memory_display || '')}</td><td>${escapeHtml(br.leased_node || '')}</td>`;
            // attach data-bytes on memory cell to help numeric sorting
            const memCell = tr.children[4];
            if (memCell && br.memory_bytes) memCell.setAttribute('data-bytes', String(br.memory_bytes));
            branchTableBody.appendChild(tr);
        }
        // apply saved sort state if the branches table header supports it
        if (window.applyCurrentSort) window.applyCurrentSort();
    }

    // Indexers tab handling: fetch and display indexer status
    function refreshIndexersTable() {
        if (!indexerTableBody) return;

        fetch('/api/indexers', { credentials: 'same-origin' })
            .then(r => r.json())
            .then(data => {
                // clear and repopulate
                indexerTableBody.innerHTML = '';
                for (const indexer of data) {
                    const tr = document.createElement('tr');

                    // Format last heartbeat
                    let heartbeatDisplay = 'Never';
                    if (indexer.last_heartbeat) {
                        heartbeatDisplay = new Date(indexer.last_heartbeat).toLocaleString();
                    }

                    // Status styling
                    let statusClass = '';
                    let statusText = indexer.status;
                    if (indexer.status === 'online') {
                        statusClass = 'style="color: #10b981;"'; // green
                    } else if (indexer.status === 'stale') {
                        statusClass = 'style="color: #f59e0b;"'; // amber
                    } else if (indexer.status === 'offline') {
                        statusClass = 'style="color: #ef4444;"'; // red
                    }

                    // Get CSRF token from the create form
                    const csrfToken = document.querySelector('input[name="csrf"]').value || '';

                    tr.innerHTML = `<td>${escapeHtml(indexer.node_id)}</td><td>${escapeHtml(indexer.endpoint)}</td><td>${escapeHtml(heartbeatDisplay)}</td><td ${statusClass}>${escapeHtml(statusText)}</td><td><form class="delete-indexer-form" data-node-id="${escapeHtml(indexer.node_id)}"><input type="hidden" name="node_id" value="${escapeHtml(indexer.node_id)}"/><input type="hidden" name="csrf" value="${escapeHtml(csrfToken)}"/><button type="submit" style="background: var(--danger); color: white; border: none; padding: 4px 8px; border-radius: 4px; cursor: pointer;">Delete</button></form></td>`;
                    indexerTableBody.appendChild(tr);
                }
                // apply saved sort state if the indexers table header supports it
                if (window.applyCurrentSort) window.applyCurrentSort();
            })
            .catch(e => {
                console.warn('Failed to fetch indexers:', e);
                indexerTableBody.innerHTML = '<tr><td colspan="5" style="text-align: center; color: var(--muted);">Failed to load indexer data</td></tr>';
            });
    }

    // Add event listener for delete indexer forms
    if (indexerTableBody) {
        indexerTableBody.addEventListener('submit', function (e) {
            const f = e.target;
            if (f && f.classList && f.classList.contains('delete-indexer-form')) {
                e.preventDefault();
                const nodeId = f.dataset.nodeId || '';
                if (!confirm(`Delete indexer '${nodeId}'? This will also release all leases held by this indexer.`)) return;
                const formData = new URLSearchParams(new FormData(f));
                fetch('/delete-indexer', {
                    method: 'POST',
                    body: formData,
                    credentials: 'same-origin',
                    headers: {
                        'X-Requested-With': 'XMLHttpRequest',
                        'Accept': 'application/json'
                    }
                }).then(r => {
                    if (r.ok) return r.json();
                    // Try to parse JSON error body
                    return r.json().then(j => {
                        let msg = 'Delete indexer failed';
                        if (j && j.error) {
                            msg = `Delete indexer failed: ${j.error}`;
                        } else {
                            msg = `Delete indexer failed: ${r.status} ${r.statusText}`;
                        }
                        throw new Error(msg);
                    }).catch(() => {
                        throw new Error(`Delete indexer failed: ${r.status} ${r.statusText}`);
                    });
                }).then(data => {
                    // remove row
                    const row = f.closest('tr'); if (row) { row.remove(); }
                    // Show success message
                    let msg = `Indexer '${nodeId}' deleted successfully!`;
                    if (data.leases_released && data.leases_released > 0) {
                        msg += ` ${data.leases_released} lease(s) were released.`;
                    }
                    alert(msg);
                }).catch(err => alert(err && err.message ? err.message : String(err)));
            }
        });
    }

    // tab switching
    if (tabReposBtn && tabBranchesBtn && tabIndexersBtn && reposTab && branchesTab && indexersTab) {
        tabReposBtn.addEventListener('click', () => {
            reposTab.style.display = '';
            branchesTab.style.display = 'none';
            indexersTab.style.display = 'none';
            tabReposBtn.setAttribute('aria-pressed', 'true');
            tabBranchesBtn.setAttribute('aria-pressed', 'false');
            tabIndexersBtn.setAttribute('aria-pressed', 'false');
            // visual tab styling
            reposTab.classList.add('tabbed');
            branchesTab.classList.remove('tabbed');
            indexersTab.classList.remove('tabbed');
            // show add/import cards
            showAddImport(true);
        });
        tabBranchesBtn.addEventListener('click', () => {
            reposTab.style.display = 'none';
            branchesTab.style.display = '';
            indexersTab.style.display = 'none';
            tabReposBtn.setAttribute('aria-pressed', 'false');
            tabBranchesBtn.setAttribute('aria-pressed', 'true');
            tabIndexersBtn.setAttribute('aria-pressed', 'false');
            // visual tab styling
            reposTab.classList.remove('tabbed');
            branchesTab.classList.add('tabbed');
            indexersTab.classList.remove('tabbed');
            // hide add/import cards when viewing branches
            showAddImport(false);
            refreshBranchesTable();
        });
        tabIndexersBtn.addEventListener('click', () => {
            reposTab.style.display = 'none';
            branchesTab.style.display = 'none';
            indexersTab.style.display = '';
            tabReposBtn.setAttribute('aria-pressed', 'false');
            tabBranchesBtn.setAttribute('aria-pressed', 'false');
            tabIndexersBtn.setAttribute('aria-pressed', 'true');
            // visual tab styling
            reposTab.classList.remove('tabbed');
            branchesTab.classList.remove('tabbed');
            indexersTab.classList.add('tabbed');
            // hide add/import cards when viewing indexers
            showAddImport(false);
            refreshIndexersTable();
        });
    }

    function showAddImport(show) {
        // Add repository and Bulk import cards are the two cards after reposTab/branchesTab/indexersTab
        // Find all .card elements under container and toggle their visibility appropriately.
        const cards = Array.from(document.querySelectorAll('.container > .card'));
        // We expect the layout: [reposTab card (index 0), branchesTab card (index 1), indexersTab card (index 2), add card (index 3), spacer, bulk card (index 5)]
        // Rather than rely strictly on indexes, find by header text
        for (const c of cards) {
            const h = c.querySelector('h2');
            if (!h) continue;
            const txt = (h.textContent || '').trim().toLowerCase();
            if (txt === 'add repository' || txt === 'bulk import from csv') {
                c.style.display = show ? '' : 'none';
            }
        }
    }

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
                // cells: 0=name,1=url,2=branches,3=freq,4=actions
                const urlCell = tr.children[1];
                const branchesCell = tr.children[2];
                const freqCell = tr.children[3];

                if (urlCell && row.url !== undefined && urlCell.textContent !== String(row.url)) urlCell.textContent = String(row.url);
                if (branchesCell) {
                    const want = row.branches == null ? '' : String(row.branches);
                    if (branchesCell.textContent !== want) branchesCell.textContent = want;
                }
                if (freqCell) {
                    const want = row.frequency == null ? '' : String(row.frequency);
                    if (freqCell.textContent !== want) freqCell.textContent = want;
                }
                // update branch-details dataset on the row so the expander shows fresh data
                if (row.branch_details) {
                    try { tr.dataset.branchDetails = JSON.stringify(row.branch_details); } catch (e) { /* ignore */ }
                } else {
                    // remove attribute when no details
                    if (tr.dataset && tr.dataset.branchDetails) delete tr.dataset.branchDetails;
                }
            }

            // add any new rows at the end
            for (const [name, row] of incoming) {
                if (seen.has(name)) continue;
                const newRow = makeRow(row.name, row.url, document.querySelector('input[name="csrf"]').value || '', row.frequency, row.branches, row.branch_details);
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

                // Also refresh indexers table if it's visible
                if (indexersTab && indexersTab.style.display !== 'none') {
                    refreshIndexersTable();
                }
            } catch (e) { console.warn('poll error', e); }
        }
        // initial fetch + interval
        fetchAndRefresh();
        setInterval(fetchAndRefresh, POLL_MS);
    })();
});
