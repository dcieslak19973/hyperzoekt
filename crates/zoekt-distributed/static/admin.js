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

    function makeRow(name, url, csrf) {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${escapeHtml(name)}</td><td>${escapeHtml(url)}</td><td><form class="delete-form" data-name="${escapeHtml(name)}"><input type="hidden" name="name" value="${escapeHtml(name)}"/><input type="hidden" name="csrf" value="${escapeHtml(csrf)}"/><button type="submit">Delete</button></form></td>`;
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
                throw new Error('create failed');
            }).then(data => {
                // append row (guard in case table body missing)
                if (repoTableBody) repoTableBody.appendChild(makeRow(data.name, data.url, data.csrf));
                createForm.reset();
            }).catch(err => alert(err));
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
                    const row = f.closest('tr'); if (row) row.remove();
                }).catch(err => alert(err));
            }
        });
    }
});
