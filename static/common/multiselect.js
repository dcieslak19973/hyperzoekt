// Lightweight tag-like searchable multiselect shared by web UI pages
export function createMultiselect({ inputId, tagsId, optionsContainerId, storageKey, apiPath = '/api/repos' }) {
    const input = document.getElementById(inputId);
    const tags = document.getElementById(tagsId);
    const optsBox = document.getElementById(optionsContainerId);
    let all = [];
    let selected = new Set(JSON.parse(localStorage.getItem(storageKey) || '[]'));

    function renderTags() {
        tags.innerHTML = '';
        for (const v of selected) {
            const t = document.createElement('span');
            t.textContent = v;
            t.style.padding = '4px 8px';
            t.style.background = 'var(--accent)';
            t.style.color = 'white';
            t.style.borderRadius = '12px';
            t.style.fontSize = '0.9rem';
            t.style.cursor = 'default';
            const x = document.createElement('button');
            x.textContent = '×';
            x.style.marginLeft = '6px';
            x.style.background = 'transparent';
            x.style.border = 'none';
            x.style.color = 'white';
            x.style.cursor = 'pointer';
            x.addEventListener('click', () => { selected.delete(v); renderTags(); persist(); });
            t.appendChild(x);
            tags.appendChild(t);
        }
    }

    function persist() {
        localStorage.setItem(storageKey, JSON.stringify(Array.from(selected)));
    }

    function showOptions(filter) {
        optsBox.innerHTML = '';
        const filtered = all.filter(r => r.name.toLowerCase().includes(filter.toLowerCase()));
        filtered.forEach(r => {
            const row = document.createElement('div');
            row.textContent = r.name;
            row.style.padding = '6px 8px';
            row.style.cursor = 'pointer';
            row.addEventListener('click', () => { selected.add(r.name); renderTags(); persist(); optsBox.style.display = 'none'; input.value = ''; });
            optsBox.appendChild(row);
        });
        optsBox.style.display = filtered.length ? 'block' : 'none';
    }

    input.addEventListener('input', (e) => { showOptions(e.target.value); });
    input.addEventListener('focus', () => { showOptions(input.value || ''); });
    document.addEventListener('click', (e) => { if (!optsBox.contains(e.target) && e.target !== input) optsBox.style.display = 'none'; });

    async function loadAll() {
        try {
            const res = await fetch(apiPath);
            all = await res.json();
            // pre-select any stored
            const stored = JSON.parse(localStorage.getItem(storageKey) || '[]');
            stored.forEach(s => selected.add(s));
            renderTags();
        } catch (e) { console.warn('failed to load repos for multiselect', e); }
    }
    loadAll();

    return {
        getSelected: () => Array.from(selected),
        add: (v) => { selected.add(v); renderTags(); persist(); },
        remove: (v) => { selected.delete(v); renderTags(); persist(); }
    };
}
