/**
 * Common theme switching functionality for zoekt-distributed UIs
 */

(function () {
    'use strict';

    const THEME_KEY = 'dzr_theme';

    /**
     * Apply the specified theme to the document
     * @param {string} theme - 'light' or 'dark'
     */
    function applyTheme(theme) {
        if (theme === 'light') {
            document.documentElement.classList.add('light-mode');
        } else {
            document.documentElement.classList.remove('light-mode');
        }
    }

    /**
     * Get the current theme from localStorage or default to 'dark'
     * @returns {string} - 'light' or 'dark'
     */
    function getCurrentTheme() {
        return localStorage.getItem(THEME_KEY) || 'dark';
    }

    /**
     * Set the theme appearance for a toggle button
     * @param {HTMLElement} btn - The toggle button element
     * @param {string} theme - Current theme ('light' or 'dark')
     */
    function setToggleAppearance(btn, theme) {
        if (!btn) return;

        const icon = btn.querySelector('.icon');
        const label = btn.querySelector('.label');

        if (!icon || !label) return;

        if (theme === 'light') {
            // Currently light -> clicking will switch to dark
            icon.textContent = '🌙';
            label.textContent = 'Dark mode';
        } else {
            // Currently dark -> clicking will switch to light
            icon.textContent = '☀️';
            label.textContent = 'Light mode';
        }
    }

    /**
     * Initialize theme switching for all toggle buttons
     */
    function initThemeToggles() {
        const saved = getCurrentTheme();
        applyTheme(saved);

        // Find all theme toggle buttons
        const toggles = Array.from(document.querySelectorAll('.theme-toggle'));

        toggles.forEach(btn => {
            // Ensure markup structure
            if (!btn.querySelector('.icon')) {
                const ic = document.createElement('span');
                ic.className = 'icon';
                ic.textContent = '🌙';
                const lb = document.createElement('span');
                lb.className = 'label';
                lb.textContent = 'Dark';
                btn.textContent = '';
                btn.appendChild(ic);
                btn.appendChild(document.createTextNode(' '));
                btn.appendChild(lb);
            }

            setToggleAppearance(btn, saved);

            btn.addEventListener('click', function () {
                const now = document.documentElement.classList.contains('light-mode') ? 'dark' : 'light';
                applyTheme(now);
                localStorage.setItem(THEME_KEY, now);
                toggles.forEach(t => setToggleAppearance(t, now));

                // Small visual pulse
                document.documentElement.animate(
                    [{ opacity: 0.98 }, { opacity: 1 }],
                    { duration: 220, easing: 'ease' }
                );
            });
        });
    }

    /**
     * HTML escape utility
     * @param {string} s - String to escape
     * @returns {string} - HTML escaped string
     */
    function escapeHtml(s) {
        return String(s).replace(/[&<>"']/g, function (c) {
            return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": "&#39;" }[c];
        });
    }

    /**
     * Human readable bytes formatting
     * @param {number} bytes - Number of bytes
     * @returns {string} - Formatted string
     */
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

    // Initialize theme toggles when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initThemeToggles);
    } else {
        initThemeToggles();
    }

    // Export utilities to global scope for use by other scripts
    window.ZoektCommon = {
        applyTheme,
        getCurrentTheme,
        setToggleAppearance,
        escapeHtml,
        humanReadableBytes
    };

})();
