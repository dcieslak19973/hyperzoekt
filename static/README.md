# Shared Static Assets

This directory contains shared static assets used by multiple HyperZoekt web interfaces:

- `styles.css` - Common CSS styles with dark-mode-first theming
- `theme.js` - JavaScript for theme switching and localStorage persistence

## Usage

All web interfaces (hyperzoekt-webui, dzr-admin, dzr-http-search) now serve these assets from this shared location at `/static/common/`.

## Benefits

- **DRY Principle**: No more duplicated CSS/JS files across crates
- **Consistent Theming**: All interfaces use the same dark-mode-first theme
- **Easy Maintenance**: Update styles in one place, affects all interfaces
- **Version Control**: Single source of truth for shared assets

## Structure

```
static/
├── common/
│   ├── styles.css    # Shared CSS with CSS custom properties for theming
│   └── theme.js      # Theme switching functionality
```

## Theme Features

- **Dark Mode Default**: All interfaces start with dark mode enabled
- **Automatic Persistence**: Theme choice is saved to localStorage
- **Smooth Transitions**: CSS transitions for theme changes
- **Accessible**: Proper contrast ratios and focus indicators</content>
<parameter name="filePath">/workspaces/hyperzoekt/static/README.md
