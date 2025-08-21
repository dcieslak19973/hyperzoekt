// TSX example

interface PointProps {
    x: number;
    y: number;
}


// Minimal TSX/TS fixture for tree-sitter testing
// This file intentionally avoids React and JSX to keep tooling simple.

interface GeometryPointProps {
    x: number;
    y: number;
}

export function formatPoint(p: GeometryPointProps): string {
    return `Point(${p.x}, ${p.y})`;
}

export const example = formatPoint({ x: 3, y: 4 });
