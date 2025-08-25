// TSX exception example
function mayThrowTsx(b: boolean) {
    if (b) throw new Error('boom');
}

try { mayThrowTsx(true); } catch (e) { console.error(e); }

export default function App() { return null; }
