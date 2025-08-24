// TSX exception example
function mayThrow(b: boolean) {
    if (b) throw new Error('boom');
}

try { mayThrow(true); } catch (e) { console.error(e); }

export default function App() { return null; }
