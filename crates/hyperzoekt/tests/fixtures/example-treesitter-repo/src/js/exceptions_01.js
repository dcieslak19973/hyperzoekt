function mayThrow() { throw new Error('e'); }
try { mayThrow(); } catch (e) { console.error(e); }
