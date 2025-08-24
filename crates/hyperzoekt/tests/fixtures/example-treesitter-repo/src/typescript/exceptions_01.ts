function mayThrow(b: boolean) { if (b) throw new Error('e'); }
try { mayThrow(true); } catch (e) { console.log(e); }
