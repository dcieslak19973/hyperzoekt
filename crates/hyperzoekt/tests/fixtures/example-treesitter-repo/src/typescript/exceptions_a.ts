function mayThrowA(b: boolean) { if (b) throw new Error('e'); }
try { mayThrowA(true); } catch (e) { console.log(e); }
