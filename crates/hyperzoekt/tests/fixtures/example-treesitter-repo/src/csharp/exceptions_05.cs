using System;
class Ex05 { void A() { try { B(); } catch when (false) { } } void B() { throw new Exception(); } }
