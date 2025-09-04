(* sample1.ml: representative real-world OCaml constructs *)

type t = A | B of int
exception E of string

let x = 42
let (y, z) = (1, 2)

let add a b = a + b

module M = struct
  let m = 1
  module Nested = struct
    let n = 0
  end
end

module F(X : sig val v : int end) = struct
  let from_x = X.v
end

let rec fib n = if n < 2 then n else fib (n - 1) + fib (n - 2)

class c = object
  method m = 1
end

let Some_val = Some 3
let Some v = Some 4
