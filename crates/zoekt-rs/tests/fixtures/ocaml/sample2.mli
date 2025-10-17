(* sample2.mli: interface sample *)

val x : int
val add : int -> int -> int
module M : sig val m : int end
type t = A | B
