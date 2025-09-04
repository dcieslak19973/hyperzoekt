(* sample4.ml: pattern edge cases and constructors *)

let Some x = Some 1
let None = None
let (a, (b, c)) = (1, (2,3))

let rec map f l = match l with | [] -> [] | h::t -> (f h)::(map f t)

module X = struct
  module Y = struct
    let yy = 1
  end
end
