(* sample3.ml: real-world-like module and variant usage *)

module Server = struct
  type state = Running | Stopped
  let start () = ()
  let stop () = ()
end

let () = Server.start ()

let%test _ = true

let (|>) x f = f x

module type S = sig
  val init: unit -> unit
end

module Impl : S = struct
  let init () = ()
end
