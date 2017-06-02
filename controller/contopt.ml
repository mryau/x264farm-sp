external sock_it_to_me : unit -> Unix.inet_addr array option = "sock_it_to_me"

let get_broadcast_addresses () =
	match sock_it_to_me () with
	| Some i -> i
	| None -> (
		(* Default to 255.255.255.255 *)
		(* Using magic because Windows doesn't know how to handle this *)
		[| Obj.magic "\255\255\255\255" |]
	)
;;

