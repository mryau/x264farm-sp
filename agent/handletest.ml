open Printf;;

let command = "x264 --quiet --crf 26 -o NUL d:\\documents\\dvd\\mkv\\fma\\46\\farm.avs";;

let (//) a b = a b;;
(*
let keep_open = 16;;
let better_priority = 6;;

let check_every = 1.0;;
let close_prob = 0.02;;

let px =
	let pm = Mutex.create () in
	fun n s -> (
		Mutex.lock pm;
		print_string n;
		print_endline s;
		Mutex.unlock pm;
	)
;;


let rec handle_guts p prio =
	let h = (match Opt.create_process_win_single command prio with
		| None -> (
			p "failed to create handle";
			failwith "X"
		)
		| Some h -> (
			p // sprintf "made handle %08X" (Obj.magic h lsl 1);
			h
		)
	) in
	let rec check_this () = (
		Thread.delay check_every;
		if Random.float 1.0 > close_prob then (
			check_this ()
		) else (
			ignore // Opt.set_process_priority h Opt.Normal;
			match Opt.kill_process_win h with
			| true -> (
				p // sprintf "killed handle %08X" (Obj.magic h lsl 1);
				handle_guts p prio
			)
			| false -> (
				p "failed to kill handle";
				()
			)
		)
	) in
	check_this ()
;;

let threads = Array.init keep_open (fun i ->
	let name = sprintf ">> %d " i in
	let p = px name in
	Thread.create (fun () ->
		p "STARTING";
		try
			handle_guts p (if i < better_priority then Opt.Below_normal else Opt.Idle);
			p "EXITING";
		with
			e -> p // sprintf "FAILING %S" (Printexc.to_string e)
	) ()
);;

Array.iter Thread.join threads;;
*)

print_endline "making stuff";;
let q = Array.init 32 (fun i ->
	Opt.create_process_win_single command (if i < 8 then Opt.Below_normal else Opt.Idle)
);;
Thread.delay 10.0;;
print_endline "killing stuff";;
for a = 8 to Array.length q - 1 do
	match q.(a) with
	| None -> ()
	| Some h -> (
		(if a >= 0 then ignore // Opt.set_process_priority h Opt.Normal);
		ignore // Opt.kill_process_win h
	)
done;;
(*
Thread.delay 10.0;;
for a = 8 to Array.length q - 1 do
	match q.(a) with
	| None -> ()
	| Some h -> (
		ignore // Opt.set_process_priority h Opt.Below_normal
	)
done;;
*)
(*
Thread.delay 10.0;;
print_endline "killing other stuff";;
for a = 0 to 1 do
	match q.(a) with
	| None -> ()
	| Some h -> (
(*		ignore // Opt*)
		ignore // Opt.kill_process_win h
	)
done;;
*)

print_endline "delaying";;
Thread.delay 1000.0;;

