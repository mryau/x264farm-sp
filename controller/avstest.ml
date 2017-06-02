open Printf;;

let (//) a b = a b;;

let script = Avs.open_avs Sys.argv.(1);;

let pm = Mutex.create ();;
let p ?(name="") o =
	Mutex.lock pm;
	printf "%s%s\n%!" name o;
	Mutex.unlock pm
;;

let thread_guts n =
	let p = p ~name:(sprintf "%06d " n) in
	let pos = Avs.pos_frame n in
	let s = String.create (script.Avs.bytes_per_frame) in

	let rec do_stuff () = (
		p // sprintf "%06d,%03d" pos.Avs.frame pos.Avs.line;
		let blitted = Avs.blit script pos s 0 script.Avs.bytes_per_frame in
		if blitted = 0 then (
			p "  DONE"
		) else (
			p // sprintf "  %8d" blitted;
			do_stuff ()
		)
	) in
	do_stuff ()
;;


let rand = Thread.create (fun () ->
	while true do
		p "Making a big array";
		let a = Array.init (Random.int 1536) (fun i ->
			String.create (Random.int 1048576)
		) in
		Thread.delay (Random.float 1.0);
		a.(0).[0] <- '#';
	done
) ();;



let gc = Thread.create (fun () ->
	while true do
		Thread.delay 10.0;
		p "GC full majoring";
		Gc.full_major ();
		p "GC done";
	done
) ();;


let num_threads = 4;;
p // sprintf "Making %d threads" num_threads;;
let a = Array.init num_threads (fun i ->
	let n = i * script.Avs.i / num_threads in
	Thread.create thread_guts n
);;

Array.iter (fun i ->
	Thread.join i
) a;;

