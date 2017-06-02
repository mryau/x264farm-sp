(*******************************************************************************
	This file is a part of x264farm-sp.

	x264farm-sp is free software; you can redistribute it and/or modify
	it under the terms of the GNU General Public License version 2 as
	published by the Free Software Foundation.

	x264farm-sp is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with x264farm-sp; if not, write to the Free Software
	Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*******************************************************************************)

open Printf;;

(*Sys.set_signal Sys.sigpipe Sys.Signal_ignore;;*)

let default_overhead = (Gc.get ()).Gc.max_overhead;;
Gc.set {(Gc.get ()) with Gc.max_overhead = 1000001};;


(*Avi.init_avi ();;*)


(********)
(* TEST *)
(********)
(*
let a = Avi.open_avi "d:\\documents\\dvd\\mkv\\FMA\\49\\farm.avs";; (*"d:\\documents\\dvd\\mkv\\amelie\\amelie.avs";;*)
let b = Avi.open_avi "d:\\documents\\dvd\\mkv\\FMA\\49\\farm.avs";; (*"d:\\documents\\dvd\\mkv\\amelie\\amelie.avs";;*)

printf "W: %d\n" a.Avi.w;;
printf "H: %d\n" a.Avi.h;;
printf "N: %d\n" a.Avi.n;;
printf "D: %d\n" a.Avi.d;;
printf "I: %d\n" a.Avi.i;;

let s = String.create (3 * a.Avi.bytes_per_frame);;

let f1 = open_out_bin "f1.raw";;
output_string f1 s;;
close_out f1;;

let got = Avi.blit_frame a 10000 s 0;;
printf "Got %d frames\n%!" got;;

let f2 = open_out_bin "f2.raw";;
output_string f2 s;;
close_out f2;;

let got = Avi.blit_frames a 20000 7 s 0;;
printf "Got %d frames\n%!" got;;

let f2 = open_out_bin "f3.raw";;
output_string f2 s;;
close_out f2;;





Avi.close_avi a;;

exit 572;;
*)
(*********)
(* !TEST *)
(*********)





let version_string = "x264farm-sp 1.02";;
if Sys.word_size = 32 then (
	printf "%s controller\n%!" version_string
) else (
	printf "%s controller %d-bit\n%!" version_string Sys.word_size
);;

let to_hex s =
	let result = String.create (2 * String.length s) in
	for i = 0 to String.length s - 1 do
		String.blit (sprintf "%02X" (int_of_char s.[i])) 0 result (2*i) 2;
	done;
	result
;;

let is_dir x = try (
	match (Unix.LargeFile.stat x).Unix.LargeFile.st_kind with
	| Unix.S_DIR -> true
	| _ -> false
) with
	_ -> false
;;
let is_file x = try (
	match (Unix.LargeFile.stat x).Unix.LargeFile.st_kind with
	| Unix.S_REG -> true
	| _ -> false
) with
	_ -> false
;;
let is_something x = is_file x || is_dir x;;

let search_for_file x = (
	if Filename.is_implicit x then (
		(* Look in all the standard places *)
		if is_file (Filename.concat Filename.current_dir_name x) then (
			(* Found it in the CWD *)
			Some (Filename.concat Filename.current_dir_name x)
		) else if is_file (Filename.concat (Filename.dirname Sys.executable_name) x) then (
			(* Found it in the executable dir *)
			Some (Filename.concat (Filename.dirname Sys.executable_name) x)
		) else (
			None
		)
	) else (
		if is_file x then (
			Some x
		) else (
			None
		)
	)
);;

let rec create_dir x =
	if is_file x then (
		false
	) else if is_dir x then (
		true (* DONE! *)
	) else (
		let ok = try
			create_dir (Filename.dirname x)
		with
			_ -> false
		in
		if ok then (
			Unix.mkdir x 0o755;
			true
		) else (
			(* Something failed along the way *)
			false
		)
	)
;;

let scanoption a = Scanf.kscanf (Scanf.Scanning.from_string a) (fun _ _ -> None);;

let ( +|  ) = Int64.add;;
let ( -|  ) = Int64.sub;;
let ( *|  ) = Int64.mul;;
let ( !|  ) = Int64.of_int;;
let ( !-  ) = Int64.to_int;;
let ( >|- ) = Int64.shift_right;;
let ( <|  ) = Int64.shift_left;;
let ( /|  ) = Int64.div;;

let i64_div_round a b = (
	let a2 = a +| (b >|- 1) in
	a2 /| b
);;

let i64_pow a b =
	let rec helper base num so_far = (
		if num <= 0 then (
			so_far
		) else (
			helper base (pred num) (so_far *| base)
		)
	) in
	helper a b 1L
;;

type 'a exn_perhaps_t = Normal of 'a | Exception of exn;;
let trap_exception a b = try Normal (a b) with x -> Exception x;;
let trap_exception2 a b c = try Normal (a b c) with x -> Exception x;;

let dev_null = if Sys.os_type = "Win32" then "NUL" else "/dev/null";;

(* 3.528575 = min (max a x) b
 * 2.788284 = if x < a then a else if x > b then b else x
 * 0.303620 = integer-specific version of the if-then version
 * Note that these are not equivalent if b < a *)
let bound a b (x:int) = if x < a then a else if x > b then b else x;;

let rec mutex_lock_2 a b = (
	Mutex.lock a;
	if Mutex.try_lock b then (
		() (* Hooray *)
	) else (
		Mutex.unlock a;
		mutex_lock_2 b a
	)
);;

exception Unexpected_packet of string;;
let unexpected_packet ?here (a,b) = match here with
	| None -> raise (Unexpected_packet (sprintf "Packet %S (length %d) was not expected" a (String.length b)))
	| Some here -> raise (Unexpected_packet (sprintf "Packet %S (length %d) was not expected here (%s)" a (String.length b) here))
;;


let prng = Random.State.make_self_init ();;

let temp_file_name prefix suffix =
  let rnd = (Random.State.bits prng) land 0xFFFFFF in
  sprintf "%s%06x%s" prefix rnd suffix
;;
(*
let temp_file prefix suffix =
  let rec try_name counter =
    if counter >= 1000 then
      invalid_arg "Types.temp_file: temp dir nonexistent or full";
    let name = temp_file_name prefix suffix in
    try
      close_desc(open_desc name [Open_wronly; Open_creat; Open_excl] 0o600);
      name
    with Sys_error _ ->
      try_name (counter + 1)
  in try_name 0
;;
*)
let open_temp_file ?(mode = [Open_text]) prefix suffix =
  let rec try_name counter =
    if counter >= 1000 then
      invalid_arg "Types.open_temp_file: temp dir nonexistent or full";
    let name = temp_file_name prefix suffix in
    try
      (name,
       open_out_gen (Open_wronly::Open_creat::Open_excl::mode) 0o600 name)
    with Sys_error _ ->
      try_name (counter + 1)
  in try_name 0
;;

let open_temp_file_unix prefix suffix flags perms =
	let rec try_name counter = (
		if counter >= 1000 then (
			invalid_arg "Types.open_temp_file_unix: temp dir nonexistent or full"
		);
		let name = temp_file_name prefix suffix in
		(try
			(name,
			 Unix.openfile name (Unix.O_WRONLY::Unix.O_CREAT::Unix.O_EXCL::flags) perms)
		with
			Unix.Unix_error _ -> try_name (counter + 1)
		)
	) in
	try_name 0
;;






type agent_info_t = {
	ai_ip : string;
	ai_port : int;
	ai_name : string;
};;

type zone_t_t = Zone_q of int | Zone_b of float;;
type zone_t = {
	zone_start : int;
	zone_end : int;
	zone_type : zone_t_t;
};;

type options_t = {
	o_x : string;
	o_i : string;
	o_o : string;
	o_config : string;
	o_logfile : string;
	o_seek : int;
	o_frames : int;
	o_zones : zone_t list;
	o_clean : bool;
	o_restart : bool;
(*	o_blit : (?last_frame:int -> Avs.t -> Avs.pos_t -> string -> int -> int -> int);*)
(*	o_slave : bool;*)
};;

type config_t = {
	c_controller_name : string;
	c_agent_port : int;
	c_controller_port : int;
	c_static_agents : agent_info_t list;
	c_temp_dir : string;
};;

type info_t = {
	i_res_x : int;
	i_res_y : int;
	i_fps_n : int;
	i_fps_d : int;
	i_fps_f : float;
	i_avs_frames : int;
	i_avs_frame_num_len : int;
	i_encode_seek : int;
	i_encode_frames : int;
	i_bytes_y : int;
	i_bytes_uv : int;
	i_bytes_per_frame : int;
	i_avs2yuv : string;
	i_encode_temp_dir : string;
(*	p : 'a. (?name:string -> ?screen:bool -> ?log:bool -> ?lock:bool -> ?time:bool -> ?fill:bool -> ('a, unit, string, unit) format4 -> 'a);*)
	p : ?name:string -> ?screen:bool -> ?log:bool -> ?lock:bool -> ?time:bool -> ?fill:bool -> string -> unit;
	pm : Mutex.t;
};;

type gop_t = {
	gop_frames : int;
	gop_filename : string;
};;

type worker_status_t = Worker_disconnected | Worker_connected | Worker_frame of int | Worker_done | Worker_dead;;

type agent_worker_t = {
	mutable worker_start : int option;
	mutable worker_current : int option;
	mutable worker_total_frames : int;
	mutable worker_total_time : float;
	mutable worker_last_updated : float;
	mutable worker_status : worker_status_t;
};;
type agent_connection_t = {
	agent_name : string;
	agent_ip : string * int;
	mutable agent_id : string;
	mutable agent_workers : agent_worker_t array;
};;

(*exception Encoding_done;;*)
exception Timeout;;
exception Dead_worker;;


(* Zone functions *)
(* Confines a set of zones to a range of frames *)
(* Also flattens and orders the zones *)
let confine_zones zone_list seek frames =
	let rec split_zone_here ({zone_start = a; zone_end = b; zone_type = t} as this_zone) = function
		| [] -> []
		| {zone_start = za; zone_end = m1; zone_type = t1} :: {zone_start = m2; zone_end = zb; zone_type = t2} :: tl when m1 + 1 = m2 && t1 = t2 -> split_zone_here this_zone ({zone_start = za; zone_end = zb; zone_type = t1} :: tl)
		| {zone_start = za; zone_end = zb; zone_type = zt} as z :: tl when za > b -> z :: tl (* Missed *)
		| {zone_start = za; zone_end = zb; zone_type = zt} as z :: tl when zb < a -> z :: split_zone_here this_zone tl (* Not yet *)
		| {zone_start = za; zone_end = zb; zone_type = zt} :: tl when za >= a && zb <= b -> {zone_start = za; zone_end = zb; zone_type = t} :: split_zone_here this_zone tl (* Take over whole zone *)
		| {zone_start = za; zone_end = zb; zone_type = zt} :: tl when za < a && zb <= b -> {zone_start = za; zone_end = pred a; zone_type = zt} :: {zone_start = a; zone_end = zb; zone_type = t} :: split_zone_here this_zone tl (* Take over end of range *)
		| {zone_start = za; zone_end = zb; zone_type = zt} :: tl when za >= a && zb > b -> {zone_start = za; zone_end = b; zone_type = t} :: {zone_start = succ b; zone_end = zb; zone_type = zt} :: tl (* Take over beginning of range *)
		| {zone_start = za; zone_end = zb; zone_type = zt} :: tl (*when za < a && zb > b*) -> {zone_start = za; zone_end = pred a; zone_type = zt} :: this_zone :: {zone_start = succ b; zone_end = zb; zone_type = zt} :: tl (* Split *)
	in
	let flattened_zones = List.fold_left (fun so_far gnu ->
		split_zone_here gnu so_far
	) [{zone_start = seek; zone_end = seek + frames - 1; zone_type = Zone_b 1.0}] zone_list in
	let rec optimize_zones = function
		| [] -> []
		| {zone_type = Zone_b 1.0} :: tl -> optimize_zones tl (* Remove the "Zone_b 1.0" zones, since frames without zones will have it implicitly *)
		| {zone_start = za; zone_end = m1; zone_type = t1} :: {zone_start = m2; zone_end = zb; zone_type = t2} :: tl when m1 + 1 = m2 && t1 = t2 -> optimize_zones ({zone_start = za; zone_end = zb; zone_type = t1} :: tl) (* Combine sequential zones *)
		| {zone_start = za; zone_end = zb; zone_type = t} :: tl -> {zone_start = za - seek; zone_end = zb - seek; zone_type = t} :: optimize_zones tl
	in
	optimize_zones flattened_zones
;;

(* Renders a zone list to a string *)
let render_zones zones =
	let string_of_zone = function
		| {zone_start = a; zone_end = b; zone_type = Zone_b f} -> sprintf "%d,%d,b=%g" a b f
		| {zone_start = a; zone_end = b; zone_type = Zone_q j} -> sprintf "%d,%d,q=%d" a b j
	in
	List.fold_left (fun so_far gnu ->
		match so_far with
		| "" -> string_of_zone gnu
		| x -> x ^ "/" ^ string_of_zone gnu
	) "" zones
;;

(* Makes a string into a zone list *)
let rec set_zones zone_string =
	if zone_string = "" then (
		[]
	) else (
		let (now,next) = match Rx.rx [Rx.dot_star_hook true; Rx.constant false "/"; Rx.dot_star true] zone_string with
			| Some [Rx.String a; Rx.String b] -> (a,b)
			| _ -> (zone_string,"")
		in
		(* Parse now *)
		match Rx.rx [
			Rx.characters_plus true [Rx.Range ('0','9')];
			Rx.constant false ",";
			Rx.characters_plus true [Rx.Range ('0','9')];
			Rx.constant false ",";
			Rx.character true [Rx.Char 'b'; Rx.Char 'B'; Rx.Char 'q'; Rx.Char 'Q'];
			Rx.constant false "=";
			Rx.characters_plus true [Rx.Range ('0','9'); Rx.Char '.'];
		] now with
		| Some [Rx.String ffrom; Rx.String fto; Rx.String ("b"|"B"); Rx.String bitrate] -> (
			printf "%d,%d,B=%f\n" (int_of_string ffrom) (int_of_string fto) (float_of_string bitrate);
			{zone_start = int_of_string ffrom; zone_end = int_of_string fto; zone_type = Zone_b (float_of_string bitrate)} :: set_zones next
		)
		| Some [Rx.String ffrom; Rx.String fto; Rx.String ("q"|"Q"); Rx.String qp] -> (
			printf "%d,%d,Q=%d\n" (int_of_string ffrom) (int_of_string fto) (int_of_string qp);
			{zone_start = int_of_string ffrom; zone_end = int_of_string fto; zone_type = Zone_q (int_of_string qp)} :: set_zones next
		)
		| _ -> (
			printf "Don't know what %S means\n" now;
			set_zones next
		)
	)
;;





(* Use the following for printing:
 * i.p // sprintf "FORMATTING" a b c
 * This function just removes the need for extra parentheses, like this:
 * i.p (sprintf "FORMATTING" a b c) *)
let (//) a b = a b;;




let rec unix_really_read d s off len = (
	if len <= 0 then () else (
		let r = Unix.read d s off len in
		if r = 0 then (
			raise End_of_file
		) else (
			unix_really_read d s (off + r) (len - r)
		)
	)
);;

let rec unix_really_write d s off len =
	if len <= 0 then () else (
		let r = Unix.write d s off len in
		if r = 0 then (
			raise End_of_file
		) else (
			unix_really_write d s (off + r) (len - r)
		)
	)
;;




let rec really_recv d s off len = (
	if len <= 0 then () else (
		let r = Unix.recv d s off len [] in
		if r = 0 then (
			raise End_of_file
		) else (
			really_recv d s (off + r) (len - r)
		)
	)
);;

(* I don't know if this is needed, but I don't like ignoring stuff *)
let rec really_send d s off len =
	if len <= 0 then () else (
		let r = Unix.send d s off len [] in
		if r = 0 then (
			raise End_of_file
		) else (
			really_send d s (off + r) (len - r)
		)
	)
;;

let rec really_send_timeout d s off len t =
	if len <= 0 then true else (
		match Unix.select [] [d] [] t with
		| (_,[_],_) -> (
			let r = Unix.send d s off len [] in
			if r = 0 then (
				raise End_of_file
			) else (
				really_send_timeout d s (off + r) (len - r) t
			)
		)
		| _ -> (
			false
		)
	)
;;

(* This should probably be part of net.ml sometime *)
let ready sock =
	match Unix.select [sock] [sock] [sock] 0.0 with
	| ([_],[_],[_]) -> (true,true,true)
	| ([_],[_], _ ) -> (true,true,false)
	| ([_], _ ,[_]) -> (true,false,true)
	| ([_], _ , _ ) -> (true,false,false)
	| ( _ ,[_],[_]) -> (false,true,true)
	| ( _ ,[_], _ ) -> (false,true,false)
	| ( _ , _ ,[_]) -> (false,false,true)
	| ( _ , _ , _ ) -> (false,false,false)
;;
let ready2 sock =
	match Unix.select [sock] [sock] [sock] 0.0 with
	| ([_],[_],[_]) -> "OOO"
	| ([_],[_], _ ) -> "OO."
	| ([_], _ ,[_]) -> "O.O"
	| ([_], _ , _ ) -> "O.."
	| ( _ ,[_],[_]) -> ".OO"
	| ( _ ,[_], _ ) -> ".O."
	| ( _ , _ ,[_]) -> "..O"
	| ( _ , _ , _ ) -> "..."
;;
class net ?print sock =
(*
	let print = match print with
		| None -> ignore
		| Some f -> f
	in
*)
	object (o)
(*
		val mutable read_first = ""
		val read_first_mutex = Mutex.create ()
*)
		method private print = (match print with
			| None -> ignore
			| Some f -> f
		)

		method really_send str from len = (
			let sent = Unix.send sock str from len [] in
			if sent = len then (
				()
			) else if sent = 0 then (
				raise End_of_file
			) else (
				o#really_send str (from + sent) (len - sent)
			)
		)

		method really_recv str from len = (
(*			Mutex.lock read_first_mutex;*)
			(try
				let rec recv_helper from len = (
(*
					let read_first_len = String.length read_first in
					if read_first_len = 0 then (
						(* NORMAL *)
*)
						let recvd = Unix.recv sock str from len [] in
						if recvd = len then (
							()
						) else if recvd = 0 then (
							raise End_of_file
						) else (
							recv_helper (from + recvd) (len - recvd)
						)
(*
					) else if read_first_len >= len then (
						(* Already in read_first *)
						String.blit read_first 0 str from len;
						read_first <- String.sub read_first len (read_first_len - len);
					) else (
						(* Some in read_first, some not *)
						String.blit read_first 0 str from read_first_len;
						read_first <- "";
						recv_helper (from + read_first_len) (len - read_first_len)
					)
*)
				) in
				recv_helper from len;
			with
				e -> ((*Mutex.unlock read_first_mutex;*) raise e)
			);
(*			Mutex.unlock read_first_mutex;*)
		)

		method really_recv_timeout str from len w = (
			let rec helper from len = (
				match Unix.select [sock] [] [] w with
				| ([_],_,_) -> (
					(* Get something *)
					let recvd = Unix.recv sock str from len [] in
					if recvd = len then (
						()
					) else if recvd = 0 then (
						raise End_of_file
					) else (
						helper (from + recvd) (len - recvd)
					)
				)
				| _ -> (
					(* TIMEOUT *)
					raise Timeout
				)
			) in
			helper from len;
		)

		(* This method does the same as o#really_recv, but it runs f:unit->bool after the data is copied.
		 * If f returns false, the data is stored in read_first. *)
(*
		method really_recv_or_not str from len f = (
			Mutex.lock read_first_mutex;
			let ok = (try
				let rec recv_helper from len = (
					let read_first_len = String.length read_first in
					if read_first_len = 0 then (
						(* Just recv *)
						let recvd = Unix.recv sock str from len [] in
						if recvd = len then (
							()
						) else if recvd = 0 then (
							raise End_of_file;
						) else (
							recv_helper (from + recvd) (len - recvd)
						)
					) else if read_first_len >= len then (
						(* Read all from read_first *)
						String.blit read_first 0 str from len;
						read_first <- String.sub read_first len (read_first_len - len)
					) else (
						(* Some in read_first, some not *)
						String.blit read_first 0 str from read_first_len;
						read_first <- "";
						recv_helper (from + read_first_len) (len - read_first_len)
					)
				) in
				recv_helper from len;
				let ok = f () in
				if not ok then (
					(* Put back into read_first *)
					read_first <- String.sub str from len;
				);
				ok
			with
				e -> (Mutex.unlock read_first_mutex; raise e)
			);
			Mutex.unlock read_first_mutex;
			ok
		)
*)
		method send t s = (
			let len = String.length s in
			let l = String.create 4 in
			Pack.packN l 0 len;
			o#really_send t 0 4;
			o#really_send l 0 4;
			o#really_send s 0 len;
		)

		method recv = (
			let t = String.create 4 in
			let l = String.create 4 in
			o#really_recv t 0 4;
(*			o#print // sprintf "Got tag %S" t;*)
			o#really_recv l 0 4;
(*			o#print // sprintf "Got length %s" (to_hex l);*)
			if l = "\x00\x00\x00\x00" then (
				(t,"")
			) else (
				let len = Pack.unpackN l 0 in
				let s = String.create len in
(*				o#print // sprintf "getting %d bytes" len;*)
				o#really_recv s 0 len;
				(t,s)
			)
		)

		method recv_timeout w = (
			let t = String.create 4 in
			let l = String.create 4 in
			o#really_recv_timeout t 0 4 w;
(*			o#print // sprintf "Got tag %S" t;*)
			o#really_recv_timeout l 0 4 w;
(*			o#print // sprintf "Got length %s" (to_hex l);*)
			if l = "\x00\x00\x00\x00" then (
				(t,"")
			) else (
				let len = Pack.unpackN l 0 in
				let s = String.create len in
(*				o#print // sprintf "getting %d bytes" len;*)
				o#really_recv_timeout s 0 len w;
				(t,s)
			)
		)

(*
		method register_events event_list = (
			let temp_string = String.create 4 in
			let rec do_event () = (
				let f () = (
					let rec check_list = function
						| [] -> false
						| (a,b) :: tl when a = temp_string -> true
						| (a,b) :: tl -> check_list tl
					in
					check_list event_list
				) in
				let ok = o#really_recv_or_not temp_string 0 4 f in
				if ok then (
					(* Do the rest of the recv *)
					let l = String.create 4 in
					o#really_recv l 0 4 in
					if l = "\x00\x00\x00\x00" then (
						(temp_string,"")
					) else (
						let len = Pack.unpackN l 0 in
						let s = String.create len in
						really_recv s 0 len;
						(temp_string, s)
					)
*)
	end
;;

let async ?print ?priority num len f_write =
	let p = match print with
		| None -> fun x -> ()
		| Some p -> p
	in
	let iq = Queue.create () in
	let iq_mutex = Mutex.create () in
	let iq_condition = Condition.create () in
	let oq = Queue.create () in
	let oq_mutex = Mutex.create () in
	let oq_condition = Condition.create () in

	(* Fill up iq *)
	for a = 1 to num do
		let s = String.create len in
		Queue.add s iq;
	done;

	(* Make the writer *)
	ignore // Thread.create (fun () ->
		(match priority with
			| None -> ()
			| Some p -> ignore // Opt.set_thread_priority p
		);
		try
			let rec write_loop () = (
				(* Check out a string *)
				Mutex.lock iq_mutex;
				while Queue.is_empty iq do
					p "ASYNC queue is empty; waiting";
					Condition.wait iq_condition iq_mutex
				done;
				let str = Queue.take iq in
				Mutex.unlock iq_mutex;

				(* Write to it *)
				match f_write str len with
				| 0 -> (
					(* No more! fail *)
					Mutex.lock oq_mutex;
					p "ASYNC input got no bytes; die";
					Queue.add None oq;
					Condition.broadcast oq_condition;
					Mutex.unlock oq_mutex;
				)
				| n -> (
					Mutex.lock oq_mutex;
					p // sprintf "ASYNC input got %d bytes" n;
					Queue.add (Some (str,n)) oq;
					Condition.broadcast oq_condition;
					Mutex.unlock oq_mutex;
					write_loop ()
				)
			) in
			write_loop ()
		with
			_ -> ()
	) ();

	(* Return reader *)
	fun () -> (
		Mutex.lock oq_mutex;
		while Queue.is_empty oq do
			Condition.wait oq_condition oq_mutex
		done;
		let got = Queue.take oq in
		Mutex.unlock oq_mutex;

		match got with
		| None -> (
			p "ASYNC output got no bytes; die";
			None
		)
		| Some (str,n) -> (
			(* Function for returning the string *)
			p // sprintf "ASYNC output got %d bytes" n;
			let f () = (
				Mutex.lock iq_mutex;
				Queue.add str iq;
				Condition.broadcast iq_condition;
				Mutex.unlock iq_mutex;
			) in
			Some (str,n,f)
		)
	)
;;

(*********)
(* SLAVE *)
(*********)
(*
type slave_t = {
	slave_bytes_per_frame : int;
	slave_i : string;
	slave_addr : Unix.sockaddr;
	slave_first_frame : int;
	slave_last_frame : int;
	slave_zones : string;
	slave_name : string;
};;

let do_slave () =
	set_binary_mode_in stdin true;

	let s_perhaps = (
		let s1 = String.create Marshal.header_size in
		unix_really_read Unix.stdin s1 0 Marshal.header_size;
		let total_size = Marshal.total_size s1 0 in
		let s2 = String.create total_size in
		String.blit s1 0 s2 0 Marshal.header_size;
		unix_really_read Unix.stdin s2 Marshal.header_size (total_size - Marshal.header_size);
		Marshal.from_string s2 0
	) in

	match s_perhaps with
	| None -> (true)
	| Some s -> (

		Random.self_init ();

		ignore // Opt.set_process_priority Opt.Below_normal;

		let rand = sprintf "%06X " (Random.int 16777216) in
		let p q = (eprintf "%s%s%s\n" s.slave_name rand q; flush stderr) in

		p "HELLOES?";

		p // sprintf "got stuff %S" (to_hex (Marshal.to_string s []));
		p // sprintf "  bytes_per_frame %d" s.slave_bytes_per_frame;
		p // sprintf "  i %s" s.slave_i;
		(match s.slave_addr with
				| Unix.ADDR_UNIX q -> p // sprintf "  addr %s" q;
			| Unix.ADDR_INET (q,r) -> p // sprintf "  addr %s:%d" (Unix.string_of_inet_addr q) r;
		);
		p // sprintf "  first frame %d" s.slave_first_frame;
		p // sprintf "  last frame %d" s.slave_last_frame;
		p // sprintf "  zones %s" s.slave_zones;
		p // sprintf "  name %s" s.slave_name;

		let keep_going_ref = ref true in
		let transfer_ok_ref = ref true in
		let keep_going_mutex = Mutex.create () in
		let keep_going_condition = Condition.create () in

		let keep_going_thread = Thread.create (fun () ->
			try
				let q = "#" in
				ignore // Unix.read Unix.stdin q 0 1;
				p "SLAVE keep going thread died from closing STDIN";
				Mutex.lock keep_going_mutex;
				keep_going_ref := false;
				Condition.signal keep_going_condition;
				Mutex.unlock keep_going_mutex;
			with
				e -> (
					p // sprintf "keep going thread died from %S" (Printexc.to_string e);
					Mutex.lock keep_going_mutex;
					keep_going_ref := false;
					Condition.signal keep_going_condition;
					Mutex.unlock keep_going_mutex;
				)
		) () in

		let send_thread_guts () = (
			try
				let str = String.create s.slave_bytes_per_frame in
				p // sprintf "making script %S" s.slave_i;
				let avi = Avi.open_avi s.slave_i in
				p "made script";
				(try
					p "making socket";
					let video_sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
					p "made socket";
					(try
						p "connecting";
						Unix.connect video_sock s.slave_addr;
						ignore // trap_exception (fun n -> Unix.setsockopt_int video_sock Unix.SO_SNDBUF n) 1048576;
						p "connected";

						(* Iterate over the frames *)
						let pos = Avs.pos_frame s.slave_first_frame in
						let rec do_stuff () = (
							Mutex.lock keep_going_mutex;
							let keep_going = !keep_going_ref in
							Mutex.unlock keep_going_mutex;
							if keep_going then (
								p // sprintf "getting frame %d.%d" pos.Avs.frame pos.Avs.line;
								let read_bytes = Avs.blit ~last_frame:s.slave_last_frame script pos str 0 s.slave_bytes_per_frame in
								if read_bytes = 0 then (
									(* Done! *)
									()
								) else (
									p // sprintf "sending stuff to %d.%d" pos.Avs.frame pos.Avs.line;
									let ok = really_send_timeout video_sock str 0 read_bytes 60.0 in
									p // sprintf "sent";
									if ok then (
										do_stuff ()
									) else (
										p "send thread died from timeout";
									)
								)
							)
						) in
						do_stuff ();
					with
						e -> (
							Unix.close video_sock;
							raise e
						)
					);
					Unix.close video_sock
				with
					e -> (
						Avs.close_avs script;
						raise e
					)
				);
				Avs.close_avs script
			with
				e -> (
					p // sprintf "WHAO! This just died with %S" (Printexc.to_string e);
					raise e
				)
		) in
		let st = Thread.create (fun () ->
			try
				send_thread_guts ()
			with
				e -> (p // sprintf "SLAVE send thread died with %S" (Printexc.to_string e))
		) () in

		Mutex.lock keep_going_mutex;
		p "main thread locked keep_going_mutex";
		while !keep_going_ref do
			p "main thread waiting on keep_going_condition";
			Condition.wait keep_going_condition keep_going_mutex;
			p "main thread going on keep_going_condition";
		done;
		p "main thread leaving condition loop";
		let is_still_ok = !transfer_ok_ref in
		Mutex.unlock keep_going_mutex;
		p "exiting or something";

		is_still_ok
	)
;;
*)


(*****************)
(* PARSE OPTIONS *)
(*****************)
(* OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO *)
let parse_these_options r = (
	let arg_x = ref "" in
	let arg_i = ref "" in
	let arg_o = ref "output.mkv" in
	let arg_config = ref "config.xml" in
	let arg_logfile = ref "out-dump.txt" in
	let arg_seek = ref 0 in
	let arg_frames = ref max_int in
	let arg_zones = ref [] in
	let more_args = ref [] in

	let arg_clean = ref false in
	let arg_restart = ref false in

	let arg_thread = ref false in

	let arg_slave = ref false in

	let specs = Arg.align [
		("-x", Arg.Set_string arg_x, "\"\" x264 encoding options (REQUIRED)");
		("-i", Arg.Set_string arg_i, "\"\" input AVS file (REQUIRED)");
		("-o", Arg.Set_string arg_o, "\"output.mkv\" output MKV file");
		("--config", Arg.Set_string arg_config, "\"config.xml\" XML configuration file");
		("--logfile", Arg.Set_string arg_logfile, "\"out-dump.txt\" Verbose log file");
		("--seek", Arg.Set_int arg_seek, "0 first frame to encode");
		("--frames", Arg.Set_int arg_frames, "(all) number of frames to encode");
		("--zones", Arg.String (fun x -> arg_zones := set_zones x), "\"\" same as x264's --zones");
		("--clean", Arg.Set arg_clean, " Delete any unused GOPs from the current temp directory");
		("--restart", Arg.Set arg_restart, " Remove all temp files from previous encodes and start over");
		("--thread", Arg.Set arg_thread, " Use multiple AVS filters at the same time (may corrupt frames!)");
(*		("--slave", Arg.Set arg_slave, " Internal use only");*)
	] in

(*	Arg.parse (Arg.align specs) (fun x -> more_args := x :: !more_args) "USAGE:";*)
	Arg.current := 0;
	(match trap_exception (fun () -> Arg.parse_argv r specs (fun x -> more_args := x :: !more_args) "USAGE:") () with
		| Exception (Arg.Bad x | Arg.Help x) -> (
			eprintf "\n%s" x;
			exit 1
		)
		| Exception e -> (printf "%s" (Printexc.to_string e))
		| Normal () -> ()
	);

	(*********)
	(* SLAVE *)
	(*********)
(*
	if !arg_slave then (
		try
			let q = do_slave () in
			if q then (
				exit 1; (* I say 1 is normal exit for the slave (0 is for the controller, you see...) *)
			) else (
				exit 2; (* 2 means error in this case *)
			)
		with
			_ -> exit 3; (* 3 is something that I didn't catch *)
	);
*)
	(* Sanity checks *)
	if !arg_i = "" then (
		eprintf "\n%s: must specify input file.\n" Sys.argv.(0);
		Arg.usage specs "USAGE:";
		exit 1
	) else if not (is_file !arg_i) then (
		eprintf "%s: input %S is not a file" Sys.argv.(0) !arg_i;
		exit 1
	);
	if not (is_file !arg_config) then (
		printf "Config %S is not a file\n" !arg_config
	);
	if is_dir !arg_o then (
		eprintf "%s: output %S is a directory" Sys.argv.(0) !arg_o;
		exit 1
	);

	{
		o_x = !arg_x;
		o_i = !arg_i;
		o_o = !arg_o;
		o_config = !arg_config;
		o_logfile = !arg_logfile;
		o_seek = !arg_seek;
		o_frames = !arg_frames;
		o_zones = confine_zones !arg_zones 0 max_int;
		o_clean = !arg_clean;
		o_restart = !arg_restart;
(*		o_blit = Avs.blit_nolock;*)
	}
);;

(* CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC *)
let parse_this_config o_config =
	let config_obj = (try
		Some (Xml.parse_file o_config)
	with
		_ -> None
	) in
	let controller_name_ref = ref "" in
	let controller_find_port_ref = ref 40704 in
	let agent_find_port_ref = ref 40705 in
	let temp_dir_ref = ref (Filename.concat Filename.temp_dir_name "x264farm-sp") in
	let agent_list_ref = ref [] in
	let parse_config_elements = function
		| Xml.Element ("port", [("controller",x);("agent",y)], _) | Xml.Element ("port", [("agent",y);("controller",x)], _) -> (controller_find_port_ref := int_of_string x; agent_find_port_ref := int_of_string y)
		| Xml.Element ("temp", _, [Xml.PCData q]) -> (
			if is_file q then (
				printf "Temp dir %S in %S is actually a file; ignoring\n" q o_config
			) else (
				if not (create_dir q) then (
					printf "Unable to create temp dir %S; ignoring\n" q
				) else (
					temp_dir_ref := q
				)
			)
		)
		| Xml.Element ("agents", _, agent_list) -> List.iter (function
			(* Parse each agent *)
			| Xml.Element ("agent", _, agent_attributes) -> (
				(* Parse the agent's attributes *)
				let agent_ip_ref = ref None in
				let agent_port_ref = ref None in
				let agent_name_ref = ref None in
				List.iter (function
					| Xml.Element ("ip", _, [Xml.PCData q]) -> agent_ip_ref := Some q
					| Xml.Element ("port", _, [Xml.PCData q]) -> agent_port_ref := Some (int_of_string q)
					| Xml.Element ("name", _, [Xml.PCData q]) -> agent_name_ref := Some q
					| Xml.Element (e,_,_) -> printf "Don't understand what element <%s> is in <agent>... ignoring\n" e
					| _ -> () (* Whatever *)
				) agent_attributes;
				match (!agent_ip_ref, !agent_port_ref, !agent_name_ref) with
				| (Some ip, Some port, Some name) -> (
					agent_list_ref := {ai_ip = ip; ai_port = port; ai_name = name} :: !agent_list_ref
				)
				| (Some ip, Some port, None) -> (
					agent_list_ref := {ai_ip = ip; ai_port = port; ai_name = sprintf "%s:%d" ip port} :: !agent_list_ref
				)
				| _ -> printf "Ignoring incomplete agent spec in config file\n"
			)
			| Xml.Element (e,_,_) -> printf "Don't understand what element <%s> is in <agents>... ignoring\n" e
			| _ -> ()
		) agent_list
		| Xml.Element (e,_,_) -> printf "Don't understand what element <%s> is in %S... ignoring\n" e o_config
		| _ -> ()
	in
	(match config_obj with
		| Some (Xml.Element ("controller-config", [("name",n)], elts)) -> (controller_name_ref := n; List.iter parse_config_elements elts)
		| Some (Xml.Element ("controller-config", _, elts)) -> List.iter parse_config_elements elts
		| Some (Xml.Element (q, _, _)) -> failwith (sprintf "Config file must have root element <controller-config>, not <%s>" q)
		| Some (Xml.PCData _) -> failwith "Config file must have root element <controller-config>, not regular data"
		| None -> printf "Config file does not seem to be XML\n"
	);

	{
		c_controller_name = !controller_name_ref;
		c_agent_port = !agent_find_port_ref;
		c_controller_port = !controller_find_port_ref;
		c_static_agents = !agent_list_ref;
		c_temp_dir = !temp_dir_ref;
	}
;;

(* IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII *)
let parse_this_encode o c = (
	(* Note that the "o" and "c" here are local! *)

	(* Matches a string against the formatting for file info *)
	let match_string file_info = (
		match trap_exception (String.rindex file_info) ':' with
		| Exception _ -> None (* No colon means no info *)
		| Normal n -> (
			let after_colon = String.sub file_info n (String.length file_info - n) in
			match scanoption after_colon ": %dx%d, %d/%d fps, %d frames" (fun res_x res_y fps_n fps_d frames -> Some (res_x, res_y, fps_n, fps_d, frames)) with
			| Some x -> Some x
			| None -> (scanoption after_colon ": %dx%d, %d fps, %d frames" (fun res_x res_y fps_n frames -> Some (res_x, res_y, fps_n, 1, frames)))
		)
	) in

	let avs2yuv = match (search_for_file "avs2yuv.exe", search_for_file "avs2yuv.bat") with
		| (Some x, _) | (_, Some x) -> "\"" ^ x ^ "\""
		| _ -> "avs2yuv" (* Let's hope it's on the path somewhere *)
	in

	(* Get AVS parameters *)
	let (res_x, res_y, fps_n, fps_d, frames) = if true then (
(*
		match Avs.info o.o_i with
		| None -> (printf "\nERROR: AVS file %S does not seem to be valid\n" o.o_i; exit 1)
		| Some q -> q
*)
(*
		match trap_exception Avs.info o.o_i with
		| Normal q -> q
		| Exception (Avs.Avs_failure x) -> (printf "\nERROR: AVS file \"%s\" returned:\n%s\n" o.o_i x; exit 1)
		| Exception _ -> (printf "\nERROR: AVS file \"%s\" does not seem to be valid\n" o.o_i; exit 1)
*)
		Mutex.lock Avi.bm;
		Avi.init_avi ();
		let n = trap_exception Avi.info_avi o.o_i in
		Avi.exit_avi ();
		Mutex.unlock Avi.bm;
		match n with
		| Normal q -> q
		| Exception _ -> (printf "\nERROR: AVS file \"%s\" does not seem to be valid\n" o.o_i; exit 1)
	) else (
		let file_info_handle = Unix.open_process_in (sprintf "\"%s -frames 1 -raw -o %s \"%s\" 2>&1\"" avs2yuv dev_null o.o_i) in
		let file_info = input_line file_info_handle in
		(match Unix.close_process_in file_info_handle with
			| Unix.WEXITED   1 -> (printf "\nERROR: avs2yuv exited with error code 1; avs2yuv responded:\n  %s" file_info; exit 1)
			| Unix.WEXITED   x -> if x <> 0 then (printf "\nERROR: avs2yuv exited with error code %d\n" x; exit 1)
			| Unix.WSIGNALED x -> (printf "\nERROR: avs2yuv was killed with signal %d\n" x; exit 1)
			| Unix.WSTOPPED  x -> (printf "\nERROR: avs2yuv was stopped with signal %d\n" x; exit 1)
		);

		match match_string file_info with
		| None -> (printf "\nERROR: AVS file %S does not seem to be valid\n" o.o_i; exit 1)
		| Some q -> q
	) in

	let seek_use = bound 0 (pred frames) o.o_seek in
	let frames_use = min o.o_frames (frames - seek_use) in


	(*********)
	(* PRINT *)
	(*********)
	let time_string () = (
		let tod = Unix.gettimeofday () in
		let cs = int_of_float (100.0 *. (tod -. floor tod)) in (* centi-seconds *)
		let lt = Unix.localtime tod in
		sprintf "%04d-%02d-%02d~%02d:%02d:%02d.%02d" (lt.Unix.tm_year + 1900) (lt.Unix.tm_mon + 1) (lt.Unix.tm_mday) (lt.Unix.tm_hour) (lt.Unix.tm_min) (lt.Unix.tm_sec) cs
	) in
	let pm = Mutex.create () in
	let print = (
		if o.o_logfile = "" || o.o_logfile = dev_null then (
			(* Don't use the log *)
			fun ?(name="") ?(screen=false) ?(log=true) ?(lock=true) ?(time=true) ?(fill=false) s -> (
				if screen then (
					(* Only do something if the screen is on *)
					if lock then Mutex.lock pm;
					print_string name;
					print_endline s;
					if lock then Mutex.unlock pm;
				)
			)
		) else (
			(* Use the log depending on ~log *)
			let ph = open_out o.o_logfile in
			fun ?(name="") ?(screen=false) ?(log=true) ?(lock=true) ?(time=true) ?(fill=false) s -> (
				if screen || log then (
					if lock then Mutex.lock pm;
					if screen then (
						print_string name;
						print_endline s;
					);
					if log then (
						let time_string = if time then (time_string () ^ "  ") else "" in
						output_string ph time_string;
						output_string ph name;
						output_string ph s;
						output_string ph "\n";
						flush ph
					);
					if lock then Mutex.unlock pm;
				)
			)
		)
	) in

	(****************************)
	(* AVS AND ENCODE DIRECTORY *)
	(****************************)
	let avs_dir = Filename.concat c.c_temp_dir (Filename.basename o.o_i ^ " " ^ to_hex (Digest.file o.o_i)) in
	let encode_md5 = (
		let str = sprintf "%s" o.o_x in
		to_hex (Digest.string str)
	) in

	(* Write the options to a file in the temp directory *)
	(try
		let time_string = time_string () in
		let argv_string = (
			let quotify x = if String.contains x ' ' then "\"" ^ x ^ "\"" else x in
			let new_argv = Array.copy Sys.argv in
			new_argv.(0) <- sprintf "<%s>" (Sys.getcwd ());
			Array.fold_left (fun so_far x -> so_far ^ "\t" ^ quotify x) "" new_argv
		) in
		let option_file = Filename.concat c.c_temp_dir "option_log.txt" in
		let option_temp_file = Filename.concat c.c_temp_dir "option_log.txt.temp" in
		if is_file option_file then (
			Unix.rename option_file option_temp_file
		) else (
			let a = open_out option_temp_file in
			close_out a
		);
		let in_file = open_in option_temp_file in
		let out_file = open_out option_file in
		output_string out_file (sprintf "%s %s\n" time_string argv_string);
		let temp_string = String.create 4096 in
		let rec write () = (
			let b = input in_file temp_string 0 4096 in
			if b <> 0 then (
				output out_file temp_string 0 b;
				write ()
			)
		) in
		write ();
		close_in in_file;
		close_out out_file;
		Unix.unlink option_temp_file;
	with
		e -> ()
	);

	{
		i_res_x = res_x;
		i_res_y = res_y;
		i_fps_n = fps_n;
		i_fps_d = fps_d;
		i_fps_f = (float_of_int fps_n) /. (float_of_int fps_d);
		i_avs_frames = frames;
		i_avs_frame_num_len = String.length (string_of_int frames);
		i_encode_seek = seek_use;
		i_encode_frames = frames_use;
		i_bytes_y = res_x * res_y;
		i_bytes_uv = res_x * res_y / 2;
		i_bytes_per_frame = res_x * res_y * 3 / 2;
		i_avs2yuv = avs2yuv;
		i_encode_temp_dir = Filename.concat avs_dir encode_md5;
		p = print;
		pm = pm;
	}
);;
(************************)
(* END OF PARSE OPTIONS *)
(************************)

(********************)
(* GLOBAL VARIABLES *)
(********************)
let o = parse_these_options Sys.argv;;
let c = parse_this_config o.o_config;;
let i = parse_this_encode o c;;
(***************************)
(* END OF GLOBAL VARIABLES *)
(***************************)


let run_pass o c i = (


	(* Make the temp dir *)
	ignore // create_dir i.i_encode_temp_dir;

	i.p // sprintf "X:       %S" o.o_x;
	i.p // sprintf "I:       %S" o.o_i;
	i.p // sprintf "O:       %S" o.o_o;
	i.p // sprintf "Config:  %S" o.o_config;
	i.p // sprintf "Logfile: %S" o.o_logfile;
	i.p // sprintf "Seek:    %d" o.o_seek;
	i.p // sprintf "Frames:  %d" o.o_frames;

	i.p "";
	i.p // sprintf "Res X:     %d" i.i_res_x;
	i.p // sprintf "Res Y:     %d" i.i_res_y;
	i.p // sprintf "FPS N:     %d" i.i_fps_n;
	i.p // sprintf "FPS D:     %d" i.i_fps_d;
	i.p // sprintf "FPS F:     %f" i.i_fps_f;
	i.p // sprintf "AVSframes: %d" i.i_avs_frames;
	i.p // sprintf "Frame len: %d" i.i_avs_frame_num_len;
	i.p // sprintf "Seek:      %d" i.i_encode_seek;
	i.p // sprintf "Frames:    %d" i.i_encode_frames;
	i.p // sprintf "Bytes Y:   %d" i.i_bytes_y;
	i.p // sprintf "Bytes uv:  %d" i.i_bytes_uv;
	i.p // sprintf "FrameByte: %d" i.i_bytes_per_frame;
	i.p // sprintf "avs2yuv:   %S" i.i_avs2yuv;
	i.p // sprintf "Temp dir:  %S" i.i_encode_temp_dir;

	(*********************)
	(* REMOVE TEMP FILES *)
	(*********************)
	if o.o_restart then (
		(* Remove EVERYTHING from the temp dir, even things that the user put there *)
		(* Just to teach people a lesson about sticking things in a temporary dir *)
		let d = Sys.readdir i.i_encode_temp_dir in
		Array.iter (fun f ->
			(* Directories can't be removed this way, but I don't put directories here *)
			ignore // trap_exception Sys.remove (Filename.concat i.i_encode_temp_dir f)
		) d
	);


	(***************)
	(* FRAME STATS *)
	(***************)
	let (stats_in, stats_out, stats_out_version) = (
		let name_1 = Filename.concat i.i_encode_temp_dir "working_stats_1.txt" in
		let name_2 = Filename.concat i.i_encode_temp_dir "working_stats_2.txt" in
		let h1 = open_in_gen [Open_rdonly; Open_text; Open_creat] 0o666 name_1 in
		let h2 = open_in_gen [Open_rdonly; Open_text; Open_creat] 0o666 name_2 in
		let version_rx = [
			Rx.constant false "#VERSION ";
			Rx.characters_plus true [Rx.Range ('0','9')]
		] in
		
		let get_version h = (try
			let rec test_line () = (
				let line = input_line h in
				match Rx.rx version_rx line with
				| Some [Rx.String v] -> Some (int_of_string v)
				| _ -> test_line ()
			) in
			test_line ()
		with
			End_of_file -> None
		) in

		let version1 = get_version h1 in
		let version2 = get_version h2 in

		close_in h1;
		close_in h2;

		match (version1, version2) with
		| (Some v1, Some v2) when v1 > v2 -> (open_in name_1, open_out name_2, succ v1)
		| (Some v1, Some v2)              -> (open_in name_2, open_out name_1, succ v2)
		| (Some v1, None)                 -> (open_in name_1, open_out name_2, succ v1)
		| (None, Some v2)                 -> (open_in name_2, open_out name_1, succ v2)
		| (None, None)                    -> (open_in name_2, open_out name_1, 1) (* Might as well start on 1 *)
	) in


	(******************)
	(* TRACKS ELEMENT *)
	(******************)
	let tracks_mutex = Mutex.create () in
	let tracks_ref = (
		if is_file (Filename.concat i.i_encode_temp_dir "TRACKS") then (
			(try
				let f = open_in_bin (Filename.concat i.i_encode_temp_dir "TRACKS") in
				let answer = (try
					let len_s = String.create 4 in
					really_input f len_s 0 4;
					let len = Pack.unpackN len_s 0 in
					let str = String.create len in
					really_input f str 0 len;
					Some str
				with
					_ -> None
				) in
				close_in f;
				ref answer
			with
				_ -> ref None
			)
		) else (
			ref None
		)
	) in
	(match !tracks_ref with
		| None -> i.p "Found no TRACKS file"
		| Some _ -> i.p "TRACKS file loaded"
	);

	let update_tracks x = (
		Mutex.lock tracks_mutex;
		(match !tracks_ref with
			| Some _ -> ()
			| None -> (
				(try
					let t = open_out_bin (Filename.concat i.i_encode_temp_dir "TRACKS") in
					let len_s = String.create 4 in
					Pack.packN len_s 0 (String.length x);
					output t len_s 0 4;
					output t x 0 (String.length x);
					tracks_ref := Some x;
					close_out t
				with
					_ -> ()
				)
			)
		);
		Mutex.unlock tracks_mutex;
	) in

	(*******)
	(* SEI *)
	(*******)
	(* Basically the same as the TRACKS file *)
	(* Except that I don't use it yet *)
	let sei_mutex = Mutex.create () in
	let sei_ref = (
		if is_file (Filename.concat i.i_encode_temp_dir "SEI") then (
			(try
				let f = open_in_bin (Filename.concat i.i_encode_temp_dir "SEI") in
				let answer = (try
					let len_s = String.create 4 in
					really_input f len_s 0 4;
					let len = Pack.unpackN len_s 0 in
					let str = String.create len in
					really_input f str 0 len;
					Some str
				with
					_ -> None
				) in
				close_in f;
				ref answer
			with
				_ -> ref None
			)
		) else (
			ref None
		)
	) in
	(match !sei_ref with
		| None -> i.p "Found no SEI file"
		| Some _ -> i.p "SEI file loaded"
	);

	let update_sei x = (
		Mutex.lock sei_mutex;
		(match !sei_ref with
			| Some _ -> ()
			| None -> (
				(try
					let t = open_out_bin (Filename.concat i.i_encode_temp_dir "SEI") in
					let len_s = String.create 4 in
					Pack.packN len_s 0 (String.length x);
					output t len_s 0 4;
					output t x 0 (String.length x);
					tracks_ref := Some x;
					close_out t
				with
					_ -> ()
				)
			)
		);
		Mutex.unlock sei_mutex;
	) in

	(*******************)
	(* FRAME VARIABLES *)
	(*******************)

	(* This is the table that contains all of the GOPs from the finished video.
	 * Each GOP seems to be about 110 frames on average, so I rounded up to accomodate multiple encodes *)
	let frame_tree = Rbtree.create ~cmp:(fun (a:int) (b:int) -> if a < b then -1 else if b < a then 1 else 0) () in
	let frame_done_to_ref = ref 0 in (* This is the first frame after the contiguous range starting at 0 *)
	let frame_mutex = Mutex.create () in
	let frame_condition = Condition.create () in

	let encoding_done_ref = ref false in
	let encoding_done_mutex = Mutex.create () in
	let encoding_done () = (
		Mutex.lock encoding_done_mutex;
		let a = !encoding_done_ref in
		Mutex.unlock encoding_done_mutex;
		a
	) in

	let is_gop_here n = (
		Mutex.lock frame_mutex;
		let out = match Rbtree.find frame_tree n with | None -> false | _ -> true in
		Mutex.unlock frame_mutex;
		out
	) in

	let find_gop_here n = (
		Mutex.lock frame_mutex;
		let out = Rbtree.find frame_tree n in
		Mutex.unlock frame_mutex;
		match out with
		| None -> None
		| Some (a,b) -> Some b
	) in

	let put_gop_here_unsafe n g z = (
		Rbtree.add frame_tree n g;
	(*
		if !frame_done_to_ref = n then (
			(* update the first non-frame *)
			frame_done_to_ref := n + g.gop_frames;
			i.p // sprintf "FRAME DONE set to %d" !frame_done_to_ref;
		);
	*)
		if n + g.gop_frames = i.i_encode_frames then (
			(* Last GOP *)
			fprintf stats_out "END %0*d %03d <%s>%s%s\n%!" i.i_avs_frame_num_len (n + i.i_encode_seek) g.gop_frames g.gop_filename (if z = "" then "" else " ") z
		) else (
			fprintf stats_out "GOP %0*d %03d <%s>%s%s\n%!" i.i_avs_frame_num_len (n + i.i_encode_seek) g.gop_frames g.gop_filename (if z = "" then "" else " ") z
		);
		Condition.broadcast frame_condition;
	) in
	let put_gop_here n g z = (
		Mutex.lock frame_mutex;
		put_gop_here_unsafe n g z;
		Mutex.unlock frame_mutex;
	) in

	let replace_gop_here_unsafe n g z = (
		let old_gop_perhaps = Rbtree.find frame_tree n in
	(*
		if !frame_done_to_ref = n then (
			(* update the first non-frame *)
			frame_done_to_ref := n + g.gop_frames;
			i.p // sprintf "FRAME DONE set to %d" !frame_done_to_ref;
		);
	*)
		let replaced = match old_gop_perhaps with
			| Some (_,h) -> (
				(* GOP already exists *)
				true
			)
			| _ -> (
				false
			)
		in
		Rbtree.add frame_tree n g;
		if n + g.gop_frames = i.i_encode_frames then (
			(* Last GOP *)
			fprintf stats_out "END %0*d %03d <%s>%s%s\n%!" i.i_avs_frame_num_len (n + i.i_encode_seek) g.gop_frames g.gop_filename (if z = "" then "" else " ") z
		) else (
			fprintf stats_out "GOP %0*d %03d <%s>%s%s\n%!" i.i_avs_frame_num_len (n + i.i_encode_seek) g.gop_frames g.gop_filename (if z = "" then "" else " ") z
		);
		Condition.broadcast frame_condition;
		replaced
	) in
	let replace_gop_here n g z = (
		Mutex.lock frame_mutex;
		let replaced = replace_gop_here_unsafe n g z in
		Mutex.unlock frame_mutex;
		replaced
	) in

	let is_done () = (
		Mutex.lock frame_mutex;
		let start_at = !frame_done_to_ref in
		Mutex.unlock frame_mutex;
		let rec check_at n = (
			if n = i.i_encode_frames then true else (
				match find_gop_here n with
				| None -> false
				| Some g -> check_at (n + g.gop_frames)
			)
		) in
		check_at start_at
	) in

	(*
	 [2771,4829]
	 [6750,8219]
	 [12848,14998]
	 [21355,22498]
	 [24136,26694]
	 [28625,29999]
	*)


	(* This function is pretty slow... oh well. Hopefully I'll just use list_noncontiguous instead *)
	let is_covered () = (
		let rec check_at n next_nogood = (
			(* n is the location to check for more GOPs
			 * next_nogood is the maximum extent of any GOP which starts before n
			 * If n is ever greater than next_nogood, then the frame specified by next_nogood has no coverage *)
			if next_nogood >= i.i_encode_frames then true else if n > next_nogood then false else (
				match find_gop_here n with
				| None -> check_at (succ n) next_nogood
				| Some g -> check_at (succ n) (max next_nogood (n + g.gop_frames))
			)
		) in
		check_at 0 0
	) in

	(* Non-tail-recursive, but should be pretty fast (and the lists aren't THAT big) *)
	(* This is really a coverage indicator *)
	(*
	let list_noncontiguous_unsafe () =
		Rbtree.fold_left (fun so_far n g ->
			let a = n in
			let b = n + g.gop_frames - 1 in
			let rec subtract_range = function
				| [] -> []
				| (hda,hdb) :: tl when hda > b -> (hda,hdb) :: tl (* Missed *)
				| (hda,hdb) :: tl when hdb < a -> (hda,hdb) :: subtract_range tl (* Not yet *)
				| (hda,hdb) :: tl when hda >= a && hdb <= b -> subtract_range tl (* Remove range completely *)
				| (hda,hdb) :: tl when hda < a && hdb >= a && hdb <= b -> (hda, pred a) :: subtract_range tl (* Remove end of range *)
				| (hda,hdb) :: tl when hda >= a && hda <= b && hdb > b -> (succ b, hdb) :: subtract_range tl (* Remove beginning of range *)
				| (hda,hdb) :: tl (*when hda < a && hdb > b*) -> (hda, pred a) :: (succ b, hdb) :: tl (* Split *)
				(* These 6 "when" clauses represent a logical tautology (according to Mathematica, and assuming a<b and hda<hdb),
				 * therefore to make OCaml stop complaining I left out the clause on the last one *)
			in
			subtract_range so_far
		) [(0, pred i.i_encode_frames)] frame_tree
	;;
	*)

	(* This looks better *)
	(*
	let list_noncontiguous_unsafe () =
		let (frame_after_last_gop, noncont_backwards) = Rbtree.fold_left (fun (next_frame, noncont_list) n g ->
			if n = next_frame then (
				(* It's contiguous *)
				(n + g.gop_frames, noncont_list)
			) else if n < next_frame then (
				(* Not yet *)
				(next_frame, noncont_list)
			) else (
				(* Non-contiguous *)
				(n + g.gop_frames, (next_frame, n - 1) :: noncont_list)
			)
		) (0,[]) frame_tree in
		if frame_after_last_gop = i.i_encode_frames then (
			let a = List.rev noncont_backwards in
			(* Update frame_done_to_ref *)
			(match a with
			| (n,_) :: tl -> (frame_done_to_ref := n; i.p // sprintf "FRAME DONE set to %d" !frame_done_to_ref)
				| _ -> ()
			);
			a
		) else (
			let a = List.rev ((frame_after_last_gop, i.i_encode_frames - 1) :: noncont_backwards) in
			(* Update frame_done_to_ref *)
			(match a with
				| (n,_) :: tl -> (frame_done_to_ref := n; i.p // sprintf "FRAME DONE set to %d" !frame_done_to_ref)
				| _ -> ()
			);
			a
		)
	;;
	*)

	(* Do a full search for each GOP. Probably slower in the short run than the above, but should pay off as !frame_done_to_ref starts getting big *)
	let list_noncontiguous_unsafe () = (
		let rec first_gop_after n list_so_far = (
			if n = i.i_encode_frames then (
				(* That's all *)
				List.rev list_so_far
			) else (
				match Rbtree.find_smallest_not_less_than frame_tree n with
				| None -> (
					(* Add the rest of the frames onto the list, then reverse *)
					List.rev ((n, i.i_encode_frames - 1) :: list_so_far)
				)
				| Some (gop_at, gop) when gop_at = n -> (
					(* Contiguous *)
					first_gop_after (gop_at + gop.gop_frames) list_so_far
				)
				| Some (gop_at, gop) -> (
					(* Noncontiguous *)
					first_gop_after (gop_at + gop.gop_frames) ((n, gop_at - 1) :: list_so_far)
				)
			)
		) in
		let a = first_gop_after !frame_done_to_ref [] in
	(*
		(match a with
			| (n,_) :: tl -> (frame_done_to_ref := n; i.p // sprintf "FRAME DONE set to %d" !frame_done_to_ref)
			| _ -> ()
		);
	*)
		a
	) in


	let list_noncontiguous () = (
		Mutex.lock frame_mutex;
		let out = list_noncontiguous_unsafe () in
		Mutex.unlock frame_mutex;
		out
	) in


	let print_ranges p pm = (
		mutex_lock_2 frame_mutex pm;
		Rbtree.iter (fun n g ->
			p // sprintf "  GOP at %*d, %3d frames = %S" i.i_avs_frame_num_len n g.gop_frames (Filename.basename g.gop_filename);
		) frame_tree;
		Mutex.unlock pm;
		Mutex.unlock frame_mutex;
	) in
	let print_ranges_unsafe p pm = (
		Mutex.lock pm;
		Rbtree.iter (fun n g ->
			p // sprintf "  GOP at %*d, %3d frames = %S" i.i_avs_frame_num_len n g.gop_frames (Filename.basename g.gop_filename);
		) frame_tree;
		Mutex.unlock pm;
	) in

	let print_noncontiguous p pm = (
		mutex_lock_2 frame_mutex pm;
		let noncont = list_noncontiguous_unsafe () in
		List.iter (fun (a,b) ->
			p // sprintf "  Need to do (%d,%d)" a b
		) noncont;
		Mutex.unlock pm;
		Mutex.unlock frame_mutex;
	) in

	let wait_until_done () = (
		Mutex.lock frame_mutex;
		i.p "WAIT waiting until done";
		while not (is_done ()) do
			i.p "WAIT keep waiting";
	(*
			print_noncontiguous i.p i.pm;
	*)
			Condition.wait frame_condition frame_mutex;
			i.p "WAIT check again";
		done;
		i.p "WAIT looks good";
		print_noncontiguous i.p i.pm;
		Mutex.unlock frame_mutex;
		i.p "WAIT I'm freeeeeee!";
	) in


	(*
	i.p ~screen:true // sprintf "Is done? %B" (is_done ());;
	i.p ~screen:true // sprintf "Is covered? %B" (is_covered ());;
	i.p ~screen:true "Ranges:";;
	List.iter (fun (a,b) ->
		i.p ~screen:true // sprintf "  %d,%d" a b
	) (list_noncontiguous ());;

	let test_range a b =
		let gop = {gop_frames = b - a + 1; gop_filename = "ahtoeu"} in
		Hashtbl.add frame_hash a gop;
		i.p ~screen:true // sprintf "If GOP is (%d,%d):" a b;
		List.iter (fun (a,b) ->
			i.p ~screen:true // sprintf "  %d,%d" a b
		) (list_noncontiguous ());
		Hashtbl.remove frame_hash a;
	;;

	let test_range_2 a b c d =
		let gop1 = {gop_frames = b - a + 1; gop_filename = "ahtoeu"} in
		let gop2 = {gop_frames = d - c + 1; gop_filename = "ahaoeu"} in
		Hashtbl.add frame_hash a gop1;
		Hashtbl.add frame_hash c gop2;
		i.p ~screen:true // sprintf "If GOPs are (%d,%d) (%d,%d):" a b c d;
		let nc2 = list_noncontiguous () in
		List.iter (fun (a,b) ->
			i.p ~screen:true // sprintf "  %d,%d" a b
		) nc2;
		Hashtbl.remove frame_hash a;
		Hashtbl.remove frame_hash c;
	;;

	test_range (-4) (-3);;
	test_range (-4) (15);;
	test_range (-4) (10000000);;
	test_range (5) (15);;
	test_range (5) (10000000);;
	test_range (9999999) (10000000);;

	test_range_2 101 199 (-51) (-49);;
	test_range_2 101 199 (-51) (51);;
	test_range_2 101 199 (-51) (151);;
	test_range_2 101 199 (-51) (251);;
	test_range_2 101 199 (49) (51);;
	test_range_2 101 199 (49) (151);;
	test_range_2 101 199 (49) (251);;
	test_range_2 101 199 (149) (151);;
	test_range_2 101 199 (149) (251);;
	test_range_2 101 199 (249) (251);;
	*)

	(****************)
	(* AGENT HASHES *)
	(****************)
	(* Need two hash tables since the agent is the same if either the ID or the (IP,port) is the same *)
	let agents_by_id = Hashtbl.create 5 in
	let agents_by_ip = Hashtbl.create 5 in
	let agents_mutex = Mutex.create () in

	let worker_threads = Hashtbl.create 10 in

	let worker_socks = Hashtbl.create 10 in
	let worker_socks_mutex = Mutex.create () in

	(* PRETTY PRINT *)
	let pretty_print_base () = (
		mutex_lock_2 frame_mutex agents_mutex;
		let noncont = list_noncontiguous_unsafe () in
		let outputted = !frame_done_to_ref in
		Mutex.unlock frame_mutex;
		let agent_status = Hashtbl.fold (fun ip agent so_far ->
			let workers = Array.map (fun w ->
				w.worker_status
			) agent.agent_workers in
			(agent.agent_id,ip,workers) :: so_far
		) agents_by_ip [] in
		Mutex.unlock agents_mutex;

		(* Add noncont data *)
		let frame_len = 98 in
		let frames = String.make frame_len '#' in
		List.iter (fun (a,b) ->
			let first_char = a * frame_len / i.i_encode_frames in
			let last_char = (b * frame_len + i.i_encode_frames - 1) / i.i_encode_frames - 1 in
			for j = first_char to last_char do
				frames.[j] <- '.'
			done
		) noncont;

		(* Now add the outputted stuff *)
		let last_char = outputted * frame_len / i.i_encode_frames - 1 in
		for j = 0 to last_char do
			frames.[j] <- '@'
		done;

		(* Add agent locations *)
		List.iter (fun (_,_,workers) ->
			Array.iter (function
				| Worker_frame x -> (
					let pos = x * frame_len / i.i_encode_frames in
					frames.[pos] <- '|'
				)
				| _ -> ()
			) workers
		) agent_status;

		let p = i.p ~screen:true ~lock:false in
		Mutex.lock i.pm;

	(*
		Rbtree.kpretty_print (i.p ~screen:false ~lock:false) frame_tree (sprintf "%*d" i.i_avs_frame_num_len) (fun g -> sprintf "%*d @ %S" i.i_avs_frame_num_len g.gop_frames (Filename.basename g.gop_filename));
	*)

		p "Working on ranges:";
		List.iter (fun (a,b) ->
			p // sprintf "  [%d,%d]" a b
		) noncont;
		p "Agents:";
		List.iter (fun (id,(ip,port),workers) ->
			p // sprintf "  %s ~ %s:%d" (to_hex id) ip port;
			Array.iter (function
				| Worker_disconnected -> p "    Disconnected"
				| Worker_connected    -> p "    Connected"
				| Worker_frame x      -> p // sprintf "    On frame %d" x
				| Worker_done         -> p "    Done with job"
				| Worker_dead         -> p "    DEAD"
			) workers;
		) agent_status;
		p (" " ^ frames);
		Mutex.unlock i.pm;
	) in
	(* !PRETTY PRINT *)

	let pretty_print = (
		let next_print_ref = ref 0.0 in
		let next_print_mutex = Mutex.create () in
		fun force -> (
(**)
			let now = Unix.gettimeofday () in
			Mutex.lock next_print_mutex;
			let ok = if force || now > !next_print_ref then (
				next_print_ref := now +. 5.0;
				true
			) else false in
			Mutex.unlock next_print_mutex;
			if ok then (
				pretty_print_base ()
			)
(**)
		)
	) in


	let add_agent agent = (
		Mutex.lock agents_mutex;
		Hashtbl.add agents_by_id agent.agent_id agent;
		Hashtbl.add agents_by_ip agent.agent_ip agent;
		Mutex.unlock agents_mutex;
	) in

	(* Don't know if I'll need/use this one *)
	let remove_agent agent = (
		Mutex.lock agents_mutex;
		Hashtbl.remove agents_by_id agent.agent_id;
		Hashtbl.remove agents_by_ip agent.agent_ip;
		Mutex.unlock agents_mutex;
	) in

	let agent_exists agent = (
		Mutex.lock agents_mutex;
		let exists = (Hashtbl.mem agents_by_id agent.agent_id) || (Hashtbl.mem agents_by_ip agent.agent_ip) in
		Mutex.unlock agents_mutex;
		exists
	) in

	let agent_exists_by_key id ip = (
		Mutex.lock agents_mutex;
		let exists = (Hashtbl.mem agents_by_id id) || (Hashtbl.mem agents_by_ip ip) in
		Mutex.unlock agents_mutex;
		exists
	) in

	let get_agent_by_key id ip = (
		Mutex.lock agents_mutex;
		let answer = (match trap_exception2 Hashtbl.find agents_by_id id with
			| Normal x -> Some x
			| _ -> (
				match trap_exception2 Hashtbl.find agents_by_ip ip with
				| Normal x -> Some x
				| _ -> None
			)
		) in
		Mutex.unlock agents_mutex;
		answer
	) in


	let update_worker_status_unsafe agent i s = (
		let this_worker = agent.agent_workers.(i) in
		this_worker.worker_status <- s;
	) in
	let update_worker_status agent i s = (
		Mutex.lock agents_mutex;
		let old_s = agent.agent_workers.(i).worker_status in
		update_worker_status_unsafe agent i s;
		Mutex.unlock agents_mutex;
		(* If this is just updating the frame numbers on a worker, the printing is not necessary *)
		match (old_s, s) with
		| (Worker_frame _, Worker_frame _) -> pretty_print false
		| _ -> pretty_print true
	) in



	let update_worker_location agent i f = (
		Mutex.lock agents_mutex;
		let this_worker = agent.agent_workers.(i) in
		let old_location = this_worker.worker_current in
		this_worker.worker_current <- Some f;
		let current_time = Unix.gettimeofday () in
		(match old_location with
			| None -> ()
			| Some old -> (
				(* Update the number of frames done *)
				this_worker.worker_total_time <- this_worker.worker_total_time +. current_time -. this_worker.worker_last_updated;
				this_worker.worker_total_frames <- this_worker.worker_total_frames - old + f;
			)
		);
		this_worker.worker_last_updated <- current_time;
		Mutex.unlock agents_mutex;
	) in

	(* A combination of update_worker_location and replace_gop_here *)
	let worker_replace_gop_here agent j n g z = (
		mutex_lock_2 frame_mutex agents_mutex;
		let replaced = replace_gop_here_unsafe n g z in
		i.p // sprintf "%s %d is now on frame %d" agent.agent_name j (n + g.gop_frames);
		Mutex.unlock frame_mutex;
		let this_worker = agent.agent_workers.(j) in
		let old_location = this_worker.worker_current in
		let new_location = n + g.gop_frames in
		this_worker.worker_current <- Some new_location;
		update_worker_status_unsafe agent j // Worker_frame new_location;
		let current_time = Unix.gettimeofday () in
		(match old_location with
			| None -> ()
			| Some old -> (
				(* Update the number of frames done *)
				this_worker.worker_total_time <- this_worker.worker_total_time +. current_time -. this_worker.worker_last_updated;
				this_worker.worker_total_frames <- this_worker.worker_total_frames + g.gop_frames;
			)
		);
		this_worker.worker_last_updated <- current_time;
		Mutex.unlock agents_mutex;
		pretty_print false;
		replaced
	) in

	(***************************)
	(* GRAB A GOOD START POINT *)
	(***************************)
	let get_start_point p agent n max_agents = (
		mutex_lock_2 frame_mutex agents_mutex;
		let ranges = list_noncontiguous_unsafe () in
		Mutex.unlock frame_mutex;

		(* Save the first un-finished frame to give it priority *)
		let first_frame_unfinished_perhaps = match ranges with
			| (a,b) :: tl -> (
				p // sprintf "GETSTART found first unfinished frame at %d" a;
				Some a
			)
			| _ -> None
		in
		let agent_locations = Hashtbl.fold (fun _ a locations_so_far ->
			Array.fold_left (fun locations_so_far w ->
	(*
				match (w.worker_current, w.worker_start) with
				| (Some x, _) | (_, Some x) -> x :: locations_so_far
				| _ -> locations_so_far
	*)
				match w.worker_status with
				| Worker_frame x -> x :: locations_so_far
				| _ -> locations_so_far
			) locations_so_far a.agent_workers
		) agents_by_id [] in

		(* Chop up ranges based on the agent locations *)
		let ranges_split_at_agents = List.fold_left (fun prev_ranges new_location ->
			let rec chop_a_range = function
				| [] -> []
				| (a,b) :: tl when a < new_location && b >= new_location -> (a, pred new_location) :: (new_location, b) :: tl
				| hd :: tl -> hd :: chop_a_range tl
			in
			chop_a_range prev_ranges
		) ranges agent_locations in

		p "GETSTART Ranges split at agents:";
		List.iter (fun (a,b) -> p // sprintf "GETSTART   [%d,%d]" a b) ranges_split_at_agents;

		(* Now shrink the ranges if the agent is at the beginning *)
		(* But only if the range is larger than a certain threshold
		 * This is to re-encode any range that an agent may have silently died on *)
		let frame_limit = 100 in
		let ranges_shrunk = List.fold_left (fun prev_ranges new_location ->
			let rec shrink_a_range = function
				| [] -> []
(*				| (a,b) :: tl when a = new_location && a <> b -> ((a + b) / 2, b) :: tl*)
				| (a,b) :: tl when a = new_location && a <> b && b - a > frame_limit -> ((a + b + 1) / 2, b) :: tl
				| hd :: tl -> hd :: shrink_a_range tl
			in
			shrink_a_range prev_ranges
		) ranges_split_at_agents agent_locations in

		p "GETSTART Ranges shrunk at agents:";
		List.iter (fun (a,b) -> p // sprintf "GETSTART   [%d,%d]" a b) ranges_shrunk;

		let start_here = match (first_frame_unfinished_perhaps, ranges_shrunk) with
			| (Some f, (a,b) :: tl) when f = a && b - a > frame_limit -> (
				(* The first range is unclaimed! DO IT *)
				(* Note that this only happens if the range is somewhat large *)
				p // sprintf "GETSTART doing first range (at %d) no matter what anybody says" f;
				agent.agent_workers.(n).worker_status <- Worker_frame f;
				Some (f, true)
			)
			| _ -> (
				(* Get the largest range which the fewest agents are currently doing *)
				let ((largest_a, largest_b), len, num_agents) = List.fold_left (fun (range_so_far, len_so_far, agents_so_far) (new_a, new_b) ->
					let new_len = new_b - new_a + 1 in
					if new_len > frame_limit then (
						(* Don't bother with counting agents, since a range this large would have been split *)
						if new_len > len_so_far then (
							((new_a, new_b), new_len, 0)
						) else (
							(range_so_far, len_so_far, agents_so_far)
						)
					) else (
						(* The range may have extra agents at the beginning *)
						let num_agents = List.fold_left (fun n x -> if x = new_a then succ n else n) 0 agent_locations in
						if num_agents = agents_so_far then (
							(* This range is tied for the number of agents; use the longer one *)
							if new_len > len_so_far then (
								((new_a, new_b), new_len, num_agents)
							) else (
								(range_so_far, len_so_far, agents_so_far)
							)
						) else if num_agents < agents_so_far then (
							(* This range has fewer agents working on it than the last; use it even if it's short *)
							((new_a, new_b), new_len, num_agents)
						) else (
							(* This range has too many agents; ignore *)
							(range_so_far, len_so_far, agents_so_far)
						)
					)
				) ((-1,-1),-1,max_int) ranges_shrunk in

				if len = -1 then (
					(* Oops. Nothing more to do, I guess *)
					p "GETSTART nothing else to do...";
					None
				) else if num_agents > max_agents then (
					(* Found a range, but there were too many agents working on it *)
					p "GETSTART found a range with a bunch of other agents on it";
					Some (largest_a, false)
				) else (
					(* The agent actually checked it out *)
					p "GETSTART found a good range";
					agent.agent_workers.(n).worker_status <- Worker_frame largest_a;
					Some (largest_a, true)
				)
			)
		in

		Mutex.unlock agents_mutex;
		start_here
	) in

	(**************)
	(* READ STATS *)
	(**************)
	(try
	(*

	Matches lines of the form:
	GOP 001234 05678 zones <filename>

	OR:
	GOP 001234 05678  <filename>
	(note the two spaces before the filename)

	*)
		let gop_rx = [
			Rx.constant false "GOP ";
			Rx.characters_plus true [Rx.Range ('0','9')];
			Rx.constant false " ";
			Rx.characters_plus true [Rx.Range ('0','9')];
	(*
			Rx.constant false " ";
			Rx.dot_star_hook true;
	*)
			Rx.constant false " <";
			Rx.dot_star_hook true;
			Rx.constant false ">";

			Rx.character_hook false [Rx.Char ' '];
			Rx.dot_star true;
		] in
		let gopend_rx = [
			Rx.constant false "END ";
			Rx.characters_plus true [Rx.Range ('0','9')];
			Rx.constant false " ";
			Rx.characters_plus true [Rx.Range ('0','9')];
	(*
			Rx.constant false " ";
			Rx.dot_star_hook true;
	*)
			Rx.constant false " <";
			Rx.dot_star_hook true;
			Rx.constant false ">";

			Rx.character_hook false [Rx.Char ' '];
			Rx.dot_star true;
		] in
		while true do
			let line = input_line stats_in in
			match Rx.rx gop_rx line with
			| Some [Rx.String seek_string; Rx.String frames_string; Rx.String filename; Rx.String zones] -> (
				let seek = int_of_string seek_string in
				let frames = int_of_string frames_string in
				if not (is_file filename) then (
					i.p // sprintf "%S does not seem to exist; throw out" line
				) else (
					if seek < i.i_encode_seek then (
						i.p // sprintf "%S is before the encode starts; throw out" line
					) else if seek + frames >= i.i_encode_seek + i.i_encode_frames then (
						i.p // sprintf "%S is after the encode ends; throw out" line
					) else (
						i.p // sprintf "%S is fine frame-wise" line;
						let compare_zones = render_zones // confine_zones o.o_zones (seek - i.i_encode_seek) frames in
						i.p // sprintf "  Comparing found zones %S with correct zones %S" zones compare_zones;
						(* Don't use put_gop_here since it updates stats_out, which will double all stats lines valid for the encode *)
(*						put_gop_here (seek - i.i_encode_seek) {gop_frames = frames; gop_filename = filename}*)
						if zones = compare_zones then (
							Rbtree.add frame_tree (seek - i.i_encode_seek) {gop_frames = frames; gop_filename = filename}
						) else (
							i.p "  Zones do not match up"
						)
					);
					output_string stats_out line;
					output_char   stats_out '\n';
				)
			)
			| None -> (
				i.p // sprintf "No GOP found in %S" line;
				match Rx.rx gopend_rx line with
				| Some [Rx.String seek_string; Rx.String frames_string; Rx.String filename; Rx.String zones] -> (
					let seek = int_of_string seek_string in
					let frames = int_of_string frames_string in
					if seek + frames = i.i_encode_seek + i.i_encode_frames then (
						(* The end is the same *)
						if not (is_file filename) then (
							i.p // sprintf "%S does not seem to exist; throw out END" line
						) else (
							if seek < i.i_encode_seek then (
								i.p // sprintf "%S is before the encode starts; throw out END" line
							) else (
								i.p // sprintf "%S is fine frame-wise END" line;
								let compare_zones = render_zones // confine_zones o.o_zones (seek - i.i_encode_seek) frames in
								i.p // sprintf "  Comparing found zones %S with correct zones %S" zones compare_zones;
								(* Don't use put_gop_here since it updates stats_out, which will double all stats lines valid for the encode *)
								if zones = compare_zones then (
									Rbtree.add frame_tree (seek - i.i_encode_seek) {gop_frames = frames; gop_filename = filename}
								) else (
									i.p "  Zones do not match up"
								)
							)
						)
					) else (
						(* Not part of this encode, but output anyway *)
						i.p // sprintf "%S does not end at the end of this encode; throw out END" line
					);
					output_string stats_out line;
					output_char   stats_out '\n';
				)
				| _ -> ()
			)
			| _ -> ()
		done
	with
		End_of_file -> ()
	);
	close_in stats_in;
	fprintf stats_out "#VERSION %d\n%!" stats_out_version;


	(***********************************************)
	(* DELETE UNUSED FILES FROM THE TEMP DIRECTORY *)
	(***********************************************)
	(* Check for GOPs which are already done *)
	if o.o_clean then (
		let d = Sys.readdir i.i_encode_temp_dir in
		let rx = Rx.rx [
			Rx.characters_plus true [Rx.Range ('0','9')];
			Rx.constant false " ";
			Rx.characters_exact false [Rx.Range ('0','9'); Rx.Range ('a','f'); Rx.Range ('A','F')] 6;
			Rx.constant false ".mkc"
		] in
		Array.iter (fun f ->
			(* Try to match up the format of the file *)
			match rx f with
			| Some [Rx.String num_string] -> (
				let n = int_of_string num_string in
				let f_full = Filename.concat i.i_encode_temp_dir f in
				match Rbtree.find frame_tree (n - i.i_encode_seek) with
				| Some (_,{gop_filename = gop_file}) -> (
					if f_full = gop_file then (
						(* They be the same *)
						i.p // sprintf "O_DELETE keeping \"%s\"" f;
					) else (
						(* The files don't match up *)
						i.p // sprintf "O_DELETE deleting \"%s\"" f;
						ignore // trap_exception Sys.remove f_full
					)
				)
				| _ -> (
					(* DNE *)
					i.p // sprintf "O_DELETE deleting \"%s\" (not in tree)" f;
					ignore // trap_exception Sys.remove f_full
				)
			)
			| _ -> (
				(* Not a GOP file *)
				i.p // sprintf "O_DELETE not deleting \"%s\" (not a GOP file)" f;
			)
		) d
	);


	(**********)
	(* DISHER *)
	(**********)
	let disher_guts (agent,n) = (
		let print_name = sprintf "%s %d " agent.agent_name n in
		let p = i.p ~name:print_name in
		p // sprintf "started up (id %d)" (Thread.id (Thread.self ()));
		let update = update_worker_status agent n in

		let timeout = 60.0 in

		update Worker_disconnected;

		let rec connection_loop () = (
			let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in

			Mutex.lock worker_socks_mutex;
			Hashtbl.replace worker_socks (Thread.id (Thread.self ())) sock;
			Mutex.unlock worker_socks_mutex;

			let keep_connecting = (try
				let rec attempt_connection tries_so_far = ( (* "None" if stop encoding, "Some s" if it did something properly *)
					if encoding_done () then (
						p "encoding seems to be done; don't bother trying to connect";
						None
					) else (
						let s_perhaps = (
							try
								Unix.connect sock (Unix.ADDR_INET (Unix.inet_addr_of_string (fst agent.agent_ip), (snd agent.agent_ip)));
								Some (new net sock)
							with
								_ -> None
						) in
						match s_perhaps with
						| None -> (
							if tries_so_far >= 24 then (
								(* Try to connect for 2 minutes, then give up *)
								raise Dead_worker
							) else (
								Thread.delay 5.0;
								attempt_connection (succ tries_so_far)
							)
						)
						| Some s -> Some s
					)
				) in
				match attempt_connection 0 with
				| None -> (
					(* Nothing more to be done *)
					p "marking agent to exit";
					false
				)
				| Some s -> (
					(* Got a socket *)
					p "socket connected";
					update Worker_connected;

					(* Now try to find a job *)
					let rec per_job () = (
	(*
						let rec get_job last_job desperateness = (
							match get_start_point p agent n with
							| None -> None
							| Some (j,0) -> (p // sprintf "got job at %d" j; Some j)
							| Some (j,w) when w <= desperateness -> (p // sprintf "got job at %d - %d others are already doing it" j w; Some j)
							| Some (j,w) when j = last_job -> (
								p // sprintf "got job at %d, which is the same job as before; adding to desperateness" j;
								Thread.delay 20.0;
								p "retrying checkout";
								get_job j (succ desperateness)
							)
							| Some (j,w) -> (
								p // sprintf "got job at %d, which is not the same job as before; resetting desperateness" j;
								Thread.delay 20.0;
								p "retrying checkout";
								get_job j 0
							)
						) in
	*)
						let rec get_job last_job desperateness = (
							match get_start_point p agent n desperateness with
							| None -> None
							| Some (j,true) -> (p // sprintf "got job at %d" j; Some j)
							| Some (j,false) when j = last_job -> (
								(* The best job is still the one which we got last time *)
								p // sprintf "got job %d, which is the same as before; adding to desperateness" j;
								Thread.delay 20.0;
								p "retrying checkout";
								get_job j (succ desperateness)
							)
							| Some (j,false) -> (
								p // sprintf "got job %d, which is different from before; resetting desperateness" j;
								Thread.delay 20.0;
								p "retrying checkout";
								get_job j 0
							)
						) in
						match get_job ~-1 0 with
						| None -> (
							p "encoding's done"
						)
						| Some j -> (
							p // sprintf "doing job %d" j;

							update // Worker_frame j;

							let avs_seek = j + i.i_encode_seek in
							let avs_frames = i.i_encode_frames - j in

							(* Render the zones *)
							let zones_here = confine_zones o.o_zones j avs_frames in
							let zone_string = render_zones zones_here in
							let send_zone_string = if zone_string = "" then "" else " --zones " ^ zone_string in
							p // sprintf "zone string is %S" zone_string;


							(* Make a big pile of data to send *)
							let send_xml = (
								let file_hash = Digest.to_hex (Digest.file o.o_i) in
								Xml.Element ("job", [], [
									Xml.Element ("version", [("major", "1"); ("minor", "1")], []);
									Xml.Element ("filename", [("hash", file_hash)], [Xml.PCData o.o_i]);
									Xml.Element ("seek", [], [Xml.PCData (string_of_int avs_seek)]);
									Xml.Element ("frames", [], [Xml.PCData (string_of_int avs_frames)]);
									Xml.Element ("options", [], [Xml.PCData (o.o_x ^ send_zone_string)]);
									Xml.Element ("resolution", [("x", string_of_int i.i_res_x); ("y", string_of_int i.i_res_y)], []);
									Xml.Element ("framerate", [("n", string_of_int i.i_fps_n); ("d", string_of_int i.i_fps_d)], []);
								]);
							) in
							p // sprintf "sending this: %S" (Xml.to_string send_xml);

							(* NETWORK CODE STARTS HERE *)
							(try
								s#send "OPTS" (Xml.to_string send_xml);

								let port_perhaps = match s#recv_timeout timeout with
									| ("PORT",x) when x = "\x00\x00" -> None
									| ("PORT",x) when String.length x = 2 -> Some (Pack.unpackn x 0)
									| e -> unexpected_packet ~here:"receiving port info" e
								in

								(match port_perhaps with
									| None -> (
										(* Agent-based *)
										p "agent is attempting agent-based encoding";

										(match s#recv_timeout timeout with
											| ("TRAX",x) -> (
												p "got track element";
												update_tracks x;
											)
											| x -> unexpected_packet ~here:"recv TRAX agent" x
										);

										(* Make something which will receive the frames *)
										let rec recv_frames from_frame filename handle = (
											match s#recv_timeout timeout with
											| ("IFRM",x) when String.length x = 4 -> (
												(* Close old handle, make new one *)
												Unix.close handle;
												let to_frame = Pack.unpackN x 0 in
												let zone_string = render_zones // confine_zones zones_here (from_frame - i.i_encode_seek - j) (to_frame - from_frame) in
												let replaced = worker_replace_gop_here agent n (from_frame - i.i_encode_seek) {gop_frames = to_frame - from_frame; gop_filename = filename} zone_string in
												p // sprintf "added GOP %d+%d = %S" (from_frame - i.i_encode_seek) (to_frame - from_frame) filename;

												if replaced || is_gop_here (to_frame - i.i_encode_seek) || encoding_done () then (
													p "GOP already exists there, or there's a GOP next, or encoding is done; finishing up job";
													s#send "STOP" "";
													(match s#recv with
														| ("EEND",_) -> ()
														| x -> unexpected_packet ~here:"recv EEND after STOP(IFRM) agent" x
													);
													()
												) else (
													(* Keep going *)
													s#send "CONT" "";
													let a = Filename.concat i.i_encode_temp_dir (sprintf "%0*d " i.i_avs_frame_num_len to_frame) in
													let (filename_new, handle_new) = open_temp_file_unix a ".mkc" [] 0o600 in
													p // sprintf "temp file is %S" filename_new;
													recv_frames to_frame filename_new handle_new
												)
											)
											| ("GOPF",x) -> (
												(* Add to the current file *)
												if encoding_done () || is_gop_here (from_frame - i.i_encode_seek) then (
													(* Or not. Encoding's done. *)
													Unix.close handle;
													s#send "STOP" "";
													(match s#recv with
														| ("EEND",_) -> ()
														| x -> unexpected_packet ~here:"recv EEND after STOP(GOPF) agent" x
													);
													()
												) else (
													s#send "CONT" "";
													ignore // Unix.write handle x 0 (String.length x);
													recv_frames from_frame filename handle
												)
											)
											| ("EEND",x) when String.length x = 4 -> (
												(* Similar to "IFRM" *)
												Unix.close handle;
												let to_frame = Pack.unpackN x 0 in
												let zone_string = render_zones // confine_zones zones_here (from_frame - i.i_encode_seek - j) (to_frame - from_frame) in
												ignore // worker_replace_gop_here agent n (from_frame - i.i_encode_seek) {gop_frames = to_frame - from_frame; gop_filename = filename} zone_string;
												()
											)
											| ("USEI",sei) -> (
												(* New SEI info *)
												p // sprintf "got SEI info %S" sei;
												recv_frames from_frame filename handle
											)
											| x -> unexpected_packet ~here:"recv_frames agent" x
										) in
										(match s#recv_timeout timeout with
											| ("IFRM",x) when String.length x = 4 -> (
												let to_frame = Pack.unpackN x 0 in
												if is_gop_here (to_frame - i.i_encode_seek) || encoding_done () then (
													p "GOP already exists, or encoding is done (that was fast); finishing up job";
													s#send "STOP" "";
													(match s#recv with
														| ("EEND",_) -> ()
														| x -> unexpected_packet ~here:"recv EEND after STOP(IFRM) agent" x
													);
													()
												) else (
													s#send "CONT" "";
													let a = Filename.concat i.i_encode_temp_dir (sprintf "%0*d " i.i_avs_frame_num_len to_frame) in
													let (filename_new, handle_new) = open_temp_file_unix a ".mkc" [] 0o600 in
													p // sprintf "temp file is %S" filename_new;
													recv_frames to_frame filename_new handle_new
												)
											)
											| x -> unexpected_packet ~here:"recv first frame agent" x
										);
										(* Done receiving stuff *)
										update Worker_connected
									)
									| Some port -> (
										(* Controller-based *)
										p // sprintf "got port %d; sending data" port;

										let video_thread_keep_going_ref = ref true in
										let video_thread_keep_going_mutex = Mutex.create () in
										let video_thread_keep_going_condition = Condition.create () in

										(* AVS2YUV-ish *)
(*
										let avs_descr = Unix.descr_of_in_channel (Unix.open_process_in (sprintf "\"%s -raw -seek %d -frames %d -o - \"%s\" 2>%s\"" i.i_avs2yuv avs_seek avs_frames o.o_i dev_null)) in
*)
	(* Make a temp file *)
	(*let (_,toomp) = open_temp_file ~mode:[Open_binary] "TOOMP " ".raw" in*)

										(* AVS-ish *)
(*
										let script = Avs.open_avs o.o_i in
										let pos = Avs.pos_frame avs_seek in
										let last_frame = i.i_encode_seek + i.i_encode_frames in
*)
										(* AVI-ish *)
										p // sprintf "opening AVS %S" o.o_i;
										Mutex.lock Avi.bm;
										let avi = Avi.open_avi o.o_i in
										Mutex.unlock Avi.bm;
										p "opened AVS";
										let pos = ref avs_seek in
										let last_frame = i.i_encode_seek + i.i_encode_frames in

										(* Make a video thread *)
										let video_guts () = (
											p "starting up video thread";

											let video_sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
											(try
												let addr = Unix.ADDR_INET (Unix.inet_addr_of_string (fst agent.agent_ip), port) in
												Unix.connect video_sock addr;
												ignore // trap_exception (fun n -> Unix.setsockopt_int video_sock Unix.SO_SNDBUF n) 1048576;

												let f_write str n = (
(*													p // sprintf "ASYNC writing %d bytes" n;*)
													Mutex.lock video_thread_keep_going_mutex;
													let keep_going = !video_thread_keep_going_ref in
													Mutex.unlock video_thread_keep_going_mutex;
													if keep_going then (

														(* AVI *)
														if !pos >= last_frame then (
															p "F_WRITE pos > last_frame";
															0
														) else (
															Mutex.lock Avi.bm;
															let avi = Avi.open_avi o.o_i in
															let wrote_frames = Avi.blit_frame avi !pos str 0 in
(*															let wrote_frames = Avi.blit_frames avi !pos 10 str 0 in*)
															Avi.close_avi avi;
															Mutex.unlock Avi.bm;
															p // sprintf "F_WRITE got %d frames" wrote_frames;
															if wrote_frames > 0 then (
																p "F_WRITE OK";
																pos := !pos + wrote_frames;
																wrote_frames * avi.Avi.bytes_per_frame
															) else (
																p "F_WRITE NOTOK!";
																0
															)
														)
														(* AVS *)
(*
														let wrote_bytes = o.o_blit ~last_frame:last_frame script pos str 0 n in
(*														p // sprintf "ASYNC got %d bytes" wrote_bytes;*)
														wrote_bytes
*)


														(* AVS2YUV *)
(*
														try
															let rec helper from num = (
																if num = 0 then from else (
																	match trap_exception (fun () -> Unix.read avs_descr str from num) () with
																	| Normal 0 | Exception _ -> from
																	| Normal got_bytes -> helper (from + got_bytes) (num - got_bytes)
																)
															) in
															helper 0 n
														with
															e -> (
(*																p // sprintf "ASYNC exception %S" (Printexc.to_string e);*)
																0
															)
*)
														) else (
(*														p "ASYNC or not (dead)";*)
														0
													)
												) in
(*												let send_these = async ~priority:Opt.Thread_priority_below_normal 2 (262144 * 8) f_write in*)
												let send_these = async ~priority:Opt.Thread_priority_below_normal 4 (avi.Avi.bytes_per_frame * 4) f_write in


												let rec video_helper () = (
													Mutex.lock video_thread_keep_going_mutex;
													let keep_going = !video_thread_keep_going_ref in
													Mutex.unlock video_thread_keep_going_mutex;
													if keep_going then (
														match send_these () with
														| None -> ()
														| Some (str,n,f) -> (
(*															p // sprintf "ASYNC got %d bytes" n;*)
															let rec send_helper off len = (
																if len = 0 then () else (
																	Mutex.lock video_thread_keep_going_mutex;
																	let keep_going = !video_thread_keep_going_ref in
																	Mutex.unlock video_thread_keep_going_mutex;
																	if keep_going then (
																		match Unix.select [] [video_sock] [] 1.0 with
																		| (_,[_],_) -> (
																			let sent = Unix.send video_sock str off len [] in
																			if sent = 0 then (
																				raise End_of_file
																			) else (
	(*output toomp str off sent;*)
																				send_helper (off + sent) (len - sent)
																			)
																		)
																		| _ -> (
																			Mutex.lock video_thread_keep_going_mutex;
																			let keep_going = !video_thread_keep_going_ref in
																			Mutex.unlock video_thread_keep_going_mutex;
																			if keep_going then (
																				send_helper off len
																			)
																		)
																	)
																)
															) in
															send_helper 0 n;
															f ();
(*															p "sent bytes";*)
															video_helper ()
														)
													) else (
														p "VIDEO helper sees stop";
														()
													)
												) in
												video_helper ();
											with
												e -> (
													p // sprintf "closing video socket (got exception %S)" (Printexc.to_string e);
													Unix.close video_sock;
													raise e
												)
											);
											p "closing video socket (normal)";
											Unix.close video_sock;
										) in

										let video_thread = Thread.create (fun () ->
											i.p // sprintf "Video thread %d starting" (Thread.id (Thread.self ()));
											i.p "Video thread starting AVI";
											Avi.init_avi ();
											i.p "Video thread started AVI";
											(try
												video_guts ()
											with
												e -> p // sprintf "video thread died with %S" (Printexc.to_string e)
											);
											i.p "Video thread exiting AVI";
											Avi.exit_avi ();
											i.p "Video thread exited AVI";
											i.p // sprintf "Video thread %d exiting" (Thread.id (Thread.self ()));
										) () in

(*
										(* Make a slave *)
										let slave_guts () = (
											let addr = Unix.ADDR_INET (Unix.inet_addr_of_string (fst agent.agent_ip), port) in
											let str = sprintf "\"%s\" --slave" Sys.executable_name in
											let opts = Some {
												slave_bytes_per_frame = i.i_bytes_per_frame;
												slave_i = o.o_i;
												slave_addr = addr;
												slave_first_frame = avs_seek;
												slave_last_frame = i.i_encode_seek + i.i_encode_frames;
												slave_zones = zone_string;
												slave_name = print_name ^ "SLAVE ";
											} in
											p // sprintf "SLAVE running the following: %S" str;
											p // sprintf "SLAVE and sending %S" (Marshal.to_string opts []);
											let out_handle = Unix.open_process_out str in
											set_binary_mode_in stdin true;
											Marshal.to_channel out_handle opts []; (* (o,c,i,addr,avs_seek,zone_string) *)
											flush out_handle;

											p "testing video_thread_keep_going_ref";
											Mutex.lock video_thread_keep_going_mutex;
											while !video_thread_keep_going_ref do
												p "waiting for video_thread_keep_going to do something exciting";
												Condition.wait video_thread_keep_going_condition video_thread_keep_going_mutex;
												p "starting up on video_thread_keep_going";
											done;
											Mutex.unlock video_thread_keep_going_mutex;

											match Unix.close_process_out out_handle with
											| Unix.WEXITED x -> p // sprintf "SLAVE exited with %d" x;
											| Unix.WSIGNALED x -> p // sprintf "SLAVE signaled with %d" x;
											| Unix.WSTOPPED x -> p // sprintf "SLAVE stopped with %d" x;
										) in
										let slave_thread = Thread.create (fun () ->
											try
												slave_guts ()
											with
												e -> (p // sprintf "SLAVE got exception %S" (Printexc.to_string e))
										) () in
*)

										(try

											(* Now get the frame elements *)
											(* TRAX *)
											(match s#recv_timeout timeout with
												| ("TRAX",x) -> (
													p "got track element";
													update_tracks x;
												)
												| x -> unexpected_packet ~here:"recv TRAX agent" x
											);


											(* Make something which will receive the frames *)
											let rec recv_frames from_frame filename handle = (
												match s#recv_timeout timeout with
												| ("IFRM",x) when String.length x = 4 -> (
													(* Close old handle, make new one *)
													Unix.close handle;
													let to_frame = Pack.unpackN x 0 in
													let zone_string = render_zones // confine_zones zones_here (from_frame - i.i_encode_seek - j) (to_frame - from_frame) in
													let replaced = worker_replace_gop_here agent n (from_frame - i.i_encode_seek) {gop_frames = to_frame - from_frame; gop_filename = filename} zone_string in
													p // sprintf "added GOP %d+%d = %S" (from_frame - i.i_encode_seek) (to_frame - from_frame) filename;

													if replaced || is_gop_here (to_frame - i.i_encode_seek) || encoding_done () then (
														p "GOP already exists there, or there's a GOP next, or encoding is done; finishing up job";
														s#send "STOP" "";
														(match s#recv with
															| ("EEND",_) -> ()
															| x -> unexpected_packet ~here:"recv EEND after STOP(IFRM) agent" x
														);
														()
													) else (
														(* Keep going *)
														s#send "CONT" "";
														let a = Filename.concat i.i_encode_temp_dir (sprintf "%0*d " i.i_avs_frame_num_len to_frame) in
														let (filename_new, handle_new) = open_temp_file_unix a ".mkc" [] 0o600 in
														p // sprintf "temp file is %S" filename_new;
														recv_frames to_frame filename_new handle_new
													)
												)
												| ("GOPF",x) -> (
													(* Add to the current file *)
													if encoding_done () || is_gop_here (from_frame - i.i_encode_seek) then (
														(* Or not. Encoding's done. *)
														Unix.close handle;
														s#send "STOP" "";
														(match s#recv with
															| ("EEND",_) -> ()
															| x -> unexpected_packet ~here:"recv EEND after STOP(GOPF) agent" x
														);
														()
													) else (
														s#send "CONT" "";
														ignore // Unix.write handle x 0 (String.length x);
														recv_frames from_frame filename handle
													)
												)
												| ("EEND",x) when String.length x = 4 -> (
													(* Similar to "IFRM" *)
													Unix.close handle;
													let to_frame = Pack.unpackN x 0 in
													let zone_string = render_zones // confine_zones zones_here (from_frame - i.i_encode_seek - j) (to_frame - from_frame) in
													ignore // worker_replace_gop_here agent n (from_frame - i.i_encode_seek) {gop_frames = to_frame - from_frame; gop_filename = filename} zone_string;
													()
												)
												| ("USEI",sei) -> (
													(* New SEI info *)
													p // sprintf "got SEI info %S" sei;
													recv_frames from_frame filename handle
												)
												| x -> unexpected_packet ~here:"recv_frames agent" x
											) in
											(match s#recv_timeout timeout with
												| ("IFRM",x) when String.length x = 4 -> (
													let to_frame = Pack.unpackN x 0 in
													if is_gop_here (to_frame - i.i_encode_seek) || encoding_done () then (
														p "GOP already exists, or encoding is done (that was fast); finishing up job";
														s#send "STOP" "";
														(match s#recv with
															| ("EEND",_) -> ()
															| x -> unexpected_packet ~here:"recv EEND after STOP(IFRM) agent" x
														);
														()
													) else (
														s#send "CONT" "";
														let a = Filename.concat i.i_encode_temp_dir (sprintf "%0*d " i.i_avs_frame_num_len to_frame) in
														let (filename_new, handle_new) = open_temp_file_unix a ".mkc" [] 0o600 in
														p // sprintf "temp file is %S" filename_new;
														recv_frames to_frame filename_new handle_new
													)
												)
												| x -> unexpected_packet ~here:"recv first frame agent" x
											);

										with
										| Invalid_argument e -> p // sprintf "connected worker got exception Invalid_argument(%S); shutting down" e
										| e -> p // sprintf "connected worker got exception %S; shutting down" (Printexc.to_string e)
										);

										(* Tell the frame server to shut down *)
										Mutex.lock video_thread_keep_going_mutex;
										p "suggesting video thread to shut down";
										video_thread_keep_going_ref := false;
										Condition.signal video_thread_keep_going_condition;
										Mutex.unlock video_thread_keep_going_mutex;

										(* Done receiving stuff *)
										update Worker_connected;

										(* Wait for the video thread to croak *)
										p "waiting for video thread to croak";
(**)										Thread.join video_thread;(**)
(*										Thread.join slave_thread;*)
										p "video thread croaked";

	(*close_out toomp;*)

										Mutex.lock Avi.bm;
										p "closing AVI";
										Avi.close_avi avi;
										p "closed AVI";
										Mutex.unlock Avi.bm;
(*										Avs.close_avs script;*)
(*										Unix.close avs_descr;*)

									)
								); (* match port_perhaps *)
							with
								| Timeout -> (
									p "network timeout; dying";
									raise Timeout
								)
							);
							(* NETWORK CODE ENDS HERE *)
							p "uhh... done with job? Restart?";
							per_job ()
						) (* Got a job *)

					) in (* per_job *)
					per_job ();

					false (* As far as I know, the only way to get here is to get None for a job, which means encoding has finished *)
				) (* match attempt_connection *)
			with
				| Dead_worker -> (
					p "worker has not responded in a while; assume dead";
					false
				)
				| Invalid_argument x -> (
					p // sprintf "agent connection failed with \"Invalid_argument(%S)\"; closing socket" x;
					true
				)
				| e -> (
					p // sprintf "agent connection failed with %S; closing socket" (Printexc.to_string e);
					true
				)
			) in
			update Worker_disconnected;
			Unix.close sock;

			Mutex.lock worker_socks_mutex;
			Hashtbl.remove worker_socks (Thread.id (Thread.self ()));
			Mutex.unlock worker_socks_mutex;

			if keep_connecting then (
				p "trying to connect again";
				connection_loop ()
			) else (
				p "giving up connecting"
			)
		) in (* connection_loop *)
		connection_loop ();

		update Worker_dead;
	) in

	(***********)
	(* !DISHER *)
	(***********)

	(**********************************)
	(* BACKGROUND OUTPUT FILE CREATOR *)
	(**********************************)
	let rec output_guts () = (
		let p = i.p ~name:"BGOUTPUT " in

		let rec gcd a b = if b = 0L then a else gcd b (Int64.rem a b) in

		let max_cluster_length_n = 2L in
		let max_cluster_length_d = 1L in
		p // sprintf "Seconds per cluster = %Ld/%Ld" max_cluster_length_n max_cluster_length_d;

		(* Find the best timecode scale *)
		let (best_tcs, tcs_per_frame_n, tcs_per_frame_d, tcs_per_second_n, tcs_per_second_d) = (

			let ns_per_frame_int = 1000000000L *| !|(i.i_fps_d) /| !|(i.i_fps_n) in
			let (ns_per_frame_rem, ns_per_frame_d) = (
				let temp_rem = Int64.rem (1000000000L *| !|(i.i_fps_d)) !|(i.i_fps_n) in
				let ns_gcd = gcd temp_rem !|(i.i_fps_n) in
				(temp_rem /| ns_gcd, !|(i.i_fps_n) /| ns_gcd)
			) in
			let ns_per_frame_n = ns_per_frame_int *| ns_per_frame_d +| ns_per_frame_rem in
			p // sprintf "%Ld+%Ld/%Ld nanoseconds per frame" ns_per_frame_int ns_per_frame_rem ns_per_frame_d;
			p // sprintf "That's %Ld/%Ld" ns_per_frame_n ns_per_frame_d;

			if ns_per_frame_d = 1L then (
				p // sprintf "Which is a whole number. Using %Ld for timecode scale" ns_per_frame_n;
				(ns_per_frame_n, 1L, 1L, !|(i.i_fps_n), !|(i.i_fps_d))
			) else (
				let min_tcs = (
					(1953125L *| max_cluster_length_n -| 1L) /| (64L *| max_cluster_length_d) +| 1L (* the -| and +| here make the division round up *)
				) in

				let ns_per_frame_factors = (
					let rec factor_me x at so_far_list = if at > x then so_far_list else (
						if Int64.rem x at = 0L then (
							(* It divides *)
							match so_far_list with
							| (a,b) :: tl when a = at -> (
								(* Add to the current power *)
								factor_me (x /| at) at ((a, succ b) :: tl)
							)
							| _ -> (
								(* Make a new power *)
								factor_me (x /| at) at ((at, 1) :: so_far_list)
							)
						) else (
							(* Try the next integer *)
							factor_me x (Int64.succ at) so_far_list
						)
					) in
					factor_me ns_per_frame_n 2L []
				) in
				p // sprintf "ns_per_frame_n (%Ld) has the following factors:" ns_per_frame_n;
				let max_factors = List.fold_left (fun so_far (num,pow) ->
					p // sprintf "  %Ld^%d" num pow;
					so_far * (pow + 1)
				) 1 ns_per_frame_factors in

				let rec num_to_powers bases b10 so_far = (match bases with
					| ((bhd, mhd) :: btl) -> (
						let current_power = Int64.rem b10 !|(mhd + 1) in
						let next_b10 = b10 /| !|(mhd + 1) in
						let next_so_far = so_far *| (i64_pow bhd !-current_power) in
						num_to_powers btl next_b10 next_so_far
					)
					| _ -> so_far
				) in

				let rec find_smallest_greater_than factors target n goto so_far = (
					if n = goto then so_far else (
						let now_factor = num_to_powers factors !|n 1L in
						if now_factor > target && (now_factor < so_far || so_far < target) then (
							find_smallest_greater_than factors target (n + 1) goto now_factor
						) else (
							find_smallest_greater_than factors target (n + 1) goto so_far
						)
					)
				) in
				let best_tcs = find_smallest_greater_than ns_per_frame_factors min_tcs 0 max_factors 0L in
				p // sprintf "Best timecode scale is %Ld" best_tcs;

				let (tcs_per_frame_n, tcs_per_frame_d) = (
					let tcs_n = ns_per_frame_n in
					let tcs_d = ns_per_frame_d *| best_tcs in
					let divider = gcd tcs_n tcs_d in
					(tcs_n /| divider, tcs_d /| divider)
				) in
				let (tcs_per_second_n, tcs_per_second_d) = (
					let tcs_d = best_tcs in
					let divider = gcd 1000000000L tcs_d in
					(1000000000L /| divider, tcs_d /| divider)
				) in
				p // sprintf "TC per frame = %Ld/%Ld" tcs_per_frame_n tcs_per_frame_d;
				(best_tcs, tcs_per_frame_n, tcs_per_frame_d, tcs_per_second_n, tcs_per_second_d)
			)
		) in (* (best_tcs, tcs_per_frame_n, tcs_per_frame_d, tcs_per_second_n, tcs_per_second_d) *)

		(* Now get the number of timecodes per cluster *)
		let (tcs_per_cluster_n, tcs_per_cluster_d) = (
			let one_factor = gcd max_cluster_length_n tcs_per_second_d in
			let mcln = max_cluster_length_n /| one_factor in
			let tpsd = tcs_per_second_d /| one_factor in
			let two_factor = gcd max_cluster_length_d tcs_per_second_n in
			let mcld = max_cluster_length_d /| two_factor in
			let tpsn = tcs_per_second_n /| two_factor in
			let tpcn = mcln *| tpsn in
			let tpcd = mcld *| tpsd in
			(tpcn, tpcd)
		) in
		p // sprintf "TC per cluster = %Ld/%Ld" tcs_per_cluster_n tcs_per_cluster_d;
		
		(* Open the output file *)
		let out = Unix.openfile o.o_o [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC] 0o600 in

		(* OUTPUT TO CLUSTER *)
		let file_exception = (try
			let ebml = {
				Matroska.id = 0x0A45DFA3;
				Matroska.inn = Matroska.Elts [
					{Matroska.id = 0x0286; Matroska.inn = Matroska.Uint 1L}; (* EBML version *)
					{Matroska.id = 0x02F7; Matroska.inn = Matroska.Uint 1L}; (* EBML read version *)
					{Matroska.id = 0x02F2; Matroska.inn = Matroska.Uint 4L}; (* Maximum ID length *)
					{Matroska.id = 0x02F3; Matroska.inn = Matroska.Uint 8L}; (* Maximum size length *)
					{Matroska.id = 0x0282; Matroska.inn = Matroska.String "matroska"}; (* Doc type *)
					{Matroska.id = 0x0287; Matroska.inn = Matroska.Uint 2L}; (* Doc type version *)
					{Matroska.id = 0x0285; Matroska.inn = Matroska.Uint 2L}; (* Doc type read version *)
				]
			} in
			Matroska.output_binary_unix out (Matroska.render_element ebml);
			let segment_id = Matroska.string_of_id 0x08538067 in
			ignore // Unix.write out segment_id 0 (String.length segment_id);
			ignore // Unix.write out "\xFF" 0 1;

			(* INFO *)
			let info = {
				Matroska.id = 0x0549A966;
				Matroska.inn = Matroska.Elts [
					{Matroska.id = 0x0D80; Matroska.inn = Matroska.String "x264farm"}; (* Muxing app *)
					{Matroska.id = 0x1741; Matroska.inn = Matroska.String "x264"}; (* Writing app *)
					{Matroska.id = 0x0AD7B1; Matroska.inn = Matroska.Uint best_tcs}; (* Timecode scale *)
					{Matroska.id = 0x0489; Matroska.inn = Matroska.Float ( (* Duration *)
						(* Calculate duration as exactly as possible *)
						let duration_int = Int64.div (!|(i.i_encode_frames) *| tcs_per_frame_n) tcs_per_frame_d in
						let duration_rem = Int64.rem (!|(i.i_encode_frames) *| tcs_per_frame_n) tcs_per_frame_d in
						p // sprintf "Duration is %Ld+%Ld/%Ld" duration_int duration_rem tcs_per_frame_d;
(*						Int64.to_float (i64_div_round (!|(i.i_encode_frames) *| tcs_per_frame_n) tcs_per_frame_d)*)
						let f = Int64.to_float duration_int +. (Int64.to_float duration_rem /. Int64.to_float tcs_per_frame_d) in
						p // sprintf "That's %f (%016LX)" f (Int64.bits_of_float f);
						f
					)}; (* Duration *)
				]
			} in
			Matroska.output_binary_unix out (Matroska.render_element info);

			(* TRACKS *)
			(* Note: we need to wait on tracks_ref (with tracks_mutex) in order to make sure we get it *)
			let encode_tracks = (
				Mutex.lock tracks_mutex;
				let rec gimme_trax () = match !tracks_ref with
					| Some x -> x
					| None -> (
						p "Tracks not available yet; keep waiting";
						Condition.wait frame_condition tracks_mutex;
						gimme_trax ()
					)
				in
				let q = gimme_trax () in
				Mutex.unlock tracks_mutex;
				p "Tracks found; continuing writing the file";
				q
			) in
			let tracks = {
				Matroska.id = 0x0654AE6B;
				Matroska.inn = Matroska.String encode_tracks
			} in
			Matroska.output_binary_unix out (Matroska.render_element tracks);

			(* Add the frames *)
			(* buffer now includes the timecode for each cluster *)
			let buffer = Bigbuffer.create 262144 in

			let offset = !|(i.i_encode_seek) in

			(* Turns the buffer into a Matroska cluster *)
			let flush_buffer () = (
				let total_length = Bigbuffer.length buffer in

				if total_length > 0 then (
					(* Only output if there is something to output (I don't really want to waste 5 extra bytes) *)
					let cluster_id = Matroska.string_of_id 0x0F43B675 in
					ignore // Unix.write out cluster_id 0 (String.length cluster_id);
					let cluster_length = Matroska.string_of_size (Some !|total_length) in
					ignore // Unix.write out cluster_length 0 (String.length cluster_length);
					Bigbuffer.output_bigbuffer_unix out buffer;
					Bigbuffer.clear buffer;
				)
			) in

		(* Inputs a frame from the specified handle and outputs it to a cluster (starts up a new cluster if necessary *)
			let get_another_frame handle (cluster_timecode,next_cluster_timecode,next_next_cluster_number) = (
(*				p // sprintf "finding 0x1F43B675 at offset %Ld (length %Ld)" (LargeFile.pos_in handle) (LargeFile.in_channel_length handle);*)
				match Matroska.input_id_and_length handle with
				| Some (0x0F43B675, _) -> ( (* SEGMENT *)
(*					p // sprintf "finding 0xE7 at offset %Ld (length %Ld)" (LargeFile.pos_in handle) (LargeFile.in_channel_length handle);*)
					match Matroska.input_id_and_length handle with
					| Some (0x67, Some l) -> ( (* TIMECODE *)
						let frame_num = (Matroska.input_uint 0L handle !-l) -| offset in
(*						p // sprintf "finding 0xA3 at offset %Ld (length %Ld)" (LargeFile.pos_in handle) (LargeFile.in_channel_length handle);*)
						match Matroska.input_id_and_length handle with
						| Some (0x23, Some l) -> ( (* SIMPLEBLOCK *)
							let frame_timecode = i64_div_round (tcs_per_frame_n *| frame_num) tcs_per_frame_d in

							let (cluster_timecode, next_cluster_timecode, next_next_cluster_number) = if frame_timecode >= next_cluster_timecode then (
								if encoding_done () then (
									i.p ~screen:true // sprintf "Writing %Ld / %d" frame_num i.i_encode_frames;
								);
								(* Start a new cluster *)
								flush_buffer ();
								Mkvbb.output_id buffer 0x67;
								let tcs = Matroska.string_of_uint frame_timecode in
								Mkvbb.output_size buffer (Some !|(String.length tcs));
								Bigbuffer.add_string buffer tcs;
								let next_new_cluster_timecode = (!|next_next_cluster_number *| tcs_per_cluster_n) /| tcs_per_cluster_d in
								p // sprintf "FLUSH! New nct=%Ld" next_new_cluster_timecode;
								(frame_timecode, next_new_cluster_timecode, succ next_next_cluster_number)
							) else (
								(* Nothing's changed *)
								(cluster_timecode, next_cluster_timecode, next_next_cluster_number)
							) in

							let frame_timecode_to_cluster = !-(frame_timecode -| cluster_timecode) in

							let l32 = !-l in
							let frame_beginning = String.create 3 in
							really_input handle frame_beginning 0 3; (* This is going to be thrown away, but I have to seek beyond it anyway *)
							frame_beginning.[0] <- '\x81';
							Pack.packn frame_beginning 1 frame_timecode_to_cluster;

							(* Add the frame header, size, and contents to the buffer *)
							Mkvbb.output_id buffer 0x23;
							Mkvbb.output_size buffer (Some l);
							Bigbuffer.add_string buffer frame_beginning;
							Bigbuffer.add_channel buffer handle (l32 - 3);

							(cluster_timecode, next_cluster_timecode, next_next_cluster_number)
						)
						| _ -> raise Not_found
					)
					| _ -> raise Not_found
				)
				| _ -> raise Not_found
			) in

			let rec do_gop here cluster_timecode next_cluster_timecode next_next_cluster_number = (
				if here = i.i_encode_frames then (
					(* DONE *)
				) else (
					Mutex.lock frame_mutex;
					while not (Rbtree.mem frame_tree here) do
						Condition.wait frame_condition frame_mutex
					done;
					let current_gop_perhaps = Rbtree.find frame_tree here in
					Mutex.unlock frame_mutex;
					match current_gop_perhaps with
					| None -> do_gop here cluster_timecode next_cluster_timecode next_next_cluster_number
					| Some (_,gop) -> (
						(* Found a GOP *)
						p // sprintf "outputting GOP at frame %d (length %d) with filename \"%s\"" here gop.gop_frames gop.gop_filename;
						let handle = open_in_bin gop.gop_filename in
						let exc_perhaps = (try
							let rec helper n ct_nct_nncn = (
								if n = 0 then (
									ct_nct_nncn
								) else (
									let gnu = get_another_frame handle ct_nct_nncn in
									helper (pred n) gnu
								)
							) in
							Normal (helper gop.gop_frames (cluster_timecode,next_cluster_timecode,next_next_cluster_number))
						with
							e -> Exception e
						) in
						close_in handle;
						match exc_perhaps with
						| Normal (ct,nct,nncn) -> (
							Mutex.lock frame_mutex;
							frame_done_to_ref := here + gop.gop_frames;
							p // sprintf "frame_done_to_ref is now %d" !frame_done_to_ref;
							Mutex.unlock frame_mutex;
							do_gop (here + gop.gop_frames) ct nct nncn
						)
						| Exception x -> raise x
					)
				)
			) in
			do_gop 0 (-1L) 0L 1;

			i.p ~screen:true "DONE WRITING!";
			None
		with
			e -> Some e
		) in
		Unix.close out;

		(* Now see if the file was written properly *)
		match file_exception with
		| None -> () (* Exit *)
		| Some Not_found -> (
			(* Looks like there was an error in one of the MKC files *)
			(* Let's remove the GOP at frame_done_to_ref, just in case there was a problem *)
			p "an error occurred when trying to output the file; trying again";
			Mutex.lock frame_mutex;
			let gop_perhaps = Rbtree.find frame_tree !frame_done_to_ref in
			Rbtree.remove frame_tree !frame_done_to_ref;
			frame_done_to_ref := 0; (* Reset the frame counter *)
			Mutex.unlock frame_mutex;
			(match gop_perhaps with
				| None -> ()
				| Some (_,gop) -> ignore // trap_exception Sys.remove gop.gop_filename (* Remove the file, too *)
			);
			output_guts ()
		)
		| Some e -> raise e
	) in
	let output_thread = Thread.create (fun () ->
		try
			output_guts ()
		with
			e -> i.p // sprintf "BGOUTPUT DIED with %S" (Printexc.to_string e)
	) () in

	(***************)
	(* PING THREAD *)
	(***************)
	let ping_guts controller_port = (
		let p = i.p ~name:"PING " in
		let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
		Unix.setsockopt sock Unix.SO_BROADCAST true;

		(* Windows does not offer the proper function to send to the broadcast address
		 * (it uses -1 as an error code, which is what 255.255.255.255 comes out as)
		 * so use magic and make the address manually.
		 * If any errors occur, they will be clear when sending the ping *)
		let address = match Sys.os_type with
			| "Win32" -> Unix.ADDR_INET (Obj.magic "\255\255\255\255", c.c_agent_port)
			| _ -> Unix.ADDR_INET (Unix.inet_addr_of_string "255.255.255.255", c.c_agent_port)
		in
		let sendthis = (
			let start = "SCON\x00\x00" ^ c.c_controller_name in
			Pack.packn start 4 controller_port;
			start ^ Digest.string start
		) in
		let sendlength = String.length sendthis in
		let rec ping_loop () = (
			p // sprintf "Pinging %S" sendthis;
			if (Unix.sendto sock sendthis 0 sendlength [] address) < sendlength then (
				p "oops. Didn't send the whole string. I wonder why...";
			);
			Thread.delay 60.0;
			(* Now check to see if the encoding is still going *)
			Mutex.lock encoding_done_mutex;
			let stop = !encoding_done_ref in
			Mutex.unlock encoding_done_mutex;
			if stop then (
				p "DEAD"
			) else (
				ping_loop ()
			)
		) in
		ping_loop ()
	) in
	(* Actual ping thread is started in the discover thread *)

	(*******************)
	(* DISCOVER THREAD *)
	(*******************)
	let discover_port_channel = Event.new_channel () in
	let discover_guts () = (
		let p = i.p ~name:"DISCOVER " in

		p "starting up";

		let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
		
		let rec try_some_ports from goto now = (
			let found = (try
				Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_any, now));
				Some now
			with
				_ -> None
			) in
			match found with
			| Some x -> x
			| None when now >= goto -> try_some_ports from goto from
			| None -> try_some_ports from goto (succ now)
		) in
		let recv_port = try_some_ports 32768 49151 40704 in
(*		ignore // try_some_ports o.o_controller_port o.o_controller_port o.o_controller_port;*)

		(* Tell the main thread about the port so it can kill off the discover thread when encoding finishes *)
		Event.sync (Event.send discover_port_channel recv_port);

		(* Now that we know the actual controller port, we can start up the ping thread *)
		ignore // Thread.create (fun p ->
			i.p // sprintf "Ping thread %d starting" (Thread.id (Thread.self ()));
			(try
				ping_guts p
			with
				_ -> ()
			);
			i.p // sprintf "Ping thread %d exiting" (Thread.id (Thread.self ()));
		) recv_port;

		let recv_string = String.create 640 in (* 640 should be enough for anybody *)

		(* Start up the discover loop *)
		let rec discover_loop () = (
			let ok = (try
				let (got_bytes, from_addr) = Unix.recvfrom sock recv_string 0 (String.length recv_string) [] in
				(* Singlepass AGEnt = SAGE, you see... *)
				if got_bytes >= 39 && String.sub recv_string 0 4 = "SAGE" && Digest.substring recv_string 0 (got_bytes - 16) = String.sub recv_string (got_bytes - 16) 16 then (
					(* Looks like a good response *)
					(* Make an agent thing *)
					let agent_ip = match from_addr with
						| Unix.ADDR_INET (ip,port) -> Unix.string_of_inet_addr ip
						| _ -> "0.0.0.0" (* Why did this connect? Throw away! *)
					in
					let agent_port = Pack.unpackn recv_string 4 in
					let agent_num = Pack.unpackC recv_string 6 in
					let agent_id = String.sub recv_string 7 16 in
					let agent_name = if got_bytes > 39 then String.sub recv_string 23 (got_bytes - 40) else (sprintf "%s:%d" agent_ip agent_port) in
					
					p // sprintf "got response from %s:%d = %d agents with ID %s under name %S" agent_ip agent_port agent_num (to_hex agent_id) agent_name;

					if agent_ip = "0.0.0.0" then (
						p "IP address no good; trying again"
					) else (
						match get_agent_by_key agent_id (agent_ip,agent_port) with
						| Some a -> (
							(* Already in the pile *)
							(* Check to see if the agent is dead *)
							p // sprintf "agent already exists; restart dead workers";
							Mutex.lock agents_mutex;
							a.agent_id <- agent_id; (* Update the agent's ID in the agent structure itself *)
							if agent_num > Array.length a.agent_workers then (
								let tod = Unix.gettimeofday () in
								(* Stick a new worker array into the agent *)
								let temp = Array.init agent_num (fun j -> 
									if j < Array.length a.agent_workers then (
										(* Copy from old array *)
										a.agent_workers.(j)
									) else (
										p // sprintf "making a new worker #%d for agent %s" j a.agent_name;
										{
											worker_start = None;
											worker_current = None;
											worker_total_frames = 0;
											worker_total_time = 0.0;
											worker_last_updated = tod;
											worker_status = Worker_dead;
										}
									)
								) in
								a.agent_workers <- temp;
							);
							for j = 0 to agent_num - 1 do
								p // sprintf "checking worker %d for life" j;
								let worker = a.agent_workers.(j) in
								if worker.worker_status = Worker_dead then (
									(* Restart the worker *)
									worker.worker_status <- Worker_disconnected;
									p // sprintf "regenerated dead worker %d" j;
									let t = Thread.create (fun () ->
										i.p // sprintf "Worker regeneration thread %d starting" (Thread.id (Thread.self ()));
										(try
											disher_guts (a,j)
										with
											e -> (
												p // sprintf "agent died (again) with %S" (Printexc.to_string e)
											)
										);
										i.p // sprintf "Worker regeneration thread %d exiting" (Thread.id (Thread.self ()));
										(* This looks bad, but it's part of a separate thread from the lock directly above *)
										Mutex.lock agents_mutex;
										worker.worker_status <- Worker_dead;
										Mutex.unlock agents_mutex;
									) () in
									Hashtbl.add worker_threads (Thread.id t) t
								)
							done;
							Mutex.unlock agents_mutex;
						)
						| None -> (
							let tod = Unix.gettimeofday () in
							let workers = Array.init agent_num (fun _ -> {
								worker_start = None;
								worker_current = None;
								worker_total_frames = 0;
								worker_total_time = 0.0;
								worker_last_updated = tod;
								worker_status = Worker_disconnected;
							}) in
							let agent_add = {
								agent_name = agent_name;
								agent_ip = (agent_ip, agent_port);
								agent_id = agent_id;
								agent_workers = workers;
							} in
							add_agent agent_add;

							(* Now that the agent has been added, start up the agent threads *)
							for j = 0 to Array.length workers - 1 do
								let t = Thread.create (fun () ->
									i.p // sprintf "Worker thread %d starting" (Thread.id (Thread.self ()));
									(try
										disher_guts (agent_add,j);
									with
										e -> (
											p // sprintf "agent died with %S" (Printexc.to_string e)
										)
									);
									p "agent exited; marking as dead";
									Mutex.lock agents_mutex;
									agent_add.agent_workers.(j).worker_status <- Worker_dead;
									Mutex.unlock agents_mutex;
									i.p // sprintf "Worker thread %d exiting" (Thread.id (Thread.self ()));
								) () in
								Hashtbl.add worker_threads (Thread.id t) t
							done
						)
					)
				) else (
					p "threw out bad response";
				);
				(* Now check to see if the encoding is still going *)
				Mutex.lock encoding_done_mutex;
				let stop = !encoding_done_ref in
				Mutex.unlock encoding_done_mutex;
				if stop then (
					p "encoding is done; exit";
					false
				) else (
					true
				)
			with
				e -> (p // sprintf "Discover failed with %S" (Printexc.to_string e); false)
			) in
			if ok then discover_loop () else ()
		) in
		discover_loop ()

	) in
	let discover_thread = Thread.create (fun () ->
		i.p // sprintf "Discover thread %d starting" (Thread.id (Thread.self ()));
		(try
			discover_guts ()
		with
			_ -> ()
		);
		i.p // sprintf "Discover thread %d exiting" (Thread.id (Thread.self ()));
	) () in

	(* Receive the discover port from the discover thread to stop the discover thread when encoding is finished *)
	let discover_port = Event.sync (Event.receive discover_port_channel) in



	(******************************************************************************)
	(******************************************************************************)
	(******************************************************************************)
	(******************************************************************************)
	(*********************** WAIT UNTIL THE ENCODE FINISHES ***********************)
	(******************************************************************************)
	(******************************************************************************)
	(******************************************************************************)
	(******************************************************************************)


	wait_until_done ();
	i.p "ENCODING DONE";

	Mutex.lock encoding_done_mutex;
	encoding_done_ref := true;
	Mutex.unlock encoding_done_mutex;

	Thread.join output_thread;

	(* Kill off the discoverer *)
	i.p "Main thread killing discoverer";
	(try
		let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
		ignore // Unix.sendto sock "DONE" 0 4 [] (Unix.ADDR_INET (Unix.inet_addr_loopback, discover_port));
		Unix.close sock
	with
		| e -> i.p // sprintf "I wonder what %S means" (Printexc.to_string e)
	);
	i.p "Main thread killed discoverer";


	(*************)
	(* EXIT HERE *)
	(*************)
	exit 0;
);;

run_pass o c i;;

(*Avi.exit_avi ();;*)

