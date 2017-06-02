(*******************************************************************************
	This file is a part of x264farm-sp.

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


(*Avi.init_avi ();;*)


(********)
(* TEST *)
(********)
(*
(* lockf *)
let a = Unix.openfile "D:\\Documents\\OCaml\\amelie-big.mkv" [Unix.O_RDWR] 0o600;;
(*Opt.lockf a Unix.F_LOCK (-3);;*)
(*Opt.lockf a Unix.F_TLOCK 0;;*)
Unix.LargeFile.lseek a 4294967298L Unix.SEEK_SET;;
Opt.lockf a Unix.F_TLOCK 0;;
(*
Opt.lockf a Unix.F_RLOCK (-2);;
Opt.lockf a Unix.F_RLOCK 17;;
Opt.lockf a Unix.F_RLOCK 19;;
Opt.lockf a Unix.F_ULOCK (-3);;
Unix.LargeFile.lseek a (400L) Unix.SEEK_END;;
Opt.lockf a Unix.F_TLOCK 0;;
*)
exit 37;;
*)


(*
(* Watch directory *)
let (a,b) = Unix.open_process "x264 --crf 22 -o D:\\Documents\\OCaml\\x264farm\\singlepass\\controller\\temp\\boopsie.mp4 d:\\documents\\dvd\\mkv\\fma\\46\\farm.avs";;
(*
let t = Thread.create (fun () ->
	let s = String.create 16384 in
	let n = open_in_bin "D:\\Documents\\OCaml\\x264farm\\singlepass\\controller\\temp\\boopsie.mp4" in
	printf "DELAYAN\n%!";
	Thread.delay 4.0;
	printf "STARTAN UP\n%!";
	while true do
		try while true do
			let q = (input n s 0 16384) in
			if q = 0 then raise End_of_file
		done with
			| End_of_file -> (
				printf "EOF at %Ld\n%!" (LargeFile.pos_in n);
				Thread.delay 0.1
			)
	done
) ();;
*)
while true do
(*	printf "Waiting...\n%!";*)
	let ok = Opt.watch_directory_c "D:\\Documents\\OCaml\\x264farm\\singlepass\\controller" true [Opt.Watch_file_name; Opt.Watch_dir_name; Opt.Watch_change_attributes; Opt.Watch_change_size; Opt.Watch_change_last_write; Opt.Watch_change_security] (None) in
	if ok then (
		try
			printf "%.2f Size changed %Ld\n%!" (Unix.gettimeofday ()) (Unix.LargeFile.stat "D:\\Documents\\OCaml\\x264farm\\singlepass\\controller\\temp\\boopsie.mp4").Unix.LargeFile.st_size;
		with
			_ -> printf "Size changed or something\n%!"
	) else (
		printf "%.2f TIMEOUT %Ld\n%!" (Unix.gettimeofday ()) (Unix.LargeFile.stat "D:\\Documents\\OCaml\\x264farm\\singlepass\\controller\\temp\\boopsie.mp4").Unix.LargeFile.st_size
	)
done;;

Unix.close_process (a,b);;

exit 220;;
*)
(*
(* C PTR test *)
let a = Array.init 40 (fun _ -> Avi.make_ptr 20000000);;
while true do
	let b = Avi.make_ptr 20000000 in
	printf "#%!";
	Thread.delay 1.0;
done;;
exit 4;;
*)
(*
(* Broadcast address test *)
let to_hex s =
	let result = String.create (2 * String.length s) in
	for i = 0 to String.length s - 1 do
		String.blit (sprintf "%02X" (int_of_char s.[i])) 0 result (2*i) 2;
	done;
	result
;;
let p = Contopt.get_broadcast_addresses ();;
Array.iter (fun i ->
	printf "%s\n" (Unix.string_of_inet_addr i)
) p;;
(*printf "%d!\n" (Contopt.sock_it_to_me ());;*)
exit 53;;
*)
(*
(* Console test *)
let q = new Console.console Console.std_output_handle;;

printf "\n\n\n\n\n\n\n\n\n%!";;

let or_die = function
	| Console.Normal _ -> ()
	| Console.Exception (a,b) -> printf "\n\n%S~%d\n\n" a b
;;

let d = 5.0;;

let print_info n a =
	printf "\n";
	printf "%S\n" n;
	match a with
	| Console.Normal b -> (
		printf "  Size x     %d\n" b.Console.size_x;
		printf "  Size y     %d\n" b.Console.size_y;
		printf "  Cursor x   %d\n" b.Console.cursor_x;
		printf "  Cursor y   %d\n" b.Console.cursor_y;
		printf "  Attributes %d\n" b.Console.attributes;
		printf "  Left       %d\n" b.Console.window_left;
		printf "  Top        %d\n" b.Console.window_top;
		printf "  Right      %d\n" b.Console.window_right;
		printf "  Bottom     %d\n" b.Console.window_bottom;
		printf "  Max size x %d\n" b.Console.max_size_x;
		printf "  Max size y %d\n" b.Console.max_size_y;
	)
	| _ -> printf "  OOPS!\n"
;;

let write = Console.really_write Console.std_output_handle;;

write "\n\nLAF\n\n";;
q#print_line (String.make 44 '#');;
q#print_line (String.make 45 '$');;
Thread.delay d;;
q#print_line_noenter "1";;
Thread.delay d;;
q#print_line_noenter "2";;
Thread.delay d;;
q#print_line_noenter "3";;
Thread.delay d;;
q#print_line_noenter "4";;

(*
q#up 1;;
write "USURPED\n";;
write (String.make 100 '$');;
write (String.make 101 '%');;
*)
(*
q#right 98;;
Thread.delay d;;
q#print_char '0';;
let q0 = q#info;;
Thread.delay d;;
q#print_char '1';;
let q1 = q#info;;
Thread.delay d;;
q#print_char '2';;
let q2 = q#info;;
Thread.delay d;;
q#print_char '3';;
let q3 = q#info;;
Thread.delay d;;
q#print_char '4';;
let q4 = q#info;;


print_info "0" q0;;
print_info "1" q1;;
print_info "2" q2;;
print_info "3" q3;;
print_info "4" q4;;
*)


exit 17;;
*)
(*
let print_output = function
	| (None, state) -> printf "NOT FOUND; state is %d\n" state;
	| (Some at, state) -> printf "Found at %d; state is %d\n" at state;
;;

let str = "\x06\x00\x00\x00\x02";;

print_output (Opt.fetch_0001 str 0 3 0);;
print_output (Opt.fetch_0001 str 3 1 3);;
print_output (Opt.fetch_0001 str 4 1 3);;

exit 401;;
*)

(*
let to_hex s =
	let result = String.create (2 * String.length s) in
	for i = 0 to String.length s - 1 do
		String.blit (sprintf "%02X" (int_of_char s.[i])) 0 result (2*i) 2;
	done;
	result
;;

let s = "\014\013\013\010\010";;
printf "%S\n%!" (to_hex s);;
let n = Opt.string_sanitize_text_mode s 0 (String.length s) 12345;;
printf "%S\n%!" (to_hex s);;

exit 400;;
*)
(*
Avi.init_avi ();;

let a = Avi.open_avi "d:\\documents\\dvd\\mkv\\FMA\\46\\farm.avs";; (*"d:\\documents\\dvd\\mkv\\amelie\\amelie.avs";;*)
let b = Avi.open_avi "d:\\documents\\dvd\\mkv\\FMA\\46\\farm.avs";; (*"d:\\documents\\dvd\\mkv\\amelie\\amelie.avs";;*)
let c = Avi.open_avi "d:\\documents\\dvd\\mkv\\FMA\\47\\farm.avs";; (*"d:\\documents\\dvd\\mkv\\amelie\\amelie.avs";;*)
let d = Avi.open_avi "d:\\documents\\dvd\\mkv\\FMA\\48\\farm.avs";; (*"d:\\documents\\dvd\\mkv\\amelie\\amelie.avs";;*)

printf "W: %d\n" a.Avi.w;;
printf "H: %d\n" a.Avi.h;;
printf "N: %d\n" a.Avi.n;;
printf "D: %d\n" a.Avi.d;;
printf "I: %d\n" a.Avi.i;;
printf "b: %d\n" a.Avi.bytes_per_frame;;

let f1 = Unix.openfile "f1.raw" [Unix.O_CREAT; Unix.O_WRONLY; Unix.O_TRUNC] 0o600;;
let f2 = Unix.openfile "f2.raw" [Unix.O_CREAT; Unix.O_WRONLY; Unix.O_TRUNC] 0o600;;
let f3 = Unix.openfile "f3.raw" [Unix.O_CREAT; Unix.O_WRONLY; Unix.O_TRUNC] 0o600;;
let f4 = Unix.openfile "f4.raw" [Unix.O_CREAT; Unix.O_WRONLY; Unix.O_TRUNC] 0o600;;

let p1 = Avi.make_ptr a.Avi.bytes_per_frame;;
let p2 = Avi.make_ptr b.Avi.bytes_per_frame;;
let p3 = Avi.make_ptr c.Avi.bytes_per_frame;;
let p4 = Avi.make_ptr d.Avi.bytes_per_frame;;


let t1 = Thread.create (fun () ->
	for i = 10000 to 20100 do
		Avi.ptr_avi_frame a i p1;
(*		Avi.write_ptr f1 p1;*)
	done
) ();;

let t2 = Thread.create (fun () ->
	for i = 20000 to 30100 do
		Avi.ptr_avi_frame b i p2;
(*		Avi.write_ptr f2 p2;*)
	done
) ();;

let t3 = Thread.create (fun () ->
	for i = 10000 to 20100 do
		Avi.ptr_avi_frame c i p3;
(*		Avi.write_ptr f1 p1;*)
	done
) ();;

let t4 = Thread.create (fun () ->
	for i = 20000 to 30100 do
		Avi.ptr_avi_frame d i p4;
(*		Avi.write_ptr f2 p2;*)
	done
) ();;

let tx = Thread.create (fun () ->
	while true do
		let b = Array.create 4000000 0 in
		let c = Array.create (Random.int 4000000) 2 in
		if Random.float 1.0 < 0.51 then Gc.compact ();
	done
) ();;

Thread.join t1;;
Thread.join t2;;
Thread.join tx;;

(*
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
*)




Avi.close_avi a;;
Avi.close_avi b;;
Avi.exit_avi ();;

exit 572;;
*)
(*********)
(* !TEST *)
(*********)





let version_string = "x264farm-sp 1.07";;

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

let rec mutex_lock_2 a b =
	Mutex.lock a;
	if Mutex.try_lock b then (
		() (* Hooray *)
	) else (
		Mutex.unlock a;
		mutex_lock_2 b a
	)
;;

let rec mutex_lock_3 a b c =
	Mutex.lock a;
	if Mutex.try_lock b then (
		if Mutex.try_lock c then (
			() (* Hooray *)
		) else (
			Mutex.unlock a;
			Mutex.unlock b;
			mutex_lock_3 c a b
		)
	) else (
		Mutex.unlock a;
		mutex_lock_3 b c a
	)
;;

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






type encode_t = Encode_mkv | Encode_mp4;;

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

type video_options_t = {
	v_x : string;
	v_i : string;
	v_o : string;
	v_encode_type : encode_t;
	v_seek : int;
	v_frames : int;
	v_zones : zone_t list;
};;

type options_t = {
(*
	o_x : string;
	o_i : string;
	o_o : string;
	o_encode_type : encode_t;
	o_seek : int;
	o_frames : int;
	o_zones : zone_t list;
*)
	o_v : video_options_t array;
	o_config : string;
	o_logfile : string;
	o_clean : bool;
	o_restart : bool;
(*	o_blit : (?last_frame:int -> Avs.t -> Avs.pos_t -> string -> int -> int -> int);*)
};;

type config_t = {
	c_controller_name : string;
	c_agent_port : int;
	c_controller_port : int;
	c_static_agents : agent_info_t list;
	c_temp_dir : string;
(*
	p : ?name:string -> ?screen:bool -> ?log:bool -> ?lock:bool -> ?time:bool -> ?fill:bool -> string -> unit;
	pm : Mutex.t;
*)
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
(*
	p : ?name:string -> ?screen:bool -> ?log:bool -> ?lock:bool -> ?time:bool -> ?fill:bool -> string -> unit;
	pm : Mutex.t;
*)
};;

type gop_t = {
	gop_frames : int;
	gop_filename : string;
};;

type vi_t = {
	v : video_options_t;
	i : info_t;
(*	vi_stats_in : in_channel;*)
	vi_stats_out : out_channel;
	vi_stats_out_version : int;
	vi_frames : (int,gop_t) Rbtree.t;
	mutable vi_frames_done_to : int;
	mutable vi_outputted : bool; (* Set if the outputter is done with this file *)
	mutable vi_track_private : (string * string * (int * int * int * int)) option; (* (track,private,(w,h,dw,dh)), perhaps *)
};;

type worker_status_t = Worker_disconnected | Worker_connected | Worker_agent of (int * int) | Worker_controller of (int * int) | Worker_done(* | Worker_dead*);;

type agent_worker_t = {
	mutable worker_total_frames : int;
	mutable worker_total_time : float;
	mutable worker_last_updated : float;
	mutable worker_status : worker_status_t;
	mutable worker_times_failed : int;
};;
type agent_connection_t = {
	agent_name : string;
	mutable agent_ip : string * int;
	mutable agent_id : string;
	mutable agent_optimal_workers : int;
	mutable agent_workers : (int,agent_worker_t) Rbtree.t;
};;

type job_t = {
	job_v : video_options_t;
	job_i : info_t;
	job_seek : int;
	job_encode_seek : int;
	job_frames : int;
	job_video_index : int;
	job_zones : zone_t list;
(*	job_checked_out : bool;*)
};;
type check_out_job_t = Job of job_t | Job_none | Job_bad of (int * int);;

(*exception Encoding_done;;*)
exception Timeout;;
exception Dead_worker;;
exception X264_error of int;;



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
 * ip // sprintf "FORMATTING" a b c
 * This function just removes the need for extra parentheses, like this:
 * ip (sprintf "FORMATTING" a b c) *)
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
			let rec recv_helper from len = (
				let recvd = Unix.recv sock str from len [] in
				if recvd = len then (
					()
				) else if recvd = 0 then (
					raise End_of_file
				) else (
					recv_helper (from + recvd) (len - recvd)
					)
			) in
			recv_helper from len;
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

	end
;;










(*********)
(* SLAVE *)
(*********)
type slave_t = {
	slave_bytes_per_frame : int;
	slave_i : string; (* INPUT FILE, not info! *)
	slave_addr : Unix.sockaddr;
	slave_key : string;
	slave_first_frame : int;
	slave_last_frame : int;
	slave_zones : string;
	slave_name : string;
};;
type slave_meta_t = Slave_do of slave_t | Slave_stop | Slave_set_priority of Opt.priority_t * Opt.thread_priority_t;;




(* This function is capable of copying data from AVS and sending data to the socket at the same time. For a very complex AVS script, it increased network usage from 18% to 22%, and took the CPU usage from 21% to 25% *)
let rec do_slave_multithread () =

	set_binary_mode_in stdin true;

(*	let s_meta = Slave_do do_this_slave in*)
	let s_meta = Marshal.from_channel stdin in

	let current_thread_priority_ref = ref Opt.Thread_priority_normal in
	let set_thread_priority_ref = ref Opt.Thread_priority_normal in
	let thread_priority_mutex = Mutex.create () in
	let change_thread_priority p = (
		Mutex.lock thread_priority_mutex;
		if !current_thread_priority_ref <> !set_thread_priority_ref then (
			(* Change the priority! *)
			p // sprintf "changing thread priority to %d" (Obj.magic !set_thread_priority_ref);
			ignore // Opt.set_thread_priority !set_thread_priority_ref;
			current_thread_priority_ref := !set_thread_priority_ref;
		);
		Mutex.unlock thread_priority_mutex;
	) in

	let keep_going_ref = ref true in
	let keep_going_mutex = Mutex.create () in
	let check_stop () = (
		Mutex.lock keep_going_mutex;
		let a = !keep_going_ref in
		Mutex.unlock keep_going_mutex;
		not a
	) in
	let stop () = (
		Mutex.lock keep_going_mutex;
		keep_going_ref := false;
		Mutex.unlock keep_going_mutex;
	) in


	match s_meta with
	| Slave_stop | Slave_set_priority _ -> false (* These don't go here! *)
	| Slave_do s -> (
(*		let p q = (eprintf "%s%s%s\n" s.slave_name rand q; flush stderr) in*)
(*		let p q = (eprintf "%s\n" q; flush stderr) in*)
		let p = (
			let m = Mutex.create () in
			fun x -> (
				Mutex.lock m;
				print_endline x;
				Mutex.unlock m;
			)
		) in
		let p q = () in

		p "starting";
		p "got job";

		let rec send_select d ptr off len num_left = (
			if num_left <= 0 then (
(*				failwith "OUT OF TIME";*)
				0
			) else (
				match Unix.select [] [d] [] 1.0 with
				| (_,[_],_) -> (
(*					printf "Selected\n%!";*)
					let r = Opt.send_ptr d ptr off len [] in
					r
				)
				| _ -> (
					printf "Not selected\n%!";
					(* Check if we should keep going *)
					if check_stop () then (
						0
					) else (
						send_select d ptr off len (pred num_left)
					)
				)
			)
		) in
		let rec really_send_parts d ptr off len n = (
			if len <= 0 then true else (
				let r = send_select d ptr off len n in
				if r = 0 then (
					false
				) else if r = len then (
					true
				) else (
					really_send_parts d ptr (off + r) (len - r) n
				);
			)
		) in

		let rec check_guts () = (
			let next_object = trap_exception Marshal.from_channel stdin in
			match next_object with
			| Normal (Slave_set_priority (pp,tp)) -> (
				p "setting priority";
				ignore // Opt.set_process_priority Opt.current_process pp;
				ignore // Opt.set_thread_priority tp; (* Might as well set this thread's priority to the same *)
				Mutex.lock thread_priority_mutex;
				set_thread_priority_ref := tp;
				Mutex.unlock thread_priority_mutex;
				check_guts ()
			)
			| _ -> (
				p "telling thread to stop";
				stop ()
			)
		) in
		ignore // Thread.create check_guts ();



		(* AVI START *)
		Avi.init_avi ();
		(try
			let avi = Avi.open_avi s.slave_i in
			let y_length = avi.Avi.h * avi.Avi.w in
			let v_offset = y_length in
			let v_length = y_length lsr 2 in
			let u_offset = y_length + v_length in
			let u_length = v_length in
			(try
				let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
				(try
					Unix.connect sock s.slave_addr;

					(* Send the key! *)
					really_send sock s.slave_key 0 (String.length s.slave_key);


(* Test the speed of select () *)
(* 10000000 selects took 32.766 seconds (305194 selects per second, 3.2766e-006 seconds per select) *)
					(* This shouldn't be noticable with a max of about 100 persecond *)
(*
					printf "!\n%!";
					let n = 10000000 in
					let ta = Unix.gettimeofday () in
					for i = 1 to n do
						()
					done;
					let tb = Unix.gettimeofday () in
					let total_time = tb -. ta in
					printf "%d nothing took %g seconds (%g nothings per second, %g seconds per nothing)\n%!" n total_time (float_of_int n /. total_time) (total_time /. float_of_int n);
					let ta = Unix.gettimeofday () in
					for i = 1 to n do
						ignore // Unix.select [] [sock] [] 0.0
					done;
					let tb = Unix.gettimeofday () in
					let total_time = tb -. ta in
					printf "%d selects took %g seconds (%g selects per second, %g seconds per select)\n%!" n total_time (float_of_int n /. total_time) (total_time /. float_of_int n);
*)
(* Events seem kind of slow... *)
(*
					let get_input_channel = Event.new_channel () in
					let send_input_channel = Event.new_channel () in

					let get_trade n = (
						Event.sync (Event.send send_input_channel n);
						let out = Event.sync (Event.receive get_input_channel) in
						out
					) in
					let send_trade n = (
						let out = Event.sync (Event.receive send_input_channel) in
						Event.sync (Event.send get_input_channel n);
						out
					) in
*)
(* This seems kind of complicated, but less so than events *)
(*
					let get_input_ref = ref None in
					let send_input_ref = ref None in
					let get_mutex = Mutex.create () in
					let send_mutex = Mutex.create () in
					let get_is_full = Condition.create () in
					let get_is_empty = Condition.create () in
					let send_is_full = Condition.create () in
					let send_is_empty = Condition.create () in
					let get_trade n = (
						Mutex.lock send_mutex;
						let rec s () = match !send_input_ref with
							| None -> (send_input_ref := Some n; Condition.signal send_is_full)
							| _ -> (Condition.wait send_is_empty send_mutex; s ())
						in
						s ();
						Mutex.unlock send_mutex;
						Mutex.lock get_mutex;
						let rec g () = match !get_input_ref with
							| None -> (Condition.wait get_is_full get_mutex; g ())
							| Some x -> (get_input_ref := None; Condition.signal get_is_empty; x)
						in
						let x = g () in
						Mutex.unlock get_mutex;
						x
					) in
					let send_trade n = (
						Mutex.lock get_mutex;
						let rec g () = match !get_input_ref with
							| None -> (get_input_ref := Some n; Condition.signal get_is_full)
							| _ -> (Condition.wait get_is_empty get_mutex; g ())
						in
						g ();
						Mutex.unlock get_mutex;
						Mutex.lock send_mutex;
						let rec s () = match !send_input_ref with
							| None -> (Condition.wait send_is_full send_mutex; s ())
							| Some x -> (send_input_ref := None; Condition.signal send_is_empty; x)
						in
						let x = s () in
						Mutex.unlock send_mutex;
						x
					) in
*)
(* GATES *)
					let make_gate n = (
						let m = Mutex.create () in
						let c = Condition.create () in
						let r = ref 0 in
						fun () -> (
							Mutex.lock m;
							incr r;
							if !r < n then (
								Condition.wait c m
							) else (
								r := 0;
								Condition.broadcast c
							);
							Mutex.unlock m;
						)
					) in

					let get_in = ref None in
					let send_in = ref None in
					let g1 = make_gate 2 in
					let g2 = make_gate 2 in
					let get_trade i = (
						g1 ();
						send_in := i;
						g2 ();
						!get_in
					) in
					let send_trade i = (
						g1 ();
						get_in := i;
						g2 ();
						!send_in
					) in

					let rec get_frame f ptr_perhaps = (
						match ptr_perhaps with
						| Some ptr -> (
							if f land 15 = 0 then (
								p "checking thread priority";
								change_thread_priority p;
							);
(*
							if f land 255 = 0 then (
								printf "Frame %d\n%!" f
							);
*)
							if f >= s.slave_last_frame then (
								ignore // (get_trade None)
							) else (
								let got = Avi.ptr_avi_frame avi f ptr in
								if got <> 1 then (
									ignore // (get_trade None)
								) else (
									get_frame (succ f) (get_trade (Some ptr))
								)
							)
						)
						| None -> ()
					) in
					let rec send_frame ptr_perhaps = (
						match ptr_perhaps with
						| Some ptr -> (
							if check_stop () then (
								ignore // (send_trade None)
							) else (
								if really_send_parts sock ptr 0 y_length 10 then (
									if really_send_parts sock ptr u_offset u_length 10 then (
										if really_send_parts sock ptr v_offset v_length 10 then (
											send_frame (send_trade (Some ptr))
										) else (
											ignore // (send_trade None)
										)
									) else (
										ignore // (send_trade None)
									)
								) else (
									ignore // (send_trade None)
								)
							)
						)
						| None -> ()
					) in

					let get_thread = Thread.create (fun () ->
						try
							get_frame s.slave_first_frame (Some (Opt.make_ptr s.slave_bytes_per_frame))
						with
							_ -> ()
					) () in

					let send_thread = Thread.create (fun () ->
						try
							send_frame (send_trade (Some (Opt.make_ptr s.slave_bytes_per_frame)))
						with
							_ -> ()
					) () in

					Thread.join get_thread;
					Thread.join send_thread;
				with
					e -> (p // sprintf "AVI thread exited (after sock) with %S" (Printexc.to_string e))
				);
				Unix.close sock;
			with
				e -> (p // sprintf "AVI thread exited (after init) with %S" (Printexc.to_string e))
			);
			Avi.close_avi avi;
		with
			e -> (p // sprintf "AVI thread exited with %S" (Printexc.to_string e))
		);
		Avi.exit_avi ();
		true
	)
;;






let rec do_slave_ptr () =

(*
	let default_overhead = (Gc.get ()).Gc.max_overhead in
	Gc.set {(Gc.get ()) with Gc.max_overhead = 1000001};
*)
	set_binary_mode_in stdin true;

	let s_meta = Marshal.from_channel stdin in

	let current_thread_priority_ref = ref Opt.Thread_priority_normal in
	let set_thread_priority_ref = ref Opt.Thread_priority_normal in
	let thread_priority_mutex = Mutex.create () in
	let change_thread_priority p = (
		Mutex.lock thread_priority_mutex;
		if !current_thread_priority_ref <> !set_thread_priority_ref then (
			(* Change the priority! *)
			p // sprintf "changing thread priority to %d" (Obj.magic !set_thread_priority_ref);
			ignore // Opt.set_thread_priority !set_thread_priority_ref;
			current_thread_priority_ref := !set_thread_priority_ref;
		);
		Mutex.unlock thread_priority_mutex;
	) in

	let keep_going_ref = ref true in
	let keep_going_mutex = Mutex.create () in
	let check_stop () = (
		Mutex.lock keep_going_mutex;
		let a = !keep_going_ref in
		Mutex.unlock keep_going_mutex;
		not a
	) in
	let stop () = (
		Mutex.lock keep_going_mutex;
		keep_going_ref := false;
		Mutex.unlock keep_going_mutex;
	) in

	match s_meta with
	| Slave_stop | Slave_set_priority _ -> false (* These don't go here! *)
	| Slave_do s -> (
(*		let p q = (eprintf "%s%s%s\n" s.slave_name rand q; flush stderr) in*)
(*		let p q = (eprintf "%s\n" q; flush stderr) in*)
		let p q = () in
		p "starting";
		p "got job";


		let rec send_select d ptr off len num_left = (
			if num_left <= 0 then (
				failwith "OUT OF TIME";
			) else (
(*				p "selecting";*)
				match Unix.select [] [d] [] 1.0 with
				| (_,[_],_) -> (
(*					p // sprintf "  sending %d+%d" off len;*)
(*					let r = Unix.send d s off len [] in*)
					let r = Opt.send_ptr d ptr off len [] in
(*					p // sprintf "  sent %d" r;*)
					r
				)
				| _ -> (
					(* Check if we should keep going *)
					if check_stop () then (failwith "STOPPED in select");
					p "  trying again";
					send_select d ptr off len (pred num_left)
				)
			)
		) in
		let rec really_send_parts d ptr off len n = (
			if len <= 0 then true else (
				p // sprintf "sending %d+%d" off len;
				let r = send_select d ptr off len n in
				p // sprintf "sent %d" r;
				if r = 0 then (
					raise End_of_file
				) else if r = len then (
					true
				) else (
					really_send_parts d ptr (off + r) (len - r) n
				);
			)
		) in

		let rec check_guts () = (
			let next_object = trap_exception Marshal.from_channel stdin in
			match next_object with
			| Normal (Slave_set_priority (pp,tp)) -> (
				p "setting priority";
				ignore // Opt.set_process_priority Opt.current_process pp;
				ignore // Opt.set_thread_priority tp; (* Might as well set this thread's priority to the same *)
				Mutex.lock thread_priority_mutex;
				set_thread_priority_ref := tp;
				Mutex.unlock thread_priority_mutex;
				check_guts ()
			)
			| _ -> (
				p "telling thread to stop";
				stop ()
			)
		) in
		ignore // Thread.create check_guts ();

		Avi.init_avi ();
		(try
			let avi = Avi.open_avi s.slave_i in
			let y_length = avi.Avi.h * avi.Avi.w in
			let v_offset = y_length in
			let v_length = y_length lsr 2 in
			let u_offset = y_length + v_length in
			let u_length = v_length in
			(try
				let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
				(try
					Unix.connect sock s.slave_addr;

					(* Send the key! *)
					really_send sock s.slave_key 0 (String.length s.slave_key);

					let ptr = Opt.make_ptr s.slave_bytes_per_frame in
					for f = s.slave_first_frame to s.slave_last_frame - 1 do
						if check_stop () then (failwith "STOPPED");
						if f land 15 = 0 then (
							p "changing thread priority (or at least checking)";
							change_thread_priority p;
						);
						p // sprintf "getting frame %d" f;
						let got = Avi.ptr_avi_frame avi f ptr in
						if got <> 1 then (
							(* Oops. Didn't get a frame *)
							failwith // sprintf "didn't get exactly one frame (expected frame #%d), got %d frames" f got
						);
						if check_stop () then (failwith "STOPPED");
						p // sprintf "sending frame %d" f;
(*						let ok = really_send_timeout sock str 0 (String.length str) 10.0 in*)

						(* Each plane needs to be sent separately since x264 expects I420-style data, whereas the AVI file outputs YV12 *)
						(* This means that the U and V planes need to be sent out of order *)
						(* Send Y plane *)
						p "Y";
						let ok = really_send_parts sock ptr 0 y_length 10 in
						if not ok then (failwith "Y NOT SENT");

						if check_stop () then (failwith "STOPPED after Y");

						(* Send U plane *)
						p "u";
						let ok = really_send_parts sock ptr u_offset u_length 10 in
						if not ok then (failwith "U NOT SENT");

						(* Send V plane *)
						p "v";
						let ok = really_send_parts sock ptr v_offset v_length 10 in
						if not ok then (failwith "V NOT SENT");

					done;
				with
					e -> (p // sprintf "AVI thread exited (after sock) with %S" (Printexc.to_string e))
				);
				Unix.close sock;
			with
				e -> (p // sprintf "AVI thread exited (after init) with %S" (Printexc.to_string e))
			);
			Avi.close_avi avi;
		with
			e -> (p // sprintf "AVI thread exited with %S" (Printexc.to_string e))
		);
		Avi.exit_avi ();
		true
	)
;;





let rec do_slave () =

	(* Prevent the strings from being moved whilst writing to them *)
	(* This probably won't be needed, since the rest of the slave program does basically nothing, so there is nothing to wait for *)
	let default_overhead = (Gc.get ()).Gc.max_overhead in
	Gc.set {(Gc.get ()) with Gc.max_overhead = 1000001};

	let sendthis a b c d = if false then (
		(* Opt.very_unsafe_send is the same as Unix.send, but with a larger buffer, no bounds checking, and compaction MUST be turned off *)
		Unix.send a b c d []
	) else (
		Unix.send a b c d []
	) in

	set_binary_mode_in stdin true;

	let s_meta = Marshal.from_channel stdin in

	let current_thread_priority_ref = ref Opt.Thread_priority_normal in
	let set_thread_priority_ref = ref Opt.Thread_priority_normal in
	let thread_priority_mutex = Mutex.create () in
	let change_thread_priority p = (
		Mutex.lock thread_priority_mutex;
		if !current_thread_priority_ref <> !set_thread_priority_ref then (
			(* Change the priority! *)
			p // sprintf "changing thread priority to %d" (Obj.magic !set_thread_priority_ref);
			ignore // Opt.set_thread_priority !set_thread_priority_ref;
			current_thread_priority_ref := !set_thread_priority_ref;
		);
		Mutex.unlock thread_priority_mutex;
	) in

	let keep_going_ref = ref true in
	let keep_going_mutex = Mutex.create () in
	let check_stop () = (
		Mutex.lock keep_going_mutex;
		let a = !keep_going_ref in
		Mutex.unlock keep_going_mutex;
		not a
	) in
	let stop () = (
		Mutex.lock keep_going_mutex;
		keep_going_ref := false;
		Mutex.unlock keep_going_mutex;
	) in

	match s_meta with
	| Slave_stop | Slave_set_priority _ -> false (* These don't go here! *)
	| Slave_do s -> (
(*		let p q = (eprintf "%s%s%s\n" s.slave_name rand q; flush stderr) in*)
(*		let p q = (eprintf "%s\n" q; flush stderr) in*)
		let p q = () in
		p "starting";
		p "got job";

		let rec send_or_else sock str from len = (
(*			p // sprintf "sending %d+%d" from len;*)
			let sent = Unix.send sock str from len [] in
(*			p // sprintf "  sent %d" sent;*)
			if sent = len then (
				()
			) else if sent = 0 then (
				failwith "socket send error"
			) else (
				send_or_else sock str (from + sent) (len - sent)
			)
		) in

		let rec send_select d s off len num_left = (
			if num_left <= 0 then (
				failwith "OUT OF TIME";
			) else (
(*				p "selecting";*)
				match Unix.select [] [d] [] 1.0 with
				| (_,[_],_) -> (
					p // sprintf "  sending %d+%d" off len;
(*					let r = Unix.send d s off len [] in*)
					let r = sendthis d s off len in
					p // sprintf "  sent %d" r;
					r
				)
				| _ -> (
					(* Check if we should keep going *)
					if check_stop () then (failwith "STOPPED in select");
					p "  trying again";
					send_select d s off len (pred num_left)
				)
			)
		) in
		let rec really_send_parts d s off len n = (
			if len <= 0 then true else (
(*				p // sprintf "sending %d+%d" off len;*)
				let r = send_select d s off len n in
(*				p // sprintf "sent %d" r;*)
				if r = 0 then (
					raise End_of_file
				) else if r = len then (
					true
				) else (
					really_send_parts d s (off + r) (len - r) n
				);
			)
		) in




		let rec check_guts () = (
			let next_object = trap_exception Marshal.from_channel stdin in
			match next_object with
			| Normal (Slave_set_priority (pp,tp)) -> (
				p "setting priority";
				ignore // Opt.set_process_priority Opt.current_process pp;
				ignore // Opt.set_thread_priority tp; (* Might as well set this thread's priority to the same *)
				Mutex.lock thread_priority_mutex;
				set_thread_priority_ref := tp;
				Mutex.unlock thread_priority_mutex;
				check_guts ()
			)
			| _ -> (
				p "telling thread to stop";
				stop ()
			)
		) in
		ignore // Thread.create check_guts ();

		Avi.init_avi ();
		(try
			let avi = Avi.open_avi s.slave_i in
			let y_length = avi.Avi.h * avi.Avi.w in
			let v_offset = y_length in
			let v_length = y_length lsr 2 in
			let u_offset = y_length + v_length in
			let u_length = v_length in
			(try
				let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
				(try
					Unix.connect sock s.slave_addr;

					(* Send the key! *)
					really_send sock s.slave_key 0 (String.length s.slave_key);

					let str = String.create s.slave_bytes_per_frame in
					for f = s.slave_first_frame to s.slave_last_frame - 1 do
						if check_stop () then (failwith "STOPPED");
						if f land 15 = 0 then (
							p "changing thread priority (or at least checking)";
							change_thread_priority p;
						);
						p // sprintf "getting frame %d" f;
						let got = Avi.blit_frame avi f str 0 in
						if got <> 1 then (
							(* Oops. Didn't get a frame *)
							failwith // sprintf "didn't get exactly one frame (expected frame #%d), got %d frames" f got
						);
						if check_stop () then (failwith "STOPPED");
						p // sprintf "sending frame %d" f;
(*						let ok = really_send_timeout sock str 0 (String.length str) 10.0 in*)

						(* Each plane needs to be sent separately since x264 expects I420-style data, whereas the AVI file outputs YV12 *)
						(* This means that the U and V planes need to be sent out of order *)
						(* Send Y plane *)
						let ok = really_send_parts sock str 0 y_length 10 in
						if not ok then (failwith "Y NOT SENT");

						if check_stop () then (failwith "STOPPED after Y");

						(* Send U plane *)
						let ok = really_send_parts sock str u_offset u_length 10 in
						if not ok then (failwith "U NOT SENT");

						(* Send V plane *)
						let ok = really_send_parts sock str v_offset v_length 10 in
						if not ok then (failwith "V NOT SENT");

					done;
				with
					e -> (p // sprintf "AVI thread exited (after sock) with %S" (Printexc.to_string e))
				);
				Unix.close sock;
			with
				e -> (p // sprintf "AVI thread exited (after init) with %S" (Printexc.to_string e))
			);
			Avi.close_avi avi;
		with
			e -> (p // sprintf "AVI thread exited with %S" (Printexc.to_string e))
		);
		Avi.exit_avi ();
		true
	)
;;




(* DO THE SLAVE HERE! *)
if Array.length Sys.argv = 2 && Sys.argv.(1) = "--slave" then (
	(* Run the slave *)
	try
		let q = match 2 with
			| 1 -> do_slave_multithread ()
			| 2 -> do_slave_ptr ()
			| _ -> do_slave ()
		in
(*		let q = if false then do_slave () else do_slave_ptr () in*)
(*		let q = do_slave_multithread () in*)
		if q then (
			exit 1; (* I say 1 is normal exit for the slave (0 is for the controller, you see...) *)
		) else (
			exit 2; (* 2 is for some sort of error *)
		)
	with
		_ -> exit 3; (* 3 is something that I didn't catch *)
);;
		
(* Put the version string here so it doesn't show up multiple times when starting slaves *)
if Sys.word_size = 32 then (
	printf "%s controller\n%!" version_string
) else (
	printf "%s controller %d-bit\n%!" version_string Sys.word_size
);;

(***************************************)
(* Non-slave controller-based encoding *)
(***************************************)
(* Doesn't work due to thread-unsafe AVIsynth FAIL *)
(*
let do_controller_not_slave p s event_channel =
	let current_thread_priority_ref = ref Opt.Thread_priority_normal in
	let set_thread_priority_ref = ref Opt.Thread_priority_normal in
	let thread_priority_mutex = Mutex.create () in
	let change_thread_priority p = (
		Mutex.lock thread_priority_mutex;
		if !current_thread_priority_ref <> !set_thread_priority_ref then (
			p // sprintf "changing thread priority to %d" (Obj.magic !set_thread_priority_ref);
			ignore // Opt.set_thread_priority !set_thread_priority_ref;
			match Opt.get_thread_priority () with
			| None -> ()
			| Some p -> current_thread_priority_ref := p
		);
		Mutex.unlock thread_priority_mutex;
	) in

	let keep_going_ref = ref true in
	let keep_going_mutex = Mutex.create () in
	let check_stop () = (
		Mutex.lock keep_going_mutex;
		let a = !keep_going_ref in
		Mutex.unlock keep_going_mutex;
		not a
	) in
	let stop () = (
		Mutex.lock keep_going_mutex;
		keep_going_ref := false;
		Mutex.unlock keep_going_mutex;
	) in

	let rec send_select d ptr off len num_left = (
		if num_left <= 0 then (
			failwith "OUT OF TIME";
		) else (
			match Unix.select [] [d] [] 1.0 with
			| (_,[_],_) -> (
				let r = Avi.send_ptr d ptr off len in
				r
			)
			| _ -> (
				(* Check if we should keep going *)
				if check_stop () then (failwith "STOPPED in select");
				p "  trying again";
				send_select d ptr off len (pred num_left)
			)
		)
	) in
	let rec really_send_parts d ptr off len n = (
		if len <= 0 then true else (
			p // sprintf "sending %d+%d" off len;
			let r = send_select d ptr off len n in
			p // sprintf "sent %d" r;
			if r = 0 then (
				raise End_of_file
			) else if r = len then (
				true
			) else (
				really_send_parts d ptr (off + r) (len - r) n
			);
		)
	) in

	let rec check_guts () = (
		let next_object = Event.sync (Event.receive event_channel) in
		match next_object with
		| Slave_set_priority (_,tp) -> (
			p "setting priority";
			ignore // Opt.set_thread_priority tp; (* Set this thread's priority to the same *)
			Mutex.lock thread_priority_mutex;
			set_thread_priority_ref := tp;
			Mutex.unlock thread_priority_mutex;
			check_guts ()
		)
		| _ -> (
			p "telling thread to stop";
			stop ()
		)
	) in
	ignore // Thread.create check_guts ();

	Avi.init_avi ();
	(try
		let avi = Avi.open_avi s.slave_i in
		let y_length = avi.Avi.h * avi.Avi.w in
		let v_offset = y_length in
		let v_length = y_length lsr 2 in
		let u_offset = y_length + v_length in
		let u_length = v_length in
		(try
			let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
			(try
				Unix.connect sock s.slave_addr;
				let ptr = Avi.make_ptr s.slave_bytes_per_frame in
				for f = s.slave_first_frame to s.slave_last_frame - 1 do
					if check_stop () then (failwith "STOPPED");
					if f land 15 = 0 then (
						p "changing thread priority (or at least checking)";
						change_thread_priority p;
					);
					p // sprintf "getting frame %d" f;
					let got = Avi.ptr_avi_frame avi f ptr in
					if got <> 1 then (
						(* Oops. Didn't get a frame *)
						failwith // sprintf "didn't get exactly one frame (expected frame #%d), got %d frames" f got
					);
					if check_stop () then (failwith "STOPPED");
					p // sprintf "sending frame %d" f;
(*					let ok = really_send_timeout sock str 0 (String.length str) 10.0 in*)

					(* Each plane needs to be sent separately since x264 expects I420-style data, whereas the AVI file outputs YV12 *)
					(* This means that the U and V planes need to be sent out of order *)
					(* Send Y plane *)
					p "Y";
					let ok = really_send_parts sock ptr 0 y_length 10 in
					if not ok then (failwith "Y NOT SENT");

					if check_stop () then (failwith "STOPPED after Y");

					(* Send U plane *)
					p "u";
					let ok = really_send_parts sock ptr u_offset u_length 10 in
					if not ok then (failwith "U NOT SENT");

					(* Send V plane *)
					p "v";
					let ok = really_send_parts sock ptr v_offset v_length 10 in
					if not ok then (failwith "V NOT SENT");

				done;
			with
				e -> (p // sprintf "AVI thread exited (after sock) with %S" (Printexc.to_string e))
			);
			Unix.close sock;
		with
			e -> (p // sprintf "AVI thread exited (after init) with %S" (Printexc.to_string e))
		);
		Avi.close_avi avi;
	with
		e -> (p // sprintf "AVI thread exited with %S" (Printexc.to_string e))
	);
	Avi.exit_avi ();
	()
;;
*)

(*****************)
(* PARSE OPTIONS *)
(*****************)
(* OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO *)
let parse_these_options r =
	let arg_x = ref [] in
	let arg_i = ref [] in
	let arg_o = ref [] in
	let arg_seek = ref [] in
	let arg_frames = ref [] in
	let arg_zones = ref [] in

	let add a x = a := x :: !a in
	let add_int a x = a := int_of_string x :: !a in
	let add_zones a x = a := set_zones x :: !a in

	let arg_config = ref "config.xml" in
	let arg_logfile = ref "out-dump.txt" in
	let arg_clean = ref false in
	let arg_restart = ref false in

	let more_args = ref [] in

	let specs = Arg.align [
		("-x",        Arg.String (add arg_x),                 "\"\" x264 encoding options (REQUIRED)");
		("-i",        Arg.String (add arg_i),                 "\"\" input AVS file (REQUIRED)");
		("-o",        Arg.String (add arg_o),       "\"output.mkv\" output MKV file");
		("--seek",    Arg.String (add_int arg_seek),             "0 first frame to encode");
		("--frames",  Arg.String (add_int arg_frames),       "(all) number of frames to encode");
		("--zones",   Arg.String (add_zones arg_zones),       "\"\" same as x264's --zones");
		("--config",  Arg.Set_string arg_config,    "\"config.xml\" XML configuration file");
		("--logfile", Arg.Set_string arg_logfile, "\"out-dump.txt\" Verbose log file");
		("--clean",   Arg.Set arg_clean,                          " Delete any unused GOPs from the current temp directory");
		("--restart", Arg.Set arg_restart,                        " Remove all temp files from previous encodes and start over");
(*		("--slave",   Arg.Unit ignore,                            " Internal use only");*) (* Handle the slave further up *)
	] in

	let usage = "\
		You may use multiple -x, -i, -o, --seek, --frames, and --zones options.\n\
		The first -x, i, etc. options will be matched together in one encode\n\
		the second ones will be in the next encode, etc.. If there are fewer\n\
		of one option than the rest, then the last of that type is used for\n\
		all of the subsequent encodes.\n\
	" in
	Arg.current := 0;
	(match trap_exception (fun () -> Arg.parse_argv r specs (fun x -> more_args := x :: !more_args) usage) () with
		| Exception (Arg.Bad x | Arg.Help x) -> (
			eprintf "\n%s" x;
			exit 1
		)
		| Exception e -> (printf "%s" (Printexc.to_string e); exit 1)
		| Normal () -> ()
	);

	(* The slave option is no longer handled here *)

	let lengths = [|
		List.length !arg_x;
		List.length !arg_i;
		List.length !arg_o;
		List.length !arg_seek;
		List.length !arg_frames;
		List.length !arg_zones;
	|] in
	let longest_list = Array.fold_left max 0 lengths in

	if longest_list > 1 && List.length !arg_o <> longest_list then (
		(* There aren't enough output filenames to store the encodes *)
		eprintf "\n%s: not enough output filenames given.\n" Sys.argv.(0);
		Arg.usage specs usage;
		exit 1;
	) else if List.length !arg_x = 0 then (
		eprintf "\n%s: must specify -x encoding options.\n" Sys.argv.(0);
		Arg.usage specs usage;
		exit 1;
	) else if List.length !arg_i = 0 then (
		eprintf "\n%s: must specify -i input AVS file.\n" Sys.argv.(0);
		Arg.usage specs usage;
		exit 1;
	) else (
		(* Pad the lists to the max length *)
(*
		let rec pad default x_ref = (
			let rec helper = function
				| y when List.length y = longest_list -> y
				| [] -> helper [default]
				| hd :: tl -> helper (hd :: hd :: tl)
			in
			x_ref := helper !x_ref;
		) in
*)
		let rev_pad default x_ref = (
			let rec helper = function
				| y when List.length y = longest_list -> y
				| [] -> helper [default]
				| hd :: tl -> helper (hd :: hd :: tl)
			in
			x_ref := List.rev (helper !x_ref);
		) in
		rev_pad "" arg_x;
		rev_pad "" arg_i;
		rev_pad "output.mkv" arg_o;
		rev_pad 0 arg_seek;
		rev_pad max_int arg_frames;
		rev_pad [] arg_zones;

		let make_v () = match (!arg_x, !arg_i, !arg_o, !arg_seek,	!arg_frames, !arg_zones) with
			| (x :: x_tl, i :: i_tl, o :: o_tl, seek :: seek_tl, frames :: frames_tl, zones :: zones_tl) -> (
				let enc_type = if String.length o > 4 && String.lowercase (String.sub o (String.length o - 4) 4) = ".mp4" then Encode_mp4 else Encode_mkv in
				arg_x := x_tl;
				arg_i := i_tl;
				arg_o := o_tl;
				arg_seek := seek_tl;
				arg_frames := frames_tl;
				arg_zones := zones_tl;
				Some {
					v_x = x;
					v_i = i;
					v_o = o;
					v_encode_type = enc_type;
					v_seek = seek;
					v_frames = frames;
					v_zones = zones;
				}
			)
			| _ -> None
		in
		let v_opt_array = Array.create longest_list None in
		for i = 0 to longest_list - 1 do
			v_opt_array.(i) <- make_v ()
		done;

		let v_array = Array.map (function
			| Some x -> x
			| None -> (
				eprintf "\n%s: wrong number of encoding parameters (this shouldn't occur).\n" Sys.argv.(0);
				Arg.usage specs usage;
				exit 1
			)
		) v_opt_array in

		{
			o_v = v_array;
			o_config = !arg_config;
			o_logfile = !arg_logfile;
			o_clean = !arg_clean;
			o_restart = !arg_restart;
		}
	)
;;



(*
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
		("--slave", Arg.Set arg_slave, " Internal use only");
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

	(* Encode type *)
	let encode_type = (
		if String.length !arg_o > 4 && String.lowercase (String.sub !arg_o (String.length !arg_o - 4) 4) = ".mp4" then (
			Encode_mp4
		) else (
			Encode_mkv
		)
	) in

	{
(*
		o_x = !arg_x;
		o_i = !arg_i;
		o_o = !arg_o;
		o_encode_type = encode_type;
		o_seek = !arg_seek;
		o_frames = !arg_frames;
		o_zones = confine_zones !arg_zones 0 max_int;
*)
		o_v = [|{
			v_x = !arg_x;
			v_i = !arg_i;
			v_o = !arg_o;
			v_encode_type = encode_type;
			v_seek = !arg_seek;
			v_frames = !arg_frames;
			v_zones = confine_zones !arg_zones 0 max_int;
		}|];
		o_config = !arg_config;
		o_logfile = !arg_logfile;
		o_clean = !arg_clean;
		o_restart = !arg_restart;
(*		o_blit = Avs.blit_nolock;*)
	}
);;
*)

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


	(*********)
	(* PRINT *)
	(*********)


	{
		c_controller_name = !controller_name_ref;
		c_agent_port = !agent_find_port_ref;
		c_controller_port = !controller_find_port_ref;
		c_static_agents = !agent_list_ref;
		c_temp_dir = !temp_dir_ref;
	}
;;

(* PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP *)
let get_print o = (
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
						flush ph;
					);
					if lock then Mutex.unlock pm;
				)
			)
		)
	) in
	(print,pm)
);;

(* IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII *)
let parse_this_encode o c v = (
	(* Note that the "o" and "c" here are local! *)
	(* Also note that o.o_v is not used; v is used for all the video-related stuff *)

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
		Mutex.lock Avi.bm;
		Avi.init_avi ();
		let n = trap_exception Avi.info_avi v.v_i in
		Avi.exit_avi ();
		Mutex.unlock Avi.bm;
		match n with
		| Normal q -> q
		| Exception _ -> (printf "\nERROR: AVS file \"%s\" does not seem to be valid\n" v.v_i; exit 1)
	) else (
		let file_info_handle = Unix.open_process_in (sprintf "\"%s -frames 1 -raw -o %s \"%s\" 2>&1\"" avs2yuv dev_null v.v_i) in
		let file_info = input_line file_info_handle in
		(match Unix.close_process_in file_info_handle with
			| Unix.WEXITED   1 -> (printf "\nERROR: avs2yuv exited with error code 1; avs2yuv responded:\n  %s" file_info; exit 1)
			| Unix.WEXITED   x -> if x <> 0 then (printf "\nERROR: avs2yuv exited with error code %d\n" x; exit 1)
			| Unix.WSIGNALED x -> (printf "\nERROR: avs2yuv was killed with signal %d\n" x; exit 1)
			| Unix.WSTOPPED  x -> (printf "\nERROR: avs2yuv was stopped with signal %d\n" x; exit 1)
		);

		match match_string file_info with
		| None -> (printf "\nERROR: AVS file \"%s\" does not seem to be valid\n" v.v_i; exit 1)
		| Some q -> q
	) in

	let seek_use = bound 0 (pred frames) v.v_seek in
	let frames_use = min v.v_frames (frames - seek_use) in

	let time_string () = (
		let tod = Unix.gettimeofday () in
		let cs = int_of_float (100.0 *. (tod -. floor tod)) in (* centi-seconds *)
		let lt = Unix.localtime tod in
		sprintf "%04d-%02d-%02d~%02d:%02d:%02d.%02d" (lt.Unix.tm_year + 1900) (lt.Unix.tm_mon + 1) (lt.Unix.tm_mday) (lt.Unix.tm_hour) (lt.Unix.tm_min) (lt.Unix.tm_sec) cs
	) in

	(*********)
	(* PRINT *)
	(*********)
(*
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
*)
	(****************************)
	(* AVS AND ENCODE DIRECTORY *)
	(****************************)
	let avs_dir = Filename.concat c.c_temp_dir (Filename.basename v.v_i ^ " " ^ to_hex (Digest.file v.v_i)) in
	let encode_md5 = (
		let str = sprintf "%s" v.v_x in
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
(*
		p = print;
		pm = pm;
*)
	}
);;
(************************)
(* END OF PARSE OPTIONS *)
(************************)







(*****************)
(* FILE CREATORS *)
(*****************)
let rec output_mkv f vi mutex condition n_in =
	let name = n_in ^ " " in
	let p = f#pn name in
	let i = vi.i in

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

	let out = Unix.openfile vi.v.v_o [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC] 0o600 in

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
			Mutex.lock mutex;
			let rec gimme_trax () = match vi.vi_track_private with
				| Some (trax,_,_) -> trax
				| None -> (
					p "Tracks not available yet; keep waiting";
					Condition.wait condition mutex;
					gimme_trax ()
				)
			in
			let q = gimme_trax () in
			Mutex.unlock mutex;
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
(*			p // sprintf "finding 0x1F43B675 at offset %Ld (length %Ld)" (LargeFile.pos_in handle) (LargeFile.in_channel_length handle);*)
			match Matroska.input_id_and_length handle with
			| Some (0x0F43B675, _) -> ( (* SEGMENT *)
(*				p // sprintf "finding 0xE7 at offset %Ld (length %Ld)" (LargeFile.pos_in handle) (LargeFile.in_channel_length handle);*)
				match Matroska.input_id_and_length handle with
				| Some (0x67, Some l) -> ( (* TIMECODE *)
					let frame_num = (Matroska.input_uint 0L handle !-l) -| offset in
(*					p // sprintf "finding 0xA3 at offset %Ld (length %Ld)" (LargeFile.pos_in handle) (LargeFile.in_channel_length handle);*)
					match Matroska.input_id_and_length handle with
					| Some (0x23, Some l) -> ( (* SIMPLEBLOCK *)
						let frame_timecode = i64_div_round (tcs_per_frame_n *| frame_num) tcs_per_frame_d in

						let (cluster_timecode, next_cluster_timecode, next_next_cluster_number) = if frame_timecode >= next_cluster_timecode then (
							if f#encoding_done then (
(*								ip ~screen:true // sprintf "Writing %Ld / %d" frame_num i.i_encode_frames;*)
(*								f#console_print_noenter // sprintf "Writing %Ld / %d" frame_num i.i_encode_frames;*)
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
				p "all frames are done";
			) else (
				p "locking frame mutex";
				Mutex.lock mutex;
				while not (Rbtree.mem vi.vi_frames here) do
					p "waiting on frame condition";
					Condition.wait condition mutex
				done;
				let current_gop_perhaps = Rbtree.find vi.vi_frames here in
				Mutex.unlock mutex;
				p "continuing";
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
						Mutex.lock mutex;
						vi.vi_frames_done_to <- here + gop.gop_frames;
						p // sprintf "frame_done_to is now %d" vi.vi_frames_done_to;
						Mutex.unlock mutex;

						(* Print out something if it's been a while *)
						let every = 1000 in
						if here / every <> (here + gop.gop_frames) / every then (
							f#pretty_print false
						);

						do_gop (here + gop.gop_frames) ct nct nncn
					)
					| Exception x -> raise x
				)
			)
		) in
		do_gop 0 (-1L) 0L 1;
(*
		f#console_print_noenter "DONE WRITING!";
		f#console_print_noenter "\n";
*)
		None
	with
		e -> Some e
	) in
	Unix.close out;

	(* Now see if the file was written properly *)
	match file_exception with
	| None -> (
		(* Exit, but flick the condition first *)
		Mutex.lock mutex;
		vi.vi_outputted <- true;
		p "flicking condition";
		Condition.broadcast condition;
		Mutex.unlock mutex;
	)
	| Some (Not_found
		| Matroska.Invalid_integer
		| Matroska.Invalid_integer_length _
		| Matroska.Invalid_float_length _
		| Matroska.Unsupported_float
		| Matroska.Invalid_ID _
		| Matroska.Unknown_ID _
		| Matroska.ID_string_too_short _
		| Matroska.Invalid_element_position _
		| Matroska.String_too_large _
		| Matroska.Loop_end
		| Matroska.Matroska_error _
	as e) -> (
		(* Looks like there was an error in one of the MKC files *)
		(* Let's remove the GOP at frame_done_to_ref, just in case there was a problem *)
		p // sprintf "an error occurred when trying to output the file (%S); trying again" (Printexc.to_string e);
		Mutex.lock mutex;
		let gop_perhaps = Rbtree.find vi.vi_frames vi.vi_frames_done_to in
		Rbtree.remove vi.vi_frames vi.vi_frames_done_to;
		vi.vi_frames_done_to <- 0; (* Reset the frame counter *)
		Mutex.unlock mutex;
		(match gop_perhaps with
			| None -> ()
			| Some (_,gop) -> ignore // trap_exception Sys.remove gop.gop_filename (* Remove the file, too *)
		);
		output_mkv f vi mutex condition name
	)
	| Some e -> raise e
;;



let rec output_mp4 f vi mutex condition n_in =
	let name = n_in ^ " " in
	let p = f#pn name in
	let i = vi.i in

	let rec gcd a b = if b = 0L then a else gcd b (Int64.rem a b) in

	let (fps_n, fps_d) = (
		let g = gcd !|(i.i_fps_n) !|(i.i_fps_d) in
		(!|(i.i_fps_n) /| g, !|(i.i_fps_d) /| g)
	) in
	let max_frames_in_any_second = int_of_float (float_of_int (i.i_fps_n + i.i_fps_d - 1) /. float_of_int i.i_fps_d) in

	p // sprintf "using FPS %Ld/%Ld" fps_n fps_d;
	p // sprintf "max %d frames in any given second" max_frames_in_any_second;

	(* MP4 timestamps start at 00:00:00 GMT Jan 1, 1904, or -2082844800 in Unix time *)
	let timestamp = Int64.of_float (Unix.time () +. 2082844800.) in

	let frames_per_chunk = 10 in

	let timescale = !-fps_n in
	let duration = fps_d *| !|(i.i_encode_frames) in

	(* Frame-tracking structures *)
	let ctts = Array.make i.i_encode_frames 0 in (* display frame number offset *)
	let stsz = Array.make i.i_encode_frames 0 in (* bytes per frame *)
	let stco_ref = ref [] in (* Stores the 64-bit file location for every frames_per_chunk frames *)
	let stss_ref = ref [] in (* Stores the frame number of all of the IDR frames (0-based!) *)
	let byterate_array = Array.create max_frames_in_any_second 0 in (* The number of bytes in any 1-second period *)
	let max_framesize_ref = ref 0 in
	let max_byterate_ref = ref 0 in
	let total_bytes_ref = ref 0L in

	let min_ctts_ref = ref 0 in

	let out = Unix.openfile vi.v.v_o [Unix.O_WRONLY; Unix.O_CREAT; Unix.O_TRUNC] 0o600 in
	let write s = ignore // Unix.write out s 0 (String.length s) in

	let file_exception = (try

		(* Output the ftyp box (it's always the same) *)
		write "\x00\x00\x00\x18\x66\x74\x79\x70\x61\x76\x63\x31\x00\x00\x00\x00\x69\x73\x6F\x6D\x61\x76\x63\x31";

		(* Now write the mdat box as a 64-bit length, and record the length position *)
		write "\x00\x00\x00\x01";
		write "mdat";
		let mdat_length_pos = Unix.LargeFile.lseek out 0L Unix.SEEK_CUR in
		write "\x00\x00\x00\x00\x00\x00\x00\x00";

		let offset = !|(i.i_encode_seek) in

		let get_another_frame handle coding_num = (
			match Matroska.input_id_and_length handle with
			| Some (0x0F43B675, _) -> ( (* SEGMENT *)
				match Matroska.input_id_and_length handle with
				| Some (0x67, Some l) -> ( (* TIMECODE *)
					let frame_num_64 = (Matroska.input_uint 0L handle !-l) -| offset in
					let frame_num = !- frame_num_64 in
					match Matroska.input_id_and_length handle with
					| Some (0x23, Some l_64) -> ( (* SIMPLEBLOCK *)
						let frame_len = !- l_64 in

						let i_frame = (
							let a = String.create 4 in
							really_input handle a 0 4;
							match a with
							| "\x81\x00\x00\x80" -> true
							| _ -> false
						) in

						(* Update the MP4 frame data *)
						p // sprintf "frame %d:" coding_num;
						ctts.(coding_num) <- frame_num - coding_num;
						min_ctts_ref := min !min_ctts_ref (frame_num - coding_num);
						p // sprintf "  delay %d (min %d)" (frame_num - coding_num) !min_ctts_ref;
						stsz.(coding_num) <- frame_len - 4; (* subtract 4 since the MKV header isn't copied *)
						p // sprintf "  length %d" (frame_len - 4);
						if coding_num mod frames_per_chunk = 0 then (
							(* It's a chunk start *)
							let pos = Unix.LargeFile.lseek out 0L Unix.SEEK_CUR in
							p // sprintf "  *chunk start at pos %Ld" pos;
							stco_ref := pos :: !stco_ref;
						);
						if i_frame then (
							(* Add I frame to the list of seek frames *)
							p "  *IDR frame";
							stss_ref := frame_num :: !stss_ref;
						);
						(
							(* Bitrate stuff *)
							let index_now = frame_num mod max_frames_in_any_second in
							(* Update the largest frame *)
							max_framesize_ref := max !max_framesize_ref (frame_len - 4);
							let current_sum = byterate_array.(index_now) in
							(* Update the maximum number of bytes in one second *)
							max_byterate_ref := max !max_byterate_ref current_sum;
							(* Reset the number of bytes on one of the running totals *)
							byterate_array.(index_now) <- 0;
							(* Add this frame length to every running total *)
							for i = 0 to max_frames_in_any_second - 1 do
								byterate_array.(i) <- byterate_array.(i) + frame_len - 4
							done;
							(* Add this frame length to the total number of bytes *)
							total_bytes_ref := !total_bytes_ref +| !|(frame_len - 4);

							p // sprintf "  max byterate is %d" !max_byterate_ref;
						);

						let frame_guts = String.create (frame_len - 4) in
						really_input handle frame_guts 0 (String.length frame_guts);
						write frame_guts

					)
					| _ -> raise Not_found
				)
				| _ -> raise Not_found
			)
			| _ -> raise Not_found
		) in

		let rec do_gop here = (
			if here = i.i_encode_frames then (
				(* DONE *)
				p "all frames are done";
			) else (
				p "locking frame mutex";
				Mutex.lock mutex;
				while not (Rbtree.mem vi.vi_frames here) do
					p "waiting on frame condition";
					Condition.wait condition mutex;
					p "going on frame condition";
				done;
				let current_gop_perhaps = Rbtree.find vi.vi_frames here in
				Mutex.unlock mutex;
				p "continuing";
				match current_gop_perhaps with
				| None -> do_gop here
				| Some (_,gop) -> (
					p // sprintf "outputting GOP at frame %d (length %d) with filename \"%s\"" here gop.gop_frames gop.gop_filename;
					if f#encoding_done then (
(*						ip ~screen:true // sprintf "Writing %d / %d" here i.i_encode_frames*)
(*						console_print_noenter // sprintf "Writing %d / %d" here i.i_encode_frames;*)
					);
					let handle = open_in_bin gop.gop_filename in
					let exc_perhaps = (try
						for i = 0 to gop.gop_frames - 1 do
							get_another_frame handle (here + i);
						done;
						Normal ()
					with
						e -> Exception e
					) in
					close_in handle;
					match exc_perhaps with
					| Normal () -> (
						Mutex.lock mutex;
						vi.vi_frames_done_to <- here + gop.gop_frames;
						p // sprintf "frame_done_to is now %d" vi.vi_frames_done_to;
						Mutex.unlock mutex;

						(* Print out something every once in a while *)
						let every = 1000 in
						if here / every <> (here + gop.gop_frames) / every then (
							f#pretty_print false
						);

						do_gop (here + gop.gop_frames)
					)
					| Exception x -> raise x
				)
			)
		) in
		do_gop 0;

		(* Now fill in the length *)
		let mdat_end_pos = Unix.LargeFile.lseek out 0L Unix.SEEK_CUR in
		let mdat_length = mdat_end_pos -| mdat_length_pos +| 8L in
		let len_string = String.create 8 in
		Pack.pack64 len_string 0 mdat_length;
		ignore // Unix.LargeFile.lseek out mdat_length_pos Unix.SEEK_SET;
		write len_string;
		ignore // Unix.LargeFile.lseek out mdat_end_pos Unix.SEEK_SET;

		p "done writing frames";

		(* Now write all the other stupid stuff *)
		(* moov *)
			(* mvhd *)
			(* iods??? *)
			(* trak *)
				(* tkhd *)
				(* mdia *)
					(* mdhd *)
					(* hdlr *)
					(* minf *)
						(* vmhd *)
						(* dinf *)
							(* dref *)
						(* stbl *)
							(* stsd *)
							(* stts *)
							(* ctts *)
							(* stss *)
							(* stsc *)
							(* stsz *)
							(* stco/co64 *)

		(* moov *)

			(* mvhd - 120 *)
		let mvhd_string = (
			p "mvhd:";
			p // sprintf "  time:      %Ld" timestamp;
			p // sprintf "  timescale: %d" timescale;
			p // sprintf "  duration:  %Ld" duration;

			let s = String.create 120 in
			Pack.packN s 0 120;
			String.blit "mvhd" 0 s 4 4;
			Pack.packN s 8 0x01000000; (* version 1, no flags *)
			Pack.pack64 s 12 timestamp;
			Pack.pack64 s 20 timestamp;
			Pack.packN s 28 timescale;
			Pack.pack64 s 32 duration;
			Pack.packN s 40 0x00010000;
			Pack.packn s 44 0x0100;
			Pack.packn s 46 0;
			Pack.packN s 48 0;
			Pack.packN s 52 0;
			Pack.packN s 56 0x00010000;
			Pack.packN s 60 0;
			Pack.packN s 64 0;
			Pack.packN s 68 0;
			Pack.packN s 72 0x00010000;
			Pack.packN s 76 0;
			Pack.packN s 80 0;
			Pack.packN s 84 0;
			Pack.packN s 88 0x40000000;
			Pack.packN s 92 0;
			Pack.packN s 96 0;
			Pack.packN s 100 0;
			Pack.packN s 104 0;
			Pack.packN s 108 0;
			Pack.packN s 112 0;
			Pack.packN s 116 2;
			s
		) in
		let mvhd_total_length = String.length mvhd_string in

		(* Now we need to get the private string *)
		Mutex.lock mutex;
		let rec gimme_private () = match vi.vi_track_private with
			| Some (_,priv,dims) -> (priv,dims)
			| None -> (
				p "private not available yet (this is probably bad)";
				Condition.wait condition mutex;
				gimme_private ()
			)
		in
		let (priv,(w,h,dw,dh)) = gimme_private () in
		Mutex.unlock mutex;
		(* Now get the SAR *)
		let (sar_num, sar_den) = (
			let top = !|h *| !|dw in
			let bottom = !|w *| !|dh in
			let g = gcd top bottom in
			(!-(top /| g), !-(bottom /| g))
		) in
		p // sprintf "SAR is %d:%d" sar_num sar_den;
		(* Figure out the actual display width and height (the MKV ones are just ratios) *)
		let (dw_int, dw_frac, dh_int, dh_frac) = if sar_num = sar_den then (
			(* 1:1 *)
			(i.i_res_x, 0, i.i_res_y, 0)
		) else if sar_num > sar_den then (
			(* big:small (wider/shorter); multiply height by SAR *)
			let w_num = i.i_res_x * sar_num in
			let w_den = sar_den in
			let w_int = w_num / w_den in
			let w_rem = w_num mod w_den in
			let w_fra = (w_rem * 65536 + w_den / 2) / w_den in
			p // sprintf "Display width  is %d/%d = %d %d/%d ~ %d %d/65536" w_num w_den w_int w_rem w_den w_int w_fra;
			p // sprintf "Display height is %d" i.i_res_y;
			(w_int, w_fra, i.i_res_y, 0)
		) else (
			(* small:big (narrower/taller); divide width by SAR *)
			let h_num = i.i_res_y * sar_den in
			let h_den = sar_num in
			let h_int = h_num / h_den in
			let h_rem = h_num mod h_den in
			let h_fra = (h_rem * 65536 + h_den / 2) / h_den in
			p // sprintf "Display width  is %d" i.i_res_x;
			p // sprintf "Display height is %d/%d = %d %d/%d ~ %d %d/65536" h_num h_den h_int h_rem h_den h_int h_fra;
			(i.i_res_x, 0, h_int, h_fra)
		) in

			(* iods??? *)
			(* trak *)
				(* tkhd - 104 *)
		let tkhd_string = (
			p "tkhd:";
			p // sprintf "  time:     %Ld" timestamp;
			p // sprintf "  duration: %Ld" duration;
			p // sprintf "  width:    %d %d/65536" dw_int dw_frac;
			p // sprintf "  height:   %d %d/65536" dh_int dh_frac;

			let s = String.create 104 in
			Pack.packN s 0 104;
			String.blit "tkhd" 0 s 4 4;
			Pack.packN s 8 0x01000000; (* version 1, no flags *)
			Pack.pack64 s 12 timestamp;
			Pack.pack64 s 20 timestamp;
			Pack.packN s 28 1;
			Pack.packN s 32 0;
			Pack.pack64 s 36 duration;
			Pack.packN s 44 0;
			Pack.packN s 48 0;
			Pack.packn s 52 0;
			Pack.packn s 54 0;
			Pack.packn s 56 0;
			Pack.packn s 58 0;
			Pack.packN s 60 0x00010000;
			Pack.packN s 64 0;
			Pack.packN s 68 0;
			Pack.packN s 72 0;
			Pack.packN s 76 0x00010000;
			Pack.packN s 80 0;
			Pack.packN s 84 0;
			Pack.packN s 88 0;
			Pack.packN s 92 0x40000000;
			Pack.packn s 96 dw_int;
			Pack.packn s 98 dw_frac;
			Pack.packn s 100 dh_int;
			Pack.packn s 102 dh_frac;
			s
		) in
		let tkhd_total_length = String.length tkhd_string in

				(* mdia *)
					(* mdhd - 44 *)
		let mdhd_string = (
			p "mdhd:";
			p // sprintf "  time:      %Ld" timestamp;
			p // sprintf "  timescale: %d" timescale;
			p // sprintf "  duration:  %Ld" duration;

			let s = String.create 44 in
			Pack.packN s 0 44;
			String.blit "mdhd" 0 s 4 4;
			Pack.packN s 8 0x01000000; (* version 1, no flags *)
			Pack.pack64 s 12 timestamp;
			Pack.pack64 s 20 timestamp;
			Pack.packN s 28 timescale;
			Pack.pack64 s 32 duration;
			Pack.packn s 40 0x55C4; (* 0x756e64, 0x150e04 = "und" in mp4's special format *)
			Pack.packn s 42 0;
			s
		) in
		let mdhd_total_length = String.length mdhd_string in

					(* hdlr - 49 *)
		let hdlr_string = (
			p "hdlr:";
			p "  \"vide\", \"x264farm handler\"";

			let s = String.create 49 in
			Pack.packN s 0 49;
			String.blit "hdlr" 0 s 4 4;
			Pack.packN s 8 0;
			Pack.packN s 12 0;
			String.blit "vide" 0 s 16 4;
			Pack.packN s 20 0;
			Pack.packN s 24 0;
			Pack.packN s 28 0;
			String.blit "x264farm handler\x00" 0 s 32 17;
			s
		) in
		let hdlr_total_length = String.length hdlr_string in

					(* minf *)
						(* vmhd - 20 *)
		let vmhd_string = (
			p "vmhd:";
			p "  useless little box";
			let s = String.create 20 in
			Pack.packN s 0 20;
			String.blit "vmhd" 0 s 4 4;
			Pack.packN s 8 1; (* version 0, flags 1 for some bizarre reason *)
			Pack.packn s 12 0;
			Pack.packn s 14 0;
			Pack.packn s 16 0;
			Pack.packn s 18 0;
			(* What a useless box! *)
			s
		) in
		let vmhd_total_length = String.length vmhd_string in

						(* dinf *)
							(* dref - 28 *)
		(* I pulled this string out of a file that x264 made - it just means "in this file" *)
		let dref_string = "\x00\x00\x00\x1C\x64\x72\x65\x66\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x0C\x75\x72\x6C\x20\x00\x00\x00\x01" in
		p "dref:";
		p "  local file";
		let dref_total_length = String.length dref_string in

						(* stbl *)
							(* stsd - 122+privatelength *)
		let stsd_string = (
			let cs = String.create 70 in (* This is the static part of the VisualSampleEntry structure *)
			Pack.packn cs 0 0;
			Pack.packn cs 2 0;
			Pack.packN cs 4 0;
			Pack.packN cs 8 0;
			Pack.packN cs 12 0;
			Pack.packn cs 16 i.i_res_x;
			Pack.packn cs 18 i.i_res_y;
			Pack.packN cs 20 0x00480000;
			Pack.packN cs 24 0x00480000;
			Pack.packN cs 28 0;
			Pack.packn cs 32 1;
			String.blit "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" 0 cs 34 32;
			Pack.packn cs 66 0x0018;
			Pack.packn cs 68 0xFFFF;

			let avcc = String.create (String.length priv + 8) in
			Pack.packN avcc 0 (String.length avcc);
			String.blit "avcC" 0 avcc 4 4;
			String.blit priv 0 avcc 8 (String.length priv);

			(* Now do the btrt box *)
			let btrt = String.create 20 in
			Pack.packN btrt 0 20;
			String.blit "btrt" 0 btrt 4 4;
			Pack.packN btrt 8 !max_framesize_ref;
			Pack.packN btrt 12 (!max_byterate_ref * 8);
			Pack.packN btrt 16 !-((8L *| fps_n *| !total_bytes_ref) /| (!|(i.i_encode_frames) *| fps_d));

			let s = String.create (8 + 8 + 8 + 8 + 70 + String.length avcc + 20(*btrt*)) in
			Pack.packN s 0 (8 + 8 + 8 + 8 + 70 + String.length avcc + 20);
			String.blit "stsd" 0 s 4 4;
			Pack.packN s 8 0;
			Pack.packN s 12 1;
			Pack.packN s 16 (8 + 8 + 70 + String.length avcc + 20(*btrt*));
			String.blit "avc1" 0 s 20 4;
			String.blit "\x00\x00\x00\x00\x00\x00\x00\x01" 0 s 24 8; (* reserved and data_reference_index *)
			String.blit cs 0 s 32 70;
			String.blit avcc 0 s 102 (String.length avcc);
			String.blit btrt 0 s (102 + String.length avcc) 20;

			p "stsd:";
			p // sprintf "  width:   %d" i.i_res_x;
			p // sprintf "  height:  %d" i.i_res_y;
			p // sprintf "  private: %s" (to_hex priv);
			p // sprintf "  bitrate:";
			p // sprintf "    max frame size: %d" !max_framesize_ref;
			p // sprintf "    max bitrate:    %d" (!max_byterate_ref * 8);
			p // sprintf "    avg bitrate:    %Ld" ((8L *| fps_n *| !total_bytes_ref) /| (!|(i.i_encode_frames) *| fps_d));

			s
		) in
		let stsd_total_length = String.length stsd_string in

		let delta = !-fps_d in

							(* stts *)
		let stts_string = (
			let s = String.create 24 in

			p "stts:";
			p // sprintf "  sample count: %d" i.i_encode_frames;
			p // sprintf "  sample delta: %d" delta;

			Pack.packN s 0 24;
			String.blit "stts" 0 s 4 4;
			Pack.packN s 8 0;
			Pack.packN s 12 1;
			Pack.packN s 16 i.i_encode_frames;
			Pack.packN s 20 delta;
			s
		) in
		let stts_total_length = String.length stts_string in

							(* ctts *)
		(* Make the ctts list with multiplicity from the ctts array *)
		let rec make_ctts so_far i last num list_length = (
			if i < 0 then (
				((num, (last - !min_ctts_ref) * delta) :: so_far, succ list_length)
			) else if ctts.(i) = last then (
				make_ctts so_far (pred i) last (succ num) list_length
			) else if num = 0 then (
				make_ctts so_far (pred i) ctts.(i) (succ num) list_length
			) else (
				make_ctts ((num, (last - !min_ctts_ref) * delta) :: so_far) (pred i) ctts.(i) 1 (succ list_length)
			)
		) in
		let (ctts_list, ctts_entries) = make_ctts [] (Array.length ctts - 1) 0 0 0 in
		let ctts_total_length = 16 + 8 * ctts_entries in
		(
			p "ctts:";
			p // sprintf "  entry number: %d (%d)" ctts_entries (List.length ctts_list);
			p // sprintf "  total length: %d" ctts_total_length;
			p "  entries:";
			List.iter (fun (num, off) -> p // sprintf "    %d @ %d" num off) ctts_list;
		);

							(* stss *)
		let rec make_stss so_far length_so_far = function
			| [] -> (so_far, length_so_far)
			| hd :: tl -> make_stss (hd :: so_far) (succ length_so_far) tl
		in
		let (stss_list, stss_entries) = make_stss [] 0 !stss_ref in
		let stss_total_length = 16 + 4 * stss_entries in
		(
			p "stss:";
			p // sprintf "  entry number: %d" stss_entries;
			p "  entries:";
			List.iter (fun frame -> p // sprintf "    %d" frame) stss_list;
		);

							(* stsc *)
		let stsc_string = if i.i_encode_frames <= frames_per_chunk then (
			(* There is only one entry -- (1, i.i_encode_frames, 1) *)
			let s = String.create 28 in
			Pack.packN s 0 28;
			String.blit "stsc" 0 s 4 4;
			Pack.packN s 8 0;
			Pack.packN s 12 1;
			Pack.packN s 16 1;
			Pack.packN s 20 i.i_encode_frames;
			Pack.packN s 24 1;
			p "stsc:";
			p // sprintf "  1 %d 1" i.i_encode_frames;
			s
		) else (
			(* There are 2 entries -- (1, frames_per_chunk, 1) & the ending chunk *)
			let s = String.create 40 in
			Pack.packN s 0 40;
			String.blit "stsc" 0 s 4 4;
			Pack.packN s 8 0;
			Pack.packN s 12 2;
			Pack.packN s 16 1;
			Pack.packN s 20 frames_per_chunk;
			Pack.packN s 24 1;
			let next_chunk = (i.i_encode_frames - 1) / frames_per_chunk + 1 in
			Pack.packN s 28 next_chunk;
			let frames_in_last_chunk = (i.i_encode_frames - 1) mod frames_per_chunk + 1 in
			Pack.packN s 32 frames_in_last_chunk;
			Pack.packN s 36 1;
			p "stsc:";
			p // sprintf "  1 %d 1" frames_per_chunk;
			p // sprintf "  %d %d 1" next_chunk frames_in_last_chunk;
			s
		) in
		let stsc_total_length = String.length stsc_string in

							(* stsz *)
		let stsz_total_length = 20 + 4 * i.i_encode_frames in

							(* stco/co64 *)
		let rec make_stco so_far length_so_far = function
			| [] -> (so_far, length_so_far)
			| hd :: tl -> make_stco (hd :: so_far) (succ length_so_far) tl
		in
		let (stco_list, stco_entries) = make_stco [] 0 !stco_ref in
		let stco_total_length = 16 + 8 * stco_entries in
		(
			p "stco:";
			p // sprintf "  entries: %d" stco_entries;
			let rec iter n = function
				| [] -> ()
				| hd :: tl -> (
					p // sprintf "    %d %d %Ld" n (n * frames_per_chunk) hd;
					iter (succ n) tl
				)
			in
			iter 0 stco_list;
		);

		(* Figure out the lengths of all the boxen *)
		(* moov  *)
			(* mvhd mvhd_total_length *)
			(* iods???  *)
			(* trak  *)
				(* tkhd tkhd_total_length *)
				(* mdia  *)
					(* mdhd mdhd_total_length *)
					(* hdlr hdlr_total_length *)
					(* minf  *)
						(* vmhd vmhd_total_length *)
						(* dinf  *)
							(* dref dref_total_length *)
						(* stbl  *)
							(* stsd stsd_total_length *)
							(* stts stts_total_length *)
							(* ctts ctts_total_length *)
							(* stss stss_total_length *)
							(* stsc stsc_total_length *)
							(* stsz stsz_total_length *)
							(* stco/co64 stco_total_length *)
		let stbl_total_length = 8 + stsd_total_length + stts_total_length + ctts_total_length + stss_total_length + stsc_total_length + stsz_total_length + stco_total_length in
		let dinf_total_length = 8 + dref_total_length in
		let minf_total_length = 8 + vmhd_total_length + dinf_total_length + stbl_total_length in
		let mdia_total_length = 8 + mdhd_total_length + hdlr_total_length + minf_total_length in
		let trak_total_length = 8 + tkhd_total_length + mdia_total_length in
		let moov_total_length = 8 + mvhd_total_length (*+ iods_total_length*) + trak_total_length in

		(* Now actually write all this garbage *)
		let writeN = (
			let temp = String.create 4 in
			fun n -> (
				Pack.packN temp 0 n;
				write temp
			)
		) in
		let write64 = (
			let temp = String.create 8 in
			fun n -> (
				Pack.pack64 temp 0 n;
				write temp
			)
		) in

		writeN moov_total_length;
		write "moov";
		write  mvhd_string;
		writeN trak_total_length;
		write "trak";
		write  tkhd_string;
		writeN mdia_total_length;
		write "mdia";
		write  mdhd_string;
		write  hdlr_string;
		writeN minf_total_length;
		write "minf";
		write  vmhd_string;
		writeN dinf_total_length;
		write "dinf";
		write  dref_string;
		writeN stbl_total_length;
		write "stbl";
		write stsd_string;
		write stts_string;

		(* ctts *)
		writeN ctts_total_length;
		write "ctts";
		writeN 0;
		writeN ctts_entries;
		List.iter (fun (a,b) -> writeN a; writeN b) ctts_list;

		(* stss *)
		writeN stss_total_length;
		write "stss";
		writeN 0;
		writeN stss_entries;
		List.iter writeN stss_list;

		write stsc_string;

		(* stsz *)
		writeN stsz_total_length;
		write "stsz";
		writeN 0;
		writeN 0;
		writeN i.i_encode_frames;
		Array.iter writeN stsz;

		(* co64 *)
		writeN stco_total_length;
		write "co64";
		writeN 0;
		writeN stco_entries;
		List.iter write64 stco_list;
(*
		console_print_noenter "DONE WRITING!";
		console_print_noenter "\n";
*)
		None
	with
		e -> Some e
	) in
	Unix.close out;

	match file_exception with
	| None -> (
		(* Exit, after hitting the condition *)
		Mutex.lock mutex;
		vi.vi_outputted <- true;
		p "flicking condition";
		Condition.broadcast condition;
		Mutex.unlock mutex;
	)
	| Some (Not_found
		| Matroska.Invalid_integer
		| Matroska.Invalid_integer_length _
		| Matroska.Invalid_float_length _
		| Matroska.Unsupported_float
		| Matroska.Invalid_ID _
		| Matroska.Unknown_ID _
		| Matroska.ID_string_too_short _
		| Matroska.Invalid_element_position _
		| Matroska.String_too_large _
		| Matroska.Loop_end
		| Matroska.Matroska_error _
	as e) -> (
		(* There's an error in an MKC file; remove the GOP at frame_done_to_ref, just in case *)
		p // sprintf "an error occurred when trying to output the file (%S); trying again" (Printexc.to_string e);
		Mutex.lock mutex;
		let gop_perhaps = Rbtree.find vi.vi_frames vi.vi_frames_done_to in
		Rbtree.remove vi.vi_frames vi.vi_frames_done_to;
		vi.vi_frames_done_to <- 0;
		Mutex.unlock mutex;
		(match gop_perhaps with
			| None -> ()
			| Some (_,gop) -> ignore // trap_exception Sys.remove gop.gop_filename
		);
		output_mp4 f vi mutex condition name
	)
	| Some e -> raise e

;;


















(*****************************)
(* MAJOR FRAME STORAGE CLASS *)
(*****************************)
(* Use c (defined before the object) and data to access everything! o should not be used *)
class frames o =
	let c = parse_this_config o.o_config in
	let (p,pm) = get_print o in
	let (window_width, console_print, console_print_noenter) = (
		let handle = Console.std_output_handle in
		let last_number_of_lines_ref = ref 0 in
		match Console.get_console_screen_buffer_info handle with
(**)
		| Console.Normal n -> (
			let console = new Console.console handle in
			let cp = p ~screen:false ~lock:false in
			let console_print sl = (
				let len = List.length sl in
				Mutex.lock pm;
				let old_len = !last_number_of_lines_ref in
				for q = 1 to len - old_len do
					(* Adds the right number of lines *)
					ignore // console#print_blank_line
				done;
				ignore // console#up (max len old_len);
				List.iter (fun x -> ignore // console#print_line x) sl;
				for q = 1 to old_len - len do
					(* Zeroes out the last lines before the end *)
					ignore // console#print_blank_line
				done;
				if old_len > len then ignore // console#up (old_len - len);
				List.iter cp sl;
				last_number_of_lines_ref := len;
				Mutex.unlock pm;
			) in
			let console_print_noenter s = (
				Mutex.lock pm;
				ignore // console#print_line s;
				ignore // console#up 1;
				cp s;
				Mutex.unlock pm;
			) in

			(n.Console.size_x, console_print, console_print_noenter)
		)
(**)
		| _ -> (
			let len = 80 in
			let cp = p ~screen:true ~lock:false in
			let normal_print sl = (
				Mutex.lock pm;
				List.iter cp sl;
				Mutex.unlock pm;
			) in
			let normal_print_noenter s = (
				let new_s = String.make len ' ' in
				String.blit s 0 new_s 0 (min (String.length s) (String.length new_s));
				new_s.[len - 1] <- '\r';

				Mutex.lock pm;
				print_string new_s;
				flush stdout;
				p ~screen:false ~lock:false s;
				Mutex.unlock pm;
			) in
			(len, normal_print, normal_print_noenter)
		)
	) in

	(* I don't really know where to put these *)
	let get_private_from_track_string s = (
		let rec check_element before = match Matroska.id_and_length_of_string before with
			| (after, 0x2E, _) -> check_element after (* Track *)
			| ((_,n), 0x23A2, Some len) -> String.sub s n !-len (* Codec private *)
			| ((_,n), _, Some len) -> check_element (s, n + !-len)
			| (after, _, None) -> check_element after (* Have to go into this element since the length is unknown *)
		in
		try
			let x = check_element (s,0) in
			p // sprintf "Got private data %S" (to_hex x);
			Some x
		with
			_ -> None
	) in
	let get_wh_from_track_string s = (
		let rec check_wh before = function
			| (Some w, Some h) -> (!-w,!-h)
			| (wp,hp) -> (
				(* Don't have some of the data *)
				match Matroska.id_and_length_of_string before with
				| (after, 0x2E, _) -> check_wh after (wp,hp) (* Track *)
				| (after, 0x60, _) -> check_wh after (wp,hp) (* Video *)
				| (during, 0x30, Some len) -> (
					let (after, w) = Matroska.uint_of_string 0L during !-len in
					check_wh after (Some w, hp)
				)
				| (during, 0x3A, Some len) -> (
					let (after, h) = Matroska.uint_of_string 0L during !-len in
					check_wh after (wp, Some h)
				)
				| ((_,n), _, Some len) -> check_wh (s, n + !-len) (wp,hp)
				| (after, _, None) -> check_wh after (wp,hp)
			)
		in
		let rec check_dwh before = function
			| (Some w, Some h, Some dw, Some dh) -> (!-w,!-h,!-dw,!-dh)
			| (wp,hp,dwp,dhp) -> (
				(* Don't have some of the data *)
				match Matroska.id_and_length_of_string before with
				| (after, 0x2E, _) -> check_dwh after (wp,hp,dwp,dhp) (* Track *)
				| (after, 0x60, _) -> check_dwh after (wp,hp,dwp,dhp) (* Video *)
				| (during, 0x30, Some len) -> (
					let (after, w) = Matroska.uint_of_string 0L during !-len in
					check_dwh after (Some w, hp, dwp, dhp)
				)
				| (during, 0x3A, Some len) -> (
					let (after, h) = Matroska.uint_of_string 0L during !-len in
					check_dwh after (wp, Some h, dwp, dhp)
				)
				| (during, 0x14B0, Some len) -> (
					let (after, dw) = Matroska.uint_of_string 0L during !-len in
					check_dwh after (wp, hp, Some dw, dhp)
				)
				| (during, 0x14BA, Some len) -> (
					let (after, dh) = Matroska.uint_of_string 0L during !-len in
					check_dwh after (wp, hp, dwp, Some dh)
				)
				| ((_,n), _, Some len) -> check_dwh (s, n + !-len) (wp,hp,dwp,dhp)
				| (after, _, None) -> check_dwh after (wp,hp,dwp,dhp)
			)
		in
		try
			let (w,h,dw,dh) = check_dwh (s,0) (None,None,None,None) in
			p // sprintf "Got dimensions %d:%d -> %d:%d" w h dw dh;
			Some (w,h,dw,dh)
		with
			_ -> (
				(* Oops. That didn't work. Let's try to just get the width and height *)
				try
					let (w,h) = check_wh (s,0) (None,None) in
					p // sprintf "Got dimensions %d:%d (no display dimensions)" w h;
					Some (w,h,w,h)
				with
					_ -> None
			)
	) in
	let full_option_of_track_option = function
		| None -> None
		| Some trax -> (
			match (get_private_from_track_string trax, get_wh_from_track_string trax) with
			| (Some priv, Some dims) -> Some (trax, priv, dims)
			| _ -> None
		)
	in

	object (x)
		method p = p
		method pn name x = p ~name:name x
		method console_print_noenter = console_print_noenter
		method c = c
(*		method pm = pm*)
		val data = (
			let version_rx2 = Rx2.make_rx [
				Rx2.Constant "#VERSION ";
				Rx2.Keep (Rx2.Plus (Rx2.Character [Rx2.Range ('0','9')]));
			] in
			let get_stats v = (
				p "GET STATS";
				let i = parse_this_encode o c v in
				ignore // create_dir i.i_encode_temp_dir;
				let name_1 = Filename.concat i.i_encode_temp_dir "working_stats_1.txt" in
				let name_2 = Filename.concat i.i_encode_temp_dir "working_stats_2.txt" in
				let h1 = open_in_gen [Open_rdonly; Open_text; Open_creat] 0o666 name_1 in
				let h2 = open_in_gen [Open_rdonly; Open_text; Open_creat] 0o666 name_2 in
				
				let get_version h = (try
					let rec test_line () = (
						let line = input_line h in
						match Rx2.rx version_rx2 line with
						| Some [v] -> Some (int_of_string v)
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

				let (input_stats, output_stats, output_version, v1_is_input) = match (version1, version2) with
					| (Some v1, Some v2) when v1 > v2 -> (open_in name_1, open_out name_2, succ v1, true )
					| (Some v1, Some v2)              -> (open_in name_2, open_out name_1, succ v2, false)
					| (Some v1, None)                 -> (open_in name_1, open_out name_2, succ v1, true )
					| (None, Some v2)                 -> (open_in name_2, open_out name_1, succ v2, false)
					| (None, None)                    -> (open_in name_2, open_out name_1, 1      , false) (* Start at version 1 *)
				in
				p // sprintf "Using stats file %d for input; %d for output" (if v1_is_input then 1 else 2) (if v1_is_input then 2 else 1);


				(* Get the privates *)
				let track_and_private = (
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
							full_option_of_track_option answer
						with
							_ -> None
						)
					) else (
						None
					)
				) in


				(* Read stats (I hope I can do it here) *)
				let frame_tree = Rbtree.create () in
				(try
					let gop_rx2 = Rx2.make_rx [
						Rx2.Constant "GOP ";
						Rx2.Keep (Rx2.Plus (Rx2.Character [Rx2.Range ('0','9')]));
						Rx2.Constant " ";
						Rx2.Keep (Rx2.Plus (Rx2.Character [Rx2.Range ('0','9')]));
						Rx2.Constant " <";
						Rx2.Keep (Rx2.Star_hook Rx2.Dot);
						Rx2.Constant ">";
						Rx2.Hook (Rx2.Constant " ");
						Rx2.Keep Rx2.Dot_star;
					] in
					let gopend_rx2 = Rx2.make_rx [
						Rx2.Constant "END ";
						Rx2.Keep (Rx2.Plus (Rx2.Character [Rx2.Range ('0','9')]));
						Rx2.Constant " ";
						Rx2.Keep (Rx2.Plus (Rx2.Character [Rx2.Range ('0','9')]));
						Rx2.Constant " <";
						Rx2.Keep (Rx2.Star_hook Rx2.Dot);
						Rx2.Constant ">";
						Rx2.Hook (Rx2.Constant " ");
						Rx2.Keep Rx2.Dot_star;
					] in
					while true do
						let line = input_line input_stats in
						p // sprintf "Trying line %S" line;
						match ((true, Rx2.rx gop_rx2 line), (false, Rx2.rx gopend_rx2 line)) with
						| ((not_end, Some [seek_string; frames_string; filename; zones]), _)
						| (_, (not_end, Some [seek_string; frames_string; filename; zones])) -> (
							(* Combine both the GOP and the GOPEND RX *)
							p // sprintf "GOT [%s] [%s] [%s] [%s] (end %B)" seek_string frames_string filename zones (not not_end);
							let seek = int_of_string seek_string in
							let frames = int_of_string frames_string in
							if not_end || (seek + frames = i.i_encode_seek + i.i_encode_frames) then (
								(* The GOP is either not at the end, or it's at the same end as the current encode *)
								if not // is_file filename then (
									p // sprintf "%S does not exist; throw out" line
								) else (
									if seek < i.i_encode_seek then (
										p // sprintf "%S is before the encode starts; throw out" line
									) else if seek + frames > i.i_encode_seek + i.i_encode_frames then (
										(* I think > is good enough here; >= will throw out this GOP even if it fits in the very end *)
										p // sprintf "%S is after the encode ends; throw out" line
									) else (
										p // sprintf "%S is fine frame-wise" line;
										let compare_zones = render_zones // confine_zones v.v_zones (seek - i.i_encode_seek) frames in
										p // sprintf "  Comparing found zones %S with correct zones %S" zones compare_zones;
										if zones = compare_zones then (
											Rbtree.add frame_tree (seek - i.i_encode_seek) {gop_frames = frames; gop_filename = filename}
										) else (
											p "  Zones do not match up"
										)
									)
								)
							) else (
								p // sprintf "%S does not end at the end of this encode; throw out" line
							);
							(* Output the stats line anyway, since it may still be good for another encode *)
							output_string output_stats line;
							output_char   output_stats '\n';
						)
						| _ -> () (* Not an important line *)
					done
				with
					End_of_file -> ()
				);
				 p "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAP";
				 close_in input_stats;
				 fprintf output_stats "#VERSION %d\n%!" output_version;


				{
					v = v;
					i = i;
(*					vi_stats_in = input_stats;*)
					vi_stats_out = output_stats;
					vi_stats_out_version = output_version;
					vi_frames = frame_tree;
					vi_frames_done_to = 0;
					vi_outputted = false;
					vi_track_private = track_and_private;
				}
			) in
			Array.map get_stats o.o_v
		)
		val mutable encoding_done = false
		val mutable output_done = false
		val frames_mutex = Mutex.create ()
		val frames_condition = Condition.create ()

		method encoding_done = (
			Mutex.lock frames_mutex;
			let is_done = if encoding_done then (
				true
			) else (
				let changed = Array.fold_left (fun so_far gnu -> so_far && gnu.vi_frames_done_to = gnu.i.i_encode_frames) true data in
				if changed then (
					encoding_done <- true;
					Condition.broadcast frames_condition;
				);
				changed
			) in
			Mutex.unlock frames_mutex;
			is_done
		)

		method output_done = (
			Mutex.lock frames_mutex;
			let is_done = if output_done then (
				true
			) else (
				let changed = Array.fold_left (fun so_far gnu -> so_far && gnu.vi_outputted) true data in
				if changed then (
					output_done <- true;
					Condition.broadcast frames_condition;
				);
				changed
			) in
			Mutex.unlock frames_mutex;
			is_done
		)

		method is_gop_here j n = (
			Mutex.lock frames_mutex;
			let out = Rbtree.mem data.(j).vi_frames n in
			Mutex.unlock frames_mutex;
			out
		)

		method find_gop_here j n = (
			Mutex.lock frames_mutex;
			let out = Rbtree.find data.(j).vi_frames n in
			Mutex.unlock frames_mutex;
			match out with
			| None -> None
			| Some (a,b) -> Some b
		)

		method update_tracks video_index trax = (
			Mutex.lock frames_mutex;
			let vi = data.(video_index) in
			(match vi.vi_track_private with
				| Some _ -> ()
				| None -> (
					(try
						let t = open_out_bin (Filename.concat vi.i.i_encode_temp_dir "TRACKS") in
						let len_s = String.create 4 in
						Pack.packN len_s 0 (String.length trax);
						output t len_s 0 4;
						output t trax 0 (String.length trax);
						vi.vi_track_private <- full_option_of_track_option (Some trax);
						close_out t
					with
						_ -> ()
					)
				)
			);
			Mutex.unlock frames_mutex;
		)

		method get_tracks video_index = (
			Mutex.lock frames_mutex;
			let t = data.(video_index).vi_track_private in
			Mutex.unlock frames_mutex;
			t
		)

(*
		method private put_gop_here_unsafe
		method put_gop_here
*)
		(* This returns whether or not the GOP was wanted *)
		(* parameters (since I didn't name them too well) are: *)
		(* job gop *)
		method private replace_gop_here_unsafe job g encode_frame = (
			let z = render_zones (confine_zones job.job_zones (encode_frame - job.job_encode_seek) g.gop_frames) in
			let dv = data.(job.job_video_index) in
			let old_gop_perhaps = Rbtree.find dv.vi_frames encode_frame in
			let already_exists = match old_gop_perhaps with
				| Some _ -> true
				| None -> false
			in
			let wanted = if dv.vi_frames_done_to < encode_frame || not already_exists then (
				(* The GOP is either new, or it's after the frames_done_to part *)
				(* There was already a GOP here, but it hasn't been outputted so it doesn't matter *)
				Rbtree.add dv.vi_frames encode_frame g;
				if encode_frame + g.gop_frames = dv.i.i_encode_frames then (
					(* Last GOP *)
					fprintf dv.vi_stats_out "END %0*d %03d <%s>%s%s\n%!" dv.i.i_avs_frame_num_len (encode_frame + job.job_i.i_encode_seek) g.gop_frames g.gop_filename (if z = "" then "" else " ") z
				) else (
					(* Not last GOP *)
					fprintf dv.vi_stats_out "GOP %0*d %03d <%s>%s%s\n%!" dv.i.i_avs_frame_num_len (encode_frame + job.job_i.i_encode_seek) g.gop_frames g.gop_filename (if z = "" then "" else " ") z
				);
				true
			) else (
				false
			) in
			p "replace_gop_here_unsafe broadcasting frames_condition";
			Condition.broadcast frames_condition;
			wanted
		)
		method replace_gop_here job g encode_frame = (
			Mutex.lock frames_mutex;
			let a = x#replace_gop_here_unsafe job g encode_frame in
			Mutex.unlock frames_mutex;
			a
		)
(*
		method is_done = (
			Mutex.lock frames_mutex;
			let start_at = Array.mapi (fun i {vi_frames_done_to = fdt} -> (i,fdt)) data in
			Mutex.unlock frames_mutex;
			let rec check_at j n = (
				if n = data.(j).i.i_encode_frames then true else (
					match x#find_gop_here j n with
					| None -> false
					| Some g -> check_at j (n + g.gop_frames)
				)
			) in
			Array.fold_left (fun so_far (i,gnu) -> so_far && check_at i gnu) true start_at
		)
*)
(*		method is_covered*)
		method private list_noncontiguous_unsafe = (
			Array.map (fun vi ->
				let rec first_gop_after n list_so_far = (
					if n = vi.i.i_encode_frames then (
						(* That's all *)
						List.rev list_so_far
					) else (
						match Rbtree.find_smallest_not_less_than vi.vi_frames n with
						| None -> (
							(* Add the rest of the frames onto the list, then reverse it *)
							List.rev ((n, vi.i.i_encode_frames - 1) :: list_so_far)
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
				first_gop_after vi.vi_frames_done_to []
			) data
		)
		method list_noncontiguous = (
			Mutex.lock frames_mutex;
			let out = x#list_noncontiguous_unsafe in
			Mutex.unlock frames_mutex;
			out
		)

		method wait_until_done = (
			Mutex.lock frames_mutex;
			p "WAIT waiting until done";
			while not x#encoding_done do
				p "WAIT keep waiting";
				Condition.wait frames_condition frames_mutex;
				p "WAIT check again";
			done;
			p "WAIT looks good";
			Mutex.unlock frames_mutex;
			p "WAIT FREE!";
		)

		method wait_until_output = (
			Mutex.lock frames_mutex;
			p "WAITOUTPUT waiting until output";
			while not x#output_done do
				p "WAITOUTPUT keep waiting";
				Condition.wait frames_condition frames_mutex;
				p "WAITOUTPUT check again";
			done;
			p "WAITOUTPUT looks good";
			Mutex.unlock frames_mutex;
			p "WAITOUTPUT FREE!";
		)


		(* Use a simple list since this shouldn't be too long, and there was no guarantee with the 2xRBtree setup that the hash key was the same as the agent's info *)
		val mutable agents = []
		val agents_mutex = Mutex.create ()



		(* Prints everything there is to print about these jobs *)
		method print_status = (
			mutex_lock_3 frames_mutex agents_mutex pm;
			let p = p ~lock:false in
			p "O:";
			p // sprintf " config:  \"%s\"" o.o_config;
			p // sprintf " logfile: \"%s\"" o.o_logfile;
			p // sprintf " clean?   %B" o.o_clean;
			p // sprintf " restart? %B" o.o_restart;

			p "C:";
			p // sprintf " controller name: %S" c.c_controller_name;
			p // sprintf " agent port:      %d" c.c_agent_port;
			p // sprintf " controller port: %d" c.c_controller_port;
			p // sprintf " static agents:   STUFF";
			p // sprintf " temp dir:        \"%s\"" c.c_temp_dir;

			p "DATA:";
			Array.iteri (fun j vi ->
				let v = vi.v in
				let i = vi.i in
				p // sprintf " %d:" j;
				p "  V:";
				p // sprintf "   input     \"%s\"" v.v_i;
				p // sprintf "   output    \"%s\"" v.v_o;
				p // sprintf "   options   \"%s\"" v.v_x;
				p // sprintf "   encode to %s" (match v.v_encode_type with | Encode_mkv -> "MKV" | Encode_mp4 -> "MP4");
				p // sprintf "   seek      %d" v.v_seek;
				p // sprintf "   frames    %d" v.v_frames;
				p // sprintf "   zones     %S" (render_zones v.v_zones);
				p "  I:";
				p // sprintf "   i_res_x:           %d" i.i_res_x;
				p // sprintf "   i_res_y:           %d" i.i_res_y;
				p // sprintf "   i_fps_n:           %d" i.i_fps_n;
				p // sprintf "   i_fps_d:           %d" i.i_fps_d;
				p // sprintf "   i_fps_f:           %g" i.i_fps_f;
				p // sprintf "   i_avs_frames:      %d" i.i_avs_frames;
				p // sprintf "   i_avs_frame_num_len: %d" i.i_avs_frame_num_len;
				p // sprintf "   i_encode_seek:     %d" i.i_encode_seek;
				p // sprintf "   i_encode_frames:   %d" i.i_encode_frames;
				p // sprintf "   i_bytes_y:         %d" i.i_bytes_y;
				p // sprintf "   i_bytes_uv:        %d" i.i_bytes_uv;
				p // sprintf "   i_bytes_per_frame: %d" i.i_bytes_per_frame;
				p // sprintf "   i_avs2yuv:         \"%s\"" i.i_avs2yuv;
				p // sprintf "   i_encode_temp_dir: \"%s\"" i.i_encode_temp_dir;
				p // sprintf "  STATS OUT VERSION: %d" vi.vi_stats_out_version;
				p // sprintf "  FRAMES DONE TO:    %d" vi.vi_frames_done_to;
				p "  GOPS:";
				Rbtree.iter (fun f gop ->
					p // sprintf "   %*d+%-3d @ %s" i.i_avs_frame_num_len f gop.gop_frames gop.gop_filename
				) vi.vi_frames
			) data;
			p "AGENTS:";
			List.iter (fun agent ->
				p "	AGENT:";
				p // sprintf "  agent name: %S" agent.agent_name;
				p // sprintf "  agent ip:   %s:%d" (fst agent.agent_ip) (snd agent.agent_ip);
				p // sprintf "  agent id:   %s" (to_hex agent.agent_id);
				p // sprintf "  agent opt:  %d" agent.agent_optimal_workers;
				p "  WORKERS:";
				Rbtree.iter (fun j w ->
					p // sprintf "   %d:" j;
(*
					p // sprintf "    start:        %s" (match w.worker_start   with | None -> "none" | Some x -> sprintf "some %d" x);
					p // sprintf "    current:      %s" (match w.worker_current with | None -> "none" | Some x -> sprintf "some %d" x);
*)
					p // sprintf "    total frames: %d" w.worker_total_frames;
					p // sprintf "    total time:   %g" w.worker_total_time;
					p // sprintf "    updated:      %g" w.worker_last_updated;
					p // sprintf "    status:       %s" (match w.worker_status with
						| Worker_disconnected     -> "disconnected"
						| Worker_connected        -> "connected"
						| Worker_agent (a,b)      -> sprintf "frame %d:%d" a b
						| Worker_controller (a,b) -> sprintf "frame %d:%d" a b
						| Worker_done             -> "done"
					);
				) agent.agent_workers;
			) agents;
			Mutex.unlock frames_mutex;
			Mutex.unlock agents_mutex;
			Mutex.unlock pm;
		)
		method private pretty_print_base = (
			x#print_status;
			mutex_lock_2 frames_mutex agents_mutex;
			let noncont = x#list_noncontiguous_unsafe in
			(*
				p "Noncont:";
				Array.iteri (fun video_index lst ->
					p // sprintf "%d:" video_index;
					List.iter (fun (a,b) -> p // sprintf "  (%d,%d)" a b) lst
				) noncont;
			*)
			let outputted = Array.map (fun {vi_frames_done_to = fdt} -> fdt) data in
			Mutex.unlock frames_mutex;
			let agent_status = List.fold_left (fun so_far agent ->
				let workers = Rbtree.fold_right (fun so_far_2 n w ->
					(* I use Rbtree.fold_right since this list is made backwards *)
					(n, {w with worker_total_frames = w.worker_total_frames}) :: so_far_2
				) [] agent.agent_workers in
				(agent.agent_name, agent.agent_ip, agent.agent_id, workers) :: so_far
			) [] agents in
			Mutex.unlock agents_mutex;

			p "A";

(*			let frame_len = window_width - 0 in*)
			let max_frames = Array.fold_left (fun so_far vi -> max so_far vi.i.i_encode_frames) 0 data in
			let max_line_width = window_width - 3 in
			let frame_len = Array.map (fun vi -> (max_line_width * vi.i.i_encode_frames + max_frames - 1) / max_frames) data in
(*			p // sprintf "Max frames to do: %d" max_frames;*)

			(* Add noncont data *)
			let frames = Array.mapi (fun video_index ranges ->
				let frame_len_now = frame_len.(video_index) in
				let frame_string = String.make frame_len_now '#' in
				let encode_frames = data.(video_index).i.i_encode_frames in
				List.iter (fun (a,b) ->
					let first_char = a * frame_len_now / encode_frames in
					let last_char = (b * frame_len_now + encode_frames - 1) / encode_frames - 1 in
					for j = first_char to last_char do
						frame_string.[j] <- '.'
					done
				) ranges;
				frame_string
			) noncont in

			p "B";
			
			(* Number of contiguous frames *)
			let cont_frames = Array.mapi (fun video_index ranges ->
				data.(video_index).i.i_encode_frames - List.fold_left (fun s (a,b) -> s + b - a + 1) 0 ranges
			) noncont in

			p "C";

			(* Now add the outputted stuff *)
			let last_char = Array.mapi (fun video_index cf ->
				outputted.(video_index) * frame_len.(video_index) / data.(video_index).i.i_encode_frames - 1
			) cont_frames in
			Array.iteri (fun video_index lc ->
				let vid = frames.(video_index) in
				for j = 0 to lc do
					vid.[j] <- '@'
				done
			) last_char;

			p "D";

			(* Add agent locations *)
			List.iter (fun (_, _, _, workers) ->
				List.iter (function
					| (_, {worker_status = (Worker_agent (v,x) | Worker_controller (v,x))}) -> (
						let pos = x * frame_len.(v) / data.(v).i.i_encode_frames in
						if pos >= 0 && pos < frame_len.(v) then (
							(* This failed if the worker hit the last frame, so only do it if it's in-bounds *)
							frames.(v).[pos] <- '|'
						)
					)
					| _ -> ()
				) workers
			) agent_status;

			p "E";

			let total_frames = Array.fold_left (fun so_far vi -> so_far + vi.i.i_encode_frames) 0 data in

			p "F";

			let print_list = List.rev (
				let rev_print_list = ref [] in
				let add s = (rev_print_list := s :: !rev_print_list) in

				(* Add workers *)
				add "Agents:";
				List.iter (fun (name,(ip,port),id,workers) ->
(*					add // sprintf "  %s ~ %s:%d" (to_hex id) ip port;*)

					let failures = List.fold_left (fun so_far (_,{worker_times_failed = tf}) -> so_far + tf) 0 workers in

					let fps = List.fold_left (fun so_far (_,{worker_total_frames = f_new; worker_total_time = s_new}) ->
						let fps_new = (if s_new = 0.0 then 0.0 else float_of_int f_new /. s_new) in
						so_far +. fps_new
					) 0.0 workers in
					if failures = 0 then (
						add // sprintf "    %s ~ %.2f fps" name fps
					) else if failures = 1 then (
						add // sprintf "    %s ~ %.2f fps -- X264 FAILED %d TIME" name fps failures
					) else (
						add // sprintf "    %s ~ %.2f fps -- X264 FAILED %d TIMES" name fps failures
					);
(*					add // sprintf "   %.2f fps" fps;*)
					List.iter (fun (n,w) ->
						let status = match w.worker_status with
							| Worker_disconnected -> "disconnected"
							| Worker_connected -> "connected"
							| Worker_agent (a,b) when Array.length data = 1 -> sprintf "running at %d" b
							| Worker_controller (a,b) when Array.length data = 1 -> sprintf "sending at %d" b
							| Worker_agent (a,b) -> sprintf "running at %d:%d" (succ a) b
							| Worker_controller (a,b) -> sprintf "sending at %d:%d" (succ a) b
							| Worker_done -> "done"
						in
						add // sprintf "        %s" status;
					) workers
				) agent_status;

				add "";
				Array.iteri (fun i s -> add (if cont_frames.(i) = data.(i).i.i_encode_frames then (
					"-- " ^ s
				) else (
					(sprintf "%2d " (min 99 (100 * cont_frames.(i) / data.(i).i.i_encode_frames)) ^ s)
				))) frames;
(*				Array.iteri (fun i s -> add // sprintf "%d/%d" s data.(i).i.i_encode_frames) cont_frames;*)

				!rev_print_list
			) in

			p "G";
			
			console_print print_list
		)
		val mutable next_pretty_print = 0.0
		val next_pretty_print_mutex = Mutex.create ()
		method pretty_print force = (
			let now = Unix.gettimeofday () in
			Mutex.lock next_pretty_print_mutex;
			let ok = if force || now > next_pretty_print then (
				next_pretty_print <- now +. 1.0;
				true
			) else false in
			Mutex.unlock next_pretty_print_mutex;
			if ok && not // x#encoding_done then (
				x#pretty_print_base
			)
		)

		method worker_should_exit agent = (
			Mutex.lock agents_mutex;
			let a = agent.agent_optimal_workers < Rbtree.length agent.agent_workers in
			Mutex.unlock agents_mutex;
			a
		)

		method private get_agent_by_key_unsafe id ip = (
			let rec helper = function
				| {agent_id = aid; agent_ip = aip} as hd :: tl when aid = id || aip = ip -> Some hd
				| hd :: tl -> helper tl
				| [] -> None
			in
			helper agents
		)
		method get_agent_by_key id ip = (
			Mutex.lock agents_mutex;
			let a = x#get_agent_by_key_unsafe id ip in
			Mutex.unlock agents_mutex;
			a
		)
		method add_agent agent = (
			Mutex.lock agents_mutex;
			p // sprintf "adding agent %s=(%s:%d)" (to_hex agent.agent_id) (fst agent.agent_ip) (snd agent.agent_ip);
			agents <- agent :: agents;
			Mutex.unlock agents_mutex;
		)
		method private update_agent_info_unsafe agent new_num new_ip new_id worker_function = (
			agent.agent_optimal_workers <- new_num;
			agent.agent_ip <- new_ip;
			agent.agent_id <- new_id;
			
			(* Now start up any new workers *)
			let num_workers_working = Rbtree.length agent.agent_workers in
			let num_workers_to_start = new_num - num_workers_working in
			let last_num = match Rbtree.last agent.agent_workers with
				| None -> -1
				| Some (n,_) -> n
			in
			for w = 1 to num_workers_to_start do
				p // sprintf "adding worker number %d" w;
				let add_num = w + last_num in
				Rbtree.add agent.agent_workers add_num {
(*
					worker_start = None;
					worker_current = None;
*)
					worker_total_frames = 0;
					worker_total_time = 0.0;
					worker_last_updated = Unix.gettimeofday ();
					worker_status = Worker_disconnected;
					worker_times_failed = 0;
				};
				(* Now start up the worker *)
				ignore // Thread.create (fun () ->
					p // sprintf "worker thread %d starting" (Thread.id (Thread.self ()));
					(try
						worker_function x (agent,add_num)
					with
						e -> (
							p // sprintf "agent died with %S" (Printexc.to_string e);
						)
					);
					p // sprintf "worker thread %d exited; deleting" (Thread.id (Thread.self ()));
					(* Delete the agent, just in case it threw an exception *)
					x#delete_worker agent add_num
				) ();
			done
		)
		method update_agent_info agent new_num new_ip new_id worker_function = (
			Mutex.lock agents_mutex;
			x#update_agent_info_unsafe agent new_num new_ip new_id worker_function;
			Mutex.unlock agents_mutex;
		)

		method private update_worker_status_unsafe agent i s = (
			match Rbtree.find agent.agent_workers i with
			| None -> None
			| Some (_,w) -> (
				let old_s = w.worker_status in
				w.worker_status <- s;
				Some (old_s,s)
			)
		)
		method update_worker_status agent i s = (
			Mutex.lock agents_mutex;
			let change = x#update_worker_status_unsafe agent i s in
			Mutex.unlock agents_mutex;
			match change with
			| Some ((Worker_agent _ | Worker_controller _), (Worker_agent _ | Worker_controller _)) -> x#pretty_print false
			| Some (a,b) when a = b -> ()
			| Some _ -> x#pretty_print true
			| None -> ()
		)

		(* Same as update_worker_status, but resets the last updated time too *)
		method private reset_worker_frame_unsafe agent i s = (
			match Rbtree.find agent.agent_workers i with
			| None -> None
			| Some (_,w) -> (
				let old_s = w.worker_status in
				w.worker_status <- s;
				w.worker_last_updated <- Unix.gettimeofday ();
				Some (old_s,s)
			)
		)
		method reset_worker_frame agent i s = (
			Mutex.lock agents_mutex;
			let change = x#reset_worker_frame_unsafe agent i s in
			Mutex.unlock agents_mutex;
			match change with
			| Some ((Worker_agent _ | Worker_controller _), (Worker_agent _ | Worker_controller _)) -> x#pretty_print false
			| Some (a,b) when a = b -> ()
			| Some _ -> x#pretty_print true
			| None -> ()
		)

		method update_worker_times_failed_unsafe agent i = (
			match Rbtree.find agent.agent_workers i with
			| None -> ()
			| Some (_,w) -> w.worker_times_failed <- succ w.worker_times_failed
		)
		method update_worker_times_failed agent i = (
			Mutex.lock agents_mutex;
			x#update_worker_times_failed_unsafe agent i;
			Mutex.unlock agents_mutex;
		)

		method get_worker_times_failed agent i = (
			Mutex.lock agents_mutex;
			let tf = match Rbtree.find agent.agent_workers i with
				| None -> 0
				| Some (_,w) -> w.worker_times_failed
			in
			Mutex.unlock agents_mutex;
			tf
		)

		method private delete_worker_unsafe agent i = (
			(* Remove the worker from the tree *)
			Rbtree.remove agent.agent_workers i;
			(* Now remove the whole agent if necessary *)
			if Rbtree.is_empty agent.agent_workers then (
				let rec helper = function
					| hd :: tl when hd == agent -> tl
					| hd :: tl -> hd :: helper tl
					| [] -> [] (* Huh. Oh well. *)
				in
				agents <- helper agents
			);
			x#pretty_print true
		)
		method delete_worker agent i = (
			Mutex.lock agents_mutex;
			x#delete_worker_unsafe agent i;
			Mutex.unlock agents_mutex;
		)

		(* updates worker location and adds GOP *)
(*
		method worker_replace_gop_here agent worker_index job gop encode_frame = (
			(* Get the zone string from the job *)
			(* Note that the zones in job.job_zones are relative to the beginning of the job *)
			mutex_lock_2 frames_mutex agents_mutex;
			let wanted = x#replace_gop_here_unsafe job gop encode_frame in
			p // sprintf "%s %d was on %d:%d" agent.agent_name worker_index job.job_video_index encode_frame;
			Mutex.unlock frames_mutex;
			let wanted = match Rbtree.find agent.agent_workers worker_index with
				| None -> false (* Tell the worker it's not needed *)
				| Some (_,w) -> (
					let old_location_perhaps = w.worker_current in
					let new_location = encode_frame + gop.gop_frames in
					w.worker_current <- Some new_location;
					let change = x#update_worker_status_unsafe agent worker_index // Worker_frame (job.job_video_index,new_location) in
					let current_time = Unix.gettimeofday () in
					(match old_location_perhaps with
						| None -> ()
						| Some old_location -> (
							(* Update the number of frames done, too *)
							w.worker_total_time <- w.worker_total_time +. current_time -. w.worker_last_updated;
							w.worker_total_frames <- w.worker_total_frames + gop.gop_frames;
						)
					);
					w.worker_last_updated <- current_time;
					(
						match change with
						| Some (Worker_frame _, Worker_frame _) -> x#pretty_print false
						| Some (a,b) when a = b -> ()
						| Some _ -> x#pretty_print true
						| None -> ()
					);
					wanted
				)
			in
			Mutex.unlock agents_mutex;
			wanted
		)
*)
		method worker_replace_gop_here agent worker_index job gop encode_frame agent_based = (
			p "WRGHA";
			mutex_lock_2 frames_mutex agents_mutex;
			p "WRGHB";
			let wanted = x#replace_gop_here_unsafe job gop encode_frame in
			p "WRGHC";
			p // sprintf "%s %d was on %d:%d wanted? %B" agent.agent_name worker_index job.job_video_index encode_frame wanted;
			Mutex.unlock frames_mutex;
			p "WRGHD";
			let really_wanted = match Rbtree.find agent.agent_workers worker_index with
				| None -> false (* This agent doesn't exist. What's up with that. *)
				| Some (_,w) -> (
					p "WRGHe";
					let old_location_perhaps = match w.worker_status with
						| Worker_agent (a,b) | Worker_controller (a,b) -> Some b
						| _ -> None
					in
					let new_location = encode_frame + gop.gop_frames in
					p "WRGHf";
					let new_worker = if agent_based then Worker_agent (job.job_video_index, new_location) else Worker_controller (job.job_video_index, new_location) in
					let change = x#update_worker_status_unsafe agent worker_index new_worker in
					let current_time = Unix.gettimeofday () in
					(
						match change with
						| Some ((Worker_agent (_,a) | Worker_controller (_,a)), (Worker_agent (_,b) | Worker_controller (_,b))) -> (
							w.worker_total_time <- w.worker_total_time +. current_time -. w.worker_last_updated;
							w.worker_total_frames <- w.worker_total_frames + gop.gop_frames;
							x#pretty_print false
						)
						| Some _ -> x#pretty_print true
						| None -> ()
					);
					w.worker_last_updated <- current_time;
					p "WRGHg";
					wanted
				)
			in
			Mutex.unlock agents_mutex;
			p "WRGHH";
			really_wanted
		)

(*		method private update_worker_status_unsafe agent i s = (
			agent.agent_workers.(i).worker_status <- s;
			match s with
			| Worker_dead -> (
				(* Check to see if all the other workers are dead too *)
				let all_dead = Array.fold_left (fun old -> 
		method update_worker_status*)

		method get_start_point agent agent_index max_agents = (
			mutex_lock_2 frames_mutex agents_mutex;
			let range_array = x#list_noncontiguous_unsafe in
			Mutex.unlock frames_mutex;

			p "GETSTART unfinished ranges:";
			Array.iteri (fun video_index ranges_here ->
				List.iter (fun (a,b) ->
					p // sprintf "GETSTART  %d: [%d,%d]" video_index a b
				) ranges_here
			) range_array;

			let first_frame_unfinished_perhaps_array = Array.mapi (fun i r ->
				match r with
				| (a,b) :: tl -> (
					p // sprintf "GETSTART first unfinished in %d is %d" i a;
					Some a
				)
				| _ -> None
			) range_array in
			
			let agent_locations = List.flatten (
				List.map (fun agent_now ->
					Rbtree.fold_left (fun so_far worker_i w ->
						match w.worker_status with
						| Worker_agent q | Worker_controller q -> q :: so_far
						| _ -> so_far
					) [] agent_now.agent_workers
				) agents
			) in
			p "GETSTART agents are at the following locations:";
			List.iter (fun (a,b) ->
				p // sprintf "GETSTART  %d:%d" a b
			) agent_locations;

			(* Chop up ranges based on agent locations *)
			let ranges_split_at_agents = Array.mapi (fun video_index video_ranges ->
				List.fold_left (fun prev_ranges (agent_video, agent_frame) ->
					if agent_video = video_index then (
						let rec chop_a_range = function
							| [] -> []
							| (a,b) :: tl when a < agent_frame && b >= agent_frame -> (a, pred agent_frame) :: (agent_frame, b) :: tl
							| hd :: tl -> hd :: chop_a_range tl
						in
						chop_a_range prev_ranges
					) else (
						(* The agent is not in this video *)
						prev_ranges
					)
				) video_ranges agent_locations
			) range_array in

			(* Shrink the ranges if the agent is at the beginning of them *)
			let frame_limit = 100 in
			let ranges_shrunk = Array.mapi (fun video_index video_ranges ->
				List.fold_left (fun prev_ranges (agent_video, agent_frame) ->
					if agent_video = video_index then (
						let rec shrink_a_range = function
							| [] -> []
							| (a,b) :: tl when a = agent_frame && a <> b && b - a > frame_limit -> ((a + b + 1) / 2, b) :: tl
							| hd :: tl -> hd :: shrink_a_range tl
						in
						shrink_a_range prev_ranges
					) else (
						(* Agent is not in this video *)
						prev_ranges
					)
				) video_ranges agent_locations
			) ranges_split_at_agents in
			p "GETSTART shrunk ranges at agents:";
			Array.iteri (fun video_index ranges_here ->
				List.iter (fun (a,b) ->
					p // sprintf "GETSTART  %d: [%d,%d]" video_index a b
				) ranges_here
			) ranges_shrunk;

			(* Find out if the first range is usable *)
			let do_first_ranges = Array.mapi (fun video_index video_ranges ->
				match (first_frame_unfinished_perhaps_array.(video_index), video_ranges) with
				| (Some f, (a,b) :: tl) when f = a -> (
					if b - a > frame_limit then (
						p // sprintf "GETSTART first frame of video %d is available" video_index;
						Some (video_index,f)
					) else (
						p // sprintf "GETSTART found video %d may have the first frame available; check" video_index;
						let rec check_agents = function
							| (agent_video,agent_frame) :: tl when agent_video = video_index && agent_frame = f -> false
							| x :: tl -> check_agents tl
							| [] -> true
						in
						let do_first_range = check_agents agent_locations in
						if do_first_range then (
							p // sprintf "GETSTART can start at video %d frame %d, since nobody else is" video_index f;
							Some (video_index,f)
						) else (
							p "GETSTART the first range is taken";
							None
						)
					)
				)
				| _ -> (
					p // sprintf "GETSTART first frame of video %d is taken" video_index;
					None
				)
			) ranges_shrunk in

			(* Now use one of the first frame ranges, if available *)
			let do_first_range = Array.fold_left (fun so_far gnu -> match (so_far,gnu) with
				| (Some a, _) -> Some a
				| (None, None) -> None
				| (None, Some (a,b)) -> (
					p // sprintf "Doing first range at %d:%d" a b;
					Some (a,b)
				)
			) None do_first_ranges in

			let start_here = match (Rbtree.find agent.agent_workers agent_index, do_first_range) with
				| (None, _) -> (
					(* OOPS. The agent doesn't exist? *)
					failwith "GETSTART agent does not exist"
				)
				| (Some (_,w), Some (a,b)) -> (
					(* The first range of some video is unclaimed; DO IT *)
					w.worker_status <- Worker_controller (a,b);
(*					Some (a, b, true)*)
					let vi = data.(a) in
					let job_frames = data.(a).i.i_encode_frames - b in
					Job {
						job_v = vi.v;
						job_i = vi.i;
						job_seek = b + vi.i.i_encode_seek;
						job_encode_seek = b;
						job_frames = job_frames;
						job_video_index = a; (* Don't know if I need this one *)
						job_zones = confine_zones vi.v.v_zones b job_frames
(*						job_checked_out = true;*)
					}
				)
				| (Some (_,w), None) -> (
					(* The first unfinished frame fell through; find the largest region with the fewest agents in each video *)
					let largest_range_data_array = Array.mapi (fun video_index video_ranges ->
						let (x,y,backwards) = List.fold_left (fun ((len_so_far, fewest_agents_so_far, ranges_so_far) as so_far) (new_a, new_b) ->
							let new_len = new_b - new_a + 1 in
							if new_len > frame_limit then (
								(* Don't bother counting agents, since if there were any agents at the beginning it would have been split *)
								if fewest_agents_so_far = 0 then (
									(* Add to the list of 0-agent ranges *)
									(max len_so_far new_len, 0, (new_a, new_b) :: ranges_so_far)
								) else (
									(* Replace the contents with this range *)
									(new_len, 0, (new_a, new_b) :: [])
								)
							) else (
								(* There may be agents here *)
								let num_agents = List.fold_left (fun n x -> if x = (video_index, new_a) then succ n else n) 0 agent_locations in
								if num_agents = fewest_agents_so_far then (
									(* Add to the list of ranges *)
									(max len_so_far new_len, num_agents, (new_a, new_b) :: ranges_so_far)
								) else if num_agents < fewest_agents_so_far then (
									(* Fewer agents than ever before *)
									(new_len, num_agents, (new_a, new_b) :: [])
								) else (
									(* Too many agents; ignore *)
									so_far
								)
							)
						) (-1,max_int,[]) video_ranges in
						(x,y,List.rev backwards)
					) ranges_shrunk in

					p "GETSTART ranges with the fewest number of agents:";
					Array.iteri (fun i (max_len, fewest_agents, ranges_with_fewest_agents) ->
						p // sprintf "GETSTART  %d - %d agents:" i fewest_agents;
						List.iter (fun (a,b) -> p // sprintf "GETSTART   [%d,%d] (%d)" a b (b - a + 1)) ranges_with_fewest_agents
					) largest_range_data_array;

					(* Find the fewest agents working out of all the videos *)
					let fewest_agents = Array.fold_left (fun so_far (_, f, _) -> min so_far f) max_int largest_range_data_array in
					p // sprintf "GETSTART fewest number of agents working is %d" fewest_agents;

					(* Cull all the videos that have more than the minimum number of agents *)
					let fewest_agents_only = Array.map (fun (new_len, num_agents, ranges) ->
						if new_len = -1 || num_agents > fewest_agents then (
							None
						) else (
							Some (new_len, ranges)
						)
					) largest_range_data_array in

					(* This gives priority to the first video *)
					(* If this number is 1.5, then the second video's ranges have to be 1.5x larger than the largest one in the first video. The third video has to be 1.5x larger than the second, etc.. *)
					let higher_video_lower_priority_base = 1.5 in

					(* This prioritizes the first ranges *)
					(* The earliest range which is larger than largest*big_enough_ratio is used, instead of the largest range itself *)
					let big_enough_ratio = 0.8 in

					let max_len_normalized_array = Array.mapi (fun video_index ml -> match ml with
						| None -> None
						| Some (len,ranges) -> Some (ranges, float_of_int len *. higher_video_lower_priority_base ** float_of_int (-video_index))
					) fewest_agents_only in
					let max_len_normalized_perhaps = Array.fold_left (fun so_far gnu -> match (so_far, gnu) with
						| (Some x, Some (_,y)) -> Some (max x y)
						| (Some x, None) | (None, Some (_,x)) -> Some x
						| (None, None) -> None
					) None max_len_normalized_array in

					match max_len_normalized_perhaps with
					| None -> (
						p // sprintf "GETSTART nothing else to do, I suppose";
						Job_none
					)
					| Some max_len_normalized -> (
						p // sprintf "GETSTART max length normalized is %g" max_len_normalized;

						(* This finds the length that a range would have to be to get considered for starting *)
						let best_range_array = Array.mapi (fun video_index -> function
							| None -> (
								None
							)
							| Some (max_len, ranges_with_fewest_agents) -> (
								let this_video_threshold = int_of_float (big_enough_ratio *. max_len_normalized *. higher_video_lower_priority_base ** float_of_int video_index) in
								if max_len > this_video_threshold then (
									(* This video has a large enough range to start encoding *)
									Some (video_index, this_video_threshold, ranges_with_fewest_agents)
								) else (
									None
								)
							)
						) fewest_agents_only in
						p "GETSTART found the following appropriate lengths:";
						Array.iteri (fun i -> function | None -> p // sprintf "GETSTART  %d: NONE" i | Some (_,x,_) -> p // sprintf "GETSTART  %d: %d" i x) best_range_array;

						(* This combines all the ranges to get the (best_video, min_range_length_considered, ranges_to_sort) *)
						let best_range_min = Array.fold_left (fun so_far gnu -> match (so_far, gnu) with
							| (None, None) -> None
							| (None, Some y) -> Some y
							| (Some x, _) -> Some x
						) None best_range_array in

						match best_range_min with
						| None -> (
							p "GETSTART didn't find any good ranges after finding the max (I don't think this should happen)";
							Job_none
						)
						| Some (video_index, min_size, ranges_to_sort) -> (
							p // sprintf "GETSTART found the best length to be in video %d with more than %d frames" video_index min_size;

							let rec find_good_range = function
								| (a,b) :: tl when b - a + 1 > min_size -> Some (a,b)
								| hd :: tl -> find_good_range tl
								| [] -> None
							in
							match find_good_range ranges_to_sort with
							| None -> (
								p "GETSTART didn't find any ranges that fit after finding a good video (I don't think this should happen)";
								Job_none
							)
							| Some (a,b) when fewest_agents > max_agents -> (
								p // sprintf "GETSTART found range %d:[%d,%d], but there are %d agents working on it" video_index a b fewest_agents;
(*								Some (video_index, a, false)*)
								Job_bad (video_index, a);
(*
								Some {
									job_v = vi.v;
									job_i = vi.i;
									job_seek = a + vi.i.i_encode_seek;
									job_encode_seek = a;
									job_frames = data.(video_index).i.i_avs_frames - b + 1;
									job_video_index = video_index;
									job_checked_out = false;
								}
*)
							)
							| Some (a,b) -> (
								p // sprintf "GETSTART doing range %d:[%d,%d]" video_index a b;
								w.worker_status <- Worker_controller (video_index,a);
(*								Some (video_index, a, true)*)
								let vi = data.(video_index) in
								let job_frames = vi.i.i_encode_frames - a in
								Job {
									job_v = vi.v;
									job_i = vi.i;
									job_seek = a + vi.i.i_encode_seek;
									job_encode_seek = a;
									job_frames = job_frames;
									job_video_index = video_index;
									job_zones = confine_zones vi.v.v_zones a job_frames
(*									job_checked_out = true;*)
								}
							)
						)
					)
				)
			in

			Mutex.unlock agents_mutex;

			start_here
		)

		initializer (

			if Array.length data > 1 then (
				let collision = Hashtbl.create (Array.length data) in
				(* Print the encodes being worked on *)
				let print_list = List.rev (
					let rev_print_list = ref [] in
(*					let add s = (rev_print_list := s :: !rev_print_list) in*)
					let add s = print_endline s in

					add "Encoding the following:";
					
					Array.iteri (fun i vi ->
						add // sprintf "  %d:" (succ i);
						add // sprintf "    Input:   %s" vi.v.v_i;
						add // sprintf "    Options: %s" vi.v.v_x;
						if vi.v.v_seek <> 0 then add // sprintf "    Seek:    %d" vi.i.i_encode_seek;
						if vi.v.v_frames <> max_int then add // sprintf "    Frames:  %d" vi.i.i_encode_frames;
						if vi.v.v_zones <> [] then add // sprintf "    Zones:   %s" (render_zones (confine_zones vi.v.v_zones vi.i.i_encode_seek vi.i.i_encode_frames));
						add // sprintf "    Output:  %s" vi.v.v_o;
						if Hashtbl.mem collision vi.i.i_encode_temp_dir then (
							let n = Hashtbl.find collision vi.i.i_encode_temp_dir in
							add "    WARNING:";
							add // sprintf "      This encode has the same settings as encode %d" (succ n);
							add // sprintf "      This may corrupt the index file, which will cause problems with";
							add // sprintf "      any encodes done after this. It should not affect the integrity";
							add // sprintf "      of the current encode, though";
						) else (
							Hashtbl.add collision vi.i.i_encode_temp_dir i
						)
					) data;

					!rev_print_list
				) in
				()
(*				List.iter print_list*)
			);

			Array.iteri (fun j vi ->
				match vi.v.v_encode_type with
				| Encode_mkv -> (
					let name = sprintf "MKV %d -> \"%s\"" j vi.v.v_o in
					ignore // Thread.create (fun () ->
						p // sprintf "%s thread starting" name;
						(try
							output_mkv x vi frames_mutex frames_condition name
						with
							e -> p // sprintf "%s thread died with %S" name (Printexc.to_string e)
						);
						p // sprintf "%s thread exiting" name;
					) ()
				)
				| Encode_mp4 -> (
					let name = sprintf "MP4 %d -> \"%s\"" j vi.v.v_o in
					ignore // Thread.create (fun () ->
						p // sprintf "%s thread starting" name;
						(try
							output_mp4 x vi frames_mutex frames_condition name
						with
							e -> p // sprintf "%s thread died with %S" name (Printexc.to_string e)
						);
						p // sprintf "%s thread exiting" name;
					) ()
				)
			) data
		)

(*

		method get_start_point p agent n max_agents = (
			;
		)
*)
	end
;;



(**********)
(* DISHER *)
(**********)
let disher_guts f (agent,n) =
	let print_name = sprintf "%s %d " agent.agent_name n in
	let p = f#pn print_name in
	let update = f#update_worker_status agent n in
	let reset = f#reset_worker_frame agent n in

	p "starting up!";

	let timeout = 60.0 in

	update Worker_disconnected;

	let rec connection_loop () = (

		(* Delay based on the number of times the agent has failed *)
		let delay = (5.0 *. float_of_int (f#get_worker_times_failed agent n)) in
		if delay > 0.0 then p // sprintf "connection delayed %g seconds due to failures" delay;
		Thread.delay delay;

		let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in

		(* Something about worker_socks_mutex? Huh? *)

		let keep_connecting = (try
			let connection_delay = 5.0 in
			let connection_timeout = 30.0 in
			let connection_attempt_start = Unix.gettimeofday () in
			let rec attempt_connection tries_so_far = ( (* "None" if stop encoding, "Some s" if it did something properly *)
				if f#encoding_done || f#worker_should_exit agent then (
					p "encoding seems to be done; don't bother trying to connect";
					None
				) else (
					p "attempting to connect";
					let current_connection_attempt_time = Unix.gettimeofday () in
					let s_perhaps = (
						try
							Unix.connect sock (Unix.ADDR_INET (Unix.inet_addr_of_string (fst agent.agent_ip), (snd agent.agent_ip)));
							Some (new net sock)
						with
							_ -> None
					) in
					p "attempted to connect";
					match s_perhaps with
					| None -> (
						let current_connection_after_time = Unix.gettimeofday () in
						if current_connection_after_time -. connection_attempt_start > connection_timeout then (
							(* TIMEOUT *)
							p "Connection timed out";
							raise Dead_worker
						) else (
							(* Wait for connection_delay seconds after the connection attempt STARTED *)
							p // sprintf "Connection failed; wait %g seconds" (connection_delay -. (current_connection_after_time -. current_connection_attempt_time));
							Thread.delay (max 0.0 (connection_delay -. (current_connection_after_time -. current_connection_attempt_time)));
							attempt_connection (succ tries_so_far)
						)
					)
					| Some s -> Some s
				)
			) in
			match attempt_connection 0 with
			| None -> (
				p "marking agent to exit (this doesn't sound like English)";
				false
			)
			| Some s -> (
				(* Got a socket *)
				p "socket connected";
				update Worker_connected;
				p "worker_connected";

				(* Now try to find a job *)
				let rec per_job () = (
					p "getting job";
					let rec get_job last_job desperateness = (
						match f#get_start_point agent n desperateness with
						| Job_none -> None
						| Job j -> (p // sprintf "got job at %d:%d" j.job_video_index j.job_encode_seek; Some j)
						| Job_bad q when q = last_job -> (
							(* The best job is still the one which we got last time *)
							p // sprintf "got job %d:%d, which is the same as before; adding to desperateness" (fst q) (snd q);
							Thread.delay 20.0;
							p "retrying checkout";
							get_job q (succ desperateness)
						)
						| Job_bad q -> (
							p // sprintf "got job %d:%d, which is different from before; resetting desperateness" (fst q) (snd q);
							Thread.delay 20.0;
							p "retrying checkout";
							get_job q 0
						)
					) in
					match get_job (-1,-1) 0 with
					| None -> (
						p "encoding's done"
					)
					| Some job -> (
						p // sprintf "doing job %d:%d" job.job_video_index job.job_encode_seek;

						let zone_string = render_zones job.job_zones in
						let send_zone_string = if zone_string = "" then "" else " --zones " ^ zone_string in
						p // sprintf "zone string is %S" zone_string;

						(* Make data to send *)
						(* Version 1.0: Base *)
						(* Version 1.1: Accept no SEI on the encodes *)
						(* Version 1.2: Accept PING and TOUT in addition to IFRM and GOPF *)
						let send_subelements = (
							let file_hash = Digest.to_hex (Digest.file job.job_v.v_i) in
							let temp = [
								Xml.Element ("version", [("major", "1"); ("minor", "3")], []);
								Xml.Element ("filename", [("hash", file_hash)], [Xml.PCData job.job_v.v_i]);
								Xml.Element ("seek", [], [Xml.PCData (string_of_int job.job_seek)]);
								Xml.Element ("frames", [], [Xml.PCData (string_of_int job.job_frames)]);
								Xml.Element ("options", [], [Xml.PCData (job.job_v.v_x ^ send_zone_string)]);
								Xml.Element ("resolution", [("x", string_of_int job.job_i.i_res_x); ("y", string_of_int job.job_i.i_res_y)], []);
								Xml.Element ("framerate", [("n", string_of_int job.job_i.i_fps_n); ("d", string_of_int job.job_i.i_fps_d)], []);
							] in
							let tracks_perhaps = f#get_tracks job.job_video_index in
							match tracks_perhaps with
							| Some (_,priv,_) -> (
								(* Got privates! *)
								p "starting NTRX_OK with privates";
								temp @ [Xml.Element ("ntrx_ok", [], [Xml.PCData "1"]) ; Xml.Element ("private", [], [Xml.PCData (to_hex priv)])]
							)
							| _ -> (
								(* No privates; have the encode send TRAX *)
								temp
							)
						) in
						let send_xml = (
							Xml.Element ("job", [], send_subelements)
						) in
						p // sprintf "sending this: %S" (Xml.to_string send_xml);

						(* NETWORK CODE STARTS HERE *)
						(try
							s#send "OPTS" (Xml.to_string send_xml);
							p "sent OPTS";

							let port_key_perhaps = match s#recv_timeout timeout with
								| ("PORT",x) when x = "\x00\x00" -> None
								| ("PORT",x) when String.length x = 2 -> Some (Pack.unpackn x 0, "")
								| ("PKEY",x) when String.length x > 2 -> Some (Pack.unpackn x 0, String.sub x 2 (String.length x - 2))
								| e -> unexpected_packet ~here:"receiving port info" e
							in

							(match port_key_perhaps with
								| None -> (
									(* Agent-based *)
									p "agent is attempting agent-based encoding";

									reset // Worker_agent (job.job_video_index,job.job_encode_seek);
									f#pretty_print true;

									(match s#recv_timeout timeout with
										| ("TRAX",x) -> (
											p "got track element";
											f#update_tracks job.job_video_index x
										)
										| ("NTRX","") -> (
											p "got no tracks"
										)
										| ("XERN",s) -> (
											p // sprintf "got x264 error number %s" s;
											let e = int_of_string s in
											f#update_worker_times_failed agent n;
											raise (X264_error e)
										)
										| x -> unexpected_packet ~here:"recv TRAX agent" x
									);

									(* Receive the frames *)
									(* from_frame, to_frame, and x are relative to the beginning of the AVS! *)
									let rec recv_frames from_frame filename handle = (
										p "getting something agent";
										match s#recv_timeout timeout with
										| ("IFRM",x) when String.length x = 4 -> (
											(* Close old handle, make new one *)
											p "got IFRM agent";
											Unix.close handle;
											p "IAA";
											let to_frame = Pack.unpackN x 0 in
											p "IAB";
											let encode_from_frame = from_frame - job.job_i.i_encode_seek in
											p "IAC";
											let gop = {gop_frames = to_frame - from_frame; gop_filename = filename} in
											p "IAD";
											let wanted = f#worker_replace_gop_here agent n job gop encode_from_frame true in
											p "IAE";
											p // sprintf "added GOP %d:%d+%d = %S" job.job_video_index encode_from_frame gop.gop_frames filename;
											if not wanted || f#is_gop_here job.job_video_index (to_frame - job.job_i.i_encode_seek) || f#encoding_done then (
												p "GOP already exists there, or there's already a GOP next, or encoding is done; finishing up job";
												s#send "STOP" "";
												(match s#recv with
													| ("EEND",_) -> ()
													| x -> unexpected_packet ~here:"recv EEND after STOP(IFRM) agent" x
												);
												()
											) else (
												s#send "CONT" "";
												let a = Filename.concat job.job_i.i_encode_temp_dir (sprintf "%0*d " job.job_i.i_avs_frame_num_len to_frame) in
												let (filename_new, handle_new) = open_temp_file_unix a ".mkc" [] 0o600 in
												p // sprintf "new temp file is %S" filename_new;
												recv_frames to_frame filename_new handle_new
											)
										)
										| ("GOPF",x) -> (
											(* Add to the current file *)
											p "got GOPF agent";
											if f#encoding_done || f#is_gop_here job.job_video_index (from_frame - job.job_i.i_encode_seek) then (
												(* Wait. Encoding's done *)
												Unix.close handle;
												p "telling agent to stop";
												s#send "STOP" "";
												(match s#recv with
													| ("EEND",_) -> ()
													| x -> unexpected_packet ~here:"recv EEND after STOP(GOPF) agent" x
												);
												()
											) else (
												p "telling agent to keep going";
												s#send "CONT" "";
												ignore // Unix.write handle x 0 (String.length x);
												recv_frames from_frame filename handle
											)
										)
										| ("EEND",x) when String.length x = 4 -> (
											(* Similar to "IFRM" *)
											p "got EEND";
											Unix.close handle;
											let to_frame = Pack.unpackN x 0 in
											p // sprintf "ENDING at AVS frame %d" to_frame;
											let encode_from_frame = from_frame - job.job_i.i_encode_seek in
											let gop = {gop_frames = to_frame - from_frame; gop_filename = filename} in

											ignore // f#worker_replace_gop_here agent n job gop encode_from_frame true;

											p "got EEND; dying";
											()
										)
										| ("USEI",sei) -> (
											(* I didn't even know I was sending the SEI. Oh well. *)
											p // sprintf "got SEI info for some reason: %S" sei;
											recv_frames from_frame filename handle
										)
										| ("PING","") -> (
											(* Do nothing *)
											p "got PING agent";
											recv_frames from_frame filename handle
										)
										| ("TOUT","") -> (
											p "got TOUT agent; dying";
											raise Timeout
										)
										| x -> unexpected_packet ~here:"recv_frames agent" x
									) in
									let rec recv_frames_initial () = (
										match s#recv_timeout timeout with
										| ("IFRM",x) when String.length x = 4 -> (
											let to_frame = Pack.unpackN x 0 in
											if f#is_gop_here job.job_video_index (to_frame - job.job_i.i_encode_seek) || f#encoding_done then (
												p "GOP already exists, or encoding is done (that was fast); finishing up job";
												s#send "STOP" "";
												(match s#recv with
													| ("EEND",_) -> ()
													| x -> unexpected_packet ~here:"recv EEND after STOP(IFRM) agent" x
												);
												()
											) else (
												s#send "CONT" "";
												let a = Filename.concat job.job_i.i_encode_temp_dir (sprintf "%0*d " job.job_i.i_avs_frame_num_len to_frame) in
												let (filename_new,handle_new) = open_temp_file_unix a ".mkc" [] 0o600 in
												p // sprintf "temp file is %S" filename_new;
												recv_frames to_frame filename_new handle_new
											)
										)
										| ("PING","") -> (
											p "got PING agent (before first I frame)";
											recv_frames_initial ()
										)
										| ("TOUT","") -> (
											p "got TOUT agent (before first I frame); dying";
											raise Timeout
										)
										| x -> unexpected_packet ~here:"recv first frame agent" x
									) in
									recv_frames_initial ();
									
									(* Done receiving stuff *)
									update Worker_connected
								)
								| Some (port, key) -> (
									(* Controller-based *)
									p // sprintf "got port %d; starting up slave to send data" port;

									reset // Worker_controller (job.job_video_index,job.job_encode_seek);
									f#pretty_print true;

									let video_thread_keep_going_ref = ref true in
									let video_thread_keep_going_mutex = Mutex.create () in
									let video_thread_keep_going_condition = Condition.create () in

									let last_frame = job.job_i.i_encode_seek + job.job_i.i_encode_frames in

									(* MAKE A SLAVE *)
									let addr = Unix.ADDR_INET (Unix.inet_addr_of_string (fst agent.agent_ip), port) in
									let str = sprintf "\"%s\" --slave" Sys.executable_name in
									let opts_guts = {
										slave_bytes_per_frame = job.job_i.i_bytes_per_frame;
										slave_i = job.job_v.v_i;
										slave_addr = addr;
										slave_key = key;
										slave_first_frame = job.job_seek;
										slave_last_frame = last_frame;
										slave_zones = zone_string;
										slave_name = print_name ^ "SLAVE ";
									} in
									let opts = Slave_do opts_guts in

									p // sprintf "SLAVE running the following: %S" str;
									p // sprintf "SLAVE and sending %S" (Marshal.to_string opts []);
									let get_out = Unix.open_process_out str in
									Marshal.to_channel get_out opts [];
									flush get_out;
									Marshal.to_channel get_out (Slave_set_priority (Opt.Below_normal, Opt.Thread_priority_above_normal)) [];
									flush get_out;

									(* Don't bother with this; AVIsynth is still not thread-safe *)
(*
									(* Try the non-slave version *)
									let event_channel = Event.new_channel () in
									let slave_p = f#pn (print_name ^ "SLAVE ") in
									let fake_slave_thread = Thread.create (fun () ->
										try
											do_controller_not_slave slave_p opts_guts event_channel
										with
										| e -> slave_p // sprintf "FAILED WITH %S" (Printexc.to_string e)
									) () in
									p "syncing priority with fake slave";
									Event.sync (Event.send event_channel (Slave_set_priority (Opt.Normal, Opt.Thread_priority_below_normal)));
*)

									let got_error = (try
										(* Now get the frame elements, just like from agent-based encoding *)

										(match s#recv_timeout timeout with
											| ("TRAX",x) -> (
												p "got track element";
												f#update_tracks job.job_video_index x
											)
											| ("NTRX","") -> (
												p "got no tracks"
											)
											| ("XERN",s) -> (
												p // sprintf "got x264 error number %s" s;
												let e = int_of_string s in
												f#update_worker_times_failed agent n;
												raise (X264_error e)
											)
											| x -> unexpected_packet ~here:"recv TRAX controller" x
										);

										(* Receive the frames *)
										(* from_frame, to_frame, and x are relative to the beginning of the AVS! *)
										let rec recv_frames from_frame filename handle = (
											match s#recv_timeout timeout with
											| ("IFRM",x) when String.length x = 4 -> (
												(* Close old handle, make new one *)
												Unix.close handle;
												let to_frame = Pack.unpackN x 0 in
												let encode_from_frame = from_frame - job.job_i.i_encode_seek in
												let gop = {gop_frames = to_frame - from_frame; gop_filename = filename} in
												let wanted = f#worker_replace_gop_here agent n job gop encode_from_frame false in
												p // sprintf "added GOP %d:%d+%d = %S" job.job_video_index encode_from_frame gop.gop_frames filename;
												if not wanted || f#is_gop_here job.job_video_index (to_frame - job.job_i.i_encode_seek) || f#encoding_done then (
													p "GOP already exists there, or there's already a GOP next, or encoding is done; finishing up job";
													s#send "STOP" "";
													(match s#recv with
														| ("EEND",_) -> ()
														| x -> unexpected_packet ~here:"recv EEND after STOP(IFRM) controller" x
													);
													()
												) else (
													s#send "CONT" "";
													let a = Filename.concat job.job_i.i_encode_temp_dir (sprintf "%0*d " job.job_i.i_avs_frame_num_len to_frame) in
													let (filename_new, handle_new) = open_temp_file_unix a ".mkc" [] 0o600 in
													p // sprintf "new temp file is %S" filename_new;
													recv_frames to_frame filename_new handle_new
												)
											)
											| ("GOPF",x) -> (
												(* Add to the current file *)
												p "got GOPF controller";
												if f#encoding_done || f#is_gop_here job.job_video_index (from_frame - job.job_i.i_encode_seek) then (
													(* Wait. Encoding's done *)
													Unix.close handle;
													s#send "STOP" "";
													(match s#recv with
														| ("EEND",_) -> ()
														| x -> unexpected_packet ~here:"recv EEND after STOP(GOPF) controller" x
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
												p "got EEND";
												Unix.close handle;
												let to_frame = Pack.unpackN x 0 in
												let encode_from_frame = from_frame - job.job_i.i_encode_seek in
												let gop = {gop_frames = to_frame - from_frame; gop_filename = filename} in

												ignore // f#worker_replace_gop_here agent n job gop encode_from_frame false;

												p "got EEND; dying";
												()
											)
											| ("USEI",sei) -> (
												(* I didn't even know I was sending the SEI. Oh well. *)
												p // sprintf "got SEI info for some reason: %S" sei;
												recv_frames from_frame filename handle
											)
											| ("PING","") -> (
												p "got PING controller";
												recv_frames from_frame filename handle
											)
											| ("TOUT","") -> (
												p "got TOUT controller; dying";
												raise Timeout
											)
											| x -> unexpected_packet ~here:"recv_frames controller" x
										) in
										let rec recv_frames_initial () = (
											match s#recv_timeout timeout with
											| ("IFRM",x) when String.length x = 4 -> (
												let to_frame = Pack.unpackN x 0 in
												if f#is_gop_here job.job_video_index (to_frame - job.job_i.i_encode_seek) || f#encoding_done then (
													p "GOP already exists, or encoding is done (that was fast); finishing up job";
													s#send "STOP" "";
													(match s#recv with
														| ("EEND",_) -> ()
														| x -> unexpected_packet ~here:"recv EEND after STOP(IFRM) controller" x
													);
													()
												) else (
													s#send "CONT" "";
													let a = Filename.concat job.job_i.i_encode_temp_dir (sprintf "%0*d " job.job_i.i_avs_frame_num_len to_frame) in
													let (filename_new,handle_new) = open_temp_file_unix a ".mkc" [] 0o600 in
													p // sprintf "temp file is %S" filename_new;
													recv_frames to_frame filename_new handle_new
												)
											)
											| ("PING","") -> (
												p "got PING controller (before first I frame)";
												recv_frames_initial ()
											)
											| ("TOUT","") -> (
												p "got TOUT controller (before first I frame); dying";
												raise Timeout
											)
											| x -> unexpected_packet ~here:"recv first frame controller" x
										) in
										recv_frames_initial ();

										(* Done receiving stuff *)
										Normal ()

									with
										| Invalid_argument e -> (
											p // sprintf "connected worker got exception Invalid_argument(%S); shutting down" e;
											Exception (Invalid_argument e)
										)
										| e -> (
											p // sprintf "connected worker got exception %S; shutting down" (Printexc.to_string e);
											Exception e
										)
									) in

									(* Tell the frame server to shut down *)
									Mutex.lock video_thread_keep_going_mutex;
									p "suggesting video thread to shut down";
									video_thread_keep_going_ref := false;
									Condition.broadcast video_thread_keep_going_condition;
									Mutex.unlock video_thread_keep_going_mutex;

									(* Done *)
									update Worker_connected;

									(* Wait for the slave to croak *)
									(try
										Marshal.to_channel get_out Slave_stop [];
										flush get_out;
										let exit_status = Unix.close_process_out get_out in
										match exit_status with
										| Unix.WEXITED x -> p // sprintf "slave exited with %d" x
										| Unix.WSTOPPED x -> p // sprintf "slave stopped with %d" x
										| Unix.WSIGNALED x -> p // sprintf "slave signaled with %d" x
									with
										_ -> ()
									);

									(* Still not worth it *)
(*
									(* Non-slave controller *)
									p "syncing end with fake slave";
									Event.sync (Event.send event_channel Slave_stop);
									p "joining fake slave";
									Thread.join fake_slave_thread;
*)
									match got_error with
									| Normal () -> ()
									| Exception e -> raise e
								) (* Controller-based *)
							); (* match port_perhaps *)
						with
							| Timeout -> (
								p "network timeout; dying";
								raise Timeout
							)
						);
						(* NETWORK CODE ENDS HERE *)
						p "done with job? Restart? Or something?";
						per_job ();
					)
				) in (* per_job *)
				per_job ();

				false (* As far as I know, the only way to get here is to get None for a job, which means encoding has finished *)
			) (* match attempt_connection 0 *)
		with
			| Dead_worker -> (
				p "worker has not responded in a while; assume dead";
				false
			)
			| X264_error e -> (
				p "worker died from an x264 error; keep trying just in case";
				true
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

		(* More stuff about worker_socks_mutex! *)

		if keep_connecting then (
			p "trying to connect again";
			connection_loop ()
		) else (
			p "giving up connecting"
		)
	) in

	ignore // trap_exception connection_loop ();

	f#delete_worker agent n;
;;

(* Background file creator... later *)

(***************)
(* PING THREAD *)
(***************)
let ping_guts f controller_port =
	let p = f#pn "PING " in
	let c = f#c in
	let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
	Unix.setsockopt sock Unix.SO_BROADCAST true;

	(* Windows does not offer the proper function to send to the broadcast address
	 * (it uses -1 as an error code, which is what 255.255.255.255 comes out as)
	 * so use magic and make the address manually.
	 * If any errors occur, they will be clear when sending the ping *)
	let address = match Sys.os_type with
(*		| "Win32" -> Unix.ADDR_INET (Obj.magic "\255\255\255\255", c.c_agent_port)*)
		| "Win32" -> Unix.ADDR_INET (Obj.magic "\192\168\001\255", c.c_agent_port)
		| _ -> Unix.ADDR_INET (Unix.inet_addr_of_string "255.255.255.255", c.c_agent_port)
	in
	let addresses = Contopt.get_broadcast_addresses () in
	let sendthis = (
		let start = "SCON\x00\x00" ^ c.c_controller_name in
		Pack.packn start 4 controller_port;
		start ^ Digest.string start
	) in
	let sendlength = String.length sendthis in
	let rec ping_loop () = (
		p // sprintf "pinging %S" sendthis;
(*
		if (Unix.sendto sock sendthis 0 sendlength [] address) < sendlength then (
			p "oops. didn't send the whole string. I wonder why..."
		);
*)
		Array.iter (fun address ->
			p // sprintf "sending to %s" (Unix.string_of_inet_addr address);
			if (Unix.sendto sock sendthis 0 sendlength [] (Unix.ADDR_INET (address, c.c_agent_port))) < sendlength then (
				p "oops. didn't send the whole string. I wonder why..."
			);
		) addresses;
		Thread.delay 60.0;
		(* Check to see if the encode is still going *)
		if f#encoding_done then (
			p "DEAD"
		) else (
			ping_loop ()
		)
	) in
	ping_loop ()
;;

let discover_guts f value mutex condition =
	let p = f#pn "DISCOVER " in

	p "starting up";

	let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
	Unix.setsockopt sock Unix.SO_REUSEADDR true;

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

	(* Send the port to the main thread so it can kill off the discover thread when it finishes *)
	Mutex.lock mutex;
	value := Some recv_port;
	Condition.broadcast condition;
	Mutex.unlock mutex;

	(* Now start up the ping thread *)
	ignore // Thread.create (fun port ->
(*		f#p // sprintf "Ping thread %d starting" (Thread.id (Thread.self ()));*)
		(try
			ping_guts f port
		with
			_ -> ()
		);
(*		f#p // sprintf "Ping thread %d exiting" (Thread.id (Thread.self ()));*)
	) recv_port;

	let recv_string = String.create 640 in (* 640 should be enough for anybody *)

	(* Start up the discover loop *)
	let rec discover_loop () = (
		let ok = (try
			let (got_bytes, from_addr) = Unix.recvfrom sock recv_string 0 (String.length recv_string) [] in
			(* Singlepass AGEnt = SAGE, you see *)
			if got_bytes >= 39 && String.sub recv_string 0 4 = "SAGE" && Digest.substring recv_string 0 (got_bytes - 16) = String.sub recv_string (got_bytes - 16) 16 then (
				(* Good response *)
				(* Make an agent *)
				let agent_ip = match from_addr with
					| Unix.ADDR_INET (ip,port) -> Unix.string_of_inet_addr ip
					| _ -> "0.0.0.0" (* What is this? IGNORE for now *)
				in
				let agent_port = Pack.unpackn recv_string 4 in
				let agent_num = Pack.unpackC recv_string 6 in
				let agent_id = String.sub recv_string 7 16 in
				let agent_name = if got_bytes > 39 then String.sub recv_string 23 (got_bytes - 39) else (sprintf "%s:%d" agent_ip agent_port) in

				p // sprintf "got response from %s:%d = %d agents with ID %s under name %S" agent_ip agent_port agent_num (to_hex agent_id) agent_name;

				if agent_ip = "0.0.0.0" then (
					p "IP address no good; trying again"
				) else (
					let a = match f#get_agent_by_key agent_id (agent_ip,agent_port) with
						| Some a -> a
						| None -> (
							let a = {
								agent_name = agent_name;
								agent_ip = (agent_ip,agent_port);
								agent_id = agent_id;
								agent_optimal_workers = agent_num;
								agent_workers = Rbtree.create ();
							} in
							f#add_agent a;
							a
						)
					in
					p "updating agent info";
					f#update_agent_info a agent_num (agent_ip,agent_port) agent_id disher_guts;
					p "updated agent info";
				)
			) else (
				p "threw out bad response"
			);
			(* Check the encode to see if it's still going *)
			not f#encoding_done
		with
			e -> ((*p // sprintf "failed with %S" (Printexc.to_string e);*) false)
		) in
		if ok then (
			discover_loop ()
		) else (
			p "encoding is done; exit"
		)
	) in
	discover_loop ()
;;
let make_discover_thread f =
	let v = ref None in
	let m = Mutex.create () in
	let c = Condition.create () in
	let t = Thread.create (fun () ->
(*		f#p // sprintf "Discover thread %d starting" (Thread.id (Thread.self ()));*)
		(try
			discover_guts f v m c
		with
			_ -> ()
		);
(*		f#p // sprintf "Discover thread %d exiting" (Thread.id (Thread.self ()));*)
	) () in
	fun () -> (
		Mutex.lock m;
		let rec check_stuff () = match !v with
			| None -> (
				Condition.wait c m;
				check_stuff ()
			)
			| Some q -> q
		in
		let port = check_stuff () in
		Mutex.unlock m;
		port
	)
;;



(************)
(* DO STUFF *)
(************)



let opts = parse_these_options Sys.argv;;

let f = new frames opts;;

let get_discover_port = make_discover_thread f;;


f#wait_until_output;;
(* Join output thread, somehow *)
f#p "Main thread killing discoverer";;
(try
	let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
	ignore // Unix.sendto sock "DONE" 0 4 [] (Unix.ADDR_INET (Unix.inet_addr_loopback, (get_discover_port ())));
	Unix.close sock
with
	e -> f#p // sprintf "I wonder what %S means..." (Printexc.to_string e)
);;
f#p "Main thread killed discoverer";;

printf "DONE\n%!";;

