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


let version_string = "x264farm-sp 1.06";;

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
	slave_first_frame : int;
	slave_last_frame : int;
	slave_zones : string;
	slave_name : string;
};;
type slave_meta_t = Slave_do of slave_t | Slave_stop | Slave_set_priority of Opt.priority_t * Opt.thread_priority_t;;


let do_this_slave = {
	slave_bytes_per_frame = (704 * 480 * 3) / 2;
	slave_i = "d:/documents/dvd/mkv/fma/48/farm.avs";
(*	slave_i = "fakevid.avs";*)
	slave_addr = Unix.ADDR_INET (Unix.inet_addr_of_string Sys.argv.(1), int_of_string Sys.argv.(2));
	slave_first_frame = 0;
	slave_last_frame = 31855;
	slave_zones = "";
	slave_name = "name";
};;


let rec do_slave_multithread () =

	set_binary_mode_in stdin true;

	let s_meta = Slave_do do_this_slave in

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
					let r = Avi.send_ptr d ptr off len in
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

(* Test the speed of select () *)
(* 10000000 selects took 32.766 seconds (305194 selects per second, 3.2766e-006 seconds per select) *)
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
(* Events are kind of slow... *)
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
					let rec get_frame f ptr_perhaps = (
						match ptr_perhaps with
						| Some ptr -> (
							if f land 15 = 0 then (
								p "checking thread priority";
								change_thread_priority p;
							);
							if f land 255 = 0 then (
								printf "Frame %d\n%!" f
							);
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
							get_frame s.slave_first_frame (Some (Avi.make_ptr s.slave_bytes_per_frame))
						with
							_ -> ()
					) () in

					let send_thread = Thread.create (fun () ->
						try
							send_frame (send_trade (Some (Avi.make_ptr s.slave_bytes_per_frame)))
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


let rec do_slave_multithread_demo () =

	let s = do_this_slave in

	(* AVI START *)
	Avi.init_avi ();
	let avi = Avi.open_avi s.slave_i in
	let y_length = avi.Avi.h * avi.Avi.w in
	let v_offset = y_length in
	let v_length = y_length lsr 2 in
	let u_offset = y_length + v_length in
	let u_length = v_length in

	let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
	Unix.connect sock s.slave_addr;



	let get_input_channel = Event.new_channel () in
	let send_input_channel = Event.new_channel () in



	let rec send_select d ptr off len num_left = (
		if num_left <= 0 then (
			printf "BLARG\n%!";
			failwith "OUT OF TIME";
		) else (
			match Unix.select [] [d] [] 1.0 with
			| (_,[_],_) -> (
(*				printf "Selected\n%!";*)
				let r = Avi.send_ptr d ptr off len in
				r
			)
			| _ -> (
				printf "Not selected\n%!";
				(* Check if we should keep going *)
(*				if check_stop () then (failwith "STOPPED in select");*)
				send_select d ptr off len (pred num_left)
			)
		)
	) in
	let rec really_send_parts d ptr off len n = (
		if len <= 0 then true else (
			let r = send_select d ptr off len n in
			if r = 0 then (
				raise End_of_file
			) else if r = len then (
				true
			) else (
				really_send_parts d ptr (off + r) (len - r) n
			);
		)
	) in



	let p = (
		let m = Mutex.create () in
		fun x -> (
			Mutex.lock m;
			print_endline x;
			Mutex.unlock m;
		)
	) in


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

	let rec get_frame f ptr_perhaps = (
		match ptr_perhaps with
		| Some ptr -> (
			if f >= s.slave_last_frame then (
				get_frame f (get_trade None)
			) else (
				let got = Avi.ptr_avi_frame avi f ptr in
				if got <> 1 then (
					get_frame (succ f) (get_trade None)
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
			if really_send_parts sock ptr 0 y_length 10 then (
				if really_send_parts sock ptr u_offset u_length 10 then (
					if really_send_parts sock ptr v_offset v_length 10 then (
						send_frame (send_trade (Some ptr))
					) else (
						send_frame (send_trade None)
					)
				) else (
					send_frame (send_trade None)
				)
			) else (
				send_frame (send_trade None)
			)
		)
		| None -> ()
	) in

	let get_thread = Thread.create (fun () ->
		try
			get_frame s.slave_first_frame (Some (Avi.make_ptr s.slave_bytes_per_frame))
		with
			_ -> ()
	) () in

	let send_thread = Thread.create (fun () ->
		try
			send_frame (send_trade (Some (Avi.make_ptr s.slave_bytes_per_frame)))
		with
			_ -> ()
	) () in
(*
	(* Put the stuff in the thing *)
	if s.slave_last_frame - s.slave_first_frame > 1 then Event.sync (Event.send get_input_channel (Some (Avi.make_ptr s.slave_bytes_per_frame)));
	if s.slave_last_frame - s.slave_first_frame > 2 then Event.sync (Event.send get_input_channel (Some (Avi.make_ptr s.slave_bytes_per_frame)));
*)
	Thread.join get_thread;
	Thread.join send_thread;

	Unix.close sock;
	Avi.exit_avi ();

	true
;;


















let rec do_slave_ptr () =

	(* Prevent the strings from being moved whilst writing to them *)
	(* This probably won't be needed, since the rest of the slave program does basically nothing, so there is nothing to wait for *)
(*
	let default_overhead = (Gc.get ()).Gc.max_overhead in
	Gc.set {(Gc.get ()) with Gc.max_overhead = 1000001};
*)
	set_binary_mode_in stdin true;

(*	let s_meta = Marshal.from_channel stdin in*)
	let s_meta = Slave_do do_this_slave in

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
					let r = Avi.send_ptr d ptr off len in
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
(*let q = if false then do_slave () else do_slave_ptr ();;*)
let q = do_slave_multithread ();;
if q then (
	exit 1; (* I say 1 is normal exit for the slave (0 is for the controller, you see...) *)
) else (
	exit 2; (* 2 is for some sort of error *)
)
;;
		
(* Put the version string here so it doesn't show up multiple times when starting slaves *)
if Sys.word_size = 32 then (
	printf "%s controller\n%!" version_string
) else (
	printf "%s controller %d-bit\n%!" version_string Sys.word_size
);;

