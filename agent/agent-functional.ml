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


(***********)
(* GLOBALS *)
(***********)
let current_version_major = 1;;
let current_version_minor = 1;;

let min_version_major = 1;;
let min_version_minor = 0;;

let version_string = "1.04";;

let dev_null = if Sys.os_type = "Win32" then "NUL" else "/dev/null";;


(*****************************)
(* HANDY FUNCTIONS AND TYPES *)
(*****************************)
(* + *)
let to_hex s =
	let result = String.create (2 * String.length s) in
	for i = 0 to String.length s - 1 do
		String.blit (Printf.sprintf "%02X" (int_of_char s.[i])) 0 result (2*i) 2;
	done;
	result
;;

(* + *)
let of_hex =
	let lookup = function
		| '0' -> 0 | '1' -> 1 | '2' -> 2 | '3' -> 3 | '4' -> 4 | '5' -> 5 | '6' -> 6 | '7' -> 7 | '8' -> 8 | '9' -> 9
		| 'a' | 'A' -> 10
		| 'b' | 'B' -> 11
		| 'c' | 'C' -> 12
		| 'd' | 'D' -> 13
		| 'e' | 'E' -> 14
		| 'f' | 'F' -> 15
		| _ -> failwith "Invalid hex string"
	in
	fun s -> (
		let result = String.create (String.length s / 2) in
		for i = 0 to String.length result - 1 do
			let a = 16 * lookup s.[i * 2 + 0] + lookup s.[i * 2 + 1] in
			result.[i] <- Char.chr a
		done;
		result
	)
;;

(* + *)
let ( +|  ) = Int64.add;;
let ( -|  ) = Int64.sub;;
let ( *|  ) = Int64.mul;;
let ( !|  ) = Int64.of_int;;
let ( !-  ) = Int64.to_int;;
let ( >|- ) = Int64.shift_right;;
let ( >|  ) = Int64.shift_right_logical;;
let ( <|  ) = Int64.shift_left;;
let ( /|  ) = Int64.div;;
let ( ||| ) = Int64.logor;;
let ( &&| ) = Int64.logand;;

exception Timeout;;

(* + *)
type ('a,'b) valid_t = Valid of 'a | Invalid of 'b;;
let (%%) m f = match m with (* Same as Haskell's >>= *)
	| Valid x -> f x
	| Invalid x -> Invalid x
;;
let (%) m f = m %% (fun _ -> f ());;
(* I don't really need return; Using "Valid x" is easier to write *)

let trap_exception a b = try Valid (a b) with x -> Invalid x;;
let trap_exception_2 a b c = try Valid (a b c) with x -> Invalid x;;
let trap_exception_3 a b c d = try Valid (a b c d) with x -> Invalid x;; (* Let's see how many of these we need! *)
let is_valid a b = match trap_exception a b with
	| Valid _ -> true
	| Invalid _ -> false
;;
let is_valid_2 a b c = match trap_exception_2 a b c with
	| Valid _ -> true
	| Invalid _ -> false
;;

(* 3.528575 = min (max a x) b
 * 2.788284 = if x < a then a else if x > b then b else x
 * 0.303620 = integer-specific version of the if-then version
 * Note that these are not equivalent if b < a *)
(* + *)
let bound a b (x:int) = if x < a then a else if x > b then b else x;;

type o = {
	o_config : string;
	o_logfile : string option;
	o_nosched : bool;
	o_cap : bool;
(*	o_help : bool;*)
};;
let default_o = {
	o_config = "config.xml";
	o_logfile = Some "out-dump.txt";
	o_nosched = false; (* Not nosched = YES schedule *)
	o_cap = false;
(*	o_help = false;*)
};;

type c = {
	c_agent_name : string;
	c_agent_port : int;
	c_controller_port : int;
	c_temp_dir : string;
	c_encodes : int;
	c_x264 : string;
	c_bases : string list;
	c_nice : int;

	c_buffer_frames : int;
};;
let default_c = {
	c_agent_name = "";
	c_agent_port = 40705;
	c_controller_port = 40704;
	c_temp_dir = Filename.concat Filename.temp_dir_name "x264farm-sp";
	c_encodes = max 1 (Opt.num_processors ());
	c_x264 = "x264";
	c_bases = [""];
	c_nice = 10;
	c_buffer_frames = 16;
};;

type i = {
	p : ?name:string -> ?screen:bool -> ?log:bool -> ?lock:bool -> ?time:bool -> ?fill:bool -> string -> unit;
	pm : Mutex.t;
};;

type job_t = {
	job_version_major : int;
	job_version_minor : int;
	job_hash : string;
	job_name : string;
	job_seek : int;
	job_frames : int;
	job_options : string;
	job_res_x : int;
	job_res_y : int;
	job_fps_n : int;
	job_fps_d : int;
	job_ntrx_ok : bool;
	job_private : string option;
};;

(* Encode_mp4 argument is the private string *)
type encode_t = Encode_mkv | Encode_mp4 | Encode_264;;

(* Use the following for printing:
 * i.p // sprintf "FORMATTING" a b c
 * This function just removes the need for extra parentheses, like this:
 * i.p (sprintf "FORMATTING" a b c)
 * Also useful for ignoring functions:
 * ignore // f a b c d
 * rather than:
 * ignore (f a b c d) *)
(* + *)
let (//) a b = a b;;
(* Or you can go the other way
 * This is good for doing something like:
 * 3 /? succ /? succ -> 5
 * without using parenthesis. If you were to use //, it would have problems:
 * succ // succ // 3 -> (succ // succ) // 3 -> ERROR *)
(* I should use @@ for the operator, or something *)
(* + *)
let (/?) a b = b a;;



(* Stop Unixy systems from failing when the x264 pipe dies *)
ignore // trap_exception_2 Sys.set_signal Sys.sigpipe Sys.Signal_ignore;;



(* + *)
let nice_to_priority q =
	if q >= 12 then (
		Opt.Idle
	) else if q >= 4 then (
		Opt.Below_normal
	) else if q >= -4 then (
		Opt.Normal
	) else if q >= -12 then (
		Opt.Above_normal
	) else if q >= -19 then (
		Opt.High
	) else (
		Opt.Realtime
	)
;;

(* ~ IO *)
let seek_add h plus =
	let a = LargeFile.pos_in h in
	trap_exception_2 LargeFile.seek_in h (a +| plus) (* This can fail if the a+plus<0 *)
;;

exception Unexpected_packet of string;;
let unexpected_packet ?here (a,b) = match here with
	| None -> Invalid (Unexpected_packet (sprintf "Packet %S (length %d) was not expected" a (String.length b)))
	| Some here -> Invalid (Unexpected_packet (sprintf "Packet %S (length %d) was not expected here (%s)" a (String.length b) here))
;;


(* Used to stop encoding and stuff, since "Valid _" implies that stuff should keep going *)
exception Stop;;


(******)
(* IO *)
(******)
(* ~ Filesystem *)
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
let is_something x = try (
	match (Unix.LargeFile.stat x).Unix.LargeFile.st_kind with
	| Unix.S_DIR | Unix.S_REG -> true
	| _ -> false
) with
	_ -> false
;;
let search_for_file x =
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
		(* Explicit; only look one place *)
		if is_file x then (
			Some x
		) else (
			None
		)
	)
;;
let rec create_dir x =
	if is_dir x then (
		true (* DONE! *)
	) else if is_something x then (
		false (* It's not a dir, but it's already here *)
	) else (
		let dirname = Filename.dirname x in
		if dirname = x then (
			(* We're at the root, but it doesn't exist (this happens if you specify q:\ in Windows and q is invalid) *)
			false
		) else (
			if create_dir dirname then (
				(* Still working *)
				match trap_exception_2 Unix.mkdir x 0o755 with
				| Valid () -> true
				| Invalid _ -> false
			) else (
				false
			)
		)
	)
;;
let rec really_read d s off len =
	if len <= 0 then Valid () else (
		let r = trap_exception (Unix.read d s off) len in
		match r with 
		| Valid 0 -> Invalid End_of_file
		| Valid r -> really_read d s (off + r) (len - r)
		| Invalid e -> Invalid e
	)
;;

(* + *)
let temp_file_name state prefix suffix =
	let s2 = Random.State.copy state in
	let rnd = (Random.State.bits s2) land 0xFFFFFF in
	(Printf.sprintf "%s%06x%s" prefix rnd suffix, s2)
;;
(* ~ filesystem *)
let open_temp_file state prefix suffix =
	let rec try_name counter use_state = (
		if counter >= 1000 then (
			Invalid Timeout
		) else (
			let (name,new_state) = temp_file_name use_state prefix suffix in
			match trap_exception (open_out_gen (Open_wronly::Open_creat::Open_excl::Open_binary::[]) 0o600) name with
			| Valid f -> Valid (name, f, new_state)
			| Invalid (Sys_error _) -> try_name (succ counter) new_state
			| Invalid e -> Invalid e
		)
	) in
	try_name 0 state
;;

let get_temp_name state prefix suffix =
	open_temp_file state prefix suffix %%
	fun (n,h,s) -> (trap_exception close_out h %% (fun () -> Valid (n,s)))
;;

(* ~ filesystem *)
let remove_file f = match trap_exception Unix.unlink f with
	| Valid () -> ()
	| Invalid (Unix.Unix_error (Unix.ENOENT,_,_)) -> () (* Couldn't be found; that makes it OK *)
	| Invalid _ -> (
		(* Wasn't deleted; try again later *)
		ignore // Thread.create (fun q ->
			Thread.delay 10.0;
			ignore // trap_exception Unix.unlink q
		) f;
		()
	)
;;

(* ~ filesystem and the program run can do anything *)
let test_command s =
	let env = Unix.environment () in
	let str_len = 16 in
	let dump_str = String.create str_len in
	let rec dump c = (
(*		printf "TEST dump\n%!";*)
		let ok = (
			(trap_exception (input c dump_str 0) str_len) %%
			(fun x -> if x > 0 then Valid true else Valid false)
		) in
		match ok with
		| Valid true -> dump c
		| _ -> ()
	) in
	(* Function *)
	(trap_exception_2 Unix.open_process_full s env) %%
	(fun (a,b,c) ->
(*		printf "TEST command before a\n%!";*)
		dump a;
(*		printf "TEST command after a, before c\n%!";*)
		dump c;
(*		printf "TEST command after c\n%!";*)
		trap_exception Unix.close_process_full (a,b,c)
	)
;;

(* ~ filesystem (random state is tossed) *)
let mp4_supported state c =
	get_temp_name state (Filename.concat c.c_temp_dir "mp4 test ") ".mp4" %%
	(fun (out_mp4, state_out) ->
		let everything_worked = (
			let do_this = sprintf "%s --quiet --crf 17 -o \"%s\" - 2x2 2>%s >%s" c.c_x264 out_mp4 dev_null dev_null in
			(trap_exception Unix.open_process_out do_this) %%
			(trap_exception Unix.close_process_out) %%
			(function
				| Unix.WEXITED 0 -> Valid (true,state_out)
				| _ -> Valid (false,state_out)
			)
		) in
		remove_file out_mp4;
		everything_worked
	)
;;

(***********)
(* CLASSES *)
(***********)
class buffered_send ?print ?(len=16384) sock =
	object (o)
		method private p = (match print with
			| None -> ignore
			| Some f -> f
		)

		val buffer = String.create (max 8 (min Sys.max_string_length len))
		val max_len = max 8 (min Sys.max_string_length len)
		val b_tag = String.create 4
		val mutable pos = 8

		(* Receive stuff *)
		method really_recv str from len = (
			if len = 0 then (
				Valid ()
			) else (
				match trap_exception (Unix.recv sock buffer from len) [] with
				| Valid 0 -> Invalid End_of_file
				| Valid n -> o#really_recv str (from + n) (len - n)
				| Invalid e -> Invalid e
			)
		)

		method recv = (
			let t_str = String.create 4 in
			let l_str = String.create 4 in
			(o#really_recv t_str 0 4) %%
			(fun () -> o#really_recv l_str 0 4) %%
			(fun () ->
				(* Now both t_str and l_str have stuff in them *)
				match l_str with
				| "\x00\x00\x00\x00" -> Valid (t_str,"")
				| _ -> (
					let l = Pack.unpackN l_str 0 in
					let s_str = String.create l in
					(o#really_recv s_str 0 l) %%
					(fun () -> Valid (t_str,s_str))
				)
			)
		)



		(* Flushes the buffer from the "from" position to "pos", then resets "pos" to 8 (where the tag and length end) *)
		(* This does NOT check the length string, so call flush_buffer instead of send_buffer directly *)
		method private send_buffer from = (
			if from = pos then (
				(* Clear the buffer *)
				pos <- 8;
				Valid ()
			) else (
				match trap_exception (Unix.send sock buffer from (pos - from)) [] with
				| Valid 0 -> Invalid End_of_file
				| Valid n -> o#send_buffer (from + n)
				| Invalid e -> Invalid e
			)
		)

		(* "Valid ()" if it's OK to continue, "Invalid Stop" if we should stop sending *)
		(* If nothing is sent, this function returns valid by default *)
		method flush_buffer = (
			if pos > 8 then (
				(* Only do this if there is data in the buffer *)
				Pack.packN buffer 4 (pos - 8);
				(o#send_buffer 0) %% (fun () -> o#recv) %%
				(function
					| ("CONT","") -> Valid ()
					| ("STOP","") -> Invalid Stop
					| x -> unexpected_packet ~here:"buffered_send#flush_buffer" x
				)
			) else (
				Valid ()
			)
		)


		method private really_send_no_buffer_check str from len = (
			if len = 0 then (
				Valid ()
			) else (
				match trap_exception (Unix.send sock str from len) [] with
				| Valid 0 -> Invalid End_of_file
				| Valid n -> o#really_send_no_buffer_check str (from + n) (len - n)
				| Invalid e -> Invalid e
			)
		)

		(* This returns the same type as flush_buffer *)
		method really_send str from len = (
			(o#flush_buffer) %%
			(fun keep_going -> (o#really_send_no_buffer_check str from len) %% (fun () -> Valid keep_going))
		)

		method send t s = (
			if String.length t <> 4 then (
				Invalid (Invalid_argument "buffered_send#send tag not 4 bytes long")
			) else (
				match o#flush_buffer with
				| (Valid () | Invalid Stop) as q -> (
					let l_str = String.create 4 in
					let l_int = String.length s in
					Pack.packN l_str 0 l_int;
					(o#really_send_no_buffer_check t 0 4) %% (fun () -> o#really_send_no_buffer_check l_str 0 4) %% (fun () -> o#really_send_no_buffer_check s 0 l_int) %% (fun () -> q)
(*
				(o#flush_buffer) %%
				(fun keep_going ->
					let l_str = String.create 4 in
					let l_int = String.length s in
					Pack.packN l_str 0 l_int;
					(o#really_send_no_buffer_check t 0 4) %% (fun () -> o#really_send_no_buffer_check l_str 0 4) %% (fun () -> o#really_send_no_buffer_check s 0 l_int) %% (fun () -> Valid keep_going)
*)
				)
				| Invalid e -> Invalid e
			)
		)

		method private send_buffered_sub_unsafe t s f l = (
			(*
			 * l >= left; pos <= 8 -> send custom string
			 * l >= left; pos >  8 -> normal; send; recurse
			 * l <  left; pos <= 8 -> set pos to 8; normal
			 * l <  left; pos >  8 -> normal
			 *)
(*
			if pos < 8 then (
				pos <- 8
			);
*)
			if t = b_tag then (
				let left = max_len - pos in
				match (l >= left, pos = 8) with
				| (true, true) -> (
					(* just send the string separate from the buffer *)
					Pack.packN buffer 4 l;
					(o#really_send_no_buffer_check buffer 0 8) %%
					(fun () -> o#really_send_no_buffer_check s f l) %%
					(fun () -> o#recv) %%
					(function
						| ("CONT","") -> Valid ()
						| ("STOP","") -> Invalid Stop
						| x -> unexpected_packet ~here:"buffered_send#send_buffered_sub" x
					)
				)
				| (true, false) -> (
					(* there is a lot to send, but the buffer is not empty; fill and send *)
					String.blit s f buffer pos left; (* This is one of the unsafe parts *)
(*					let b4pos = pos in*)
					pos <- pos + l;
					match o#flush_buffer with
					| Valid () -> o#send_buffered_sub_unsafe t s (f + left) (l - left)
					| Invalid e -> Invalid e
				)
				| (false, _) -> (
					(* Put stuff in the buffer like normal, but don't send *)
					trap_exception (String.blit s f buffer pos) l %%
					fun () -> Valid ()
				)
			) else (
				(* The tags don't match; send the current buffer and reset *)
				match o#flush_buffer with
				| Valid () -> (
					String.blit t 0 buffer 0 4;
					String.blit t 0 b_tag 0 4;
					o#send_buffered_sub_unsafe t s f l
				)
				| Invalid e -> Invalid e
			)
		)

		method send_buffered_sub t s f l = (
			if f < 0 || f + l > String.length s || String.length t <> 4 then (
				Invalid (Invalid_argument "buffered_send#send_buffered_sub")
			) else (
				o#send_buffered_sub_unsafe t s f l
			)
		)
		(* This version is to enable easier monad binding *)
		(* Use "... ## o#send_buffered_sub_m t s f l" *)
		(* Instead of "... ## fun () -> o#send_buffered_sub t s f l" *)
		method send_buffered_sub_m t s f l () = o#send_buffered_sub t s f l

		method send_buffered t s = (
			if String.length t <> 4 then (
				Invalid (Invalid_argument "buffered_send#send_buffered tag not 4 bytes long")
			) else (
				o#send_buffered_sub_unsafe t s 0 (String.length s)
			)
		)
		(* Ditto with the easier monad binding *)
		method send_buffered_m t s () = o#send_buffered t s

	end
;;

let send_gen_m str from len sock =
	let rec helper f l = (
		if l = 0 then (
			Valid sock
		) else (
			let sent = trap_exception (Unix.send sock str f l) [] in
			match sent with
			| Valid 0 -> Invalid End_of_file
			| Valid x -> helper (f + x) (l - x)
			| Invalid e -> Invalid e
		)
	) in
	helper from len
;;
let recv_gen_m str from len sock =
	let rec helper f l = (
		if l = 0 then (
			Valid sock
		) else (
			let recvd = trap_exception (Unix.recv sock str f l) [] in
			match recvd with
			| Valid 0 -> Invalid End_of_file
			| Valid x -> helper (f + x) (l - x)
			| Invalid e -> Invalid e
		)
	) in
	helper from len
;;
let recv_new_m len (so_far, sock) =
	let str = String.create len in
	let rec helper f l = (
		if l = 0 then (
			Valid (str :: so_far, sock)
		) else (
			match trap_exception (Unix.recv sock str f l) [] with
			| Valid 0 -> Invalid End_of_file
			| Valid x -> helper (f + x) (l - x)
			| Invalid e -> Invalid e
		)
	) in
	helper 0 len
;;
let send_m ((t,s),sock) =
	let len = String.length s in
	let l = String.create 4 in
	Pack.packN l 0 len;
	Valid sock %% send_gen_m t 0 4 %% send_gen_m l 0 4 %% send_gen_m s 0 len
;;
(* This is getting scary... *)
let recv_m sock =
	Valid ([], sock) %%
	recv_new_m 4 %%
	recv_new_m 4 %%
	(function
		| ([len_bin; tag], sock) -> (
			(* Get the string based on the length *)
			let len = Pack.unpackN len_bin 0 in
			recv_new_m len ([tag], sock)
		)
		| _ -> Invalid End_of_file (* Can this happen? *)
	)
	%%
	(function
		| ([str;tag], sock) -> Valid ((tag,str), sock)
		| _ -> Invalid End_of_file
	)
;;
(* To be honest, I don't really know what the last few functions have been doing. I'll probably figure it out eventually *)

(**********************)
(* START CONSTRUCTION *)
(**********************)



(* This returns "Valid o" if the arguments were parsed correctly *)
(* and "Invalid s" if not, where s is a string describing the problem *)
(* If s = "" then the --help option was given and help should be displayed *)
(* + *)
let make_o arg_list =
	(* Helper returns "Some o" if everything makes sense, or "None" if there is an unrecognized option *)
	let rec helper o l = match l with
		| [] -> Valid o
		| "--config" :: config :: tl -> helper {o with o_config = config} tl
		| "--logfile" :: "" :: tl -> helper {o with o_logfile = None} tl
		| "--logfile" :: logfile :: tl -> helper {o with o_logfile = Some logfile} tl
		| "--nosched" :: tl -> helper {o with o_nosched = true} tl
		| "--cap" :: tl -> helper {o with o_cap = true} tl
		| "--help" :: tl -> Invalid ""
		| x :: tl -> Invalid (sprintf "Unknown option: %S\n" x)
	in
	match arg_list with
	| [] -> Valid default_o (* Whatever *)
	| hd :: tl -> helper default_o tl
;;

(* ~ IO *)
let make_c o =
	let rec parse_bases mo = match mo with
		| [] -> Some []
		| hd :: tl -> (
			match (hd, parse_bases tl) with
			| (_, None) -> None
			| (Xml.Element ("base", _, []), Some y) -> Some ("" :: y)
			| (Xml.Element ("base", _, [Xml.PCData q]), Some y) when is_dir q -> Some (q :: y)
			| (Xml.Element ("base", _, [Xml.PCData q]), Some y) -> Some y (* Ignore nonexistant directories *)
			| _ -> None
		)
	in
	let rec parse_elements c l = match l with
		| [] -> Valid c
		| Xml.Element ("port", ([("controller",x);("agent",y)] | [("agent",y);("controller",x)]), _) :: tl when is_valid int_of_string x && is_valid int_of_string y -> parse_elements {c with c_agent_port = bound 1024 65535 (int_of_string y); c_controller_port = bound 1024 65535 (int_of_string x)} tl;
		| Xml.Element ("port", _, _) :: tl -> Invalid "Unknown <port> element format"
		| Xml.Element ("encodes", _, [Xml.PCData q]) :: tl when is_valid int_of_string q -> parse_elements {c with c_encodes = max 0 (int_of_string q)} tl
		| Xml.Element ("encodes", _, _) :: tl -> Invalid "Unknown <encodes> element format"
		| Xml.Element ("temp", _, [Xml.PCData q]) :: tl -> (
			if create_dir q then (
				parse_elements {c with c_temp_dir = q} tl
			) else (
				(* Just ignore *)
				parse_elements c tl
			)
		)
		| Xml.Element ("temp", _, _) :: tl -> Invalid "Unknown <temp> element format"
		| Xml.Element ("x264", _, [Xml.PCData q]) :: tl -> parse_elements {c with c_x264 = q} tl
		| Xml.Element ("x264", _, _) :: tl -> Invalid "Unknown <x264> element format"
		| Xml.Element ("base", _, [Xml.PCData q]) :: tl when is_dir q -> parse_elements {c with c_bases = q :: []} tl
		| Xml.Element ("base", _, _) :: tl -> Invalid "Unknown <base> element format"
		| Xml.Element ("bases", _, base_list) :: tl -> (
			match parse_bases base_list with
			| Some bases -> parse_elements {c with c_bases = bases} tl
			| None -> Invalid "Unknown <bases> element format"
		)
		| Xml.Element ("nice", _, [Xml.PCData q]) :: tl when is_valid int_of_string q -> parse_elements {c with c_nice = bound 0 19 (int_of_string q)} tl
		| Xml.Element ("nice", _, _) :: tl -> Invalid "Unknown <nice> element format"
		| Xml.Element ("buffer-frames", _, [Xml.PCData q]) :: tl when is_valid int_of_string q -> parse_elements {c with c_buffer_frames = max 0 (int_of_string q)} tl
		| Xml.Element ("buffer-frames", _, _) :: tl -> Invalid "Unknown <buffer-frames> element format"
		| _ :: tl -> parse_elements c tl
	in
	let parse_obj = trap_exception Xml.parse_file o.o_config in
	match parse_obj with
	| Invalid e -> Invalid (sprintf "Config file does not seem to be valid: %s" (Printexc.to_string e))
	| Valid (Xml.Element ("agent-config", [("name",n)], elts)) -> parse_elements {default_c with c_agent_name = n} elts
	| Valid (Xml.Element ("agent-config", _, elts)) -> parse_elements default_c elts
	| Valid (Xml.Element (q, _, _)) -> Invalid (sprintf "Config file must have root <agent-config>, not <%s>" q)
	| Valid (Xml.PCData _) -> Invalid "Config file must have root <agent-config>, not regular data"
;;

(* ~ IO *)
let make_i o =
	let pm = Mutex.create () in
	let print = (
		let time_string () = (
			let tod = Unix.gettimeofday () in
			let cs = int_of_float (100.0 *. (tod -. floor tod)) in (* centi-seconds *)
			let lt = Unix.localtime tod in
			sprintf "%04d-%02d-%02d~%02d:%02d:%02d.%02d" (lt.Unix.tm_year + 1900) (lt.Unix.tm_mon + 1) (lt.Unix.tm_mday) (lt.Unix.tm_hour) (lt.Unix.tm_min) (lt.Unix.tm_sec) cs
		) in
		match o.o_logfile with
		| None -> (
			(* Don't use the log *)
			fun ?(name="") ?(screen=false) ?(log=true) ?(lock=true) ?(time=true) ?(fill=false) s -> (
				if screen then (
					(* Only do something if the screen is on *)
					if lock then (
						Mutex.lock pm;
						print_string name;
						print_endline s;
						Mutex.unlock pm;
					) else (
						print_string name;
						print_endline s;
					)
				)
			)
		)
		| Some f -> (
			let ph_perhaps = trap_exception open_out f in
			match ph_perhaps with
			| Valid ph -> (
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
			| Invalid e -> (
				printf "WARNING: Log file \"%s\" can not be opened - %s\n%!" f (Printexc.to_string e);
				(* Don't use the log (since it can't be opened) *)
				fun ?(name="") ?(screen=false) ?(log=true) ?(lock=true) ?(time=true) ?(fill=false) s -> (
					if screen then (
						(* Only do something if the screen is on *)
						if lock then (
							Mutex.lock pm;
							print_string name;
							print_endline s;
							Mutex.unlock pm;
						) else (
							print_string name;
							print_endline s;
						)
					)
				)
			)
		)
	) in

	{
		p = print;
		pm = pm;
	}
;;

(************)
(* AGENT ID *)
(************)
(* x Random.State.make_self_init uses voodoo magic; modifies state if given *)
let new_agent_id state =
	let use = Random.State.copy state in
	let id = String.create 16 in
	for i = 0 to 15 do
		id.[i] <- Char.chr (Random.State.int use 256)
	done;
	(id,use)
;;

(********)
(* PING *)
(********)
(* ~ Networking *)
let send_ping c agent_id port = (* Note that this port is the one to tell the controller to connect to *)
	let str1 = "SAGE\x00\x00\x00" ^ agent_id ^ c.c_agent_name in
	Pack.packn str1 4 port;
	Pack.packC str1 6 c.c_encodes;
	let str = str1 ^ Digest.string str1 in
	fun to_addr -> (
		try
			let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
			let sent_ok = try
				Unix.setsockopt sock Unix.SO_BROADCAST true;
				let sent = Unix.sendto sock str 0 (String.length str) [] to_addr in
				if sent <> String.length str then (
					Invalid "Not all of the ping was sent"
				) else (
					Valid ()
				)
			with
				e -> Invalid (Printexc.to_string e)
			in
			Unix.close sock;
			sent_ok
		with
			e -> Invalid (Printexc.to_string e)
	)
;;

(********************)
(* AGENT-BASED BASE *)
(********************)
(* + *)
let split_last_dir x = (Filename.dirname x, Filename.basename x);;
let rec split_on_dirs_rev x =
	let (dir, base) = split_last_dir x in
	if dir = x then (
		if base = "." then (
			dir :: []
		) else (
			base :: dir :: []
		)
	) else if base = "." then (
		split_on_dirs_rev dir
	) else (
		base :: split_on_dirs_rev dir
	)
;;
let split_on_dirs x = List.rev (split_on_dirs_rev x);;

(* This function checks a SINGLE dir structure for a file *)
(* ~ IO *)
let find_file_with_md5 base file md5 =
	let on_dirs = split_on_dirs file in
	let rec check_for_file current_list = (
		let dir_now = List.fold_left (fun so_far gnu -> if so_far = "" then gnu else Filename.concat so_far gnu) "" current_list in
		let full_file = (if base = "" then dir_now else Filename.concat base dir_now) in
		if is_file full_file then (
			(* Found a potential match *)
			match trap_exception Digest.file full_file with
			| Valid q when q = md5 -> (
				(* FOUND! *)
				Some full_file
			)
			| _ -> (
				if base = "" then (
					None
				) else (
					match current_list with
					| hd :: tl -> check_for_file tl
					| [] -> None
				)
			)
		) else (
			if base = "" then (
				None
			) else (
				match current_list with
				| hd :: tl -> check_for_file tl
				| [] -> None
			)
		)
	) in
	check_for_file on_dirs
;;
(* ~ (IO) *)
let rec find_base name md5 = function
	| [] -> None
	| base :: tl -> (
		match find_file_with_md5 base name md5 with
		| None -> find_base name md5 tl
		| Some x -> Some x
	)
;;

(*********************)
(* FAILPIPE DETECTOR *)
(*********************)
(* Returns (x264_ok, failpipe) *)
(* ~ runs external command *)
let check_x264 c = if Sys.os_type = "Win32" then (
	let do_this = sprintf "%s --quiet --qp 0 -o NUL - 2x2 2>&1" c.c_x264 in
	let return_me = (
		(trap_exception Unix.open_process do_this) %%
		(fun (pipein,pipeout) ->
			let wrote_ok = trap_exception_2 output_string pipeout "\x1A\x1A\x1A\x1A\x1A\x1A" in
			let close_ok = trap_exception close_out pipeout in
			let get = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" in
			let rec dump c so_far = (
				let ok = trap_exception (input c get 0) 64 in
				match ok with
				| Valid 0 -> Valid so_far
				| Valid x -> dump c (so_far + x)
				| e -> e
			) in
			let got_bytes = dump pipein 0 in
			let exited_ok = trap_exception Unix.close_process (pipein,pipeout) in
			match (wrote_ok, close_ok, exited_ok, got_bytes) with
			| (Valid (), Valid (), Valid (Unix.WEXITED 0), Valid x) when x > 2 -> Valid (true, false)
			| (Valid (), Valid (), Valid (Unix.WEXITED 0), _) -> Valid (true, true)
			| _ -> Valid (false, false)
		)
	) in
	match return_me with
	| Valid x -> x
	| Invalid _ -> (false, false)
) else (
	let do_this = sprintf "%s --version 2>&1" c.c_x264 in
	let return_me = (
		(trap_exception Unix.open_process_in do_this) %%
		(trap_exception Unix.close_process_in)
	) in
	match return_me with
	| Valid (Unix.WEXITED 0) -> (true, false)
	| _ -> (false, false)
);;

(*************************)
(* ENCODING FILE SUPPORT *)
(*************************)

let make_tracks p c state job =
	(get_temp_name state (Filename.concat c.c_temp_dir "temp tracks ") ".mkv") %%
	(fun (temp_name,s) ->
		let everything_worked = (
			let do_this = sprintf "%s %s --fps %d/%d --no-psnr --no-ssim --quiet -o \"%s\" - %dx%d" c.c_x264 job.job_options job.job_fps_n job.job_fps_d temp_name job.job_res_x job.job_res_y in
			trap_exception Unix.open_process_out do_this %%
			(fun put ->
				let frame_size = job.job_res_x * job.job_res_y * 3 / 2 in
				let frame = String.make (min 16384 frame_size) 'E' in
				let rec helper send_more = (
					if send_more = 0 then (
						Valid ()
					) else (
						let send_now = min 16384 send_more in
						match trap_exception (output put frame 0) send_now with
						| Valid () -> helper (send_more - send_now)
						| Invalid e -> Invalid e
					)
				) in
				let returnme = (
					(helper frame_size) %%
					(fun () -> trap_exception flush put)
				) in
				(trap_exception Unix.close_process_out put) %%
				(fun _ -> returnme)
			) %%
			(* Now the temp file is written *)
			(fun () -> trap_exception open_in_bin temp_name) %%
			(fun t ->
				(* Delay until the file has some data in it *)
				while LargeFile.in_channel_length t = 0L do (* I don't think I need to trap exceptions here *)
					p "MAKE_TRACKS delaying";
					Thread.delay 0.05
				done;
				let rec helper () = (
					let idlen = Matroska.input_id_and_length t in (* This function raises no exceptions *)
					match idlen with
					| Some (0x08538067, _) -> (
						p "MAKE_TRACKS segment";
						helper ()
					)
					| Some (0x0654AE6B, Some l) -> (
						(* TRAX! *)
						let tracks = String.create !-l in
						let track_pos = LargeFile.pos_in t in
						match trap_exception (really_input t tracks 0) (String.length tracks) with
						| Valid () -> (
							p // sprintf "MAKE_TRACKS found track element of length %d" (String.length tracks);
							let priv_perhaps = (
								LargeFile.seek_in t track_pos; (* This can't fail since we get track_pos from a position in the file (it can't be negative) *)
								let rec priv_helper () = (
									match Matroska.input_id_and_length t with
									| Some (0x2E, _) -> priv_helper () (* Track *)
									| Some (0x23A2, Some len) -> (
										let priv = String.create !-len in
										trap_exception (really_input t priv 0) (String.length priv) %% (fun () -> Valid priv)
									)
									| Some (_, Some len) -> (seek_add t len %% priv_helper) (* Skip over unknown of known length *)
									| Some (_, None) -> priv_helper () (* Can't skip over element of unknown length *)
									| None -> Invalid End_of_file
								) in
								priv_helper ()
							) in
							match priv_perhaps with
							| Valid pp -> Valid (tracks,pp,s)
							| Invalid e -> Invalid e
						)
						| Invalid e -> Invalid e
					)
					| Some (_, Some l) -> (
						(* Something else; skip *)
						match seek_add t l with
						| Valid _ -> (
							p "MAKE_TRACKS skip";
							helper ()
						)
						| Invalid x -> Invalid x
					)
					| None | Some (_, None) -> (
						p "MAKE_TRACKS found something weird; mope";
						Invalid Not_found
					)
				) in
				let helped = helper () in
				ignore // trap_exception close_in t;
				helped
			)
		) in
		remove_file temp_name;
		everything_worked
	)
;;

let rec get_unix_handle p name tries =
	match trap_exception Unix.LargeFile.stat name with
	| Valid {Unix.LargeFile.st_size = s} when s > 0L -> (
		trap_exception_3 Unix.openfile name [Unix.O_RDWR] 0o600
	)
	| _ when tries <= 0 -> (
		Invalid End_of_file
	)
	| _ -> (
		Thread.delay 1.0;
		get_unix_handle p name (pred tries)
	)
;;

(* ~ IO *)
class expandable_handle h =
	object (o)
		val max_times = 600

		val mutable bit = 8
		val mutable data = 0

		method private read_base s f l times = (
			match trap_exception (Unix.read h s f) l with
			| Valid x when x = l -> Valid ()
			| Valid 0 when times = 0 -> Invalid Timeout
			| Invalid e when times = 0 -> Invalid e
			| Valid 0 | Invalid _ -> (
				Thread.delay 0.1;
				o#read_base s f l (pred times)
			)
			| Valid x -> o#read_base s (f + x) (l - x) max_times
		)

		method read n = (
			(trap_exception String.create n) %%
			(fun s ->
				(trap_exception_3 Unix.LargeFile.lseek h 0L Unix.SEEK_CUR) %%
				(fun first_pos ->
					match o#read_base s 0 n max_times with
					| Valid () -> Valid s
					| Invalid e -> (
						(* Reset the pos if something went wrong *)
						ignore // trap_exception_3 Unix.LargeFile.lseek h first_pos Unix.SEEK_SET;
						Invalid e
					)
				)
			)
		)

		(* All of these methods invalidate the bitstream position *)
		method seek_byte n = (
			bit <- 8; (* Invalidate the bit *)
			(trap_exception_3 Unix.LargeFile.lseek h n Unix.SEEK_SET) %% (fun x -> Valid ())
		)
		method seek_add n = (bit <- 8; trap_exception_3 Unix.LargeFile.lseek h n Unix.SEEK_CUR)
		method seek_end n = (bit <- 8; trap_exception_3 Unix.LargeFile.lseek h n Unix.SEEK_END)
		method pos_byte = o#seek_add 0L
		method len_byte = o#pos_byte %% (fun pos -> (o#seek_end 0L) %% (fun the_end -> (o#seek_byte pos) %% (fun _ -> Valid the_end)))

		method close = trap_exception Unix.close h

		(* These methods are NOT FUNCTIONAL. They raise exceptions and stuff to make the connection with Bitstream better *)
(*< b : int -> int; g : int; pos : int64; seek : int64 -> 'a; .. >*)
		method pos = (
			let file_pos = Unix.LargeFile.lseek h 0L Unix.SEEK_CUR in
			((file_pos -| 1L) <| 3) +| !|bit
		)
		method seek t = (
			let seek_byte = t >| 3 in
			let seek_bit = !-(t &&| 7L) in
			ignore // Unix.LargeFile.lseek h seek_byte Unix.SEEK_SET;
			if seek_bit = 0 then (
				(* Force a read next time *)
				bit <- 8
			) else (
				(* Read a bit *)
				bit <- seek_bit;
				match o#read 1 with
				| Valid q -> data <- Char.code q.[0]
				| Invalid e -> raise e
			)
		)
		method private bs so_far n = (
			if n = 0 then (
				so_far
			) else if bit > 7 then (
				bit <- bit - 8;
				match o#read 1 with
				| Valid q -> (data <- Char.code q.[0]; o#bs so_far n)
				| Invalid e -> raise e
			) else if bit = 0 && n >= 8 then (
				(* Read a whole byte *)
				let now = (so_far lsl 8) lor data in
				bit <- 8;
				o#bs now (n - 8)
			) else (
				let now = (so_far lsl 1) lor ((data lsr (7 - bit)) land 1) in
				bit <- succ bit;
				o#bs now (pred n)
			)
		)
		method b n = o#bs 0 n
		method g = (
			let rec count_zeros so_far = (
				match o#bs 0 1 with
				| 0 -> count_zeros (succ so_far)
				| _ -> so_far
			) in
			let zeros = count_zeros 0 in
			pred (o#bs 1 zeros)
		)

	end
;;


(**************)
(* SEND FRAME *)
(**************)
(* Attempts to send a frame over the network... *)
(* I'm going to try to make this format-neutral *)
(* A length from the string is given to support large buffers which contain smaller files *)
(* ~ Networking *)
let send_frame (p : string (*-> string*) -> unit) s display_num is_iframe (str:string) (len:int) =
	(* Send the I-frame info if necessary *)
	let keep_going_i = if is_iframe then (
		let frame_num_string = String.create 4 in
		Pack.packN frame_num_string 0 display_num;
		p "send_frame sending I frame info";
		match s#send "IFRM" frame_num_string with
		| Invalid e -> (p // sprintf "send_frame tried to send I frame info but got %S" (Printexc.to_string e); Invalid e)
		| Valid () -> (
			match s#recv with
			| Valid ("CONT","") -> (p "send_frame keep going"; Valid ())
			| Valid ("STOP","") -> (p "send_frame controller says stop when sending I frame"; Invalid Stop)
			| Valid (a,b) -> (p // sprintf "send_frame send I frame but got tag %S" a; unexpected_packet ~here:"send_frame I frame" (a,b))
			| Invalid e -> (p // sprintf "send_frame sent I frame info but got %S" (Printexc.to_string e); Invalid e)
		)
	) else (
		(* Don't send I frame; just return true by default *)
		Valid ()
	) in
	let frame_num_matroska = Matroska.string_of_uint !|display_num in
	keep_going_i %%
	s#send_buffered_m "GOPF" (Matroska.string_of_id 0x0F43B675) %%
	s#send_buffered_m "GOPF" (Matroska.string_of_size None) %%
	s#send_buffered_m "GOPF" (Matroska.string_of_id 0x67) %%
	s#send_buffered_m "GOPF" (Matroska.string_of_size (Some !|(String.length frame_num_matroska))) %%
	(if is_iframe then (
		s#send_buffered_m "GOPF" "\x81\x00\x00\x80" (* I FRAME *)
	) else (
		s#send_buffered_m "GOPF" "\x81\x00\x00\x00" (* Not I frame *)
	)) %%
	s#send_buffered_sub_m "GOPF" str 0 len %%
	fun () -> Valid ()
;;

(********************)
(* GET PRIVATE DATA *)
(********************)
let get_private p c s state job =
	match (job.job_ntrx_ok, job.job_private) with
	| (true, Some priv_string) -> (
		s#send "NTRX" "" %%
		fun () -> trap_exception Bitstream.get_private (new Bitstream.bg_string priv_string) %%
		fun priv -> Valid (priv,state)
	)
	| _ -> (
		make_tracks p c state job %%
		fun (trax_string, priv_string, new_state) -> (
			s#send "TRAX" trax_string %%
			fun () -> trap_exception Bitstream.get_private (new Bitstream.bg_string priv_string) %%
			fun priv -> Valid (priv,new_state)
		)
	)
;;


(*************)
(*************)
(**** MP4 ****)
(*************)
(*************)

(* Returns the pos of the first NAL in an MP4 file *)
(* ~ IO *)
let rec mp4_find_bitstream_data h =
	(h#read 4) %%
	(fun s1 -> (h#read 4) %% (fun s2 -> Valid (s1,s2))) %%
	(function
		| ("\x00\x00\x00\x00","\x00\x00\x00\x00") | ("mdat",_) -> (
			(* Found the mdat! This function should return "Valid 24L", I believe *)
			h#seek_add 8L
		)
		| ("ftyp",len_str) -> (
			(* Not yet *)
			let len = !|(Pack.unpackN len_str 0) in (* This function can't fail since the string is always 4 bytes *)
			(h#seek_add (len -| 8L)) %% (fun _ -> mp4_find_bitstream_data h) (* I hope this turns out to be tail-recursive... *)
		)
		| _ -> Invalid Not_found
	)
;;

(* Returns Valid (pos,len,frame_slice) if NAL is frame *)
(* If NAL is not a frame, it is skipped over to get to a frame *)
(* Note that the pos and len of the frame are from the BEGINNING of the length, which is usually 4 bytes before the beginning of the NAL *)
(* ~ IO *)
let rec mp4_find_next_frame h priv =
	h#pos_byte %%
	(fun frame_start ->
		(trap_exception h#b (priv.Bitstream.private_nalu_size lsl 3)) %%
		(fun len ->
			let len_plus_size = len + priv.Bitstream.private_nalu_size in
			match trap_exception (Bitstream.get_nal !|len (Some priv.Bitstream.private_sps) (Some priv.Bitstream.private_pps)) h with
			| Valid {Bitstream.nal_unit_type = (Bitstream.Nal_type_idr slice | Bitstream.Nal_type_non_idr slice)} -> (
				(* It's a frame! *)
				Valid (frame_start, len_plus_size, slice)
			)
			| Valid _ -> (
				match h#seek_byte (frame_start +| !|len_plus_size) with
				| Valid () -> mp4_find_next_frame h priv
				| Invalid e -> Invalid e
			)
			| Invalid e -> Invalid e
		)
	)
;;

(* I think I need "s" here *)
let rec mp4_parse_frames p s h priv last_i coding_num more so_far =
	if more <= 0 then (
		p "MP4 parse_frames went off the end";
		Valid so_far
	) else (
		match mp4_find_next_frame h priv with
		| Valid (pos, len, {Bitstream.pic_order_cnt = Bitstream.Slice_header_pic_order_cnt_type_0 poc}) -> (
			p // sprintf "MP4 parse_frames coding order frame %d with POC %d (%d left)\n" so_far poc more;
			let is_i = (poc = 0) in
			let display_num = if is_i then (
				coding_num
			) else (
				last_i + (poc lsr 1)
			) in
			p // sprintf "MP4 parse_frames is display order frame %d" display_num;

			let keep_going = (
				if is_i then (
					p // sprintf "MP4 parse_frames sending I frame info for frame %d" display_num;
					let frame_num_string = String.create 4 in
					Pack.packN frame_num_string 0 display_num;
					s#send "IFRM" frame_num_string;
					match s#recv with
					| Valid ("CONT","") -> Valid ()
					| Valid x -> unexpected_packet ~here:"mp4_parse_frames recv from IFRM" x
					| Invalid x -> Invalid x
				) else (
					Valid ()
				)
			) in

			keep_going %%
			(fun () -> h#seek_byte pos) %%
			(fun () -> h#read len) %%
			(fun frame_guts ->
				let frame_num_matroska = Matroska.string_of_uint !|display_num in

				s#send_buffered   "GOPF" (Matroska.string_of_id 0x0F43B675) %%
				s#send_buffered_m "GOPF" (Matroska.string_of_size None) %%
				s#send_buffered_m "GOPF" (Matroska.string_of_id 0x67) %%
				s#send_buffered_m "GOPF" (Matroska.string_of_size (Some !|(String.length frame_num_matroska))) %%
				s#send_buffered_m "GOPF" frame_num_matroska %%
				s#send_buffered_m "GOPF" (Matroska.string_of_id 0x23) %%
				s#send_buffered_m "GOPF" (Matroska.string_of_size (Some !|(4 + String.length frame_guts))) %%
				(if is_i then
					s#send_buffered_m "GOPF" "\x81\x00\x00\x80" (* Simple block header + I frame (timecode is always 0 since we start a new segment for each frame *)
				else
					s#send_buffered_m "GOPF" "\x81\x00\x00\x00" (* Not an I frame *)
				) %%
				s#send_buffered_m "GOPF" frame_guts
			) %%
			(fun () -> mp4_parse_frames p s h priv (if is_i then coding_num else last_i) (succ coding_num) (pred more) (succ so_far))
		)
		| Valid (pos, len, other) -> (
			(* It's a valid frame, but it doesn't have any POC info! *)
			p // sprintf "frame at location %Ld does not have POC info! FREAK OUT!" pos;
			Invalid Not_found
		)
		| Invalid e -> (
			p // sprintf "no frame found? Exception %S?" (Printexc.to_string e);
			Invalid e
		)
	)
;;


(*
				(h#seek_byte pos) %%
				(fun () -> h#read len) %%
				(fun frame_guts ->
					;
*)


let mp4_parse_file p c s state job temp_name =
	p "MP4 parser starting up";
	get_private p c s state job %%
	fun (priv,state2) -> (
		p "MP4 got privates";
		get_unix_handle p temp_name 60 %%
		fun handle -> (
			p "MP4 opened temp file handle";
			let h = new expandable_handle handle in
			mp4_find_bitstream_data h %%
			fun bitstream_start_pos -> (
				p // sprintf "MP4 found bitstream pos at %Ld" bitstream_start_pos;
				mp4_parse_frames p s h priv 0 0 job.job_frames 0 %%
				(fun frames -> Valid (frames,state2))
			)
		)
	)
;;


(*
let rec mp4_parse_file p s job temp_name win_handle_option =

	p "MP4 starting up parser";

	(* Private string *)
	let priv = match (job.job_ntrx_ok, job.job_private) with
		| (true, Some priv_string) -> (
			s#send "NTRX" "";
			Bitstream.get_private
*)

(********)
(* TEST *)
(********)
(* WARNING: Here there be imperative programming! *)
printf "\n\n";;
let args = Array.to_list Sys.argv;;
let o_perhaps = make_o args;;
match o_perhaps with
| Valid o -> (
	let state = Random.State.make_self_init () in
	let (id,s1) = new_agent_id state in
	printf "ID %S\n" (to_hex id);
	printf "config  %S\n" o.o_config;
	printf "logfile %S\n" (match o.o_logfile with | None -> "<NONE>" | Some x -> x);
	printf "nosched %B\n" o.o_nosched;
	printf "cap     %B\n" o.o_cap;
	let c = (match make_c o with
		| Valid c -> c
		| Invalid s -> (printf "  C INVALID: %S\n" s; default_c)
	) in
	printf "  agent name      %S\n" c.c_agent_name;
	printf "  agent port      %d\n" c.c_agent_port;
	printf "  controller port %d\n" c.c_controller_port;
	printf "  temp dir        %S\n" c.c_temp_dir;
	printf "  encodes         %d\n" c.c_encodes;
	printf "  x264            %S\n" c.c_x264;
	printf "  bases          %s\n" (List.fold_left (fun so_far gnu -> sprintf "%s %S" so_far gnu) "" c.c_bases);
	printf "  buffer frames   %d\n%!" c.c_buffer_frames;
	let s2 = (match mp4_supported state c with
		| Valid (b,s) -> (printf "  MP4 supported?  %B\n" b; s)
		| Invalid e -> (printf "  MP4 supported?  ERROR %S\n" (Printexc.to_string e); state)
	) in
	let (x264_ok, failpipe) = check_x264 c in
	printf "  x264 OK         %B\n" x264_ok;
	printf "  failpipe        %B\n" failpipe;
	let job = {
		job_version_major = 1;
		job_version_minor = 0;
		job_hash = "12345";
		job_name = "d:\\documents\\dvd\\mkv\\amelie\\amelie.avs";
		job_seek = 0;
		job_frames = 1000;
		job_options = "--crf 20";
		job_res_x = 704;
		job_res_y = 352;
		job_fps_n = 24000;
		job_fps_d = 1001;
		job_ntrx_ok = false;
		job_private = None;
	} in
	let i = make_i o in
	let s3 = (match make_tracks i.p c s2 job with
		| Valid (trax,priv,s) -> (
			i.p ~screen:true // sprintf "  TRAX: %S" (to_hex trax);
			i.p ~screen:true // sprintf "  PRIV: %S" (to_hex priv);
			s
		)
		| Invalid e -> (i.p ~screen:true // sprintf "  FAIL %S" (Printexc.to_string e); s2)
	) in
	i.p ~screen:true "HALLOES?";
(*	printf "help    %B\n" o.o_help;*)
)
| Invalid x -> (
	printf "%s\n" x
);;


(* What is the difference between PPS 0x68EE3C80 and 0x68EE06F2 ? *)
(* 13 bits of each PPS are read, getting to the pic_order_present flag *)
let sps = match Bitstream.get_nal 21L None None (new Bitstream.bg_string "\x67\x4D\x40\x33\x9A\x74\x05\x81\x6D\x08\x00\x00\x1F\x48\x00\x05\xDC\x04\x78\xC1\x95") with
	| {Bitstream.nal_unit_type = Bitstream.Nal_type_sps x} -> x
	| _ -> failwith "DETH"
;;
let n1 = new Bitstream.bg_string "\x68\xEE\x3C\x80";;
let pps = match Bitstream.get_nal 4L (Some [|sps|]) None n1 with
	| {Bitstream.nal_unit_type = Bitstream.Nal_type_pps x} -> x
	| _ -> failwith "DETH"
;;

printf "PPS1:\n";;
printf " pos:       %Ld\n" n1#pos;;
printf " id:        %d\n" pps.Bitstream.pic_parameter_set_id;;
printf " ent_code:  %B\n" pps.Bitstream.entropy_coding_mode_flag;;
printf " POP:       %B\n" pps.Bitstream.pic_order_present_flag;;
printf " # slice g: %d\n" pps.Bitstream.num_slice_groups;;

let n2 = new Bitstream.bg_string "\x68\xEE\x06\xF2";;
let pps = match Bitstream.get_nal 4L (Some [|sps|]) None n2 with
	| {Bitstream.nal_unit_type = Bitstream.Nal_type_pps x} -> x
	| _ -> failwith "DETH"
;;

printf "PPS2:\n";;
printf " pos:       %Ld\n" n2#pos;;
printf " id:        %d\n" pps.Bitstream.pic_parameter_set_id;;
printf " ent_code:  %B\n" pps.Bitstream.entropy_coding_mode_flag;;
printf " POP:       %B\n" pps.Bitstream.pic_order_present_flag;;
printf " # slice g: %d\n" pps.Bitstream.num_slice_groups;;

(* Expandable handle *)
let (readme,writeme) = Unix.pipe ();;
let a = new expandable_handle readme;;
printf "Doing stuff\n%!";;
let mt = Thread.create (fun (w,n) ->
	Thread.delay w;
	printf "WRITING %S\n%!" n;
	Unix.write writeme n 0 (String.length n)
);;

ignore // mt (0.2,"1234");;
ignore // mt (0.4,"ABCD");;

(a#read 5) %% (fun n -> printf "GOT %S\n%!" n; Valid ());;

let h = Unix.openfile "out.txt" [Unix.O_RDONLY] 0o600;;
let a = new expandable_handle h;;
a#read 1000000;;


(* MP4 NAL tests *)
let h = new expandable_handle (Unix.openfile "../test/f21.mp4" [Unix.O_RDONLY] 0o600);;
printf "%02X\n%!" (h#b 4);;
printf "%02X\n%!" (h#b 4);;
printf "%02X\n%!" (h#b 4);;
printf "%02X\n%!" (h#b 4);;
printf "%02X\n%!" (h#b 4);;
printf "%02X\n%!" (h#b 4);;
printf "%02X\n%!" (h#b 4);;
printf "%02X\n%!" (h#b 8);;
printf "%02X\n%!" (h#b 4);;
printf "%02X\n%!" (h#b 8);;
printf "%02X\n%!" (h#b 8);;
printf "%02X\n%!" (h#b 8);;
printf "%02X\n%!" (h#b 8);;
ignore // h#seek_byte 397588L;;
let priv = Bitstream.get_private h;;
ignore // h#seek_byte 40L;;

while true do (
	match mp4_find_next_frame h priv with
(*	| Valid (a,b,c) -> printf "Found frame at %Ld of length %d\n%!" a b;*)
	| Valid (a,b,{Bitstream.pic_order_cnt = Bitstream.Slice_header_pic_order_cnt_type_0 poc} as c) -> printf "%9Ld - %9Ld (%d)\n%!" a (a +| !|b) poc
	| Invalid e -> printf "Found %S instead of frame\n%!" (Printexc.to_string e);
) done;;

(*
type pic_parameter_set = {
	pic_parameter_set_id : int;
	seq_parameter_set : seq_parameter_set;
	entropy_coding_mode_flag : bool;
	pic_order_present_flag : bool;
	num_slice_groups : int;
};; (* I think that's all I need *)
*)
