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

(*
ocamlopt.opt -o agent -I ../common -rectypes -thread unix.cmxa threads.cmxa xml-light.cmxa ..\common\opt-c.obj opt.cmx pack.cmx rx.cmx matroska.cmx agent.ml
*)

open Printf;;



(********)
(* TEST *)
(********)
(* Jobs *)
(*
let a = Opt.set_job_information (Obj.magic 0) {Opt.job_no_limit with Opt.job_object_limit_workingset = Some (1,4)} (*{
	Opt.job_object_limit_active_process = Some 2;
	Opt.job_object_limit_affinity = None;
	Opt.job_object_limit_breakaway_ok = false;
	Opt.job_object_limit_die_on_unhandled_exception = false;
	Opt.job_object_limit_job_memory = None;
	Opt.job_object_limit_job_time = Some 3.0;
	Opt.job_object_limit_kill_on_job_close = false;
	Opt.job_object_limit_preserve_job_time = false;
	Opt.job_object_limit_priority_class = None;
	Opt.job_object_limit_process_memory = None;
	Opt.job_object_limit_process_time = None;
	Opt.job_object_limit_scheduling_class = None;
	Opt.job_object_limit_silent_breakaway_ok = false;
	Opt.job_object_limit_workingset = None;
}*);;
printf "%B!\n" a;;

exit 4;;
*)
(*
let pr x = printf "%s\n%!" x;;
match Opt.create_job_object () "Bill the job" with
| None -> pr "create job"
| Some j -> (
(*	match Opt.create_process_flags_win_full "x264.bat" [|
		"--crf"; "26";
		"-o"; "NUL";
		"d:\\documents\\dvd\\mkv\\fma\\48\\farm.avs" |] Opt.Normal [Opt.Create_suspended] with*)
	match Opt.create_process_flags_win_full "perl" [|
		"-e";
		"system 'pause'" |] Opt.Normal [Opt.Create_suspended] with
	| None -> pr "create process"
	| Some ((ph,th),i,o,e) -> (
		Thread.delay 2.0;
		match Opt.assign_process_to_job_object j ph with
		| false -> pr "assign"
		| true -> (
			pr "assign OK";
			ignore (read_line ());
			pr "starting thread";
			match Opt.resume_thread th with
			| false -> pr "resume thread"
			| true -> (
				pr "resume thread OK";
				match Opt.close_thread_handle th with
				| false -> pr "close thread FAILED"
				| true -> (
					ignore (read_line ());
					let dun = Opt.set_job_information j {Opt.job_no_limit with Opt.job_object_limit_affinity = Some 3} in
					printf "Set? %B\n%!" dun;
					ignore (read_line ());
(*					let dun = Opt.set_job_information j {Opt.job_no_limit with Opt.job_object_limit_affinity = Some 12; Opt.job_object_limit_priority_class = Some Opt.Idle} in*)
(*					printf "Set? %B\n%!" dun;*)
					ignore (read_line ());
					match Opt.terminate_job_object j 1 with
					| false -> pr "terminate FAILED"
					| true -> pr "terminate OK"
				)
			)
		)
	)
);;
flush stdout;;
ignore (read_line ());;
exit 57;;
*)
(*
match Opt.create_job_object () "Bob the job" with
| None -> printf "JOB MUFFINS\n";
| Some j -> (
(*	match Opt.create_process_win_full "perl" [| "-e"; "system \"pause\"" |] Opt.Normal with*)
	match Opt.create_process_win_full "x264.bat" [|
		"--crf"; "26";
		"-o"; "NUL";
		"d:\\documents\\dvd\\mkv\\fma\\48\\farm.avs" |] Opt.Normal with
	| None -> printf "MUFFINS\n";
	| Some (p,i,o,e) -> (
		Thread.delay 5.0;
		match Opt.assign_process_to_job_object j p with
		| false -> printf "ASSIGN MUFFINS\n";
		| true -> (
			printf "ASSIGN OK\n%!";
			Thread.delay 5.0;
(*
			match Opt.kill_process_win p with
			| true -> printf "KILT\n";
			| false -> printf "STILL ALIVE\n";
*)
			match Opt.terminate_job_object j 1 with
			| false -> printf "TERMINATE MUFFINS\n";
			| true -> printf "TERMINATE OK\n";
		)
	)
);;
flush stdout;;
Thread.delay 1000.0;;
exit 57;;
*)
(*
(*
match Opt.get_process_affinity Opt.current_process with
	| None -> printf "Got nothing\n"
	| Some (sys,proc) -> printf "%08X\n%08X\n" sys proc
;;
printf "Set affinity worked? %B\n" (Opt.set_process_affinity Opt.current_process 0x000F0001);
match Opt.get_process_affinity Opt.current_process with
	| None -> printf "Got nothing\n"
	| Some (sys,proc) -> printf "%08X\n%08X\n" sys proc
;;
exit 4;;
*)


(
(*	match Opt.create_process_win_full "cmd" [| "/c"; "perl"; "-e"; "while (<>) {print; flush STDOUT}" |] Opt.Normal with*)
	match Opt.create_process_win_full "x264" [| "--crf"; "30"; "--verbose"; "-o"; "NUL"; "d:\\documents\\dvd\\mkv\\amelie\\amelie.avs" |] Opt.Normal with
(*	match Opt.create_process_win_full "perl.exe" [| "-e"; "use FileHandle; binmode STDERR, ':crlf'; print STDERR 'aoeui'; while (<>) {print STDERR}" |] Opt.Normal with*)
	| None -> printf "NUFFIN!\n%!";
	| Some (h,i,o,e) -> (
(*
		let w = Thread.create (fun () ->
			let a = "***\n" in
			while true do
				ignore (Unix.write i a 0 4);
				printf "w%!";
				Thread.delay 1.0;
			done
		) () in
*)
		let r = Thread.create (fun () ->
			while true do
				let q = String.create 4096 in
				let n = (Unix.read e q 0 4096) in
				printf "R %d\n%!" n;
			done
		) () in
		Thread.delay 1000.0;
	)
);;
(*
(
	let (oc,ic,ec) = Unix.open_process_full "perl -e \"while (<>) {print}\"" (Unix.environment ()) in
	let o = Unix.descr_of_in_channel oc in
	let i = Unix.descr_of_out_channel ic in
	let e = Unix.descr_of_in_channel ec in
	let w = Thread.create (fun () ->
		let a = "*\n" in
		while true do
			ignore (Unix.write i a 0 2);
			printf "w%!";
		done
	) () in
	let r = Thread.create (fun () ->
		while true do
			let q = String.copy "x" in
			ignore (Unix.read o q 0 1);
			printf "R%!";
		done
	) () in
	Thread.delay 1000.0;
);;
*)
(*
(
	let (o,i,e) = Opt.open_process_full_new "x264.exe" "x264 --crf 32 --frames 1000 --verbose -o NUL d:\\documents\\dvd\\mkv\\amelie\\amelie.avs" (Unix.environment ()) in
	(*
	let o = Unix.descr_of_in_channel oc in
	let i = Unix.descr_of_out_channel ic in
	let e = Unix.descr_of_in_channel ec in
	*)
	let r = Thread.create (fun () ->
		let s = String.create 1024 in
		while true do
			let read_bytes = Unix.read e s 0 (String.length s) in
			printf "got %d bytes\n%!" read_bytes;
			if read_bytes = 0 then raise End_of_file
		done
	) () in
	Thread.delay 1000.0;
);;
*)


exit 476;;
*)
(*********)
(* !TEST *)
(*********)












(* Stop compaction in order to increase coherency with Opt.string_sanitize_text_mode *)
(*
let default_overhead = (Gc.get ()).Gc.max_overhead;;
Gc.set {(Gc.get ()) with Gc.max_overhead = 1000001};;
*)

let version_string = "x264farm-sp 1.07";;
if Sys.word_size = 32 then (
	printf "%s agent\n%!" version_string
) else (
	printf "%s agent %d-bit\n%!" version_string Sys.word_size
);;

(* These are the transmission version numbers, NOT the program version numbers *)
let current_version_major = 1;;
let current_version_minor = 3;;

let min_version_major = 1;;
let min_version_minor = 0;;

let to_hex s =
	let result = String.create (2 * String.length s) in
	for i = 0 to String.length s - 1 do
		String.blit (Printf.sprintf "%02X" (int_of_char s.[i])) 0 result (2*i) 2;
	done;
	result
;;

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

let ( +|  ) = Int64.add;;
let ( -|  ) = Int64.sub;;
let ( *|  ) = Int64.mul;;
let ( !|  ) = Int64.of_int;;
let ( !-  ) = Int64.to_int;;
let ( >|- ) = Int64.shift_right;;
let ( <|  ) = Int64.shift_left;;
let ( /|  ) = Int64.div;;
let ( ||| ) = Int64.logor;;
let ( &&| ) = Int64.logand;;


(* This will return either:
 * None
 * Some [Rx.String "framenum"; Rx.String "frametype"]
 * depending on whether it matches or not *)
(*
let is_frame_line = Rx.rx [
	Rx.dot_star_hook   false ;
	Rx.constant        false "frame=";
	Rx.dot_star_hook   false ;
	Rx.characters_plus  true [Rx.Range ('0','9')];
	Rx.dot_star_hook   false ;
	Rx.constant        false "Slice:";
	Rx.characters_plus  true [Rx.Char 'I'; Rx.Char 'P'; Rx.Char 'B'; Rx.Char 'i'; Rx.Char 'b'];
];;
*)


type 'a exn_perhaps_t = Normal of 'a | Exception of exn;;
let trap_exception a b = try Normal (a b) with x -> Exception x;;
let trap_exception_2 a b c = try Normal (a b c) with x -> Exception x;;

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
  Printf.sprintf "%s%06x%s" prefix rnd suffix
;;

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



type o = {
	o_config : string;
	o_logfile : string;
	o_affinity : bool;

	o_agent_name : string;
	o_agent_port : int;
	o_controller_port : int;
	o_temp_dir : string;
	o_encodes : int;
	o_x264 : string;
	o_bases : string list;
	o_nice : int;

	o_buffer_frames : int;
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

exception Timeout;;
exception X264_error of int;;

(* Use the following for printing:
 * i.p // sprintf "FORMATTING" a b c
 * This function just removes the need for extra parentheses, like this:
 * i.p (sprintf "FORMATTING" a b c)
 * Also useful for ignoring functions:
 * ignore // f a b c d
 * rather than:
 * ignore (f a b c d) *)
let (//) a b = a b;;

(* Make sure a command executes properly *)
let test_command =
	let env = Unix.environment () in
	let str_len = 16 in
	let dump_str = String.create str_len in
	let rec dump c = if input c dump_str 0 str_len > 0 then dump c in
	fun s -> (
		let (a,b,c) = Unix.open_process_full s env in
		dump a;
		dump c;
		Unix.close_process_full (a,b,c)
	)
;;

(* Instead of just checking the validity of the command, this will return the command's STDOUT if it exited properly *)
let test_command_and_return =
	let env = Unix.environment () in
	let read_length = 64 in
	fun s -> (
		let write_string = String.create read_length in
		let buf = Buffer.create read_length in
		let rec dump c = if input c write_string 0 read_length > 0 then dump c in
		let rec keep_reading c = (
			let num_read = input c write_string 0 read_length in
			if num_read > 0 then (
				Buffer.add_substring buf write_string 0 num_read;
				keep_reading c
			) else (
				()
			)
		) in
		let (a,b,c) = Unix.open_process_full s env in
		dump c;
		keep_reading a;
		match Unix.close_process_full (a,b,c) with
		| Unix.WEXITED 0 -> Some (Buffer.contents buf)
		| _ -> None
	)
;;

(* Removes the endline(s) from a string *)
(* Not particularly efficient, but it's easy to write *)
let rec chop s =
	let l = String.length s in
	if l > 0 && (s.[l - 1] = '\x0D' || s.[l - 1] = '\x0A') then (
		chop (String.sub s 0 (l - 1))
	) else (
		s
	)
;;

(* Somewhat arbitrary mapping of Unix niceness to Windows process priorities *)
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

(* To prevent Unixy systems from crashing when x264 dies *)
ignore // trap_exception_2 Sys.set_signal Sys.sigpipe Sys.Signal_ignore;;


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
(*
let rec really_send d s off len = (
	if len <= 0 then () else (
		let r = Unix.send d s off len [] in
		if r = 0 then (
			raise End_of_file
		) else (
			really_send d s (off + r) (len - r)
		)
	)
);;
*)

let rec really_read d s off len =
	if len <= 0 then () else (
		let r = Unix.read d s off len in
		if r = 0 then (
			raise End_of_file
		) else (
			really_read d s (off + r) (len - r)
		)
	)
;;










class net ?print sock =
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

		method getsockname = Unix.getsockname sock
		method getpeername = Unix.getpeername sock
		method close = Unix.close sock

		method really_recv str from len = (
			let rec recv_helper from len = (
				(* NORMAL *)
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

		method send t s = (
			let len = String.length s in
			let l = String.create 4 in
			Pack.packN l 0 len;
			o#print // sprintf "Sending tag %S" t;
			o#print // sprintf "Sending string %S" s;
			o#really_send t 0 4;
			o#really_send l 0 4;
			o#really_send s 0 len;
		)

		method recv = (
			let t = String.create 4 in
			let l = String.create 4 in
			o#really_recv t 0 4;
			o#print // sprintf "Got tag %S" t;
			o#really_recv l 0 4;
			o#print // sprintf "Got length %s" (to_hex l);
			if l = "\x00\x00\x00\x00" then (
				(t,"")
			) else (
				let len = Pack.unpackN l 0 in
				let s = String.create len in
				o#print // sprintf "getting %d bytes" len;
				o#really_recv s 0 len;
				(t,s)
			)
		)
	end
;;












(*class buffered_send ?print ?(len=4088) ?str s tag =*)
class buffered_send ?print ?(len=16384) ?str s tag =
	let (buffer,max_len) = match str with
		| Some s when String.length s > 16 -> (s, String.length s - 8)
		| _ -> (String.create (len + 8), len)
	in
	object (o)
		method private print = (match print with
			| None -> ignore
			| Some f -> f
		)

		val buffer = buffer
		val max_len = max_len
		val mutable pos = 0

		method private send = (
			o#print "sending all";
			s#really_send buffer 0 (max_len + 8);
			pos <- 0
		)

		method sub s f l = (
			o#print // sprintf "Sub sending %d+%d, pos=%d/%d" f l pos max_len;
			let num_bytes = min l (max_len - pos) in
			String.blit s f buffer (pos + 8) num_bytes;
			pos <- pos + num_bytes;
			if pos = max_len then (
				o#send;
				o#sub s (f + num_bytes) (l - num_bytes)
			)
		)
		method string s = o#sub s 0 (String.length s)

		method flush = (
			if pos > 0 then (
				let l = String.sub buffer 4 4 in
				Pack.packN buffer 4 pos;
				s#really_send buffer 0 (pos + 8);
				pos <- 0;
				String.blit l 0 buffer 4 4
			) (* Don't do anything if there is nothing to send *)
		)

		val mutable keep_going = true

		method sub_req t f l = (
			if keep_going then (
(*				o#print // sprintf "Sub_req sending %d+%d, pos=%d/%d" f l pos max_len;*)
				let num_bytes = min l (max_len - pos) in
				String.blit t f buffer (pos + 8) num_bytes;
				pos <- pos + num_bytes;
				if pos = max_len then (
					o#send;
					match s#recv with
					| ("CONT","") -> o#sub_req t (f + num_bytes) (l - num_bytes)
					| ("STOP","") -> (keep_going <- false; false)
					| x -> unexpected_packet ~here:"buffered_send#sub_req" x
				) else (
					true
				)
			) else (
				false
			)
		)

		method string_req s = o#sub_req s 0 (String.length s)

		method flush_req = (
			if keep_going then (
				if pos > 0 then (
					let l = String.sub buffer 4 4 in
					Pack.packN buffer 4 pos;
					s#really_send buffer 0 (pos + 8);
					pos <- 0;
					String.blit l 0 buffer 4 4;
					match s#recv with
					| ("CONT","") -> true
					| ("STOP","") -> (keep_going <- false; false)
					| x -> unexpected_packet ~here:"buffered_send#flush_req" x
				) else (
					true
				)
			) else (
				false
			)
		)

		method req = keep_going

		method length = pos

		initializer (
			String.blit tag 0 buffer 0 4;
			Pack.packN buffer 4 max_len;
		)
	end
;;




let async ?print num len f_write =
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
					p // sprintf "ASYNC oq elements: %d" (Queue.length oq);
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

type async3_t = {
	asb : string;
	mutable asn : int;
};;
let async3 buffer_size f_write f_read =
	let b1 = {
		asb = String.create buffer_size;
		asn = 1;
	} in
	let b2 = {
		asb = String.create buffer_size;
		asn = 1;
	} in
	let b3 = {
		asb = String.create buffer_size;
		asn = 1;
	} in
	let m1 = Mutex.create () in
	let m2 = Mutex.create () in
	let m3 = Mutex.create () in

	let channel = Event.new_channel () in

	ignore (Thread.create (fun () ->
		try
			let rec write1 () = (
				if b1.asn > 0 then (
					let wrote = f_write b1.asb buffer_size in
					b1.asn <- wrote;
					if wrote > 0 then (
						Mutex.lock m2;
						Mutex.unlock m1;
						write2 ()
					) else (
						Mutex.unlock m1
					)
				) else (
					Mutex.unlock m1
				)
			)
			and write2 () = (
				if b2.asn > 0 then (
					let wrote = f_write b2.asb buffer_size in
					b2.asn <- wrote;
					if wrote > 0 then (
						Mutex.lock m3;
						Mutex.unlock m2;
						write3 ()
					) else (
						Mutex.unlock m2
					)
				) else (
					Mutex.unlock m2
				)
			)
			and write3 () = (
				if b3.asn > 0 then (
					let wrote = f_write b3.asb buffer_size in
					b3.asn <- wrote;
					if wrote > 0 then (
						Mutex.lock m1;
						Mutex.unlock m3;
						write1 ()
					) else (
						Mutex.unlock m3
					)
				) else (
					Mutex.unlock m3
				)
			) in

			(* The first writer has to be different... *)
			Mutex.lock m1;
			Event.sync (Event.send channel ());
			write1 ()
		with
			_ -> ()
	) ());

	let rec read1 () = (
		if b1.asn > 0 then (
			let keep_going = f_read b1.asb b1.asn in
			if keep_going then (
				Mutex.lock m2;
				Mutex.unlock m1;
				read2 ()
			) else (
				b1.asn <- 0;
				Mutex.unlock m1
			)
		) else (
			Mutex.unlock m1
		)
	)
	and read2 () = (
		if b2.asn > 0 then (
			let keep_going = f_read b2.asb b2.asn in
			if keep_going then (
				Mutex.lock m3;
				Mutex.unlock m2;
				read3 ()
			) else (
				b2.asn <- 0;
				Mutex.unlock m2
			)
		) else (
			Mutex.unlock m2
		)
	)
	and read3 () = (
		if b3.asn > 0 then (
			let keep_going = f_read b3.asb b3.asn in
			if keep_going then (
				Mutex.lock m1;
				Mutex.unlock m3;
				read1 ()
			) else (
				b3.asn <- 0;
				Mutex.unlock m3
			)
		) else (
			Mutex.unlock m3
		)
	) in

	(* Wait until the writer has locked the first mutex *)
	Event.sync (Event.receive channel);
	Mutex.lock m1;
	read1 ()
;;


(*
let async4 buffer_size f_write f_read =
	let b1 = {
		asb = String.create buffer_size;
		asn = 1;
	} in
	let b2 = {
		asb = String.create buffer_size;
		asn = 1;
	} in
	let b3 = {
		asb = String.create buffer_size;
		asn = 1;
	} in
	let m1 = Mutex.create () in
	let m2 = Mutex.create () in
	let m3 = Mutex.create () in

	let channel = Event.new_channel () in

	ignore (Thread.create (fun () ->
		try
			let rec write1 () = (
				if b1.asn > 0 then (
					let wrote = f_write b1.asb buffer_size in
					b1.asn <- wrote;
					if wrote > 0 then (
						Mutex.lock m2;
						Mutex.unlock m1;
						write2 ()
					) else (
						Mutex.unlock m1
					)
				) else (
					Mutex.unlock m1
				)
			)
			and write2 () = (
				if b2.asn > 0 then (
					let wrote = f_write b2.asb buffer_size in
					b2.asn <- wrote;
					if wrote > 0 then (
						Mutex.lock m3;
						Mutex.unlock m2;
						write3 ()
					) else (
						Mutex.unlock m2
					)
				) else (
					Mutex.unlock m2
				)
			)
			and write3 () = (
				if b3.asn > 0 then (
					let wrote = f_write b3.asb buffer_size in
					b3.asn <- wrote;
					if wrote > 0 then (
						Mutex.lock m1;
						Mutex.unlock m3;
						write1 ()
					) else (
						Mutex.unlock m3
					)
				) else (
					Mutex.unlock m3
				)
			) in

			(* The first writer has to be different... *)
			Mutex.lock m1;
			Event.sync (Event.send channel ());
			write1 ()
		with
			_ -> ()
	) ());

	let rec read1 () = (
		if b1.asn > 0 then (
			let keep_going = f_read b1.asb b1.asn in
			if keep_going then (
				Mutex.lock m2;
				Mutex.unlock m1;
				read2 ()
			) else (
				b1.asn <- 0;
				Mutex.unlock m1
			)
		) else (
			Mutex.unlock m1
		)
	)
	and read2 () = (
		if b2.asn > 0 then (
			let keep_going = f_read b2.asb b2.asn in
			if keep_going then (
				Mutex.lock m3;
				Mutex.unlock m2;
				read3 ()
			) else (
				b2.asn <- 0;
				Mutex.unlock m2
			)
		) else (
			Mutex.unlock m2
		)
	)
	and read3 () = (
		if b3.asn > 0 then (
			let keep_going = f_read b3.asb b3.asn in
			if keep_going then (
				Mutex.lock m1;
				Mutex.unlock m3;
				read1 ()
			) else (
				b3.asn <- 0;
				Mutex.unlock m3
			)
		) else (
			Mutex.unlock m3
		)
	) in

	(* Wait until the writer has locked the first mutex *)
	Event.sync (Event.receive channel);
	Mutex.lock m1;
	read1 ()
;;
*)













type mkv_status_t =
	| MKV_keep_going
	| MKV_new_frame
	| MKV_new_ct of int64
	| MKV_new_tcs of int64
	| MKV_new_nbp of int64
	| MKV_duration_location of int64
	| MKV_EOF_in
	| MKV_EOF_out
	| MKV_stop
;;

let seek_add h plus = (
	let a = LargeFile.pos_in h in
	LargeFile.seek_in h (a +| plus)
);;
let i64_div_round a b = (
	let a2 = a +| (b >|- 1) in
	a2 /| b
);;

let read_xiph_from_string s n =
	let rec helper so_far now = match s.[now] with
		| '\xFF' -> helper (so_far + 255) (now + 1)
		| x -> (so_far + Char.code x, now - n + 1)
	in
	helper 0 n
;;



class bigbuffer_bitstream p bigbuf =
	object(o)
		val bb = bigbuf
		val mutable bit = 8
		val mutable byte = -1
		val mutable data = 0

		method bs s n = (
			if n = 0 then (
				s
			) else if bit >= 8 then (
				(* Read more *)
				bit <- bit - 8;
				byte <- byte + 1;
				data <- Char.code (Bigbuffer.nth bb byte);
				o#bs s n
			) else if bit = 0 && n >= 8 then (
				(* Read a whole byte *)
				let now = (s lsl 8) lor data in
				bit <- 8;
				o#bs now (n - 8)
			) else (
				(* Read a bit *)
				let now = (s lsl 1) lor ((data lsr (7 - bit)) land 1) in
				bit <- succ bit;
				o#bs now (pred n)
			)
		)

		method b n = (
(*			p // sprintf "BIGBUFFER reading %d bits from %d:%d (length %d)" n byte bit (Bigbuffer.length bb);*)
			let q = o#bs 0 n in
(*			p // sprintf "BIGBUFFER now at %d:%d" byte bit;*)
			q
		)

		method g = (
			let rec count_zeros so_far = (
				match o#bs 0 1 with
				| 0 -> count_zeros (succ so_far)
				| _ -> so_far
			) in
			let zeros = count_zeros 0 in
			pred (o#bs 1 zeros)
		)

		method pos = (
			!|((byte lsl 3) + bit)
		)

		method seek t64 = (
			let t = !-t64 in
			let seek_byte = t lsr 3 in
			let seek_bit = t land 7 in
			if seek_bit = 0 then (
				(* Force a read next time *)
				byte <- seek_byte - 1;
				bit <- 8;
			) else (
				bit <- seek_bit;
				byte <- seek_byte;
				data <- Char.code (Bigbuffer.nth bb byte);
			)
		)

		method pos_byte = (
			!|(byte + (bit lsr 3))
		)

		method seek_byte t64 = (
			let t = !-t64 in
			bit <- 8;
			byte <- pred t;
		)

		method reset = (
			bit <- 8;
			byte <- -1;
		)
	end
;;



(************************)
(* READ COMMAND OPTIONS *)
(************************)
let o = (
	let config_ref = ref "config.xml" in
	let logfile_ref = ref "out-dump.txt" in
	let affinity_ref = ref true in
	let arg_parse = Arg.align [
		("--config", Arg.Set_string config_ref, sprintf "%S XML configuration file" !config_ref);
		("--logfile", Arg.Set_string logfile_ref, sprintf "%S Verbose log file" !logfile_ref);
		("--nosched", Arg.Clear affinity_ref, " Don't allocate processors for x264");
		("--cap", Arg.String (fun s ->
			match trap_exception (Unix.openfile s [Unix.O_WRONLY]) 0o600 with
			| Normal a -> (
				let s = "\x00\x00\x00\x01\x0B" in (* End of stream NAL (it doesn't matter what it is, since only the 0001 is needed to tell the reader where the end of the previous NAL is) *)
				ignore // Unix.LargeFile.lseek a 0L Unix.SEEK_END;
				ignore // Unix.write a s 0 (String.length s);
				Unix.close a;
				exit 0
			)
			| _ -> (exit (-1))
		), "\"\" Internal use only");
	] in
	Arg.parse arg_parse (fun x -> config_ref := x) "USAGE:";

	if not (is_file !config_ref) then (
		printf "Config %S is not a file\n" !config_ref
	);

	(*************************)
	(* PARSE THE CONFIG FILE *)
	(*************************)
	let parse_obj = (try
		Some (Xml.parse_file !config_ref)
	with
		_ -> None
	) in
	
	let encodes_ref = ref (
		let string_opt_is_int = function
			| None -> None
			| Some s -> (
				match trap_exception int_of_string (chop s) with
				| Normal n -> Some n
				| Exception _ -> None
			)
		in
		let int_opt = Opt.num_processors () in
		if int_opt = 0 then (
			(* First try a Mac-like sysctl (does NOT count SMT) *)
			let string_phys = test_command_and_return "sysctl -n hw.physicalcpu" in
			match string_opt_is_int string_phys with
			| Some n -> ((*printf "Got %d from sysctl hw.physicalcpu\n" n;*) n)
			| None -> (
				(* Now do a BSD-style one (probably does count SMT) *)
				let string_ncpu = test_command_and_return "sysctl -n hw.ncpu" in
				match string_opt_is_int string_ncpu with
				| Some n -> ((*printf "Got %d from sysctl hw.ncpu\n" n;*) n)
				| None -> (
					(* Linux? Appears to not count SMT *)
					let string_proc = test_command_and_return "cat /proc/cpuinfo | grep -c processor" in
					match string_opt_is_int string_proc with
					| Some n -> ((*printf "Got %d from proc/cpuinfo\n" n;*) n)
					| None -> (
						(* Maybe it's Windows and we missed it in Opt.num_processors (does count SMT) *)
						match trap_exception (fun x -> int_of_string (Sys.getenv x)) "NUMBER_OF_PROCESSORS" with
						| Normal n -> ((*printf "Got %d from %%NUMBER_OF_PROCESSORS%%\n" n;*) n)
						| Exception _ -> (
(*							printf "Didn't find anything!\n";*)
							(* Oh well; default to 1 encode *)
							1
						)
					)
				)
			)
		) else (
(*			printf "Got %d from Opt.num_processors\n" int_opt;*)
			int_opt
		)
		) in (* Change to be based off the number of processors *)
	let agent_name_ref = ref "" in
	let controller_find_port_ref = ref 40704 in
	let agent_find_port_ref = ref 40705 in
	let temp_dir_ref = ref (Filename.concat Filename.temp_dir_name "x264farm-sp") in
	let x264_ref = ref "x264" in
	let bases_ref = ref [""] in
	let nice_ref = ref 10 in
	let buffer_frames_ref = ref 16 in
	let parse_config_elements = function
		| Xml.Element ("port", [("controller",x);("agent",y)], _) | Xml.Element ("port", [("agent",y);("controller",x)], _) -> (controller_find_port_ref := int_of_string x; agent_find_port_ref := int_of_string y)
		| Xml.Element ("encodes", _, [Xml.PCData q]) -> encodes_ref := int_of_string q
		| Xml.Element ("temp", _, [Xml.PCData q]) -> (
			if is_file q then (
				printf "Temp dir %S in %S is actually a file; ignoring\n" q !config_ref
			) else (
				if not (create_dir q) then (
					printf "Unable to create temp dir %S; ignoring\n" q
				) else (
					temp_dir_ref := q
				)
			)
		)
		| Xml.Element ("x264",_,[Xml.PCData x]) -> x264_ref := x
		| Xml.Element ("base",_,[Xml.PCData x]) -> (
			if is_dir x then (
				bases_ref := x :: []
			) else (
				printf "WARNING: cannot find base directory \"%s\"; ignoring\n" x
			)
		)
		| Xml.Element ("bases", _, base_list) -> (
			let rec iterate = function
				| [] -> []
				| (Xml.Element ("base", _, [])) :: tl -> "" :: iterate tl
				| (Xml.Element ("base", _, [Xml.PCData x])) :: tl when is_dir x -> x :: iterate tl
				| (Xml.Element ("base", _, [Xml.PCData x])) :: tl -> (printf "WARNING: cannot find base directory \"%s\"; ignoring\n" x; iterate tl)
				| hd :: tl -> iterate tl
			in
			bases_ref := iterate base_list
		)
		| Xml.Element ("nice", _, [Xml.PCData x]) -> (let n = int_of_string x in nice_ref := min 19 (max 0 n))
		| Xml.Element ("buffer-frames", _, [Xml.PCData x]) -> (buffer_frames_ref := int_of_string x)
		| Xml.Element (e,_,_) -> printf "Don't understand what element <%s> in is %S... ignoring\n" e !config_ref
		| _ -> ()
	in
	(match parse_obj with
		| Some (Xml.Element ("agent-config", [("name",n)], elts)) -> (agent_name_ref := n; List.iter parse_config_elements elts)
		| Some (Xml.Element ("agent-config", _, elts)) -> List.iter parse_config_elements elts
		| Some (Xml.Element (q, _, _)) -> failwith (sprintf "Config file must have root element <agent-config>, not <%s>" q)
		| Some (Xml.PCData _) -> failwith "Config file must have root element <agent-config>, not regular data"
		| None -> printf "Config file does not seem to be XML\n"
	);

	ignore // create_dir !temp_dir_ref;

	{
		o_config = !config_ref;
		o_logfile = !logfile_ref;
		o_affinity = !affinity_ref;

		o_agent_name = !agent_name_ref;
		o_agent_port = !agent_find_port_ref;
		o_controller_port = !controller_find_port_ref;
		o_temp_dir = !temp_dir_ref;
		o_encodes = !encodes_ref;
		o_x264 = !x264_ref;
		o_bases = !bases_ref;
		o_nice = !nice_ref;

		o_buffer_frames = !buffer_frames_ref;
	}
);;
(**************************)
(* END OF COMMAND OPTIONS *)
(**************************)







let i =
	let pm = Mutex.create () in
	let print = (
		let time_string () = (
			let tod = Unix.gettimeofday () in
			let cs = int_of_float (100.0 *. (tod -. floor tod)) in (* centi-seconds *)
			let lt = Unix.localtime tod in
			sprintf "%04d-%02d-%02d~%02d:%02d:%02d.%02d" (lt.Unix.tm_year + 1900) (lt.Unix.tm_mon + 1) (lt.Unix.tm_mday) (lt.Unix.tm_hour) (lt.Unix.tm_min) (lt.Unix.tm_sec) cs
		) in
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

	{
		p = print;
		pm = pm;
	}
;;





(************)
(* AGENT ID *)
(************)
let agent_id = String.create 16;;
(match (trap_exception Unix.stat "/dev/urandom", trap_exception Unix.stat "/dev/random") with
	| (Normal {Unix.st_kind = Unix.S_CHR}, _) -> (
		(* Try urandom first, because it should be faster than random *)
		let h = open_in_bin "/dev/urandom" in
		really_input h agent_id 0 16;
		i.p "Got 16 bytes from /dev/urandom";
		close_in h;
	)
	| (_, Normal {Unix.st_kind = Unix.S_CHR}) -> (
		(* BSD doesn't use urandom, so fall back to random *)
		let h = open_in_bin "/dev/random" in
		really_input h agent_id 0 16;
		i.p "Got 16 bytes from /dev/random";
		close_in h;
	)
	| _ -> (
		(* Something else, like *GASP* Windows *)
		(* Just use self_init... it should be random enough *)
		i.p "Probably Windows... making up 16 random bytes";
		Random.self_init ();
		for i = 0 to 15 do
			agent_id.[i] <- Char.chr (Random.int 256)
		done;
	)
);;




(* Figure out if the x264 on this platform needs some help with piping binary data *)
(* Some builds of x264 on Windows uses text-mode for stdin, which closes the pipe when a 0x1A character is received *)
(* This code checks for that problem (and also checks to make sure x264 exists) *)
let (x264_ok, stupid_pipe) = if Sys.os_type = "Win32" then (
	let do_this = sprintf "%s --quiet --qp 0 -o NUL - 2x2 2>&1" o.o_x264 in
	i.p do_this;
	let (pipein, pipeout) = Unix.open_process do_this in
	let wrote_ok = try
		output_string pipeout "\x1A\x1A\x1A\x1A\x1A\x1A";
		close_out pipeout;
		true
	with
		_ -> false
	in
	let get = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" in
	let got_bytes = input pipein get 0 64 in
	i.p // sprintf "got %d bytes - %S" got_bytes (String.sub get 0 got_bytes);
	let exited_ok = match trap_exception Unix.close_process (pipein,pipeout) with
		| Normal (Unix.WEXITED 0) -> true
		| _ -> false
	in
	if wrote_ok && exited_ok && got_bytes > 2 then (
		(* Everything worked *)
		(true, false)
	) else if wrote_ok && exited_ok then (
		(* x264farm exists, but pipes stupidly *)
		(true, true)
	) else (
		(* Writing didn't happen *)
		(false, false)
	)
) else (
	(* Unix doesn't have this problem, but check x264 anyway *)
	let do_this = sprintf "%s --version 2>&1" o.o_x264 in
	let pipein = Unix.open_process_in do_this in
	let exited_ok = match trap_exception Unix.close_process_in pipein with
		| Normal (Unix.WEXITED 0) -> true
		| _ -> false
	in
	(exited_ok, false)
);;

if not x264_ok then (
	printf "\nERROR: There seems to be a problem with the encoder.\nMake sure \"%s\" exists or change the encoder name in the config file\n" o.o_x264;
	exit 1
);;

(*************************)
(* FIND OUT IF MP4 IS OK *)
(*************************)
let mp4_supported = (
	let (out_mp4,closeme) = open_temp_file (Filename.concat o.o_temp_dir "mp4 test ") ".mp4" in
	close_out closeme;
	let do_this = sprintf "%s --quiet --crf 17 -o \"%s\" - 2x2 2>%s >%s" o.o_x264 out_mp4 dev_null dev_null in
	i.p do_this;
	let proc = Unix.open_process_out do_this in
	let ret = (match Unix.close_process_out proc with
		| Unix.WEXITED 0 -> true
		| _ -> false
	) in
	ignore // Sys.remove out_mp4;
	ret
);;
(*
let mp4_supported = false;;
printf ">>> MP4 TURNED OFF! <<<\n%!";;
*)
i.p // sprintf "MP4 supported? %B" mp4_supported;;


(* List the config options *)
i.p ~screen:true // sprintf "Agent ID: %s" (to_hex agent_id);;
i.p ~screen:true // sprintf "  Config file:  %s" o.o_config;;
i.p ~screen:true // sprintf "  Log file:     %s" o.o_logfile;;
i.p ~screen:true // sprintf "  Agent name:   %s" o.o_agent_name;;
i.p ~screen:true // sprintf "  Agent port:   %d" o.o_agent_port;;
i.p ~screen:true // sprintf "  Control port: %d" o.o_controller_port;;
i.p ~screen:true // sprintf "  Temp dir:     %s" o.o_temp_dir;;
i.p ~screen:true // sprintf "  Encodes:      %d" o.o_encodes;;
i.p ~screen:true // sprintf "  x264 name:    %s" o.o_x264;;
i.p ~screen:true // sprintf "  Niceness:     %d" o.o_nice;;
match o.o_bases with
	| [] -> i.p ~screen:true "  AGENT-BASED ENCODING TURNED OFF";
	| b -> (
		i.p ~screen:true // sprintf "  Bases:";
		List.iter (fun str ->
			i.p ~screen:true // sprintf "    \"%s\"" str
		) b
	)
;;
if stupid_pipe then (
	i.p ~screen:true "NOTE: The chosen x264 executable is slow with controller-based encoding"
);;
(* d:\Programs\Multimedia\x264\620-st.exe --quiet --qp 0 -o NUL - 2x2 < 6x1a.txt 2> out.txt *)

(********)
(* PING *)
(********)
let send_ping port = ( (* This port is the AGENT'S bound port *)
	let str1 = "SAGE\x00\x00\x00" ^ agent_id ^ o.o_agent_name in
	Pack.packn str1 4 port;
	Pack.packC str1 6 o.o_encodes;
	let str = str1 ^ Digest.string str1 in
	let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
	Unix.setsockopt sock Unix.SO_BROADCAST true;
	fun to_addr -> (
		let sent = Unix.sendto sock str 0 (String.length str) [] to_addr in
		if sent <> String.length str then (
			i.p // sprintf "Oops. Ping only sent %d / %d bytes. I wonder why..." sent (String.length str)
		);
	)
);;


(*******************)
(* LISTENER THREAD *)
(*******************)
let listen_guts agent_connected_port =
	let recv_sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
	Unix.bind recv_sock (Unix.ADDR_INET (Unix.inet_addr_any, o.o_agent_port));

	i.p "Listener set up";

	let send_ping_to = send_ping agent_connected_port in

	let recv_string = String.create 640 in (* 640 should be enough for anybody *)
	while true do
		try
			let (got_bytes, from_addr) = Unix.recvfrom recv_sock recv_string 0 (String.length recv_string) [] in
			match from_addr with
			| Unix.ADDR_INET (from_ip, from_port) when got_bytes >= 22 && Digest.substring recv_string 0 (got_bytes - 16) = String.sub recv_string (got_bytes - 16) 16 -> (
				let controller_name = if got_bytes > 22 then String.sub recv_string 6 (got_bytes - 22) else (sprintf "%s:%d" (Unix.string_of_inet_addr from_ip) from_port) in
				i.p // sprintf "Received ping from %S" controller_name;
				send_ping_to (Unix.ADDR_INET (from_ip, Pack.unpackn recv_string 4));
			)
			| _ -> (i.p "Listener threw out improper ping")
		with
			e -> (
				i.p // sprintf "Listener failed: %S" (Printexc.to_string e);
				Thread.delay 10.0
			)
	done
;;

(********************)
(* AGENT-BASED BASE *)
(********************)
let split_last_dir x = (Filename.dirname x, Filename.basename x);;
let rec split_on_dirs_rev x = (
	let (dir, base) = split_last_dir x in
	if dir = x then (
		if base = "." then (
			[dir]
		) else (
			base :: [dir]
		)
	) else if dir = "\\" && String.length x >= 2 && String.sub x 0 2 = "\\\\" then (
		(* Fixes network paths *)
		if base = "." then (
			["\\\\"]
		) else (
			base :: ["\\\\"]
		)
	) else if base = "." then (
		split_on_dirs_rev dir
	) else (
		base :: (split_on_dirs_rev dir)
	)
);;
let split_on_dirs x = List.rev (split_on_dirs_rev x);;

let find_file_with_md5 base file md5 =
	let on_dirs = split_on_dirs file in
	let rec check_for_file current_list = (
		let dir_now = List.fold_left (fun so_far gnu -> if so_far = "" then gnu else Filename.concat so_far gnu) "" current_list in
		let full_file = (if base = "" then dir_now else Filename.concat base dir_now) in (* The if statement here lets you use <base/> to mean exactly where the controller is using it *)
(*		i.p "Testing file \"%s\": " full_file;*)
		if is_file full_file then (
			i.p // sprintf "%S %S" (Digest.file full_file) md5;
			if Digest.file full_file = md5 then (
				i.p // sprintf "Testing file \"%s\": FOUND!" full_file;
				Some full_file
			) else (
				if base = "" then (
					i.p // sprintf "Testing file \"%s\": MD5 does not match up (no base)" full_file;
					None
				) else (
					i.p // sprintf "Testing file \"%s\": MD5 does not match up" full_file;
					match current_list with
					| hd :: tl -> check_for_file tl
					| [] -> (None)
				)
			)
		) else (
			if base = "" then (
				i.p // sprintf "Testing file \"%s\": not a file (no base)" full_file;
				None
			) else (
				i.p // sprintf "Testing file \"%s\": not a file" full_file;
				match current_list with
				| hd :: tl -> check_for_file tl
				| [] -> (None)
			)
		)
	) in
	check_for_file on_dirs
;;
let find_base name md5 =
	let rec iterate = function
		| [] -> None
		| base :: tl -> (
			match find_file_with_md5 base name md5 with
			| None -> iterate tl
			| Some x -> Some x
		)
	in
	iterate o.o_bases
;;



















(*****************)
(* MKV READ GUTS *)
(*****************)
let mkv_read_guts p s job temp_name bs win_handle_option = (

	p "starting up MKV reader";

	let h_unix = (
		let rec helper tries = (
			let stuff = (try
(*				Some (open_in_bin temp_name)*)
				if (Unix.LargeFile.stat temp_name).Unix.LargeFile.st_size > 0L then (
					Some (Unix.openfile temp_name [Unix.O_RDWR] 0o600)
				) else (
					p "temp file has nothing in it yet; waiting...";
					None
				)
			with
				_	-> None
			) in
			match stuff with
			| Some x -> x
			| None when tries = 0 -> ( (* Give up *)
				p "giving up trying to find temp file";
(*
				(match win_handle_option with
					| None -> ()
					| Some wh -> ignore // Opt.kill_process_win wh
				);
*)
				raise Not_found
			)
			| None -> (Thread.delay 1.0; helper (pred tries))
		) in
		helper 60
	) in
	let is_sparse = Opt.set_sparse h_unix in
	p // sprintf "temp file sparse? %B" is_sparse;
	let set_zero_data = (if is_sparse then Opt.set_zero_data h_unix else fun a b -> false) in
	let h = Unix.in_channel_of_descr h_unix in
	let is_exception = (try
		let rec reader ct tcs nbp duration_location next_frame = (
			let before_pos = LargeFile.pos_in h in
			let before_len = LargeFile.in_channel_length h in
			let status = try
				match Matroska.input_id_and_length h with
				| Some (0x08538067, _) | Some (0x0549A966, _) | Some (0x0F43B675, _) -> (
					(* SEGMENT             INFO                   CLUSTER *)
					MKV_keep_going
				)
				| Some (0x0654AE6B, Some l) -> (
					(* TRACKS *)
					(* Just give the whole thing to the controller *)
					let q = String.create !-l in
					really_input h q 0 (String.length q);
					s#send "TRAX" q;
					MKV_keep_going
				)
				| Some (0x0AD7B1, Some l) when l <= 8L -> (
					(* TIMECODE SCALE *)
					let new_tcs = Matroska.input_uint 0L h !-l in
					MKV_new_tcs new_tcs
				)
				| Some (0x0489, Some l) when l <= 8L -> (
					let current_pos = LargeFile.pos_in h in
					p // sprintf "duration location is at %Ld" current_pos;
					seek_add h l;
					MKV_duration_location current_pos
				)
				| Some (0x67, Some l) when l <= 8L -> (
					(* CLUSTER TIMECODE *)

					(* Let's use this element to set the file sparse *)
					if before_pos >= 2097152L then (
						let floored = before_pos &&| 0xFFFFFFFFFFF00000L in (* Floors to nearest MB *)
						let zeroed = set_zero_data (floored -| 0x100000L) floored in
						if zeroed then (
							p // sprintf "zeroed from %Ld to %Ld (pos is %Ld)" (floored -| 0x100000L) floored before_pos
						) else (
							p // sprintf "failed to zero data (pos:%Ld, len:%Ld)" before_pos before_len
						)
					);

					let new_ct = Matroska.input_uint 0L h !-l in
					MKV_new_ct new_ct
				)
				| Some (0x20, Some l) -> (
					(* BLOCK GROUP *)
					(* Save the location of the next block group (or whatever should be after this *)
					MKV_new_nbp (LargeFile.pos_in h +| l)
				)
				| Some (0x21, Some l) -> (
					(* XXX BLOCK XXX *)

					if nbp > before_len then (
						(* There can't be a frame here, since the location of the next block is after the end of the file *)
						p // sprintf "EOF: %Ld > %Ld" nbp before_len;
						MKV_EOF_in
					) else (
						(* The file should be large enough to contain the entire frame *)

						let b = Matroska.parse_block_header h l in

						let frame_guts = String.create !-(b.Matroska.block_frame_bytes) in
						really_input h frame_guts 0 (String.length frame_guts);

						let total_timecode = !|(b.Matroska.block_timecode) +| ct in
						let total_time_n = total_timecode *| tcs *| !|(job.job_fps_n) in
						let total_time_d = 1000000000L *| !|(job.job_fps_d) in
						let frame_num_64 = i64_div_round total_time_n total_time_d +| !|(job.job_seek) in

						(* If the current position is the calculated end of the block group,
						 * then there is no reference block, and therefore this frame is an I frame *)
						let i_frame = (LargeFile.pos_in h = nbp) in
						let keep_going = if i_frame then (
							(* Output the frame number *)
							let frame_num = !-frame_num_64 in
							let frame_num_string = String.create 4 in
							Pack.packN frame_num_string 0 frame_num;
							p // sprintf "MKV read thread sending I frame %d" frame_num;
							if bs#flush_req then (
								s#send "IFRM" frame_num_string;
								match s#recv with
								| ("CONT","") -> (p "keep going"; true)
								| ("STOP","") -> (p "controller says stop"; false)
								| _ -> (p "got something weird; better stop"; false)
							) else (
								(* Controller said stop when flushed *)
								false
							)
						) else (
							true
						) in

						if keep_going then (

							let send_frame_guts = if frame_num_64 = 0L && (job.job_version_major >= 2 || job.job_version_minor >= 1) then (
								(* First, get rid of the SEI on the first frame of the encode *)
								(* But only if the controller sent version 1.1 *)
								if (
									String.length frame_guts > 6 &&
									String.sub frame_guts 0 2 = "\x00\x00" &&
									(frame_guts.[2] = '\x01' || frame_guts.[2] = '\x02') && (* I don't think this can be 3... although I don't know why it can also be 2 *)
									String.sub frame_guts 4 2 = "\x06\x05"
								) then (
									(* The beginning is OK; check the length and name *)
									match trap_exception_2 read_xiph_from_string frame_guts 6 with
									| Exception _ -> frame_guts
									| Normal (sei_len, len_len) -> (
										if String.length frame_guts > 6 + sei_len + len_len && sei_len > 16 && String.sub frame_guts (6 + len_len) 16 = "\xDC\x45\xE9\xBD\xE6\xD9\x48\xB7\x96\x2C\xD8\x20\xD9\x23\xEE\xEF" then (
											let real_frame_start = 6 + len_len + sei_len + 1 in
											(* Send the stuff here *)
											s#send "USEI" (String.sub frame_guts 0 real_frame_start);
											String.sub frame_guts real_frame_start (String.length frame_guts - real_frame_start)
										) else (
											frame_guts
										)
									)
								) else (
									frame_guts
								)
							) else (
								frame_guts
							) in

							(* Now send the frame *)
(*							p // sprintf "MKV read thread sending frame data %Ld (pos %d)" frame_num_64 next_frame;*)

							ignore // bs#string_req (Matroska.string_of_id 0x0F43B675); (* Segment ID *)
							ignore // bs#string_req (Matroska.string_of_size None); (* Segment size *)

							let frame_num_matroska = Matroska.string_of_uint frame_num_64 in
							ignore // bs#string_req (Matroska.string_of_id 0x67); (* Timecode ID *)
							ignore // bs#string_req (Matroska.string_of_size (Some !|(String.length frame_num_matroska))); (* Timecode size *)
							ignore // bs#string_req frame_num_matroska; (* Timecode *)
							ignore // bs#string_req (Matroska.string_of_id 0x23); (* Simple block *)
							ignore // bs#string_req (Matroska.string_of_size (Some !|(4 + String.length send_frame_guts)));
							if i_frame then (
								ignore // bs#string_req "\x81\x00\x00\x80" (* Simple block header (timecode=0 thanks to the new segment for each frame) *)
							) else (
								ignore // bs#string_req "\x81\x00\x00\x00" (* Not an I frame *)
							);
							let req = bs#string_req send_frame_guts in
							if req then (
								(* Everything was sent OK *)
								MKV_new_frame
							) else (
								(* Controller said stop *)
								MKV_stop
							)
						) else (
							MKV_stop
						)
					)
				)
				| Some (x, Some l) -> (
					(* Unimportant block of known length *)
					seek_add h l;
					MKV_keep_going
				)
				| Some (x, None) -> (
					(* Unimportant block of unknown length (shouldn't happen) *)
					MKV_keep_going
				)
				| None -> (
					MKV_EOF_out
				)
			with
				End_of_file -> MKV_EOF_in
			in


			match status with
			| MKV_keep_going -> reader ct tcs nbp duration_location next_frame
			| MKV_new_frame -> reader ct tcs nbp duration_location (succ next_frame)
			| MKV_new_ct  new_ct  -> reader new_ct tcs nbp duration_location next_frame
			| MKV_new_tcs new_tcs -> reader ct new_tcs nbp duration_location next_frame
			| MKV_new_nbp new_nbp -> reader ct tcs new_nbp duration_location next_frame
			| MKV_duration_location new_dl -> reader ct tcs nbp (Some new_dl) next_frame
			| MKV_EOF_in -> (
				(* There was an EOF in the middle of an element; this means that this can't be the end *)
				LargeFile.seek_in h before_pos;
				let rec check_length times = (
					if times = 0 then (
						(* Give up waiting *)
						p "MKV read thread timed out reading MKV file in the middle of an element; raising EOF";
						raise End_of_file
					) else (
						let new_len = LargeFile.in_channel_length h in
						if new_len > before_len then (
							(* Something was added to the file since this iteration started *)
							reader ct tcs nbp duration_location next_frame
						) else (
							p "MKV read thread waiting (in)...";
							Thread.delay 1.0;
							check_length (pred times)
						)
					)
				) in
				check_length 240 (* Do it a bunch since there REALLY should be more *)
			)
			| MKV_EOF_out -> (
				(* This may be the end of the file, so check the duration *)
				match duration_location with
				| None -> (
					p "MKV read thread waiting for more, but there is no duration location";
					LargeFile.seek_in h before_pos;
					let rec check_length times = (
						if times = 0 then (
							(* Give up waiting *)
							p "MKV read thread timed out reading MKV file outside an element (no DL); raising EOF";
							raise End_of_file
						) else (
							let new_len = LargeFile.in_channel_length h in
							if new_len > before_len then (
								(* Something was added to the file since this iteration started *)
								reader ct tcs nbp duration_location next_frame
							) else (
								p "MKV read thread waiting (no DL)...";
								Thread.delay 1.0;
								check_length (pred times)
							)
						)
					) in
					check_length 240 (* Do it a bunch since there should at least be a duration *)
				)
				| Some dl -> (
					p // sprintf "MKV read thread waiting for more, and checking the 4 bytes at %Ld to see if anything's there" dl;
					let str = String.copy "\x00\x00\x00\x00" in
					let rec check_stuff times = (
						if times = 0 then (
							(* Give up *)
							p "MKV read thread timed out waiting for either the duration or a file extension; raising EOF";
							raise End_of_file
						) else (
							LargeFile.seek_in h dl;
							really_input h str 0 4;
							if str = "\x00\x00\x00\x00" then (
								(* No duration yet; check the length *)
								let new_len = LargeFile.in_channel_length h in
								if new_len > before_len then (
									(* At least there's more to the file *)
									LargeFile.seek_in h before_pos;
									reader ct tcs nbp duration_location next_frame
								) else (
									p "MKV read thread waiting (DL)...";
									Thread.delay 1.0;
									check_stuff (pred times)
								)
							) else (
								(* The duration changed, but now check to see that the length did NOT change *)
								let new_len = LargeFile.in_channel_length h in
								if new_len = before_len then (
									(* HUZZAH! *)
									p "MKV read thread got a duration, and the last length was the final one; exit";
									next_frame
								) else (
									(* Something else may have been written to the file right before the duration was checked *)
									p "MKV read thread got a duration, but the length changed since it was checked; re-run";
									LargeFile.seek_in h before_pos;
									reader ct tcs nbp duration_location next_frame
								)
							)
						)
					) in
					check_stuff 60
				)
			)
			| MKV_stop -> (
				(* Controller says to give up *)
				match win_handle_option with
				| None -> next_frame
				| Some win_handle -> (
					p "killing off x264";
(*					ignore // Opt.kill_process_win win_handle;*)
					next_frame
				)
			)
		) in
		let total_frames = reader 0L 1000000L 0L None 0 in
		Normal total_frames
	with
		e -> Exception e
	) in
	Unix.close h_unix;
	match is_exception with
	| Normal total_frames -> (
		let total_frame_string = String.create 4 in
		Pack.packN total_frame_string 0 (total_frames + job.job_seek);
		ignore // bs#flush_req;
		s#send "EEND" total_frame_string
	)
	| Exception e -> (
		match win_handle_option with
		| None -> raise e
		| Some win_handle -> (
(*			ignore // Opt.kill_process_win win_handle;*)
			raise e
		)
	)
);;
(******************)
(* !MKV READ GUTS *)
(******************)



(*****************************************)
(* GET MKV TRACKS WITHOUT DOING ANYTHING *)
(*****************************************)
let make_tracks p job = (
	let temp_name = (
		let (n,h) = open_temp_file ~mode:[Open_binary] (Filename.concat o.o_temp_dir "temp tracks ") ".mkv" in
		close_out h;
		n
	) in

	let do_this = sprintf "%s %s --fps %d/%d --no-psnr --no-ssim --quiet -o \"%s\" - %dx%d" o.o_x264 job.job_options job.job_fps_n job.job_fps_d temp_name job.job_res_x job.job_res_y in
	p // sprintf "MAKE_TRACKS doing this: %S" do_this;
	let put = Unix.open_process_out do_this in

	(* Do a single frame *)
	let frame_size = job.job_res_x * job.job_res_y * 3 / 2 in
	let frame = String.make (min 16384 frame_size) 'E' in
	let rec helper send_more = (
		if send_more = 0 then (
			()
		) else (
			let send_now = min 16384 send_more in
			p // sprintf "MAKE_TRACKS sending %d bytes out of %d left" send_now send_more;
			output put frame 0 send_now;
			helper (send_more - send_now)
		)
	) in
	(* Trap any exceptions created here, in case the x264 options are bad *)
	(try
		helper frame_size;
		flush put
	with
		_ -> ()
	);

	(* Exit x264 to see how it was handled *)
	let exited = Unix.close_process_out put in
	(match exited with
		| Unix.WEXITED 0 -> p "MAKE_TRACKS x264 exited normally"
		| Unix.WEXITED x -> (p // sprintf "MAKE_TRACKS x264 exited with %d!" x; raise (X264_error x))
		| Unix.WSTOPPED x -> (p // sprintf "MAKE_TRACKS x264 stopped with %d!" x; raise (X264_error x))
		| Unix.WSIGNALED x -> (p // sprintf "MAKE_TRACKS x264 signaled with %d!" x; raise (X264_error x))
	);

	(* Now open the file to get at the tasty, tasty track element *)
	let t = open_in_bin temp_name in
	let perhaps = (try
		while LargeFile.in_channel_length t = 0L do
			p "MAKE_TRACKS delaying";
			Thread.delay 0.01
		done;
		let rec helper () = (
			let idlen = Matroska.input_id_and_length t in
			(match idlen with
				| None -> p "MAKE_TRACKS none?"
				| Some (id, None) -> p // sprintf "MAKE_TRACKS ID %08X, len ?" id
				| Some (id, Some len) -> p // sprintf "MAKE_TRACKS ID %08X, len %Ld" id len
			);
			match idlen with
			| Some (0x08538067, _) -> (
				(* SEGMENT *)
				p "MAKE_TRACKS segment";
				helper ()
			)
			| Some (0x0654AE6B, Some l) -> (
				(* TRAX! *)
				let tracks = String.create !-l in
				let tracks_pos = LargeFile.pos_in t in
				really_input t tracks 0 (String.length tracks);
				p // sprintf "MAKE_TRACKS found track element %S" (to_hex tracks);
				let priv = (
					LargeFile.seek_in t tracks_pos;
					let rec priv_helper () = (
						match Matroska.input_id_and_length t with
						| Some (0x2E, _) -> priv_helper () (* Track *)
						| Some (0x23A2, Some len) -> (
							let priv = String.create !-len in
							really_input t priv 0 !-len;
							p // sprintf "MAKE_TRACKS found private element %S" (to_hex priv);
							priv
						)
						| Some (_, Some len) -> (seek_add t len; priv_helper ()) (* Skip over unknown of known length *)
						| Some (_, None) -> priv_helper () (* Unknown of unknown length? *)
						| None -> raise End_of_file
					) in
					priv_helper ()
				) in

				(tracks,priv)
			)
			| Some (_, Some l) -> (
				(* Something else; skip *)
				seek_add t l;
				p "MAKE_TRACKS skip";
				helper ()
			)
			| None | Some (_, None) -> (
				(* Either an invalid ID, or an unknown element of unknown size *)
				p "MAKE_TRACKS found something weird; better mope";
				failwith "make_tracks found invalid or unexpected ID"
			)
		) in
		Normal (helper ())
	with
		e -> Exception e
	) in
	close_in t;
	
	(match trap_exception Sys.remove temp_name with
		| Normal _ -> ()
		| Exception _ -> (
			ignore // Thread.create (fun () ->
				try
					Thread.delay 10.0;
					Sys.remove temp_name
				with
					e -> p // sprintf "failed to remove temp file: %S" (Printexc.to_string e)
			) ()
		)
	);

	match perhaps with
	| Normal x -> x
	| Exception e -> raise e

);;


(*****************)
(* MP4_READ_GUTS *)
(*****************)
type mp4_status_t =
	| MP4_keep_going
	| MP4_new_frame
	| MP4_new_idr_frame
	| MP4_stop
	| MP4_eof
	| MP4_exception of exn
;;
let mp4_read_guts p s job temp_name bs win_handle_option =

	p "starting up MP4 reader";

	let check_net_status = job.job_version_major >= 2 || job.job_version_minor >= 2 in
	let check_net_begin_every = 5 in
	let check_net_add = 1 in
	let check_net_max = 20 in

	(* Get the private string *)
	let priv = match (job.job_ntrx_ok, job.job_private) with
		| (true, Some priv_string) -> (
			s#send "NTRX" "";
			Bitstream.get_private (new Bitstream.bg_string priv_string)
		)
		| _ -> (
			try
				let (tracks_string, priv_string) = make_tracks p job in
				s#send "TRAX" tracks_string;
				Bitstream.get_private (new Bitstream.bg_string priv_string)
			with
			| X264_error x -> (
				p "MP4 thread got x264 error";
				s#send "XERN" (string_of_int x);
				raise (X264_error x)
			)
		)
	in

	let h = (
		let rec helper tries check_net_next check_net_next_reset = (
			p // sprintf "helping with %d tries left" tries;
			let stuff = (try
				if (Unix.LargeFile.stat temp_name).Unix.LargeFile.st_size > 0L then (
					Some (Unix.openfile temp_name [Unix.O_RDWR] 0o600) (* O_RDWR is needed for sparsifying the file *)
				) else (
					p "temp file has nothing in it yet; waiting...";
					None
				)
			with
				_ -> None
			) in
			match stuff with
			| Some x -> x
			| None when tries = 0 -> (
				p "giving up trying to find temp file";
(*
				(match win_handle_option with
					| None -> ()
					| Some wh -> ignore // Opt.kill_process_win wh
				);
*)
				if check_net_status then s#send "TOUT" "";
				raise Not_found
			)
			| None when check_net_status -> (
				if check_net_next <= 0 then (
					(* Check the network status by sending something to the controller, but only if the controller expects it *)
					p "sending PING since the file does not exist yet";
					s#send "PING" "";
					let next = min (check_net_next_reset + check_net_add) check_net_max in
					helper (pred tries) next next
				) else (
					(* Don't check with the controller this time; this is to decrease network traffic to the controller *)
					Thread.delay 1.0;
					helper (pred tries) (pred check_net_next) check_net_next_reset
				)
			)
			| None -> (
				Thread.delay 1.0;
				helper (pred tries) check_net_next check_net_next_reset
			)
		) in
		helper 60 check_net_begin_every check_net_begin_every
	) in
	let is_sparse = Opt.set_sparse h in
	p // sprintf "temp file sparse? %B" is_sparse;
	let set_zero_data = (if is_sparse then Opt.set_zero_data h else fun a b -> false) in

	let is_exception = (try

		(* First, find the beginning of the MP4 data *)
		let rec mp4_parser first_tries tries check_net_next check_net_next_reset = (
			let pos = Unix.LargeFile.lseek h 0L Unix.SEEK_CUR in
			let ok = try
				let l = String.create 4 in
				let t = String.create 4 in
				if Unix.read h l 0 4 <> 4 then raise End_of_file;
				if Unix.read h t 0 4 <> 4 then raise End_of_file;
				match (l,t) with
				| ("\x00\x00\x00\x00","\x00\x00\x00\x00") | (_, "mdat") -> (
					(* Found it! *)
					let pos_now = Unix.LargeFile.lseek h 8L Unix.SEEK_CUR in (* Go forward a bit to avoid the 64-bit length padding *)
					Normal (Some pos_now)
				)
				| (len_str, "ftyp") -> (
					(* Not yet *)
					let len = Pack.unpackN len_str 0 in
					ignore // Unix.LargeFile.lseek h (pos +| !|len) Unix.SEEK_SET;
					Normal None
				)
				| _ -> (
					failwith "muffin"
				)
			with
				e -> Exception e
			in
			match ok with
			| Normal (Some x) -> x (* Done! (x is the file offset of the first NAL size, which should probably be 40L) *)
			| Normal None -> mp4_parser first_tries first_tries check_net_begin_every check_net_begin_every (* Keep going *)
			| Exception (Failure "muffin") -> (
				p "MP4 file is screwed";
				if check_net_status then s#send "TOUT" "";
				raise Not_found
			)
			| Exception e -> (
				ignore // Unix.LargeFile.lseek h pos Unix.SEEK_SET;
				if tries = 0 then (
					if check_net_status then s#send "TOUT" "";
					raise e
				) else (
					if check_net_status then (
						if check_net_next = 0 then (
							p "sending PING since the file doesn't have the right headers";
							s#send "PING" "";
							let next = min (check_net_next_reset + check_net_add) check_net_max in
							mp4_parser first_tries (pred tries) next next
						) else (
							Thread.delay 1.0;
							mp4_parser first_tries (pred tries) (pred check_net_next) check_net_next_reset
						)
					) else (
						(* Try again *)
						Thread.delay 1.0;
						mp4_parser first_tries (pred tries) check_net_next check_net_next_reset
					)
				)
			)
		) in
		let bitstream_start = mp4_parser 60 60 check_net_begin_every check_net_begin_every in
		p // sprintf "MP4 bitstream starts at %Ld" bitstream_start;
		ignore // Unix.LargeFile.lseek h bitstream_start Unix.SEEK_SET; (* Just in case *)

		(* Change the check_net variables to reflect the difference in Thread.delay times here *)
		let check_net_multiplier = 10 in
		let check_net_begin_every = check_net_multiplier * check_net_begin_every in
		let check_net_add = check_net_multiplier * check_net_add in
		let check_net_max = check_net_multiplier * check_net_max in


		let bg = new Bitstream.bg_fd h in
		let len_bits = priv.Bitstream.private_nalu_size lsl 3 in
		p // sprintf "NALU size is %d bits" len_bits;
		(* NOW ITERATE THROUGH THE NAL THINGIES *)
		let rec do_nal last_idr frames first_tries tries check_net_next check_net_next_reset = (
			p "MP4 doing NAL";
			if frames = job.job_frames then (
				(* DONE! *)
				job.job_frames
			) else (
				let pos_start = Unix.LargeFile.lseek h 0L Unix.SEEK_CUR in
				let len_start = (
					let a = Unix.LargeFile.lseek h 0L Unix.SEEK_END in
					ignore // Unix.LargeFile.lseek h pos_start Unix.SEEK_SET;
					a
				) in

				let ok = (try
					let len = bg#b len_bits in
					let nal_pos_start = bg#pos_byte in
(*					p // sprintf "MP4 next NAL has length %d BYTES, starting from pos %Ld" len nal_pos_start;*)
					let nal = Bitstream.get_nal !|len (Some priv.Bitstream.private_sps) (Some priv.Bitstream.private_pps) bg in
					match nal.Bitstream.nal_unit_type with
					| Bitstream.Nal_type_sei {Bitstream.payload_type = Bitstream.Sei_user_data_unregistered {Bitstream.sei_unregistered_uuid = "\xDC\x45\xE9\xBD\xE6\xD9\x48\xB7\x96\x2C\xD8\x20\xD9\x23\xEE\xEF"}} -> (
						p // sprintf "MP4 got x264 SEI; ignoring";
(*						bg#reset_start (nal_pos_start +| !|len);*)
						bg#seek_byte (nal_pos_start +| !|len);
						MP4_keep_going
					)
					| Bitstream.Nal_type_idr slice | Bitstream.Nal_type_non_idr slice -> (
						let i_frame = match nal.Bitstream.nal_unit_type with
							| Bitstream.Nal_type_idr _ -> true
							| _ -> false
						in

						let display_frame_num = match (i_frame, slice) with
							| (true, _) -> frames + job.job_seek
							| (false, {Bitstream.pic_order_cnt = (Bitstream.Slice_header_pic_order_cnt_type_0 x)}) -> last_idr + (x lsr 1) + job.job_seek
							| _ -> (
								p "MP4 can't get poc... guessing";
								frames + job.job_seek
							)
						in

						if i_frame then (
(*							p // sprintf "MP4 got IDR frame %d" display_frame_num;*)
							(* Might as well sparsify the file here *)
							if pos_start > 2097152L then (
								let floored = pos_start &&| 0xFFFFFFFFFFF00000L in (* Floors to the nearest MB *)
								let zeroed = set_zero_data 0x100000L floored in
								if zeroed then (
									p // sprintf "MP4 zeroed from %Ld to %Ld (current NAL starts at %Ld)" 0x100000L floored pos_start
								) else (
									p // sprintf "MP4 failed to zero data (pos:%Ld, len:%Ld)" pos_start len_start
								)
							)
						) else (
(*							p // sprintf "MP4 got normal frame %d" display_frame_num*)
						);

						(* Copy frame guts to string *)
						let frame_guts = String.create (priv.Bitstream.private_nalu_size + len) in
						ignore // Unix.LargeFile.lseek h pos_start Unix.SEEK_SET;
						really_read h frame_guts 0 (String.length frame_guts);

(*						bg#reset_start (Unix.LargeFile.lseek h 0L Unix.SEEK_CUR);*)

						(* See if the controller needs more *)
						let keep_going = if i_frame then (
							let frame_num_string = String.create 4 in
							Pack.packN frame_num_string 0 display_frame_num;
							p "MP4 sending I frame info";
							if bs#flush_req then (
								s#send "IFRM" frame_num_string;
								match s#recv with
								| ("CONT","") -> (p "MP4 keep going"; true)
								| ("STOP","") -> (p "MP4 controller says stop"; false)
								| _ -> (p "MP4 got something weird; better stop"; false)
							) else (
								false
							)
						) else (
							true
						) in

						if keep_going then (
							(* Send frame *)



							ignore // bs#string_req (Matroska.string_of_id 0x0F43B675); (* Segment ID *)
							ignore // bs#string_req (Matroska.string_of_size None); (* Segment size *)

							let frame_num_matroska = Matroska.string_of_uint !|display_frame_num in
							ignore // bs#string_req (Matroska.string_of_id 0x67); (* Timecode ID *)
							ignore // bs#string_req (Matroska.string_of_size (Some !|(String.length frame_num_matroska))); (* Timecode size *)
							ignore // bs#string_req frame_num_matroska; (* Timecode *)
							ignore // bs#string_req (Matroska.string_of_id 0x23); (* Simple block *)
							ignore // bs#string_req (Matroska.string_of_size (Some !|(4 + String.length frame_guts)));
							if i_frame then (
								ignore // bs#string_req "\x81\x00\x00\x80" (* Simple block header (timecode=0 thanks to the new segment for each frame) *)
							) else (
								ignore // bs#string_req "\x81\x00\x00\x00" (* Not an I frame *)
							);
							let req = bs#string_req frame_guts in

							if req then (
								if i_frame then (
									MP4_new_idr_frame
								) else (
									MP4_new_frame
								)
							) else (
								MP4_stop
							)
						) else (
							MP4_stop
						)
					)
					| nt -> (
						p // sprintf "ERROR: Don't understand what NAL type %s is; might as well keep going" (Bitstream.string_of_nal_type nt);
						bg#seek_byte (nal_pos_start +| !|len);
						MP4_keep_going
					)
				with
					| End_of_file -> MP4_eof
					| e -> MP4_exception e
				) in
				match ok with
				| MP4_keep_going -> do_nal last_idr frames first_tries first_tries check_net_begin_every check_net_begin_every
				| MP4_new_frame -> do_nal last_idr (succ frames) first_tries first_tries check_net_begin_every check_net_begin_every
				| MP4_new_idr_frame -> do_nal frames (succ frames) first_tries first_tries check_net_begin_every check_net_begin_every
				| MP4_stop -> frames (* DONE! *)
				| MP4_eof when tries <= 0 -> (
					p "MP4 got too many EOFs";
					(* Don't bother flushing bs here; it will just get tossed anyway *)
					if check_net_status then s#send "TOUT" "";
					raise End_of_file
				)
				| MP4_eof -> (
					p "MP4 got end of file; try again";
(*					bg#reset_start pos_start;*)
					bg#seek_byte pos_start;
					if check_net_status then (
						if check_net_next = 0 then (
							if bs#flush_req then (
								p "sending PING since the file is not big enough";
								s#send "PING" "";
								let next = min (check_net_next_reset + check_net_add) check_net_max in
								Thread.delay 0.1;
								do_nal last_idr frames first_tries (pred tries) next next
							) else (
								(* The controller said stop to the flush *)
								frames
							)
						) else (
							Thread.delay 0.1;
							do_nal last_idr frames first_tries (pred tries) (pred check_net_next) check_net_next_reset
						)
					) else (
						Thread.delay 0.1; (* Waiting for 1 second can result in dozens of frames going by, so wait less *)
						do_nal last_idr frames first_tries (pred tries) check_net_next check_net_next_reset
					)
				)
				| MP4_exception e -> (
					p // sprintf "MP4 got exception %S; dying" (Printexc.to_string e);
					if check_net_status then s#send "TOUT" "";
					raise e
				)
			)
		) in
		let frames_done = do_nal 0 0 600 600 check_net_begin_every check_net_begin_every in

(*		Thread.delay 1000.0;*)

		Normal frames_done
	with
		e -> Exception e
	) in
	Unix.close h;
	match is_exception with
	| Normal total_frames -> (
(*
		(match win_handle_option with
			| None -> ()
			| Some wh -> ignore // Opt.kill_process_win wh
		); (* If this doesn't get done, the computer will do bad things *)
*)
		let total_frame_string = String.create 4 in
		Pack.packN total_frame_string 0 (total_frames + job.job_seek);
		ignore // bs#flush_req;
		s#send "EEND" total_frame_string
	)
	| Exception e -> (
		match win_handle_option with
		| None -> raise e
		| Some win_handle -> (
(*			ignore // Opt.kill_process_win win_handle;*)
			raise e
		)
	)
;;
(******************)
(* !MP4_READ_GUTS *)
(******************)


(************************)
(* BYTESTREAM_READ_GUTS *)
(************************)
(*
 * Turns out that the writing for the raw bitstream is buffered,
 * although not nearly to the same extent as MKV. (4096-byte chunks)
 *)
type nal_or_new_offset = Nal_nal of Bitstream.nal_unit | Nal_new_offset of int;;
let bytestream_read_guts p s job temp_name bs win_handle_option =
	let verbose = false in

	p "starting up 264 reader";

	let check_net_status = job.job_version_major >= 2 || job.job_version_minor >= 2 in
	let check_net_begin_every = 5 in
	let check_net_add = 1 in
	let check_net_max = 20 in
	let thread_delay = 1.0 in
	let timeout = 60 in

	(* Send tracks (or not, depending on the job) *)
	(match job.job_ntrx_ok with
		| true -> (
			s#send "NTRX" ""
		)
		| false -> (
			(* Have to make one up *)
			try
				let (tracks_string, _) = make_tracks p job in
				s#send "TRAX" tracks_string
			with
			| X264_error x -> (
				p "264 thread got x264 error";
				s#send "XERN" (string_of_int x);
				raise (X264_error x)
			)
		)
	);

	let h = (
		let rec helper tries check_net_next check_net_next_reset = (
			p // sprintf "helping with %d tries left" tries;
			let stuff = (try
				if (Unix.LargeFile.stat temp_name).Unix.LargeFile.st_size > 0L then (
					Some (Unix.openfile temp_name [Unix.O_RDWR] 0o600) (* O_RDWR is needed for sparsifying the file *)
				) else (
					p "temp file has nothing in it yet; waiting...";
					None
				)
			with
				_ -> None
			) in
			match stuff with
			| Some x -> x
			| None when tries = 0 -> (
				p "giving up trying to find temp file";
(*
				(match win_handle_option with
					| None -> ()
					| Some wh -> ignore // Opt.kill_process_win wh
				);
*)
				if check_net_status then s#send "TOUT" "";
				raise Not_found
			)
			| None when check_net_status -> (
				if check_net_next <= 0 then (
					p "sending PING since the file does not exist yet";
					s#send "PING" "";
					let next = min (check_net_next_reset + check_net_add) check_net_max in
					helper (pred tries) next next
				) else (
					Thread.delay thread_delay;
					helper (pred tries) (pred check_net_next) check_net_next_reset
				)
			)
			| None -> (
				Thread.delay thread_delay;
				helper (pred tries) (pred check_net_next) check_net_next_reset
			)
		) in
		helper timeout check_net_begin_every check_net_begin_every
	) in
	let is_sparse = Opt.set_sparse h in
	p // sprintf "temp file sparse? %B" is_sparse;
	let set_zero_data = (if is_sparse then Opt.set_zero_data h else fun a b -> false) in

	let is_exception = (try

		let len = max 5 4096 in
		let str = String.create len in
		let valid_ref = ref 0 in

		let buf = Bigbuffer.create 262144 in (* This seems to be a reasonable size *)
		let bg = new bigbuffer_bitstream p buf in

		(* These two references are to positions in the file, not the buffer *)
(*		let nal_start = ref 0L in*)
		let str_start = ref 0L in

		let sparse_distance = 0x100000L in
		let last_sparse = ref 0x100000L in
		let next_sparse = ref (!last_sparse +| sparse_distance) in

		let check_net_multiplier = 10 in
		let check_net_begin_every = check_net_multiplier * check_net_begin_every in
		let check_net_add = check_net_multiplier * check_net_add in
		let check_net_max = check_net_multiplier * check_net_max in
		let thread_delay = thread_delay /. float_of_int check_net_multiplier in
		let timeout = timeout * check_net_multiplier in

		let rec read_more left check_net_next check_net_next_reset = (
			(* This replaces the string's data with more data from the file *)
			p // sprintf "264 filling up the string buffer (%d,%d,%d)" left check_net_next check_net_next_reset;
			str_start := Unix.LargeFile.lseek h 0L Unix.SEEK_CUR;
			let read_bytes = Unix.read h str 0 len in
			if read_bytes = 0 then (
				if left = 0 then (
					if check_net_status then s#send "TOUT" "";
					raise End_of_file
				) else (
					if check_net_status then (
						if check_net_next = 0 then (
							if bs#flush_req then (
								p "sending PING since the file is not big enough";
								s#send "PING" "";
								let next = min (check_net_next_reset + check_net_add) check_net_max in
								Thread.delay thread_delay;
								read_more (pred left) next next
							) else (
								(* Controller said stop to the flush *)
								raise End_of_file
							)
						) else (
							(* Don't send anything to the controller now *)
							Thread.delay thread_delay;
							read_more (pred left) (pred check_net_next) check_net_next_reset
						)
					) else (
						Thread.delay thread_delay;
						read_more (pred left) check_net_next check_net_next_reset
					)
(*
					if verbose then p "264 READ_MORE delaying...";
					Thread.delay 0.1;
					read_more (pred left)
*)
				)
			) else (
				if verbose then p // sprintf "264 READ_MORE got %d bytes" read_bytes;
				valid_ref := read_bytes;
			)
		) in

		(* This function will fill up the buffer with a given NAL, reading from the file as it needs *)
		(* After a new NAL is found, it will return the location in the buffer of the new NAL *)
		(* The state will always be 0 after this returns, so it need not be returned with the location *)
		let rec keep_reading_nal off state = (
			if verbose then p // sprintf "264 reading NAL with offset %d and initial state %d" off state;
(*			p // sprintf "264 current string is %S" (to_hex (String.sub str 0 !valid_ref));*)
			let (next_nal_perhaps, new_state) = Opt.fetch_0001 str off (!valid_ref - off) state in
			match next_nal_perhaps with
			| None -> (
				(* No new NAL found; output the entire contents *)
				if verbose then p "264 no NAL found; outputting entire contents";
				Bigbuffer.add_substring buf str off (!valid_ref - off);
				read_more timeout check_net_begin_every check_net_begin_every;
				keep_reading_nal 0 new_state
			)
			| Some next_nal_start when next_nal_start < 4 -> (
				(* The last few bytes of the previous read are invalid! *)
				if verbose then p "264 NAL found before position 4; this means some bytes previously read are invalid";
				Bigbuffer.shorten buf (4 - next_nal_start);
				(* Finish here *)
				if verbose then p // sprintf "264 buffer now contains a complete NAL";
				next_nal_start
			)
			| Some next_nal_start when next_nal_start = !valid_ref -> (
				(* The NAL starts on the next byte to be read, so read it and return 0 *)
				if verbose then p // sprintf "264 NAL starts on next byte after the string; input and return 0 (prev length %d)" (Bigbuffer.length buf);
				Bigbuffer.add_substring buf str off (next_nal_start - 4 - off);
				if verbose then p // sprintf "264 NAL now has length %d" (Bigbuffer.length buf);
				read_more timeout check_net_begin_every check_net_begin_every;
				if verbose then p // sprintf "264 buffer now contains a complete NAL";
				0
			)
			| Some next_nal_start -> (
				(* Read the last part into the buffer *)
				if verbose then p // sprintf "264 found end of NAL; next NAL starts as %d" next_nal_start;
				Bigbuffer.add_substring buf str off (next_nal_start - 4 - off);
				if verbose then p // sprintf "264 buffer now contains a complete NAL";
				next_nal_start
			)
		) in

		let first_nal_start = keep_reading_nal 0 0 in
		p // sprintf "264 first NAL is at buffer position %d (should be 4)" first_nal_start;
		(* Read the SPS and PPS (and skip the SEI) *)
		let rec get_sps_and_pps this_nal_start sps_perhaps pps_perhaps = (
(*			let (this_nal_idr, this_nal_type) = Bitstream.nal_id_of_char str.[this_nal_start] in*)
			match Bitstream.nal_id_of_char str.[this_nal_start] with
			| Some (_, (Bitstream.Nal_id_non_idr | Bitstream.Nal_id_idr)) -> (
				(* Done! *)
				p "264 got a frame; start up the frame reader";
				(this_nal_start, sps_perhaps, pps_perhaps)
			)
			| Some (_, nal_id) -> (
				(* Not a frame; read it into the buffer *)
				p // sprintf "264 got NAL type %s" (Bitstream.string_of_nal_id nal_id);
				Bigbuffer.clear buf;
				let next_nal_start = keep_reading_nal this_nal_start 0 in
				bg#reset; (* Start over from the beginning of the buffer *)
				match Bitstream.get_nal !|(Bigbuffer.length buf) sps_perhaps pps_perhaps bg with
				| {Bitstream.nal_unit_type = Bitstream.Nal_type_sps sps} -> (
					p "264 got SPS:";
					p // sprintf "  log2_max_frame_num = %d" sps.Bitstream.log2_max_frame_num     ;
					p // sprintf "  num_ref_frames     = %d" sps.Bitstream.num_ref_frames         ;
					p // sprintf "  pic_width_in_mbs   = %d" sps.Bitstream.pic_width_in_mbs       ;
					p // sprintf "  pic_height_in_maps = %d" sps.Bitstream.pic_height_in_map_units;
					get_sps_and_pps next_nal_start (Some [|sps|]) pps_perhaps
				)
				| {Bitstream.nal_unit_type = Bitstream.Nal_type_pps pps} -> (
					get_sps_and_pps next_nal_start sps_perhaps (Some [|pps|])
				)
				| _ -> get_sps_and_pps next_nal_start sps_perhaps pps_perhaps (* Skip *)
			)
			| None -> (
				(* Not a valid NAL! *)
				p // sprintf "264 byte 0x%02X is not a valid NAL start" (Char.code str.[this_nal_start]);
				failwith "Not a valid NAL start when looking for SPS and PPS NALs"
			)
		) in
		let (first_frame_nal_start, sps_array, pps_array) = match get_sps_and_pps first_nal_start None None with
			| (ffns, Some sps_array, Some pps_array) -> (ffns, sps_array, pps_array)
			| _ -> (
				p "264 reader didn't find an SPS and PPS NAL";
				failwith "SPS and PPS not found at beginning of file"
			)
		in

		let rec keep_reading_frames this_nal_start nth_frame last_i_frame = (
			p // sprintf "264 attempting to get frame %d from string position %d (last I frame was %d)" nth_frame this_nal_start last_i_frame;
			if nth_frame = job.job_frames then (
				p "264 got enough frames";
				nth_frame
			) else (
				let nal_start_char = str.[this_nal_start] in
				match Bitstream.nal_id_of_char nal_start_char with
				| Some (_, Bitstream.Nal_id_end_of_stream) -> nth_frame (* DONE! *)
				| Some (_, nal_id) -> (
					(* Read the NAL *)
					if verbose then p // sprintf "264 got NAL type %s" (Bitstream.string_of_nal_id nal_id);
				
					(* Send an I-frame now (since we know it's an I frame) *)
					let i_frame = match nal_id with
						| Bitstream.Nal_id_idr -> true
						| _ -> false
					in

					(* Sparsify the file *)
					if !str_start > !next_sparse then (
						let sparse_to = !str_start &&| 0xFFFFFFFFFFF00000L in
						p // sprintf "sparsifying from %Ld to %Ld (pos is %Ld, >%Ld)" !last_sparse sparse_to !str_start !next_sparse;
						ignore // set_zero_data !last_sparse sparse_to;
						last_sparse := sparse_to;
						next_sparse := sparse_to +| sparse_distance;
					);


					let keep_going = if i_frame then (
						(* Output the frame number *)
						let frame_num_absolute = nth_frame + job.job_seek in
						let frame_num_string = String.create 4 in
						Pack.packN frame_num_string 0 frame_num_absolute;
						p // sprintf "264 sending I frame %d" frame_num_absolute;
						if bs#flush_req then (
							s#send "IFRM" frame_num_string;
							match s#recv with
							| ("CONT","") -> (p "264 controller says keep going"; true)
							| ("STOP","") -> (p "264 controller says stop"; false)
							| _ -> (p "got something weird; better stop"; false)
						) else (
							(* Controller said stop when flushed *)
							false
						)
					) else (
						true
					) in

					if keep_going then (
(*						p "264 getting NAL from buffer";*)
						Bigbuffer.clear buf;
						let next_nal_start = keep_reading_nal this_nal_start 0 in
						bg#reset;
(*						p "264 got NAL from buffer";*)
						match Bitstream.get_nal !|(Bigbuffer.length buf) (Some sps_array) (Some pps_array) bg with
						| {Bitstream.nal_unit_type = Bitstream.Nal_type_non_idr ({Bitstream.pic_order_cnt = Bitstream.Slice_header_pic_order_cnt_type_0 poc} as frame_nal)}
						| {Bitstream.nal_unit_type = Bitstream.Nal_type_idr     ({Bitstream.pic_order_cnt = Bitstream.Slice_header_pic_order_cnt_type_0 poc} as frame_nal)} -> (
							(* IT'S A FRAME! *)
							p "264 NAL is frame";

							(* Get the frame number *)
							let (next_i_frame, frame_num_64) = if i_frame then (
								(nth_frame, !|(nth_frame + job.job_seek))
							) else (
								(last_i_frame, !|(last_i_frame + poc lsr 1 + job.job_seek))
							) in
							p // sprintf "264 I frame is %d current frame number is %Ld" next_i_frame frame_num_64;

							ignore // bs#string_req (Matroska.string_of_id 0x0F43B675); (* Segment *)
							ignore // bs#string_req (Matroska.string_of_size None); (* Segment size *)

							let frame_num_matroska = Matroska.string_of_uint frame_num_64 in
							ignore // bs#string_req (Matroska.string_of_id 0x67); (* Timecode ID *)
							ignore // bs#string_req (Matroska.string_of_size (Some !|(String.length frame_num_matroska))); (* Timecode size *)
							ignore // bs#string_req frame_num_matroska;
							ignore // bs#string_req (Matroska.string_of_id 0x23); (* Simple block *)
							ignore // bs#string_req (Matroska.string_of_size (Some !|(4 + 4 + Bigbuffer.length buf))); (* Length of frame (add 4 for the MKV frame header and 4 for the NAL unit size length *)

							if i_frame then (
								ignore // bs#string_req "\x81\x00\x00\x80" (* Simple block header, I frame, timecode 0 *)
							) else (
								ignore // bs#string_req "\x81\x00\x00\x00" (* Simple block header, NOT I frame, timecode 0 *)
							);

							let len_string = String.create 4 in
							Pack.packN len_string 0 (Bigbuffer.length buf);
							ignore // bs#string_req len_string;

							(* Output the frame directly from the bigbuffer *)
							Bigbuffer.iter buf (fun _ bb_str str_len ->
								ignore // bs#sub_req bb_str 0 str_len
							);
							if bs#req then (
								keep_reading_frames next_nal_start (succ nth_frame) next_i_frame
							) else (
								p "264 controller said stop while sending the frame";
								nth_frame
							)
						)
						| {Bitstream.nal_unit_type = (Bitstream.Nal_type_non_idr _ | Bitstream.Nal_type_idr _)} -> (
							(* It's a frame, but the POC isn't right *)
							p "264 found a frame that doesn't use the right POC! PANIC!";
							failwith "unknown POC type"
						)
						| _ -> (
							(* Not a frame! Why is this here? *)
							p // sprintf "264 found a non-frame NAL; skipping (trying to get frame %d, last I frame %d" nth_frame last_i_frame;
							keep_reading_frames next_nal_start nth_frame last_i_frame
						)
					) else (
						(* Done! *)
						nth_frame
					)
				)
				| None -> (
					(* This isn't a NAL! *)
					p // sprintf "264 the byte 0x%02X is not a valid NAL! Giving up..." (Char.code nal_start_char);
					raise Not_found
				)
			)
		) in
		let total_frames = keep_reading_frames first_frame_nal_start 0 0 in
(*
		p "DELETE THE THREAD DELAY!!!111";
		Thread.delay 10000.00;
*)
		Normal total_frames
	with
		e -> (
			p // sprintf "264 reader failed with %S" (Printexc.to_string e);
			Exception e
		)
	) in
	Unix.close h;
	match is_exception with
	| Normal total_frames -> (
(*
		(match win_handle_option with
			| None -> ()
			| Some wh -> ignore // Opt.kill_process_win wh
		);
*)
		let total_frame_string = String.create 4 in
		Pack.packN total_frame_string 0 (total_frames + job.job_seek);
		ignore // bs#flush_req;
		s#send "EEND" total_frame_string
	)
	| Exception e -> (
		match win_handle_option with
		| None -> raise e
		| Some win_handle -> (
(*			ignore // Opt.kill_process_win win_handle;*)
			raise e
		)
	)
;;
(*************************)
(* !BYTESTREAM_READ_GUTS *)
(*************************)









(****************************)
(* AGENT-BASED X264 HANDLES *)
(****************************)
(*
 * This section keeps track of the agent-based x264 processes and tries to optimize them.
 * It does the following every 5 seconds, but NOT if there are fewer x264 processes than CPUs:
	 * Every process is given an affinity to process on (currently) exactly one CPU
	 * If there are more processes than CPUs, the last process does NOT get a specific affinity
	 * Processes are set to idle priority so that there are the same number of non-idle processes as there are CPUs
	 * (obviously if the specified priority IS idle, then this has no effect)
	 *)
let add_handle = if o.o_affinity && Sys.os_type = "Win32" then (
	let p = i.p ~name:"WINHANDLE " in
	let agent_handle_hash = Rbtree.create () in
	let agent_handle_mutex = Mutex.create () in
	let agent_handle_condition = Condition.create () in (* Wait on this whenever the hash is empty *)
	let sys_mask = match Opt.get_process_affinity Opt.current_process with
		| None -> -2 (* Probably an invalid mask *)
		| Some (proc,sys) -> sys
	in
	let num_processors = (
		let t = Opt.num_processors () in
		if t <= 0 then 32 else t (* This basically disables the affinity checking when the number of processors is unknown *)
	) in

	let orig_priority = nice_to_priority o.o_nice in

	(* Make the affinity-handler thread *)
	let rec affinity_guts () = (
		p "checking affinity";
		Mutex.lock agent_handle_mutex;
		
		(* First, clean out the old processes *)
		let clean_these = Rbtree.fold_left (fun so_far thread (job,handle) ->
			match Opt.get_exit_code_win handle with
			| Some None -> (
				(* Thread has not exited *)
				so_far
			)
			| _ -> (
				(* Thread exited or there's something wrong with it *)
				p // sprintf "cleaning up handle from thread %d" thread;
				thread :: so_far
			)
		) [] agent_handle_hash in
		List.iter (fun t ->
(*
			let handle = Rbtree.find agent_handle_hash t in
			(* Don't worry about killing the processes here, since the previous loop already determined that they have exited *)
			(* Actually, don't even worry about closing the handle, since the thread should take care of that *)
			Hashtbl.remove agent_handle_hash t
*)
(*
			(match Rbtree.find agent_handle_hash t with
				| Some (_,h) -> (

					let changed = Opt.set_process_priority h Opt.Normal in
					let kilt = Opt.kill_process_win h in
					let closed = Opt.close_handle h in
					p // sprintf "removed %d. Changed? %B Kilt? %B Closed? %B" t changed kilt closed

				)
				| None -> ()
			);
*)
			Rbtree.remove agent_handle_hash t
		) clean_these;

		(* Don't bother doing anything while it's empty *)
		while Rbtree.length agent_handle_hash = 0 do
			p "agent handle hash is empty";
			Condition.wait agent_handle_condition agent_handle_mutex;
		done;

		let len = Rbtree.length agent_handle_hash in
		p // sprintf "hash now has length %d" len;
		if len >= num_processors then (
			let ppp = max 1 ((num_processors + len - 1) / len) in (* This is Processes Per Processor, you see *)
			p // sprintf "giving each process %d processor%s" ppp (if ppp > 1 then "s" else "");
			let rec copy_mask cpme = (
				if cpme = 0 then (
					0
				) else (
					cpme lor (copy_mask (cpme lsl num_processors))
				)
			) in
			let proto_mask = copy_mask ((1 lsl ppp) - 1) in
			p // sprintf "process mask is %08X land %08X" proto_mask sys_mask;
			ignore // Rbtree.fold_left (fun j thread (job,handle) ->
(*
				let affinity_set_to = if j = len - 1 && len > num_processors then (
					(* This ensures that, if there are more x264 processes than we have processors, *)
					(* when one dies this x264 will take over its processor. It softens the blow *)
					(* before the affinity is recalculated *)
					p // sprintf "giving thread %2d FREE RANGE" thread;
					let ok = Opt.set_process_affinity handle sys_mask in
					p // sprintf "  %B" ok;
					sys_mask
				) else (
					let mood = (j * ppp) mod num_processors in
					let mask = proto_mask lsr mood in
					p // sprintf "giving thread %2d the mask %08X land %08X" thread mask sys_mask;
					let ok = Opt.set_process_affinity handle (mask land sys_mask) in
					p // sprintf "  %B" ok;
					mask land sys_mask
				) in

				let priority_set_to = if j >= num_processors then (
					(* Set all extra processes to idle priority *)
					p // sprintf "making thread %2d go to idle priority" thread;
					let ok = Opt.set_process_priority handle Opt.Idle in
					p // sprintf "  %B" ok;
					Opt.Idle
				) else (
					(* Set the priority back to what the user specified *)
					p // sprintf "making thread %2d go to priority %d" thread o.o_nice;
					let ok = Opt.set_process_priority handle orig_priority in
					p // sprintf "  %B" ok;
					orig_priority
				) in
*)
				let affinity_set_to = if j = len - 1 && len > num_processors then (
					(* This ensures that, if there are more x264 processes than we have processors, *)
					(* when one dies this x264 will take over its processor. It softens the blow *)
					(* before the affinity is recalculated *)
					sys_mask
				) else (
					let mood = (j * ppp) mod num_processors in
					let mask = proto_mask lsr mood in
					mask land sys_mask
				) in

				let priority_set_to = if j >= num_processors then (
					(* Set all extra processes to idle priority *)
					Opt.Idle
				) else (
					(* Set the priority back to what the user specified *)
					orig_priority
				) in

				let job_set_ok = Opt.set_job_information job {Opt.job_no_limit with Opt.job_object_limit_affinity = Some affinity_set_to; Opt.job_object_limit_priority_class = Some priority_set_to} in
				p // sprintf "Job set OK? %B" job_set_ok;

				succ j
			) 0 agent_handle_hash;
		) else if len > 0 then (
			(* Not many things running; just give them each full access *)
			p // sprintf "it's a bit sparse; removing affinity mask";
			Rbtree.iter (fun thread (job,handle) ->
(*
				let ok = Opt.set_process_affinity handle sys_mask in
				p // sprintf "  %d %B" thread ok;
*)
				let ok = Opt.set_job_information job {Opt.job_no_limit with Opt.job_object_limit_affinity = Some sys_mask; Opt.job_object_limit_priority_class = Some orig_priority} in
				p // sprintf "  %d %B" thread ok;
			) agent_handle_hash
		);
		Mutex.unlock agent_handle_mutex;
		Thread.delay 5.0;
		affinity_guts ()
	) in
	let affinity_thread = Thread.create (fun () ->
		try
			affinity_guts ()
		with
			e -> (p // sprintf "DIED WITH %s" (Printexc.to_string e))
	) () in

	(* Now the add_handle function *)
	fun p j h -> (
		let thread = Thread.id (Thread.self ()) in
		Mutex.lock agent_handle_mutex;
(*
		if Hashtbl.mem agent_handle_hash thread then (
			(* It looks like the thread has moved on without the old handle; kill it *)
			let old_handle = Hashtbl.find agent_handle_hash thread in
			ignore // Opt.kill_process_win old_handle;
			ignore // Opt.close_handle old_handle;
			Hashtbl.replace agent_handle_hash thread h;
		) else (
			Hashtbl.add agent_handle_hash thread h
		);
*)
		(match Rbtree.find agent_handle_hash thread with
			| Some (_,old_handle) -> (
				(* The thread has moved on; kill the handle *)
(*
				let kilt = Opt.kill_process_win old_handle in
				let closed = Opt.close_handle old_handle in
				p // sprintf "kilt? %B closed? %B" kilt closed;
*)
			)
			| _ -> ()
		);
		Rbtree.add agent_handle_hash thread (j,h);

		let () = p // sprintf "WINHANDLE now has %d handles in the hashtable" (Rbtree.length agent_handle_hash) in
		Condition.signal agent_handle_condition;
		Mutex.unlock agent_handle_mutex;
	)
) else (
	(* Don't set the affinity *)
	i.p ~name:"WINHANDLE " "user disabled affinity setting; not keeping track of handles";
	fun a b c -> ()
);;
















(*******************)
(* CONNECTION GUTS *)
(*******************)
let connection_guts s =
	let print_name = sprintf "AGENT %d " (Thread.id (Thread.self ())) in
	let p = i.p ~name:print_name in
	p "started up";

	let rec per_job () = (
		p "doing a job";
		(* Get the connection options *)
		let job = match s#recv with
			| ("OPTS",opts) -> (
				p // sprintf "Got job options %S" opts;
				let xml_or_not = trap_exception Xml.parse_string opts in

				let version_major_ref = ref None in
				let version_minor_ref = ref None in
				let filename_ref = ref None in
				let seek_ref = ref 0 in
				let frames_ref = ref 0 in
				let options_ref = ref None in
				let resx_ref = ref None in
				let resy_ref = ref None in
				let fpsn_ref = ref None in
				let fpsd_ref = ref None in
				let ntrx_ok_ref = ref false in
				let private_ref = ref None in

				let parse_opt = function
					| Xml.Element ("version", [("major",a); ("minor",b)], []) -> (version_major_ref := Some (int_of_string a); version_minor_ref := Some (int_of_string b))
					| Xml.Element ("filename", [("hash",h)], [Xml.PCData name]) -> filename_ref := Some (h, name)
					| Xml.Element ("seek", _, [Xml.PCData s]) -> seek_ref := int_of_string s
					| Xml.Element ("frames", _, [Xml.PCData f]) -> frames_ref := int_of_string f
					| Xml.Element ("options", _, [Xml.PCData o]) -> options_ref := Some o
					| Xml.Element ("options", _, []) -> options_ref := Some ""
					| Xml.Element ("resolution", [("x", x); ("y", y)], _) -> (resx_ref := Some (int_of_string x); resy_ref := Some (int_of_string y))
					| Xml.Element ("framerate", [("n", n); ("d", d)], _) -> (fpsn_ref := Some (int_of_string n); fpsd_ref := Some (int_of_string d))
					| Xml.Element ("ntrx_ok", _, [Xml.PCData m]) -> ntrx_ok_ref := int_of_string m <> 0
					| Xml.Element ("private", _, [Xml.PCData h]) when String.length h > 0 -> private_ref := Some (of_hex h)
					| Xml.Element (x,_,_) -> p // sprintf "unknown element <%s> in options %S" x opts
					| Xml.PCData x -> p // sprintf "why is there bare data in the option string? %S" opts
				in
				(match xml_or_not with
					| Normal (Xml.Element ("job", [], opt_list)) -> List.iter parse_opt opt_list
					| Normal _ -> p // sprintf "Got incorrect job description"
					| Exception e -> p // sprintf "Got exception %S" (Printexc.to_string e)
				);
				match (!version_major_ref, !version_minor_ref, !filename_ref, !options_ref, !resx_ref, !resy_ref, !fpsn_ref, !fpsd_ref) with
				| (Some version_major, Some version_minor, Some (hash, name), Some options, Some resx, Some resy, Some fpsn, Some fpsd) -> (
					(* Looks goob *)
					if version_major <> current_version_major || version_minor <> current_version_minor then (
						(* Not the same version *)
						p // sprintf "job version %d.%d does not match up with internal version %d.%d. Something odd may happen" version_major version_minor current_version_major current_version_minor;
					);
					{
						job_version_major = version_major;
						job_version_minor = version_minor;
						job_hash = of_hex hash;
						job_name = name;
						job_seek = !seek_ref;
						job_frames = !frames_ref;
						job_options = options;
						job_res_x = resx;
						job_res_y = resy;
						job_fps_n = fpsn;
						job_fps_d = fpsd;
						job_ntrx_ok = !ntrx_ok_ref;
						job_private = !private_ref;
					}
				)
				| (Some version_major, Some version_minor, _, _, _, _, _, _) -> (
					(* At least we got the version *)
					p // sprintf "Got versions, others wrong in %S" opts;
					failwith "bad job"
				)
				| _ -> (
					p // sprintf "Got something weird in %S" opts;
					failwith "bad job"
				)
			)
			| e -> unexpected_packet ~here:"receive job" e
		in
		(* Got job options *)

		(* Is the file local? *)
		let use_file_option = find_base job.job_name job.job_hash in
		(match use_file_option with
			| None -> p "No base found"
			| Some x -> p // sprintf "Found file \"%s\"" x
		);

		(* Figure out what type of encoding to use *)
		let encode = (
			match mp4_supported with
			| true -> (
				(* Write to MP4 since it's easy and supported *)
				p "encoding to MP4";
				Encode_mp4
			)
			| false -> (
				(* Use bitstream since it's not too hard either *)
				p "encoding to 264 bitstream";
				Encode_264
			)
			| _ -> (
				(* I guess MKV isn't used any more *)
				p "encoding to MKV";
				Encode_mkv
			)
		) in
(*		let encode = Encode_264 in*)

		match use_file_option with
		| Some f when Sys.os_type = "Win32" -> (
			(*********************************************************************)
			(* AGENT AGENT AGENT AGENT AGENT AGENT AGENT AGENT AGENT AGENT AGENT *)
			(*********************************************************************)
			(* Only on Windows (there is a whole bunch of Windows-specific code in here) *)
			s#send "PORT" "\x00\x00";

			(* Make a job worthy of storing x264 *)
			let job_name = sprintf "x264farm thread %d" (Thread.id (Thread.self ())) in
			let win_job = match Opt.create_job_object () job_name with
				| None -> failwith "can't create Windows job"
				| Some j -> j
			in

			let job_ok = (match encode with
				| Encode_264 -> (
					(*******************)
					(* 264 264 264 264 *)
					(*******************)

					(* Get an output file *)
					let temp_name = (
						let (n,h) = open_temp_file ~mode:[Open_binary] (Filename.concat o.o_temp_dir (sprintf "temp %d " job.job_seek)) ".264" in
						close_out h;
						n
					) in

					p // sprintf "got temp file %S" temp_name;

					let do_this = sprintf "%s %s --no-psnr --no-ssim --quiet --seek %d --frames %d -o \"%s\" \"%s\"" o.o_x264 job.job_options job.job_seek job.job_frames temp_name f in
					p // sprintf "doing this: %S" do_this;

(*					let win_handle = match Opt.create_process_win_single do_this (nice_to_priority o.o_nice) with*)
					let (win_handle, thread_handle) = match Opt.create_process_flags_win_single do_this (nice_to_priority o.o_nice) [Opt.Create_suspended] with
						| None -> failwith "can't create process"
						| Some p -> p
					in
				
					p "CREATED WIN PROCESS";
					add_handle p win_job win_handle;

					if Opt.assign_process_to_job_object win_job win_handle then (
						if Opt.resume_thread thread_handle then (
							p // sprintf "Process added to job %S" job_name;
							ignore // Opt.close_thread_handle thread_handle;
						) else (
							ignore // Opt.close_thread_handle thread_handle;
							failwith "can't resume x264 thread"
						)
					) else (
						ignore // Opt.close_thread_handle thread_handle;
						failwith "can't add x264 process to job"
					);

					(* Make another thread which waits for x264 to exit *)
					let t2 = Thread.create (fun () ->
						try
							let rec helper () = (
								match Opt.wait_for_process win_handle None with
								| Some false -> (
									p "CAP still waiting";
									helper ()
								)
								| Some true -> (
									p "CAP sees done";
									let do_this_cap = sprintf "\"\"%s\" --cap \"%s\"\"" Sys.executable_name temp_name in
									p // sprintf "CAP capping file with >>%s<<" do_this_cap;
									let cap_exit_code = Sys.command do_this_cap in
									p // sprintf "CAP finished with %d" cap_exit_code;
									()
								)
								| None -> (
									p "WAIT failed...";
									()
								)
							) in
							helper ()
						with
							e -> p // sprintf "WAIT failed with %S" (Printexc.to_string e)
					) () in
							

					let job_ok = (try

						let bs = new buffered_send (*_print:p*) s "GOPF" in

						let bytestream_read_thread = Thread.create (fun () ->
							try
								bytestream_read_guts p s job temp_name bs (Some win_handle)
							with
								e -> p // sprintf "264 read thread died with %S" (Printexc.to_string e)
						) () in

						Thread.join bytestream_read_thread;
						p "joined 264 read thread";

						Normal ()
					with
						e -> (
							p // sprintf "thread failed with %S; giving up" (Printexc.to_string e);
							Exception e
						)
					) in
(*
					let kilt = Opt.set_process_priority win_handle Opt.Normal && Opt.kill_process_win win_handle in
					p // sprintf "killed x264 %B" kilt;
*)
					(match trap_exception Sys.remove temp_name with
						| Normal _ -> ()
						| Exception _ -> (
							ignore // Thread.create (fun () ->
								try
									Thread.delay 10.0;
									Sys.remove temp_name
								with
									e -> p // sprintf "failed to remove temp file: %S" (Printexc.to_string e)
							) ()
						)
					);

					job_ok
(*
				match job_ok with
				| Exception e -> (raise e)
				| Normal () -> (per_job ())
*)
				)
				| Encode_mp4 -> (
					(*******************)
					(* MP4 MP4 MP4 MP4 *)
					(*******************)

					(* Get an output file *)
					let temp_name = (
						let (n,h) = open_temp_file ~mode:[Open_binary] (Filename.concat o.o_temp_dir (sprintf "temp %d " job.job_seek)) ".mp4" in
						close_out h;
						n
					) in

					p // sprintf "got temp file %S" temp_name;

					let do_this = sprintf "%s %s --no-psnr --no-ssim --quiet --seek %d --frames %d -o \"%s\" \"%s\"" o.o_x264 job.job_options job.job_seek job.job_frames temp_name f in
					p // sprintf "doing this: %S" do_this;
	
(*					let win_handle = match Opt.create_process_win_single do_this (nice_to_priority o.o_nice) with*)
					let (win_handle, thread_handle) = match Opt.create_process_flags_win_single do_this (nice_to_priority o.o_nice) [Opt.Create_suspended] with
						| None -> failwith "can't create process"
						| Some p -> p
					in
(*
					if Random.bool () || Random.bool () then (
						p "setting x264 priority to IDLE";
						ignore // Opt.set_process_priority win_handle Opt.Idle
					);
*)
					add_handle p win_job win_handle;
					p "CREATED WIN PROCESS";

					if Opt.assign_process_to_job_object win_job win_handle then (
						if Opt.resume_thread thread_handle then (
							p // sprintf "Process added to job %S" job_name;
							ignore // Opt.close_thread_handle thread_handle;
						) else (
							ignore // Opt.close_thread_handle thread_handle;
							failwith "can't resume x264 thread"
						)
					) else (
						ignore // Opt.close_thread_handle thread_handle;
						failwith "can't add x264 process to job"
					);

					let job_ok = (try

						let bs = new buffered_send (*_print:p*) s "GOPF" in

						(* If we don't have a private string, make one up *)
(*
						let priv_string = (match (job.job_ntrx_ok, job.job_private) with
							| (true, Some priv_string) -> (
								p "sending NTRX since controller said it was OK";
								s#send "NTRX" "";
								priv_string
							)
							| _ -> (
								(* Don't have a private string OR must send TRAX *)
								p "making up TRAX element since it's needed";
								let (tracks_string, priv_string) = make_tracks p job in
								s#send "TRAX" tracks_string;
								priv_string
							)
						) in

						let priv = Bitstream.get_private (new Bitstream.bg_string priv_string) in
*)
(*
						p // sprintf "profile   %d" priv.Bitstream.private_profile;
						p // sprintf "level     %d" priv.Bitstream.private_level;
						p // sprintf "NALU size %d" priv.Bitstream.private_nalu_size;
*)
						let mp4_read_thread = Thread.create (fun () ->
							try
								mp4_read_guts p s job temp_name (*priv*) bs (Some win_handle)
							with
								e -> p // sprintf "MP4 read thread died with %S" (Printexc.to_string e)
						) () in

						Thread.join mp4_read_thread;
						p "joined MP4 read thread";

						Normal ()
					with
						e -> (
							p // sprintf "thread failed with %S; giving up" (Printexc.to_string e);
							Exception e
						)
					) in
(*
					let kilt = Opt.set_process_priority win_handle Opt.Normal && Opt.kill_process_win win_handle in
					p // sprintf "killed x264 %B" kilt;
*)
					(match trap_exception Sys.remove temp_name with
						| Normal _ -> ()
						| Exception _ -> (
							ignore // Thread.create (fun () ->
								try
									Thread.delay 10.0;
									Sys.remove temp_name
								with
									e -> p // sprintf "failed to remove temp file: %S" (Printexc.to_string e)
							) ()
						)
					);

					job_ok
(*
				match job_ok with
				| Exception e -> (raise e)
				| Normal () -> (per_job ())
*)
				)
				| Encode_mkv -> (
					(*******************)
					(* MKV MKV MKV MKV *)
					(*******************)

					(* Get an output MKV file *)
					let temp_name = (
						let (n,h) = open_temp_file ~mode:[Open_binary] (Filename.concat o.o_temp_dir (sprintf "temp %d " job.job_seek)) ".mkv" in
						close_out h;
						n
					) in

					p // sprintf "got temp file %S" temp_name;

					let do_this = sprintf "%s %s --no-psnr --no-ssim --quiet --seek %d --frames %d -o \"%s\" \"%s\"" o.o_x264 job.job_options job.job_seek job.job_frames temp_name f in
					p // sprintf "doing this: %S" do_this;

(*					let win_handle = (match Opt.create_process_win_single do_this (nice_to_priority o.o_nice) with*)
					let (win_handle, thread_handle) = match Opt.create_process_flags_win_single do_this (nice_to_priority o.o_nice) [Opt.Create_suspended] with
						| None -> failwith "can't create process"
						| Some p -> p
					in

					add_handle p win_job win_handle;
					p "CREATED WIN PROCESS";

					if Opt.assign_process_to_job_object win_job win_handle then (
						if Opt.resume_thread thread_handle then (
							p // sprintf "Process added to job %S" job_name;
							ignore // Opt.close_thread_handle thread_handle;
						) else (
							ignore // Opt.close_thread_handle thread_handle;
							failwith "can't resume x264 thread"
						)
					) else (
						ignore // Opt.close_thread_handle thread_handle;
						failwith "can't add x264 process to job"
					);

					let job_ok = (try

						let bs = new buffered_send (*~print:p*) s "GOPF" in

						let mkv_read_thread = Thread.create (fun () ->
							try
(*								mkv_read_guts ()*)
								mkv_read_guts p s job temp_name bs (Some win_handle)
							with
								e -> p // sprintf "MKV read thread died with %S" (Printexc.to_string e)
						) () in


						Thread.join mkv_read_thread;
						p "joined MKV read thread";

						Normal ()
					with
						e -> (
							p // sprintf "thread failed with %S; give up" (Printexc.to_string e);
							Exception e
						)
					) in
(*
					let kilt = Opt.set_process_priority win_handle Opt.Normal && Opt.kill_process_win win_handle in
					p // sprintf "killed x264 %B" kilt;
*)
					(match trap_exception Sys.remove temp_name with
						| Normal _ -> ()
						| Exception _ -> (
							ignore // Thread.create (fun () ->
								try
									Thread.delay 10.0;
									Sys.remove temp_name
								with
									e -> p // sprintf "failed to remove temp file: %S" (Printexc.to_string e)
							) ()
						)
					);

					job_ok
(*
				match job_ok with
				| Exception e -> (raise e)
				| Normal () -> (per_job ())
*)
				)
			) in (* job_ok *)
			p // sprintf "Terminating job %S" job_name;
			p // sprintf "  %B" (Opt.terminate_job_object win_job 1);
			p // sprintf "  %B" (Opt.close_job_handle win_job);

			match job_ok with
			| Exception e -> raise e
			| Normal () -> per_job ()
		)
		| _ -> (
			(*********************************************************************)
			(* CONTROLLER CONTROLLER CONTROLLER CONTROLLER CONTROLLER CONTROLLER *)
			(*********************************************************************)

			(* Make a destupidifier *)
			(* Perhaps I should move all of the stupid code here... *)
			let destupidify = if stupid_pipe then (
				let n_ref = ref (Random.bits ()) in
				fun s pos len -> (
					n_ref := Opt.string_sanitize_text_mode s pos len !n_ref
				)
			) else (
				(* Ignore *)
				fun s pos len -> ()
			) in
			let destupidify_ptr = if stupid_pipe then (
				let n_ref = ref (Random.bits ()) in
				fun ptr pos len -> (
					n_ref := Opt.ptr_sanitize_text_mode ptr pos len !n_ref
				)
			) else (
				(* Ignore *)
				fun ptr pos len -> ()
			) in

			let video_sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
			p // sprintf "RCVBUF size was %d" (Unix.getsockopt_int video_sock Unix.SO_RCVBUF);
			ignore // trap_exception (fun n -> Unix.setsockopt_int video_sock Unix.SO_RCVBUF n) 1048576;
			p // sprintf "RCVBUF size now %d" (Unix.getsockopt_int video_sock Unix.SO_RCVBUF);

			let exit_x264_ref = ref false in
			let exit_x264_mutex = Mutex.create () in


			(* Get a good port *)
			let rec try_some_ports from goto now = (
				let found = (try
					Unix.bind video_sock (Unix.ADDR_INET (Unix.inet_addr_any, now));
					Some now
				with
					_ -> None
				) in
				match found with
				| Some x -> x
				| None when now >= goto -> try_some_ports from goto from
				| None -> try_some_ports from goto (succ now)
			) in
			let video_port = try_some_ports 49152 65535 56792 in (* Making up numbers! *)

			p // sprintf "connected to video port %d" video_port;

			(* Make a secret string to send to the controller to make sure the second connection is to the right computer (only for version >= 1.3) *)
			let key = if job.job_version_major >= 2 || job.job_version_minor >= 3 then (
				String.create 4
			) else (
				""
			) in
			for i = 0 to String.length key - 1 do
				key.[i] <- Char.chr (Random.int 256);
			done;

			(* Send the port number to send the video through *)
			let send_string = String.create 2 in
			Pack.packn send_string 0 video_port;
			if key = "" then (
				s#send "PORT" send_string;
			) else (
				(* Send a port and a key *)
				s#send "PKEY" (send_string ^ key);
			);


			(* Get a good file *)
			let temp_name = (
				let extension = match encode with
					| Encode_mkv -> ".mkv"
					| Encode_mp4 -> ".mp4"
					| Encode_264 -> ".264"
				in
				let (n,h) = open_temp_file ~mode:[Open_binary] (Filename.concat o.o_temp_dir (sprintf "temp %d " job.job_seek)) extension in
				close_out h;
				n
			) in

			p // sprintf "got temp file %S" temp_name;


			(* Connect to controller before starting up x264 *)
			Unix.listen video_sock 1;
			let rec accept_for_video () = (
				let (sock_connected, from) = Unix.accept video_sock in
				match (from, s#getpeername) with
				| (Unix.ADDR_INET (x,_), Unix.ADDR_INET (y,_)) (*when x = y*) -> (
					(* Check the key to make sure it matches *)
					let key_check = String.copy key in
					really_recv sock_connected key_check 0 (String.length key);
					if key_check = key then (
						p "got video connection";
						sock_connected
					) else (
						p // sprintf "got an invalid key %S <> %S" (to_hex key) (to_hex key_check);
						Unix.close sock_connected;
						accept_for_video ()
					)
				)
				| (Unix.ADDR_INET (x,y), _) -> (
					(* Something else connected? *)
					p // sprintf "got a connection from %s:%d? CLOSED" (Unix.string_of_inet_addr x) y;
					Unix.close sock_connected;
					accept_for_video ()
				)
				| _ -> (
					p "this connection makes no sense. CLOSED";
					Unix.close sock_connected;
					accept_for_video ()
				)
			) in
			let connected_video_sock = accept_for_video () in

(*
				let do_this = sprintf "%s %s --no-psnr --no-ssim --quiet --seek %d --frames %d -o \"%s\" %s && \"%s\" --cap \"%s\" || \"%s\" --cap \"%s\"" o.o_x264 job.job_options job.job_seek job.job_frames temp_name f Sys.executable_name temp_name Sys.executable_name temp_name in
*)

			(* NOW start up x264 *)
			let do_this = (
				let use_nice = o.o_nice <> 0 && (
					match test_command "nice -n 1 echo nice" with
					| Unix.WEXITED 0 -> ((*p "Exited 0";*) true)
					| _ -> false
				) in
				(* Use "nice" if it works, and make sure to add the "end of stream" NAL to any raw bitstream files *)
				match (encode, use_nice) with
				| (Encode_264, true)  -> sprintf "nice -n %d %s %s --fps %d/%d --no-psnr --no-ssim --quiet -o \"%s\" - %dx%d 2>&1 && \"%s\" --cap \"%s\" || \"%s\" --cap \"%s\"" o.o_nice o.o_x264 job.job_options job.job_fps_n job.job_fps_d temp_name job.job_res_x job.job_res_y Sys.executable_name temp_name Sys.executable_name temp_name
				| (Encode_264, false) -> sprintf            "%s %s --fps %d/%d --no-psnr --no-ssim --quiet -o \"%s\" - %dx%d 2>&1 && \"%s\" --cap \"%s\" || \"%s\" --cap \"%s\""          o.o_x264 job.job_options job.job_fps_n job.job_fps_d temp_name job.job_res_x job.job_res_y Sys.executable_name temp_name Sys.executable_name temp_name
				| (_, true) ->           sprintf "nice -n %d %s %s --fps %d/%d --no-psnr --no-ssim --quiet -o \"%s\" - %dx%d 2>&1"                                               o.o_nice o.o_x264 job.job_options job.job_fps_n job.job_fps_d temp_name job.job_res_x job.job_res_y
				| (_, false) ->          sprintf						"%s %s --fps %d/%d --no-psnr --no-ssim --quiet -o \"%s\" - %dx%d 2>&1"                                                        o.o_x264 job.job_options job.job_fps_n job.job_fps_d temp_name job.job_res_x job.job_res_y
			) in
			p // sprintf "doing this: %S" do_this;

			let put = Unix.open_process_out do_this in
			let put_descr = Unix.descr_of_out_channel put in

			(* The thread for capturing video from the controller *)
			let video_guts_as () = (
				p "starting up video thread";
				let f_write str n = (
					try
						let rec helper from num = (
							Mutex.lock exit_x264_mutex;
							let exit_x264 = !exit_x264_ref in
							Mutex.unlock exit_x264_mutex;
							if exit_x264 then (
								p "async thread told to exit";
								0
							) else (
								if num = 0 then from else (
									match Unix.select [connected_video_sock] [] [] 100.0 with
									| ([_],_,_) -> (
										(* OK *)
										let recv_bytes = Unix.recv connected_video_sock str from num [] in
										if recv_bytes = 0 then from else (
											helper (from + recv_bytes) (num - recv_bytes)
										)
									)
									| _ -> (
										raise Timeout
									)
								)
							)
						) in
						helper 0 n
					with
						_ -> 0
				) in
				let read = (
					let total_bytes = o.o_buffer_frames * (job.job_res_x * job.job_res_y * 3 / 2) in
(*					let num_buffers = max 2 ((total_bytes + 1048575) / 1048576) in*)

					let num_buffers = max 2 ((total_bytes + 1048575) / 1048576) in
					let buffer_size = max 4096 ((total_bytes + num_buffers - 1) / num_buffers) in

					p // sprintf "ASYNC using %d buffers of size %d" num_buffers buffer_size;
					async ~print:p num_buffers buffer_size f_write
				) in
				(try
					let rec keep_recving () = (
						Mutex.lock exit_x264_mutex;
						let exit_x264 = !exit_x264_ref in
						Mutex.unlock exit_x264_mutex;
						if exit_x264 then (
							p "video thread told to exit";
							()
						) else (
							match read () with
							| None -> () (* Nothing else *)
							| Some (str,n,f) -> (
								(* Got something *)
								(* Destupidify here since we can affect a large amount of data at a time *)
								destupidify str 0 n;
								output put str 0 n;
								f (); (* Tell async to recycle the string *)
								keep_recving ()
							)
						)
					) in
					keep_recving ()
				with
					e -> p // sprintf "video thread died with %S" (Printexc.to_string e)
				);

				p "closing PUT";
				close_out put;
			) in (* video_guts *)

			let video_guts_as3 () = (
				p "starting up video thread AS3";
				let exit_x264 () = (
					Mutex.lock exit_x264_mutex;
					let a = !exit_x264_ref in
					Mutex.unlock exit_x264_mutex;
					a
				) in
				let f_write str n = (
					try
						let rec helper from num = (
							if exit_x264 () then (
								p "async write told to exit";
								0
							) else (
								if num = 0 then from else (
									match Unix.select [connected_video_sock] [] [] 100.0 with
									| ([_],_,_) -> (
										(* OK *)
										let recv_bytes = Unix.recv connected_video_sock str from num [] in
										if recv_bytes = 0 then from else (
											p // sprintf "Got %d bytes" recv_bytes;
											helper (from + recv_bytes) (num - recv_bytes)
										)
									)
									| _ -> raise Timeout
								)
							)
						) in
						helper 0 n
					with
						_ -> 0
				) in
				let f_read str n = (
					if exit_x264 () then (
						p "async read told to exit";
						false
					) else (
						try
							destupidify str 0 n;
(*							output put str 0 n;*)
							Unix.write put_descr str 0 n;
							true
						with
							_ -> false
					)
				) in
				let f_read_fake str n = (
					p "!";
					true
				) in
				let total_bytes = o.o_buffer_frames * (job.job_res_x * job.job_res_y * 3 / 2) in
				let buffer_size = min Sys.max_string_length (max 4096 (4096 * ((total_bytes + 12287) / 12288))) in
				p // sprintf "async buffers: %d bytes in 3x%d" total_bytes buffer_size;
				async3 buffer_size f_write f_read;
				p "closing PUT";
				close_out put
			) in


			let video_guts_ptr () = (
				(* This uses two pointers to outside the OCAML stack, so as to do things multi-threaded and avoid the GC. While one pointer is being outputted to x264, the other receives data from the network. When they are both done they swap *)
				p "starting up video thread PTR";
				let exit_x264 () = (
					Mutex.lock exit_x264_mutex;
					let a = !exit_x264_ref in
					Mutex.unlock exit_x264_mutex;
					a
				) in

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

				let recv_in = ref None in
				let put_in = ref None in
				let g1 = make_gate 2 in
				let g2 = make_gate 2 in
				let recv_trade i = (
					g1 ();
					put_in := i;
					g2 ();
					!recv_in
				) in
				let put_trade i = (
					g1 ();
					recv_in := i;
					g2 ();
					!put_in
				) in


				let rec recv_guts ptr_perhaps = (
					match ptr_perhaps with
					| None -> (p "recv async thread exiting because put async thread exited")
					| Some ptr -> (
						if exit_x264 () then (
							p "recv async thread sees x264 died";
							ignore // recv_trade None
						) else (
							let rec helper f l = (
								if exit_x264 () then (
									p "recv async helper told to exit";
									0
								) else (
									if l = 0 then f else (
										match Unix.select [connected_video_sock] [] [] 100.0 with
										| ([_],_,_) -> (
											let recv_bytes = Opt.recv_ptr connected_video_sock ptr f l [] in
											if recv_bytes = 0 then f else (
												p // sprintf "Got %d bytes" recv_bytes;
												helper (f + recv_bytes) (l - recv_bytes)
											)
										)
										| _ -> raise Timeout
									)
								)
							) in
(*							match trap_exception (Opt.recv_ptr connected_video_sock ptr 0 (Opt.length_ptr ptr)) [] with*)
							match trap_exception_2 helper 0 (Opt.length_ptr ptr) with
							| Exception e -> (
								p // sprintf "recv async thread failed with %S" (Printexc.to_string e);
								ignore // recv_trade None
							)
							| Normal 0 -> (
								p "recv async thread found nothing in the pipe";
								ignore // recv_trade None
							)
							| Normal n -> (
								recv_guts (recv_trade (Some (ptr,n)))
							)
						)
					)
				) in
				let rec put_guts ptr_perhaps = (
					match ptr_perhaps with
					| None -> (p "put async thread exiting because recv async thread exited")
					| Some (ptr,n) -> (
						if exit_x264 () then (
							p "put async thread sees x264 died";
							ignore // recv_trade None
						) else (
							destupidify_ptr ptr 0 n;
							let rec keep_writing f l = (
								let wrote = Opt.write_ptr put_descr ptr f l in
								p // sprintf "Wrote %d bytes of %d at %d" wrote l f;
								if exit_x264 () then (
									false
								) else if wrote <> l then (
									keep_writing (f + wrote) (l - wrote)
								) else (
									true
								)
							) in
							match trap_exception_2 keep_writing 0 n with
							| Normal true -> put_guts (put_trade (Some ptr))
							| Normal false -> (
								p "put async thread found x264 died while outputting to it";
								ignore // put_trade None
							)
							| Exception e -> (
								p // sprintf "put async thread failed with %S" (Printexc.to_string e);
								ignore // put_trade None
							)
						)
					)
				) in

				(* Always use at least 2 frames for the buffer *)
				let bytes_per_buffer = (max 2 o.o_buffer_frames) * job.job_res_x * job.job_res_y * 3 / 4 in

				let recv_thread = Thread.create (fun () ->
					try
						recv_guts (Some (Opt.make_ptr bytes_per_buffer))
					with
						_ -> ()
				) () in
				let put_thread = Thread.create (fun () ->
					try
						put_guts (put_trade (Some (Opt.make_ptr bytes_per_buffer)))
					with
						_ -> ()
				) () in

				Thread.join recv_thread;
				Thread.join put_thread;

				close_out put
			) in




			let video_thread = Thread.create (fun () ->
				try
					match 1 with
					| 1 -> video_guts_ptr ()
					| _ -> video_guts_as3 ()
				with
					e -> (p // sprintf "video thread REALLY died with %S" (Printexc.to_string e))
			) () in

			(* FILE_READ_GUTS *)
			let bs = new buffered_send ~print:p s "GOPF" in
			let file_read_thread = Thread.create (fun () ->
				try
					match encode with
					| Encode_mkv ->	mkv_read_guts p s job temp_name bs None
					| Encode_mp4 -> mp4_read_guts p s job temp_name bs None
					| Encode_264 -> bytestream_read_guts p s job temp_name bs None
				with
					e -> p // sprintf "File read thread died with %S" (Printexc.to_string e)
			) () in

			(* After everything's done, then join the video thread *)
			Thread.join video_thread;
			p "joined video thread";
			Thread.join file_read_thread;
			p "joined file read thread";

			Unix.close video_sock;
			p "closed video sock";

			(* Now delete the temp file *)
			(match trap_exception Sys.remove temp_name with
				| Normal _ -> ()
				| Exception _ -> (
					ignore // Thread.create (fun () ->
						try
							Thread.delay 10.0;
							Sys.remove temp_name
						with
							e -> p // sprintf "failed to remove temp file: %S" (Printexc.to_string e)
					) ()
				)
			);

			per_job ()

		) (* use_file_option *)
	) in (* per_job *)

	per_job ()

;;


(****************************)
(* Start up the connections *)
(****************************)
let set_up_server () =
	let recv_sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
	let bound_port = (
		let rec try_some_ports from goto now = (
			let found = (try
				Unix.bind recv_sock (Unix.ADDR_INET (Unix.inet_addr_any, now));
				Some now
			with
				_ -> None
			) in
			match found with
			| Some x -> x
			| None when now >= goto -> try_some_ports from goto from
			| None -> try_some_ports from goto (succ now)
		) in
		try_some_ports 32768 49151 40701
	) in

	Unix.listen recv_sock 10;

	(* Send an initial ping *)
	(match Sys.os_type with
		| "Win32" -> send_ping bound_port (Unix.ADDR_INET (Obj.magic "\255\255\255\255", o.o_controller_port))
		| _       -> send_ping bound_port (Unix.ADDR_INET (Unix.inet_addr_of_string "255.255.255.255", o.o_controller_port))
	);

	(* Start up the UDP listener thread to respond to any broadcasts *)
	ignore // Thread.create (fun p ->
		try
			listen_guts p
		with
			e -> (i.p // sprintf "Listener thread died with %S" (Printexc.to_string e))
	) bound_port;

	while true do
		(* MAIN THREAD *)
		i.p "Accepting";
		let (sock, addr) = Unix.accept recv_sock in
		let (ip, port) = match addr with
			| Unix.ADDR_INET (x,y) -> ((Unix.string_of_inet_addr x), y)
			| _ -> ("UNIX???", 0)
		in

		i.p // sprintf "Got connection from %s:%d" ip port;

		let s = new net ~print:(i.p ~name:"NET: ") sock in

		ignore // Thread.create (fun s ->
			try
				(try
					connection_guts s
				with
					e -> (s#close; raise e)
				)
			with
				e -> (i.p // sprintf "Connection thread died with %S" (Printexc.to_string e))
		) s;
	done
;;
set_up_server ();;
