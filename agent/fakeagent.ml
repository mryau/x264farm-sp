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









(* Stop compaction in order to increase coherency with Opt.string_sanitize_text_mode *)
let default_overhead = (Gc.get ()).Gc.max_overhead;;
Gc.set {(Gc.get ()) with Gc.max_overhead = 1000001};;


let version_string = "x264farm-sp 1.06";;
if Sys.word_size = 32 then (
	printf "%s agent\n%!" version_string
) else (
	printf "%s agent %d-bit\n%!" version_string Sys.word_size
);;

(* These are the transmission version numbers, NOT the program version numbers *)
let current_version_major = 1;;
let current_version_minor = 2;;

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
(* Or you can go the other way
 * This is good for doing something like:
 * 3 /? succ /? succ -> 5
 * without using parenthesis. If you were to use //, it would have problems:
 * succ // succ // 3 -> (succ // succ) // 3 -> ERROR *)
let (/?) a b = b a;;

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
ignore // trap_exception2 Sys.set_signal Sys.sigpipe Sys.Signal_ignore;;


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

















let p = print_endline;;

let stupid_pipe = false;;


let do_this () =

			let destupidify = if stupid_pipe then (
				let n_ref = ref (Random.bits ()) in
				fun s pos len -> (
					n_ref := Opt.string_sanitize_text_mode s pos len !n_ref
				)
			) else (
				(* Ignore *)
				fun s pos len -> ()
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




			(* Connect to controller before starting up x264 *)
			Unix.listen video_sock 1;
			let rec accept_for_video () = (
				let (sock_connected, from) = Unix.accept video_sock in
				p "get video connection";
				sock_connected
			) in
			let connected_video_sock = accept_for_video () in

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
					let total_bytes = 4055040 in
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
(*								output put str 0 n;*)
								f (); (* Tell async to recycle the string *)
								keep_recving ()
							)
						)
					) in
					keep_recving ()
				with
					e -> p // sprintf "video thread died with %S" (Printexc.to_string e)
				);

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
(*							Unix.write put_descr str 0 n;*)
							true
						with
							_ -> false
					)
				) in
				let f_read_fake str n = (
					p "!";
					true
				) in
				let total_bytes = 4055040 in
				let buffer_size = min Sys.max_string_length (max 4096 (4096 * ((total_bytes + 12287) / 12288))) in
				p // sprintf "async buffers: %d bytes in 3x%d" total_bytes buffer_size;
				async3 buffer_size f_write f_read;
			) in

			let video_thread = Thread.create (fun () ->
				try
					video_guts_as3 ()
				with
					e -> (p // sprintf "video thread REALLY died with %S" (Printexc.to_string e))
			) () in

			(* After everything's done, then join the video thread *)
			Thread.join video_thread;
			p "joined video thread";

			Unix.close video_sock;
			p "closed video sock";

;;

while true do
	do_this ()
done;;

