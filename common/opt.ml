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

type priority_t = Above_normal | Below_normal | High | Idle | Normal | Realtime;;
type process_flag_t =
	| Create_breakaway_from_job
	| Create_default_error_mode
	| Create_new_console
	| Create_new_process_group
	| Create_no_window
	| Create_protected_process (* Might as well add it for completeness *)
	| Create_preserve_code_authz_level
	| Create_separate_wow_vdm
	| Create_shared_wow_vdm
	| Create_suspended
	| Create_unicode_environment
	| Debug_only_this_process
	| Debug_process
	| Detached_process
	| Extended_startupinfo_present
;;
type process_handle;;
type thread_handle;;

external set_sparse : Unix.file_descr -> bool = "set_sparse"
external set_zero_data : Unix.file_descr -> int64 -> int64 -> bool = "set_zero_data"
external num_processors_c : unit -> int = "num_processors"

external create_process_win_c : string -> priority_t -> process_handle option = "create_process_win"
external create_process_flags_win_c : string -> priority_t -> process_flag_t list -> (process_handle * thread_handle) option = "create_process_flags_win"
external resume_thread : thread_handle -> bool = "resume_thread"
external kill_process_win : process_handle -> bool = "kill_process_win"
external get_exit_code_win_c : process_handle -> bool * int = "get_exit_code"

(* All handles close the same way *)
external close_handle : process_handle -> bool = "close_handle"
external close_thread_handle : thread_handle -> bool = "close_handle"

external exit_process_c : int -> unit = "exit_process"
external get_current_process : unit -> process_handle = "get_current_process"
let current_process = get_current_process ();;

external wait_for_process_c : process_handle -> int option -> int = "wait_for_process"
let wait_for_process a b =
	match wait_for_process_c a b with
	| 0 -> Some true  (* OK *)
	| 1 -> Some false (* Timeout *)
	| _ -> None       (* Fail? *)
;;

external create_process_win_full_c : string -> priority_t -> Unix.file_descr -> Unix.file_descr -> Unix.file_descr -> process_handle option = "create_process_win_full"
external create_process_flags_win_full_c : string -> priority_t -> process_flag_t list -> (Unix.file_descr * Unix.file_descr * Unix.file_descr) -> (process_handle * thread_handle) option = "create_process_flags_win_full"

type thread_priority_t =
	| Thread_priority_above_normal  (* +1 *)
	| Thread_priority_below_normal  (* -1 *)
	| Thread_priority_highest       (* +2 *)
	| Thread_priority_idle          (*  1 *)
	| Thread_priority_lowest        (* -2 *)
	| Thread_priority_time_critical (* 15 *)
	| Thread_priority_normal        (*  0 *)

external get_process_priority : process_handle -> priority_t option = "get_process_priority"
external set_process_priority : process_handle -> priority_t -> bool = "set_process_priority"
external get_thread_priority : unit -> thread_priority_t option = "get_thread_priority"
external set_thread_priority : thread_priority_t -> bool = "set_thread_priority"

external get_process_affinity : process_handle -> (int * int) option = "get_process_affinity"
external set_process_affinity : process_handle -> int -> bool = "set_process_affinity"

external string_replace_c : string -> char -> char -> unit = "string_replace_c";;
external string_replace_c_random : string -> char -> int -> int -> int -> int = "string_replace_c_random";;
(*external string_replace_c_random2 : string -> char -> int -> int -> int -> int = "string_replace_c_random2";;*)
external string_sanitize_text_mode_unsafe : string -> int -> int -> int -> int = "string_sanitize_text_mode";;
let string_sanitize_text_mode str off len nm1 =
	if (
		off + len > String.length str ||
		off < 0 ||
		len < 0
	) then (
		invalid_arg "Opt.string_sanitize_text_mode";
	) else (
		string_sanitize_text_mode_unsafe str off len nm1
	)
;;

external fetch_0001_c : string -> int -> int -> int -> bool * int * int = "fetch_0001";;
let fetch_0001 str off len state =
	if off + len > String.length str || off < 0 || len < 0 then (
		invalid_arg "Opt.fetch_0001";
	) else (
		let (found, at, out_state) = fetch_0001_c str off len state in
		if found then (
			(Some at, out_state)
		) else (
			(None, out_state)
		)
	)
;;

external print_stuff : 'a -> unit = "print_stuff"

let num_processors () =
	let n = num_processors_c () in
	if n = 0 then (
		(* Have to do some other stuff *)
		try
			let proc_list = Sys.readdir "/proc/acpi/processor" in
			Array.length proc_list
		with
			_ -> 1
	) else (
		n
	)
;;


let create_process_win =
	let has_spaces x = String.contains x ' ' in
	fun name arg_array priority -> (
		let s = Array.fold_left (fun so_far gnu ->
			if has_spaces gnu then (
				(* Quotify *)
				so_far ^ " \"" ^ String.escaped gnu ^ "\""
			) else (
				(* Don't quotify *)
				so_far ^ " " ^ gnu
			)
		) ("\"" ^ String.escaped name ^ "\"") arg_array in
		create_process_win_c s priority
	)
;;

let create_process_win_single = create_process_win_c;;

let create_process_win_full_single s p =
	let (i_out, i_in) = Unix.pipe () in
	let (o_out, o_in) = Unix.pipe () in
	let (e_out, e_in) = Unix.pipe () in
	Unix.set_close_on_exec i_in;
	Unix.set_close_on_exec o_out;
	Unix.set_close_on_exec e_out;
	match create_process_win_full_c s p i_out o_in e_in with
	| Some ph -> Some (ph, i_in, o_out, e_out)
	| None -> (
		Unix.close i_out;
		Unix.close i_in;
		Unix.close o_out;
		Unix.close o_in;
		Unix.close e_out;
		Unix.close e_in;
		None
	)
;;
let create_process_win_full =
	let has_spaces x = String.contains x ' ' in
	fun name arg_array priority -> (
		let s = Array.fold_left (fun so_far gnu ->
			if has_spaces gnu then (
				(* Quotify *)
				so_far ^ " \"" ^ String.escaped gnu ^ "\""
			) else (
				(* Don't quotify *)
				so_far ^ " " ^ gnu
			)
		) ("\"" ^ String.escaped name ^ "\"") arg_array in
		create_process_win_full_single s priority
	)
;;

(* Now with flags *)
let create_process_flags_win =
	let has_spaces x = String.contains x ' ' in
	fun name arg_array priority -> (
		let s = Array.fold_left (fun so_far gnu ->
			if has_spaces gnu then (
				(* Quotify *)
				so_far ^ " \"" ^ String.escaped gnu ^ "\""
			) else (
				(* Don't quotify *)
				so_far ^ " " ^ gnu
			)
		) ("\"" ^ String.escaped name ^ "\"") arg_array in
		create_process_flags_win_c s priority
	)
;;

let create_process_flags_win_single = create_process_flags_win_c;;

let create_process_flags_win_full_single s p f =
	let (i_out, i_in) = Unix.pipe () in
	let (o_out, o_in) = Unix.pipe () in
	let (e_out, e_in) = Unix.pipe () in
	Unix.set_close_on_exec i_in;
	Unix.set_close_on_exec o_out;
	Unix.set_close_on_exec e_out;
	match create_process_flags_win_full_c s p f (i_out, o_in, e_in) with
	| Some ph -> Some (ph, i_in, o_out, e_out)
	| None -> (
		Unix.close i_out;
		Unix.close i_in;
		Unix.close o_out;
		Unix.close o_in;
		Unix.close e_out;
		Unix.close e_in;
		None
	)
;;
let create_process_flags_win_full =
	let has_spaces x = String.contains x ' ' in
	fun name arg_array priority flags -> (
		let s = Array.fold_left (fun so_far gnu ->
			if has_spaces gnu then (
				(* Quotify *)
				so_far ^ " \"" ^ String.escaped gnu ^ "\""
			) else (
				(* Don't quotify *)
				so_far ^ " " ^ gnu
			)
		) ("\"" ^ String.escaped name ^ "\"") arg_array in
		create_process_flags_win_full_single s priority flags
	)
;;





(* This has a pretty stupid return type. I should probably fix it sometime *)
let get_exit_code_win h =
	let (ok,code) = get_exit_code_win_c h in
	match (ok,code) with
	| (false,_) -> None
	| (true,259) -> Some None
	| (true,x) -> Some (Some x)
;;

let exit_process =
	match Sys.os_type with
	| "Win32" -> exit_process_c
	| _ -> exit
;;

(*
 * Turns a requested final base priority into a (process,thread) priority pair
 * which yields that final base priority.
 * Note that the Normal priority class is not used, due to it changing with respect to
 * whether or not the process is in the foreground. (Normal,Normal) gives 9/7 (fg/bg)
 * Also note that the priorities [-7,-3] and [3,6] are not used and therefore
 * bases [17,21] and [26,30] are unavailable.
 * Base priority 0 is only available to the page zeroer, so is not included here
 *)
let priorities_of_base_priority = function
	|  1 -> (Idle, Thread_priority_idle)
	|  2 -> (Idle, Thread_priority_lowest)
	|  3 -> (Idle, Thread_priority_below_normal)
	|  4 -> (Idle, Thread_priority_normal)
	|  5 -> (Below_normal, Thread_priority_below_normal)
	|  6 -> (Below_normal, Thread_priority_normal)
	|  7 -> (Below_normal, Thread_priority_above_normal)
	|  8 -> (Below_normal, Thread_priority_highest)
(*	|  8 -> (Above_normal, Thread_priority_lowest)*) (* Below normal, highest looks better *)
	|  9 -> (Above_normal, Thread_priority_below_normal)
	| 10 -> (Above_normal, Thread_priority_normal)
	| 11 -> (Above_normal, Thread_priority_above_normal)
	| 12 -> (High, Thread_priority_below_normal)
	| 13 -> (High, Thread_priority_normal)
	| 14 -> (High, Thread_priority_above_normal)
	| 15 -> (High, Thread_priority_highest)
	| 16 -> (Realtime, Thread_priority_idle)
	| 22 -> (Realtime, Thread_priority_below_normal)
	| 23 -> (Realtime, Thread_priority_normal)
	| 24 -> (Realtime, Thread_priority_above_normal)
	| 25 -> (Realtime, Thread_priority_highest)
	| 31 -> (Realtime, Thread_priority_time_critical)
	|  _ -> (Normal, Thread_priority_normal) (* Might as well... *)
;;

(**************)
(* C PTR OPTS *)
(**************)
type ptr;;
external make_ptr : int -> ptr = "make_ptr";;

external length_ptr : ptr -> int = "length_ptr";;

external c_ptr_of_string : string -> ptr -> unit = "ptr_of_string";;
let ptr_of_string s =
	let p = make_ptr (String.length s) in
	c_ptr_of_string s p;
	p
;;

external c_string_of_ptr : ptr -> string -> unit = "string_of_ptr";;
let string_of_ptr p =
	let s = String.create (length_ptr p) in
	c_string_of_ptr p s;
	s
;;

external send_ptr_unsafe : Unix.file_descr -> ptr -> int -> int -> Unix.msg_flag list -> int = "send_ptr";;
let send_ptr fd ptr ofs len =
	if ofs < 0 || len < 0 || ofs + len > length_ptr ptr then (
		invalid_arg "Opt.send_ptr"
	) else (
		send_ptr_unsafe fd ptr ofs len
	)
;;

external recv_ptr_unsafe : Unix.file_descr -> ptr -> int -> int -> Unix.msg_flag list -> int = "recv_ptr";;
let recv_ptr fd ptr ofs len =
	if ofs < 0 || len < 0 || ofs + len > length_ptr ptr then (
		invalid_arg "Opt.recv_ptr"
	) else (
		recv_ptr_unsafe fd ptr ofs len
	)
;;

external ptr_sanitize_text_mode_unsafe : ptr -> int -> int -> int -> int = "ptr_sanitize_text_mode";;
let ptr_sanitize_text_mode ptr off len nm1 =
	if (
		off + len > length_ptr ptr ||
		off < 0 ||
		len < 0
	) then (
		invalid_arg "Opt.ptr_sanitize_text_mode";
	) else (
		ptr_sanitize_text_mode_unsafe ptr off len nm1
	)
;;

external write_ptr_unsafe : Unix.file_descr -> ptr -> int -> int -> int = "write_ptr";;
let write_ptr fd ptr ofs len =
	if ofs < 0 || len < 0 || ofs + len > length_ptr ptr then (
		Printf.printf "Bad args: ofs=%d, len=%d\n%!" ofs len;
		invalid_arg "Opt.write_ptr"
	) else (
		write_ptr_unsafe fd ptr ofs len
	)
;;

(********)
(* JOBS *)
(********)
type job_handle;;

external create_job_object_c : unit -> string -> int * job_handle = "create_job_object";;
let create_job_object a b =
	match create_job_object_c a b with
	| (0,n) -> Some n
	| (e,_) -> None
;;

external assign_process_to_job_object : job_handle -> process_handle -> bool = "assign_process_to_job_object";;

external terminate_job_object : job_handle -> int -> bool = "terminate_job_object";;

(* This is the same as closing any other handle *)
external close_job_handle : job_handle -> bool = "close_handle";;

type job_limit_t = {
	job_object_limit_active_process : int option;
	job_object_limit_affinity : int option;
	job_object_limit_breakaway_ok : bool;
	job_object_limit_die_on_unhandled_exception : bool;
	job_object_limit_job_memory : int option;
	job_object_limit_job_time : float option;
	job_object_limit_kill_on_job_close : bool;
	job_object_limit_preserve_job_time : bool;
	job_object_limit_priority_class : priority_t option;
	job_object_limit_process_memory : int option;
	job_object_limit_process_time : float option;
	job_object_limit_scheduling_class : int option;
	job_object_limit_silent_breakaway_ok : bool;
	job_object_limit_workingset : (int * int) option;
};;
let job_no_limit = {
	job_object_limit_active_process = None;
	job_object_limit_affinity = None;
	job_object_limit_breakaway_ok = false;
	job_object_limit_die_on_unhandled_exception = false;
	job_object_limit_job_memory = None;
	job_object_limit_job_time = None;
	job_object_limit_kill_on_job_close = false;
	job_object_limit_preserve_job_time = false;
	job_object_limit_priority_class = None;
	job_object_limit_process_memory = None;
	job_object_limit_process_time = None;
	job_object_limit_scheduling_class = None;
	job_object_limit_silent_breakaway_ok = false;
	job_object_limit_workingset = None;
};;
external set_job_information : job_handle -> job_limit_t -> bool = "set_job_extended_information";;

