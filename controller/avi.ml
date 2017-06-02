(* PTR STUFF *)
(*
type ptr;;
external make_ptr : int -> ptr = "make_ptr";;

external length_ptr : ptr -> int = "length_ptr";;

external send_all_ptr_c : Unix.file_descr -> ptr -> unit = "send_all_ptr";;
let send_all_ptr = match Sys.os_type with
	| "Win32" -> send_all_ptr_c
	| _ -> (fun a b -> invalid_arg "Avi.send_all_ptr not implemented")
;;

external send_ptr_c : Unix.file_descr -> ptr -> int -> int -> int = "send_ptr";;
let send_ptr = match Sys.os_type with
	| "Win32" -> (fun fd ptr ofs len ->
		if ofs < 0 || len < 0 || ofs > length_ptr ptr then (
			invalid_arg "Avi.send_ptr"
		) else (
			send_ptr_c fd ptr ofs len
		)
	)
	| _ -> (fun a b c d -> invalid_arg "Avi.send_ptr not implemented")
;;

external write_ptr : Unix.file_descr -> ptr -> unit = "write_ptr";;
*)
(* END PTR STUFF *)


type avi_t;;
type t = {
	avi : avi_t;
	w : int;
	h : int;
	n : int;
	d : int;
	i : int;
	bytes_per_frame : int;
};;
exception Avi_failure of string;;


external info_avi_c : string -> (int * int * int * int * int) option = "info_avi";;
let info_avi str = match info_avi_c str with
	| None -> raise (Avi_failure "Error info opening file")
	| Some x -> x
;;

external init_avi : unit -> unit = "init_avi";;
external exit_avi : unit -> unit = "exit_avi";;

external open_avi_c : string -> t option = "open_avi";;
let open_avi str = match open_avi_c str with
	| None -> raise (Avi_failure "Error open opening file")
	| Some x -> x
;;

external close_avi : t -> unit = "close_avi";;

external blit_frame_unsafe : t -> int -> string -> int -> int = "blit_avi_frame_unsafe";;

let bm = Mutex.create ();;
let blit_frame t n s f =
	if f < 0 || f > String.length s - t.bytes_per_frame then (
		invalid_arg "Avi.blit_frame";
	) else (
		blit_frame_unsafe t n s f
	)
(*
	Mutex.lock bm;
	let g = blit_frame_unsafe t n s f in
	Mutex.unlock bm;
	g
*)
;;



external blit_frames_unsafe : t -> int -> int -> string -> int -> int = "blit_avi_frames_unsafe";;
let blit_frames t f n s o =
	if o < 0 || o > String.length s - t.bytes_per_frame then (
		invalid_arg "Avi.blit_frames";
	) else (
		let real_n = String.length s / t.bytes_per_frame in
		blit_frames_unsafe t f real_n s o
	)
;;


(* AVI PTR *)
external ptr_avi_frame_unsafe : t -> int -> Opt.ptr -> int = "ptr_avi_frame_unsafe";;
let ptr_avi_frame t f p =
	if t.bytes_per_frame > Opt.length_ptr p then (
		invalid_arg "Avi.ptr_avi_frame"
	) else (
		ptr_avi_frame_unsafe t f p
	)
;;

