type script_t;;
type clip_t;;
type t = {
	script : script_t;
	clip : clip_t;
	w : int;
	h : int;
	n : int;
	d : int;
	i : int;
	bytes_per_frame : int;
};;
type pos_t = {
	mutable frame : int;
	mutable line : int;
};;
type ('a,'b) aorb = A of 'a | B of 'b;;
exception Avs_failure of string;;

(*external info : string -> (int * int * int * int * int) option = "info";;*)

external test : string -> string = "test";;

external open_avs_c : string -> int -> (t,string) aorb = "open_avs";;
let open_avs ?(mem=0) file =
	match open_avs_c file mem with
	| A q -> q
	| B error -> raise (Avs_failure error)
;;

external close_avs : t -> unit = "close_avs";;

(* This gets info about an already-opened file *)
external get_info : t -> int * int * int * int * int = "get_info";;

(* This opens a file to get info *)
external info_c : string -> ((int * int * int * int * int), string) aorb = "info";;
let info file =
	match info_c file with
	| A q -> q
	| B error -> raise (Avs_failure error)
;;

(*external get_frame : t -> int -> string = "get_frame";;*)

external blit_frames_unsafe : t -> int -> int -> string -> int -> unit = "blit_frames_unsafe";;
external blit_frame_unsafe : t -> int -> string -> int -> unit = "blit_frame_unsafe";;
external blit_y_unsafe : t -> int -> string -> int -> unit = "blit_y_unsafe";;
external blit_uv_unsafe : t -> int -> string -> int -> unit = "blit_uv_unsafe";;
external blit_y_line_unsafe : t -> int -> int -> string -> int -> unit = "blit_y_line_unsafe";;
external blit_u_line_unsafe : t -> int -> int -> string -> int -> unit = "blit_u_line_unsafe";;
external blit_v_line_unsafe : t -> int -> int -> string -> int -> unit = "blit_v_line_unsafe";;

(*external dither : int -> int = "dither";;*)

let pos_frame n = {
	frame = n;
	line = 0;
};;

let pos_start () = {
	frame = 0;
	line = 0;
};;

(* pos.line is [0,h) for Y, [h,3h/2) for u, [3h/2,2h) for v *)
(* Changes pos and returns the number of bytes read *)
let bm = Mutex.create ();;
let blit ?last_frame script pos str off len =
	let last = match last_frame with
		| None -> script.i
		| Some x -> min script.i x
	in
	if (
		len < script.w || (* I don't really want to support sub-line reads *)
		off < 0 ||
		off > String.length str - len
	) then (
		(* Length and offset don't add up *)
		invalid_arg "Avs.blit";
	) else if pos.frame = script.i && pos.line = 0 && len > 0 then (
		(* Fell off the end of the script *)
		raise End_of_file
	) else if pos.frame < 0 || pos.line < 0 || pos.frame >= script.i || pos.line >= script.h lsl 1 then (
		(* Frames don't add up *)
		invalid_arg "Avs.blit";
	) else if pos.frame >= last then (
		(* Over the edge *)
		0
	) else if pos.line = 0 && len >= script.bytes_per_frame then (
		(* Can read one or more whole frames *)
		(* Make sure it doesn't run off the end of the encode *)
		let num_frames = min (last - pos.frame) (len / script.bytes_per_frame) in
		Mutex.lock bm;
		blit_frames_unsafe script pos.frame num_frames str off;
		Mutex.unlock bm;
		pos.frame <- pos.frame + num_frames;
		script.bytes_per_frame * num_frames
	) else if pos.line = 0 && len >= script.h * script.w then (
		(* Can read at least the Y data *)
		Mutex.lock bm;
		blit_y_unsafe script pos.frame str off;
		Mutex.unlock bm;
		pos.line <- script.h;
		script.w * script.h
	) else if pos.line = script.h && len >= (script.h * script.w) lsr 1 then (
		(* Can read the u and v planes *)
		Mutex.lock bm;
		blit_uv_unsafe script pos.frame str off;
		Mutex.unlock bm;
		pos.line <- 0;
		pos.frame <- pos.frame + 1;
		(script.w * script.h) lsr 1
	) else if pos.line < script.h then (
		(* Read one Y line *)
		Mutex.lock bm;
		blit_y_line_unsafe script pos.frame pos.line str off;
		Mutex.unlock bm;
		pos.line <- pos.line + 1;
		script.w
	) else if pos.line < (script.h * 3) lsr 1 then (
		(* Read one u line *)
		Mutex.lock bm;
		blit_u_line_unsafe script pos.frame (pos.line - script.h) str off;
		Mutex.unlock bm;
		pos.line <- pos.line + 1;
		script.w lsr 1
	) else (
		(* Read one v line *)
		Mutex.lock bm;
		blit_v_line_unsafe script pos.frame (pos.line - ((script.h * 3) lsr 1)) str off;
		Mutex.unlock bm;
		pos.line <- pos.line + 1;
		if pos.line = script.h lsl 1 then (
			pos.line <- 0;
			pos.frame <- pos.frame + 1;
		);
		script.w lsr 1
	)
;;

let blit_nolock ?last_frame script pos str off len =
	let last = match last_frame with
		| None -> script.i
		| Some x -> min script.i x
	in
	if (
		len < script.w || (* I don't really want to support sub-line reads *)
		off < 0 ||
		off > String.length str - len
	) then (
		(* Length and offset don't add up *)
		invalid_arg "Avs.blit_nolock";
	) else if pos.frame = script.i && pos.line = 0 && len > 0 then (
		(* Fell off the end of the script *)
		raise End_of_file
	) else if pos.frame < 0 || pos.line < 0 || pos.frame >= script.i || pos.line >= script.h lsl 1 then (
		(* Frames don't add up *)
		invalid_arg "Avs.blit";
	) else if pos.frame >= last then (
		(* Over the edge *)
		0
	) else if pos.line = 0 && len >= script.bytes_per_frame then (
		(* Can read one or more whole frames *)
		(* Make sure it doesn't run off the end of the encode *)
		let num_frames = min (last - pos.frame) (len / script.bytes_per_frame) in
		blit_frames_unsafe script pos.frame num_frames str off;
		pos.frame <- pos.frame + num_frames;
		script.bytes_per_frame * num_frames
	) else if pos.line = 0 && len >= script.h * script.w then (
		(* Can read at least the Y data *)
		blit_y_unsafe script pos.frame str off;
		pos.line <- script.h;
		script.w * script.h
	) else if pos.line = script.h && len >= (script.h * script.w) lsr 1 then (
		(* Can read the u and v planes *)
		blit_uv_unsafe script pos.frame str off;
		pos.line <- 0;
		pos.frame <- pos.frame + 1;
		(script.w * script.h) lsr 1
	) else if pos.line < script.h then (
		(* Read one Y line *)
		blit_y_line_unsafe script pos.frame pos.line str off;
		pos.line <- pos.line + 1;
		script.w
	) else if pos.line < (script.h * 3) lsr 1 then (
		(* Read one u line *)
		blit_u_line_unsafe script pos.frame (pos.line - script.h) str off;
		pos.line <- pos.line + 1;
		script.w lsr 1
	) else (
		(* Read one v line *)
		blit_v_line_unsafe script pos.frame (pos.line - ((script.h * 3) lsr 1)) str off;
		pos.line <- pos.line + 1;
		if pos.line = script.h lsl 1 then (
			pos.line <- 0;
			pos.frame <- pos.frame + 1;
		);
		script.w lsr 1
	)
;;





