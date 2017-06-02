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
let ( ||| ) = Int64.logor;;
let ( &&| ) = Int64.logand;;
let (  <| ) = Int64.shift_left;;
let (  >| ) = Int64.shift_right_logical;;
let (  @| ) a b = Int64.of_int (Char.code (a.[b]));;
let (  !| ) a   = Char.chr (Int64.to_int a);;
*)

exception Invalid_integer;;
exception Invalid_integer_length of int;;
exception Invalid_float_length of int;;
exception Unsupported_float;;
exception Invalid_ID of int64;;
exception Unknown_ID of (int * int64);;
exception ID_string_too_short of (int * int);;
exception Invalid_element_position of string;;
exception String_too_large of (int64 * int64);;
exception Loop_end;;
exception Matroska_error of string;;

let ( +|  ) = Int64.add;;
let ( -|  ) = Int64.sub;;
let ( *|  ) = Int64.mul;;
let ( /|  ) = Int64.div;;
let ( <|  ) = Int64.shift_left;;
let ( >|  ) = Int64.shift_right_logical;;
let ( >|- ) = Int64.shift_right;;
let ( ||| ) = Int64.logor;;
let ( &&| ) = Int64.logand;;
let ( !|  ) = Int64.of_int;;
let ( !-  ) = Int64.to_int;;

let to_hex s =
	let result = String.create (2 * String.length s) in
	for i = 0 to String.length s - 1 do
		String.blit (Printf.sprintf "%02X" (int_of_char s.[i])) 0 result (2*i) 2;
	done;
	result
;;


(* Input from file *)

let rec input_uint so_far f len =
	if len > 8 || len < 0 then (
		raise (Invalid_integer_length len)
	) else if len = 0 then (
		so_far
	) else (
		let b = input_byte f in
		input_uint ((so_far <| 8) ||| ( !| b )) f (pred len)
	)
;;

let rec input_sint so_far f len =
	if len > 8 || len < 0 then (
		raise (Invalid_integer_length len)
	) else if len = 0 then (
		so_far
	) else (
		(* This extends the sign to the end of the int64 *)
		let b = ( !| (input_byte f)) <| 56 >|- 56 in
		input_uint b f (pred len)
	)
;;

(* Don't bother with 64-bit ints here; instead convert at the end *)
(* In order to overflow a 31-bit OCaml int, this would need over 4000 bytes *)
let rec input_xiph so_far f =
	let b = input_byte f in
	if b = 255 then (
		input_xiph (so_far + 255) f
	) else (
		!| (so_far + b)
	)
;;

let input_len f =
	let b = input_byte f in
	let (len, mask) = if b >= 0x80 then (
		(* 1 byte *)
		( !| (b land 0x7F), 0x7FL)
	) else if b >= 0x40 then (
		(* 2 bytes *)
		(input_uint !|(b land 0x3F) f 1, 0x3FFFL)
	) else if b >= 0x20 then (
		(* 3 bytes *)
		(input_uint !|(b land 0x1F) f 2, 0x1FFFFFL)
	) else if b >= 0x10 then (
		(* 4 bytes *)
		(input_uint !|(b land 0x0F) f 3, 0x0FFFFFFFL)
	) else if b >= 0x08 then (
		(* 5 bytes *)
		(input_uint !|(b land 0x07) f 4, 0x07FFFFFFFFL)
	) else if b >= 0x04 then (
		(* 6 bytes *)
		(input_uint !|(b land 0x03) f 5, 0x03FFFFFFFFFFL)
	) else if b >= 0x02 then (
		(* 7 bytes *)
		(input_uint !|(b land 0x01) f 6, 0x01FFFFFFFFFFFFL)
	) else if b  = 0x01 then (
		(* 8 bytes *)
		(input_uint 0L              f 7, 0x00FFFFFFFFFFFFFFL)
	) else (
		(* b = 0x00; INVALID *)
		raise Invalid_integer
	) in
	if len = mask then None else Some len
;;

let input_float f len =
	if len = 4 then (
		let f64 = input_sint 0L f 4 in
		Int32.float_of_bits (Int64.to_int32 f64)
	) else if len = 8 then (
		Int64.float_of_bits (input_sint 0L f 8)
	) else (
		raise (Invalid_float_length len)
	)
;;

(* ID is now an int, not an int64 *)
let input_id f =
	let b = input_byte f in
	if b >= 0x80 then (
		(* 1 byte *)
		b land 0x7F
	) else if b >= 0x40 then (
		(* 2 bytes *)
		let c = input_byte f in
		((b land 0x3F) lsl 8) lor c
	) else if b >= 0x20 then (
		(* 3 bytes *)
		let c = input_byte f in
		let d = input_byte f in
		((b land 0x1F) lsl 16) lor (c lsl 8) lor d
	) else if b >= 0x10 then (
		(* 4 bytes *)
		let c = input_byte f in
		let d = input_byte f in
		let e = input_byte f in
		((b land 0x0F) lsl 24) lor (c lsl 16) lor (d lsl 8) lor e
	) else (
		raise (Invalid_ID (LargeFile.pos_in f))
	)
;;


let input_id_and_length f =
	let pos_now = LargeFile.pos_in f in
	let stuff = (try
		let id = input_id f in
		let len = input_len f in
		Some (id,len)
	with
	| End_of_file -> None
	) in
	match stuff with
	| None -> (LargeFile.seek_in f pos_now; stuff)
	| _ -> stuff
;;


type lacing_t = Lacing_none | Lacing_xiph of unit | Lacing_ebml of unit | Lacing_fixed of int * int;;
type block_t = {
	mutable block_track_number : int64;
	mutable block_timecode : int;
	mutable block_invisible : bool;
	mutable block_lacing : lacing_t;
	mutable block_frame_bytes : int64;
}

let parse_block_header f len =
	let pos_start = LargeFile.pos_in f in
	let track_num = match input_len f with
		| Some l -> l
		| None -> -1L
	in
	let timecode = (
		let a = input_byte f in
		let b = input_byte f in
		if a >= 0x80 then (
			(* Negative *)
			(-1) lsl 16 lor (a lsl 8) lor b
		) else (
			(a lsl 8) lor b
		)
	) in
	let flag = input_byte f in
	let invisible = flag land 0x08 <> 0 in
	let (lacing, bytes_left) = (match flag land 0x60 with
		| 0x00 -> (Lacing_none, pos_start +| len -| LargeFile.pos_in f)
		| 0x02 -> (Lacing_xiph (), pos_start +| len -| LargeFile.pos_in f)
		| 0x04 -> (Lacing_ebml (), pos_start +| len -| LargeFile.pos_in f)
		|  _   -> (
			let num_frames = input_byte f + 1 in
			let pos_frame_start = LargeFile.pos_in f in
			let total_len = pos_start +| len -| pos_frame_start in
			(Lacing_fixed (num_frames, (!- total_len) / num_frames), total_len)
		)
	) in
	{
		block_track_number = track_num;
		block_timecode = timecode;
		block_invisible = invisible;
		block_lacing = lacing;
		block_frame_bytes = bytes_left;
	}
;;





(**********)
(* OUTPUT *)
(**********)

let rec string_of_uint n =
	if n < 0L then (
		raise (Matroska_error "signed integer passed to string_of_uint")
	) else if n <= 0xFFL then (
		String.make 1 (Char.chr (!- n))
	) else (
		(string_of_uint (n >| 8)) ^ (String.make 1 (Char.chr (!- (n &&| 0xFFL))))
	)
;;

let rec string_of_sint n =
	if n <= 127L && n >= 0L then (
		String.make 1 (Char.chr (!- n))
	) else if n < 0L && n >= -127L then (
		String.make 1 (Char.chr (!- n + 256))
	) else (
		(string_of_sint (n >|- 8)) ^ (String.make 1 (Char.chr (!- (n &&| 0xFFL))))
	)
;;

let string_of_id id =
	if id < 0x80 then (
		(* 1 *)
		let a = String.create 1 in
		a.[0] <- Char.chr (0x80 lor id);
		a
	) else if id < 0x4000 then (
		(* 2 *)
		let a = String.create 2 in
		a.[0] <- Char.chr (0x40 lor (id lsr 8));
		a.[1] <- Char.chr (id land 0xFF);
		a
	) else if id < 0x200000 then (
		(* 3 *)
		let a = String.create 3 in
		a.[0] <- Char.chr (0x20 lor (id lsr 16));
		a.[1] <- Char.chr ((id lsr 8) land 0xFF);
		a.[2] <- Char.chr (id land 0xFF);
		a
	) else if id < 0x10000000 then (
		(* 4 *)
		let a = String.create 4 in
		a.[0] <- Char.chr (0x10 lor (id lsr 24));
		a.[1] <- Char.chr ((id lsr 16) land 0xFF);
		a.[2] <- Char.chr ((id lsr 8) land 0xFF);
		a.[3] <- Char.chr (id land 0xFF);
		a
	) else (
		raise (Matroska_error "ID too high")
	)
;;

let string_of_size = function
	| None -> String.copy "\xFF"
	| Some s -> (
		if s < 0x7FL then (
			string_of_uint (0x80L ||| s)
		) else if s < 0x3FFFL then (
			string_of_uint (0x4000L ||| s)
		) else if s < 0x1FFFFFL then (
			string_of_uint (0x200000L ||| s)
		) else if s < 0x0FFFFFFFL then (
			string_of_uint (0x10000000L ||| s)
		) else if s < 0x07FFFFFFFFL then (
			string_of_uint (0x0800000000L ||| s)
		) else if s < 0x03FFFFFFFFFFL then (
			string_of_uint (0x040000000000L ||| s)
		) else if s < 0x01FFFFFFFFFFFFL then (
			string_of_uint (0x02000000000000L ||| s)
		) else if s < 0x00FFFFFFFFFFFFFFL then (
			string_of_uint (0x0100000000000000L ||| s)
		) else (
			raise (Matroska_error "size too big")
		)
	)
;;

let string_of_block_header b =
	let tracknum = string_of_size (Some b.block_track_number) in
	let timecode = String.create 2 in
	timecode.[0] <- Char.chr ((b.block_timecode lsr 8) land 0xFF);
	timecode.[1] <- Char.chr (b.block_timecode land 0xFF);
	let flag = if b.block_invisible then "\x08" else "\x00" in
	(match b.block_lacing with
		| Lacing_none -> ()
		| _ -> failwith "lacing not supported"
	);
	tracknum ^ timecode ^ flag
;;
			

(*
let string_of_size = function
	| None -> String.copy "\xFF"
	| Some s -> (
		if s < 0x7F then (
			let a = String.create 1 in
			a.[0] <- Char.chr (0x80L ||| s);
		) else if s < 0x3FFF then (
			let a = String.create 2 in
			a.[0] <- Char.chr ();
			a.[1] <- Char.chr ();
		) else if s < 0x1FFFFF then (
			let a = String.create 3 in
			a.[0] <- Char.chr ();
			a.[1] <- Char.chr ();
			a.[2] <- Char.chr ();
		) else if s < 0x0FFFFFFF then (
			let a = String.create 4 in
			a.[0] <- Char.chr ();
			a.[1] <- Char.chr ();
			a.[2] <- Char.chr ();
			a.[3] <- Char.chr ();
		) else if s < 0x07FFFFFFFF then (
			let a = String.create 5 in
			a.[0] <- Char.chr ();
			a.[1] <- Char.chr ();
			a.[2] <- Char.chr ();
			a.[3] <- Char.chr ();
			a.[4] <- Char.chr ();
		) else if s < 0x03FFFFFFFFFF then (
			let a = String.create 6 in
			a.[0] <- Char.chr ();
			a.[1] <- Char.chr ();
			a.[2] <- Char.chr ();
			a.[3] <- Char.chr ();
			a.[4] <- Char.chr ();
			a.[5] <- Char.chr ();
		) else if s < 0x01FFFFFFFFFFFF then (
			let a = String.create 7 in
			a.[0] <- Char.chr ();
			a.[1] <- Char.chr ();
			a.[2] <- Char.chr ();
			a.[3] <- Char.chr ();
			a.[4] <- Char.chr ();
			a.[5] <- Char.chr ();
			a.[6] <- Char.chr ();
		) else if s < 0x00FFFFFFFFFFFFFF then (
			let a = String.create 8 in
			a.[0] <- Char.chr ();
			a.[1] <- Char.chr ();
			a.[2] <- Char.chr ();
			a.[3] <- Char.chr ();
			a.[4] <- Char.chr ();
			a.[5] <- Char.chr ();
			a.[6] <- Char.chr ();
			a.[7] <- Char.chr ();
		) else (
			raise (Matroska_error "size too big")
		)
	)
;;
*)






type binary_t = B_b of (int64 * binary_t list) | B_s of (int64 * string list);;
type elt_t = {
	id : int;
	inn : innards_t;
} and innards_t =
	| In_sint of int64
	| In_uint of int64
	| In_float of float
	| In_string of string
	| In_elts of elt_t list
;;

let rec render_element elt = (
	let id = string_of_id elt.id in
	let id_len = String.length id in
	let binary = (match elt.inn with
		| In_sint i -> (
			let s = string_of_sint i in
			let l = string_of_size (Some !|(String.length s)) in
			B_s (!|(id_len + String.length l + String.length s), [id;l;s])
		)
		| In_uint i -> (
			let s = string_of_uint i in
			let l = string_of_size (Some !|(String.length s)) in
			B_s (!|(id_len + String.length l + String.length s), [id;l;s])
		)
		| In_float f -> (
			let s = String.create 8 in
			let l = string_of_size (Some 8L) in
			let i = Int64.bits_of_float f in
			s.[0] <- Char.chr !-((i >| 56) &&| 0xFFL);
			s.[1] <- Char.chr !-((i >| 48) &&| 0xFFL);
			s.[2] <- Char.chr !-((i >| 40) &&| 0xFFL);
			s.[3] <- Char.chr !-((i >| 32) &&| 0xFFL);
			s.[4] <- Char.chr !-((i >| 24) &&| 0xFFL);
			s.[5] <- Char.chr !-((i >| 18) &&| 0xFFL);
			s.[6] <- Char.chr !-((i >|  8) &&| 0xFFL);
			s.[7] <- Char.chr !-((i      ) &&| 0xFFL);
			B_s (!|(id_len + String.length l + String.length s), [id;l;s])
		)
		| In_string s -> (
			let len_s = string_of_size (Some !|(String.length s)) in
			B_s (!|(id_len + String.length len_s + String.length s), [id;len_s;s])
		)
		| In_elts el -> (
			let rendered = List.map render_element el in
			let size = List.fold_left (fun so_far -> function
				| B_s (g,_) | B_b (g,_) -> so_far +| g
			) 0L rendered in
			let len_s = string_of_size (Some size) in
			B_b (!|(id_len + String.length len_s) +| size, (B_s (!|id_len, [id])) :: (B_s (!|(String.length len_s), [len_s])) :: rendered)
		)
	) in
	binary
);;

let rec output_binary h = function
	| B_b (_, b_list) -> (
		List.iter (fun x -> output_binary x) b_list
	)
	| B_s (_, s_list) -> (
		List.iter (fun x -> output h x 0 (String.length x)) s_list
	)
;;
(*
let ebml = {
	id = 0x0A45DFA3;
	inn = In_elts [
		{id = 0x0282; inn = In_string "matroska"};
		{id = 0x0287; inn = In_uint 2L};
		{id = 0x0285; inn = In_uint 2L};
	]
};;
*)



(*
	| In_sint x -> (
			let str = string_of_sint x in
			let len = string_of_size (Some !|(String.length str)) in
			B_s (!|(String.length str + String.length len + id_len), [id; len; str])
		)
		| In_uint x -> (
			let str = string_of_uint x in
			let len = string_of_size (Some !|(String.length str)) in
			B_s (!|(String.length str + String.length len + id_len), [id; len; str])
		)
		| In_float x -> (
			let i64 = Int64.to_bits x in
			let str = String.create 9 in
			str.[0] <- '\x88';
			str.[1] <- Char.chr !|((i64 >| 56) &&| 0xFFL);
			str.[2] <- Char.chr !|((i64 >| 48) &&| 0xFFL);
			str.[3] <- Char.chr !|((i64 >| 40) &&| 0xFFL);
			str.[4] <- Char.chr !|((i64 >| 32) &&| 0xFFL);
			str.[5] <- Char.chr !|((i64 >| 24) &&| 0xFFL);
			str.[6] <- Char.chr !|((i64 >| 18) &&| 0xFFL);
			str.[7] <- Char.chr !|((i64 >|  8) &&| 0xFFL);
			str.[8] <- Char.chr !|((i64      ) &&| 0xFFL);
			B_s (9L +| id_len64, [id; str])
		)
		| In_string x -> (
			let len = string_of_size (Some !|(String.length x)) in
			B_s (!|(String.length x + String.length len + id_len), [id; len; x])
		)
		| In_elts l -> (
			let rendered = List.map render_element l in
			let len = List.fold_left (fun so_far -> function
				| B_s (g,_) | B_b (g,_) -> so_far +| g
			) id_len64 rendered in
			B_b (len, id :: rendered)
		)
		| In_binary b -> (
			match b with
			| B_s (len, strings) ->;
*)
















(********)
(* TEST *)
(********)
(*
let h = open_in_bin Sys.argv.(1);;
let seek_add plus = (
	let a = LargeFile.pos_in h in
	LargeFile.seek_in h (a +| plus)
);;
let digits x =
	if x < 128 then 2
	else if x < 16384 then 4
	else if x < 2097152 then 6
	else if x < 268435456 then 8
	else 10
;;
let rec do_stuff () = match input_id_and_length h with
	| Some (id, Some l) -> (
		let id_string = Printf.sprintf "0x%0*X" (digits id) id in
		Printf.printf "%10s  " id_string;
		let seek = (match id with
			| 0x0A45DFA3 -> (Printf.printf "EBML\n"; false)
			| 0x0286     -> (Printf.printf " EBML version\n"; true)
			| 0x02F7     -> (Printf.printf " EBML read version\n"; true)
			| 0x02F2     -> (Printf.printf " EBML max ID length\n"; true)
			| 0x02F3     -> (Printf.printf " EBML max size length\n"; true)
			| 0x0282     -> (Printf.printf " EBML doc type\n"; true)
			| 0x0287     -> (Printf.printf " EBML doc type version\n"; true)
			| 0x0285     -> (Printf.printf " EBML doc type read version\n"; true)
			| 0x0549A966 -> (Printf.printf " INFO\n"; false)
			| 0x0654AE6B -> (Printf.printf " TRACKS\n"; false)
			| 0x0F43B675 -> (Printf.printf " CLUSTER\n"; false)
			| 0x0D80     -> (Printf.printf "  Muxing app\n"; true)
			| 0x1741     -> (Printf.printf "  Writing app\n"; true)
			| 0x0AD7B1   -> (Printf.printf "  Timecode scale\n"; true)
			| 0x0489     -> (Printf.printf "  Duration\n"; true)
			| 0x2E       -> (Printf.printf "  Track entry\n"; false)
			| 0x57       -> (Printf.printf "   Track number\n"; true)
			| 0x33C5     -> (Printf.printf "   Track UID\n"; true)
			| 0x03       -> (Printf.printf "   Track type\n"; true)
			| 0x1C       -> (Printf.printf "   Lacing used\n"; true)
			| 0x06       -> (Printf.printf "   Codec ID\n"; true)
			| 0x23A2     -> (Printf.printf "   Codec private\n"; true)
			| 0x03E383   -> (Printf.printf "   Default duration\n"; true)
			| 0x60       -> (Printf.printf "   Video settings\n"; false)
			| 0x30       -> (Printf.printf "    Width\n"; true)
			| 0x3A       -> (Printf.printf "    Height\n"; true)
			| 0x14B0     -> (Printf.printf "    Display width\n"; true)
			| 0x14BA     -> (Printf.printf "    Display height\n"; true)
			| 0x67       -> (Printf.printf "  Timecode\n"; true)
			| 0x20       -> (Printf.printf "  Block group\n"; false)
			| 0x21       -> (Printf.printf "   Block\n"; true)
			| 0x7B       -> (Printf.printf "   Reference block\n"; true)
			| _ -> (Printf.printf "*** length %Ld\n" l; true)
		) in
		if seek then seek_add l;
		do_stuff ()
	)
	| Some (id, None) -> (
		let id_string = Printf.sprintf "0x%0*X" (digits id) id in
		Printf.printf "%10s  " id_string;
		(match id with
			| 0x08538067 -> (Printf.printf "SEGMENT\n")
			| _ -> (Printf.printf "*** length unknown\n")
		);
		do_stuff ()
	)
	| None -> (
		Printf.printf "done\n";
	)
;;
do_stuff ();;
*)

(*
type parse_return_t = 
	| Parse_keep_going
	| Parse_ct  of int64
	| Parse_tcs of int64
	| Parse_nbp of int64
	| Parse_EOF
	| Parse_done;;
let matroska_read_gops h fpsn fpsd =
	let seek_add plus = (
		let a = LargeFile.pos_in h in
		LargeFile.seek_in h (a +| plus)
	) in
	let i64_div_round a b = (
		let a2 = a +| (b >|- 1) in
		a2 /| b
	) in

	Printf.printf "Reading frameses\n";

	LargeFile.seek_in h 0L;
	let rec helper ct tcs nbp = ( (* nbp = next blockgroup pos *)
		let before_pos = LargeFile.pos_in h in
		let before_len = LargeFile.in_channel_length h in
		let ct_and_tcs = (try
			match input_id_and_length h with
			| Some (0x08538067, _) | Some (0x0549A966, _) | Some (0x0F43B675, _) -> (
				(*    SEGMENT                INFO                   CLUSTER *)
				(* Parse these *)
				Parse_keep_going
			)
			| Some (0x0AD7B1, Some l) when l <= 8L -> (
				(* TIMECODE SCALE *)
				let new_tcs = input_uint 0L h !-l in
				Parse_tcs new_tcs
			)
			| Some (0x67, Some l) when l <= 8L -> (
				(* CLUSTER TIMECODE *)
				let new_tc = input_uint 0L h !-l in
				Parse_ct new_tc
			)
			| Some (0x20, Some l) -> (
				(* BLOCK GROUP *)
				Parse_nbp (LargeFile.pos_in h +| l)
			)
			| Some (0x21, Some l) -> (
				(* XXX BLOCK XXX *)
				let b = parse_block_header h l in

				let total_timecode = !|(b.block_timecode) +| ct in
				let total_time_n = total_timecode *| tcs *| fpsn in
				let total_time_d = 1000000000L *| fpsd in
				let frame_num = i64_div_round total_time_n total_time_d in

				(* If there is any data after the block that is in the block group, it's a frame reference, and therefore not an I frame *)
				let i_frame = (b.block_frame_bytes +| LargeFile.pos_in h = nbp) in

				if frame_num = 0L then (
					Printf.printf "FIRST FRAME\n";
					Printf.printf "GOPD 0\n";
				) else if i_frame then (
					Printf.printf "DONE\n";
					Printf.printf "GOPD %Ld\n" frame_num
				);

				Printf.printf "  OUTPUT FRAME %Ld\n" frame_num;

				let s = String.create !-(b.block_frame_bytes) in
				really_input h s 0 (String.length s);

				Parse_keep_going
			)
			| Some (x, Some l) -> (
				(* Unimportant block of known length *)
				seek_add l;
				Parse_keep_going
			)
			| Some (x, None) -> (
				(* Unimportant block of unknown length (this shouldn't happen) *)
				Parse_keep_going
			)
			| None -> Parse_EOF
		with
			End_of_file -> Parse_EOF
		) in
		match ct_and_tcs with
		| Parse_keep_going  -> helper ct tcs nbp
		| Parse_ct  new_ct  -> helper new_ct tcs nbp
		| Parse_tcs new_tcs -> helper ct new_tcs nbp
		| Parse_nbp new_nbp -> helper ct tcs new_nbp
		| Parse_done -> ()
		| Parse_EOF -> (
			(* Wait until the file is larger *)
			LargeFile.seek_in h before_pos;
			let rec attempt_read times_left = (
				if times_left = 0 then (
					raise End_of_file
				) else (
					let new_len = LargeFile.in_channel_length h in
					if new_len > before_len then (
						helper ct tcs nbp
					) else (
						(* Nothing happened *)
						Thread.delay 1.0;
						attempt_read (pred times_left)
					)
				)
			) in
			attempt_read 60
		)
	) in
	helper 0L 1000000L 0L
;;

let h = open_in_bin "test2.mkv";;
matroska_read_gops h 24000L 1001L;;
*)






(*
type parse_return_t = Parse_ctp_tcs of int64 option * int64 | Parse_EOF | Parse_done;;
let matroska_read_to_frame h f fpsn fpsd cluster_timecode_perhaps timecode_scale =
	let ( +|  ) = Int64.add in
	let ( !|  ) = Int64.of_int in
	let ( !-  ) = Int64.to_int in
	let ( >|- ) = Int64.shift_right in
	let ( <|  ) = Int64.shift_left in
	let ( /|  ) = Int64.div in
	let seek_add plus = (
		let a = LargeFile.pos_in h in
		LargeFile.seek_in h (a +| plus)
	) in
	let i64_div_round a b = (
		let a2 = a +| (b >|- 1) in
		a2 /| b
	) in

	Printf.printf "Reading to frame %Ld:\n" f;

	let rec helper ctp tcs = (
		let before_pos = LargeFile.pos_in h in
		let before_len = LargeFile.in_channel_length h in
		let ctp_and_tcs = (try
			match input_id_and_length h with
			| Some (0x08538067, _) -> (
				(* SEGMENT *)
				Parse_ctp_tcs (ctp,tcs)
			)
			| Some (0x0549A966, _) -> (
				(* INFO *)
				Parse_ctp_tcs (ctp,tcs)
			)
			| Some (0x0AD7B1, Some l) when l <= 8L -> (
				(* TIMECODE SCALE *)
				let new_tcs = input_uint 0L h !-l in
				Parse_ctp_tcs (ctp,new_tcs)
			)
			| Some (0x0F43B675, _) -> (
				(* CLUSTER *)
				Parse_ctp_tcs (ctp,tcs)
			)
			| Some (0x67, Some l) when l <= 8L -> (
				(* CLUSTER TIMECODE *)
				let new_tc = input_uint 0L h !-l in
				Parse_ctp_tcs ((Some new_tc),tcs)
			)
			| Some (0x20, _) -> (
				(* BLOCK GROUP *)
				Parse_ctp_tcs (ctp,tcs)
			)
			| Some (0x21, Some l) -> (
				(* XXX BLOCK XXX *)
				let b = parse_block_header h l in

				match ctp with
				| None -> (failwith "invalid Matroska file")
				| Some ct -> (
					let total_timecode = !|(b.block_timecode) +| ct in
					let total_time_n = total_timecode *| tcs *| fpsn in
					let total_time_d = 1000000000L *| fpsd in
					let frame_num = i64_div_round total_time_n total_time_d in
					Printf.printf "CT: %Ld\n" ct;
					Printf.printf "BT: %d\n" b.block_timecode;
					Printf.printf "## = %Ld / %Ld = %Ld\n" total_time_n total_time_d frame_num;
					if frame_num >= f then (
						(* Oops. We went too far *)
						Printf.printf "  Too far: %Ld\n" frame_num;
						LargeFile.seek_in h before_pos;
						Parse_done
					) else (
						(* Parse parse parse *)
						Printf.printf "  Doing frame %Ld\n" frame_num;

						let o label s = (
							Printf.printf "%20s: %S\n" label (to_hex s)
						) in

						let put_frame_here = String.create !-(b.block_frame_bytes) in
						really_input h put_frame_here 0 (String.length put_frame_here);

						let after_frame_pos = LargeFile.pos_in h in
						let is_i_frame = (
							match input_id h with
							| 0x7B -> false
							| _ -> true
						) in
						LargeFile.seek_in h after_frame_pos;

						o "Segment ID" (string_of_id 0x0F43B675);
						o "Segment size" (string_of_size None);
						o "Timecode ID" (string_of_id 0x67);
						let frame_num_string = string_of_uint frame_num in
						o "Timecode size" (string_of_size (Some !|(String.length frame_num_string)));
						o "Timecode" frame_num_string;
						o "Simple block ID" (string_of_id 0x23);
						o "Simple block size" (string_of_size (Some !|(4 + String.length put_frame_here)));
						o "Block header" "\x81\x00\x00\x00";
						Printf.printf "%20s: %d\n" "Block contents" (String.length put_frame_here);
						o "I FRAME" (if is_i_frame then "\x01" else "\x00");

						Parse_ctp_tcs (ctp,tcs)
					)
				)
			)
			| Some (0x7B, Some l) -> (
				(* I-frames have no reference blocks *)
				(* Everything else seems to have them *)
				Printf.printf "  Found reference block; parse\n";
				let rb = input_sint 0L h ( !- l ) in
				Printf.printf "   #Reference %Ld\n" rb;
				Parse_ctp_tcs (ctp,tcs)
			)
			| Some (x, Some l) -> (
				(* Unimportant block of known length *)
				seek_add l;
				Parse_ctp_tcs (ctp,tcs)
			)
			| Some (x, None) -> (
				(* Unimportant block of unknown length (this shouldn't happen) *)
				Parse_ctp_tcs (ctp,tcs)
			)
			| None -> Parse_EOF
		with
			End_of_file -> Parse_EOF
		) in
		match ctp_and_tcs with
		| Parse_ctp_tcs (new_ctp,new_tcs) -> helper new_ctp new_tcs
		| Parse_done -> (ctp,tcs)
		| Parse_EOF -> (
			(* Wait until the file is larger *)
			LargeFile.seek_in h before_pos;
			let rec attempt_read times_left = (
				if times_left = 0 then (
					raise End_of_file
				) else (
					let new_len = LargeFile.in_channel_length h in
					if new_len > before_len then (
						helper ctp tcs
					) else (
						(* Nothing happened *)
						Thread.delay 1.0;
						attempt_read (pred times_left)
					)
				)
			) in
			attempt_read 60
		)
	) in
	helper cluster_timecode_perhaps timecode_scale
;;
					)
				)
			) in
			attempt_read 60
		)
	) in
	helper cluster_timecode_perhaps timecode_scale
;;

let h = open_in_bin "test2.mkv";;

let (ctp,tcs) = matroska_read_to_frame h 0L 24000L 1001L None 1000000L;;
let (ctp,tcs) = matroska_read_to_frame h 2115L 24000L 1001L ctp tcs;;
let (ctp,tcs) = matroska_read_to_frame h 2130L 24000L 1001L ctp tcs;;
*)






(*
let seek_add f plus =
	let a = LargeFile.pos_in f in
	LargeFile.seek_in f (a +| plus)
;;

let f = open_in_bin "test.mkv";;

let rec keep_matching cluster_timecode timecode_scale =
	match input_id_and_length f with
	| Some (0x0A45DFA3, Some l) -> (
		Printf.printf "Found EBML; skip\n";
		seek_add f l;
		keep_matching cluster_timecode timecode_scale
	)
	| Some (0x08538067, None) -> (
		Printf.printf "Found Matroska segment; parse\n";
		keep_matching cluster_timecode timecode_scale
	)
	| Some (0x0549A966, Some l) -> (
		Printf.printf "  Found segment info; parse\n";
		keep_matching cluster_timecode timecode_scale
	)
	| Some (0x0D80, Some l) -> (
		Printf.printf "    Found muxing app; skip\n";
		seek_add f l;
		keep_matching cluster_timecode timecode_scale
	)
	| Some (0x1741, Some l) -> (
		Printf.printf "    Found writing app; skip\n";
		seek_add f l;
		keep_matching cluster_timecode timecode_scale
	)
	| Some (0x0AD7B1, Some l) -> (
		Printf.printf "    Found timecode scale; parse\n";
		let tcs = input_uint 0L f !-l in
		Printf.printf "     #Timecode scale %Ld\n" tcs;
		keep_matching cluster_timecode tcs
	)
	| Some (0x0489, Some l) -> (
		Printf.printf "    Found duration; skip\n";
(*
		let dur = input_float f !-l in
		Printf.printf "     #Duration %f\n" dur;
*)
		seek_add f l;
		keep_matching cluster_timecode timecode_scale
	)
	| Some (0x0654AE6B, Some l) -> (
		Printf.printf "  Found tracks; parse\n";
(*		seek_add f l;*)
		keep_matching cluster_timecode timecode_scale
	)
	| Some (0x2E, Some l) -> (
		Printf.printf "    Found tracks entry; parse\n";
		keep_matching cluster_timecode timecode_scale
	)
	| Some (0x03E383, Some l) -> (
		Printf.printf "      Found default duration\n";
		let dd = input_uint 0L f !-l in
		Printf.printf "       #%Ld\n" dd;
(*		seek_add f l;*)
		keep_matching cluster_timecode timecode_scale
	)
(*
	| Some (0x0F43B675, Some l) -> (
		Printf.printf "  Found cluster; parse\n";
		keep_matching None timecode_scale
	)
	| Some (0x67, Some l) -> (
		Printf.printf "    Found timecode; parse\n";
		let tc = input_uint 0L f ( !- l ) in
		Printf.printf "     #Timecode %Ld\n" tc;
		keep_matching (Some tc) timecode_scale
	)
	| Some (0x20, Some l) -> (
		Printf.printf "    Found block group; parse\n";
		keep_matching cluster_timecode timecode_scale
	)
	| Some (0x21, Some l) -> (
		Printf.printf "      Found block; parse specially\n";
		let block = parse_block_header f l in
		Printf.printf "       #Block track number: %Ld\n" block.block_track_number;
		Printf.printf "       #Block timecode:     %d\n" block.block_timecode;
		Printf.printf "       #Block invisible:    %B\n" block.block_invisible;
		seek_add f block.block_frame_bytes;
		(match cluster_timecode with
			| None -> () (* Uhhh. What? *)
			| Some tc -> (
				Printf.printf "       #Total timecode: %Ld\n" (tc +| !| (block.block_timecode));
				let float_timecode = Int64.to_float (tc +| !| (block.block_timecode)) in
				let fps = 24000.0 /. 1001.0 in
				let float_tcs = Int64.to_float timecode_scale /. 1000000000.0 in
				let float_frame = float_timecode *. float_tcs *. fps in
				Printf.printf "       #Frame number:   %f\n" float_frame;
			)
		);
(*		seek_add f l;*)
		keep_matching cluster_timecode timecode_scale
	)
	| Some (0x7B, Some l) -> (
		(* I-frames have no reference blocks *)
		(* Everything else seems to have them *)
		Printf.printf "      Found reference block; parse\n";
		let rb = input_sint 0L f ( !- l ) in
		Printf.printf "       #Reference %Ld\n" rb;
(*		seek_add f l;*)
		keep_matching cluster_timecode timecode_scale
	)
*)
	| Some (x, Some l) -> (
		Printf.printf "Found ID 0x%08X of length %Ld; skip\n" x l;
		seek_add f l;
		keep_matching cluster_timecode timecode_scale
	)
	| None -> (
		Printf.printf "EOF!\n";
	)
;;
keep_matching None 1000000L;;
*)

