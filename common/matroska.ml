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


(* Input from string *)
let rec uint_of_string so_far (s,n) len =
	if len > 8 || len < 0 then (
		raise (Invalid_integer_length len)
	) else if len = 0 then (
		((s,n),so_far)
	) else (
		let b = Char.code s.[n] in
		uint_of_string ((so_far <| 8) ||| ( !| b)) (s, succ n) (pred len)
	)
;;

let sint_of_string so_far (s,n) len =
	if len > 8 || len < 0 then (
		raise (Invalid_integer_length len)
	) else if len = 0 then (
		((s,n),so_far)
	) else (
		let b = ( !| (Char.code s.[n])) <| 56 >|- 56 in
		uint_of_string b (s, succ n) (pred len)
	)
;;

let rec xiph_of_string so_far (s,n) =
	let b = Char.code s.[n] in
	if b = 255 then (
		xiph_of_string (so_far + 255) (s, succ n)
	) else (
		((s, succ n), !| (so_far + b))
	)
;;

let len_of_string (s,n) =
	let b = Char.code s.[n] in
	let n = succ n in (* Don't want to forget this *)
	let ((ret,len),mask) = if b >= 0x80 then (
		(* 1 byte *)
		(uint_of_string !|(b land 0x7F) (s,n) 0, 0x7FL)
	) else if b >= 0x40 then (
		(* 2 bytes *)
		(uint_of_string !|(b land 0x3F) (s,n) 1, 0x3FFFL)
	) else if b >= 0x20 then (
		(* 3 bytes *)
		(uint_of_string !|(b land 0x1F) (s,n) 2, 0x1FFFFFL)
	) else if b >= 0x10 then (
		(* 4 bytes *)
		(uint_of_string !|(b land 0x0F) (s,n) 3, 0x0FFFFFFFL)
	) else if b >= 0x08 then (
		(* 5 bytes *)
		(uint_of_string !|(b land 0x07) (s,n) 4, 0x07FFFFFFFFL)
	) else if b >= 0x04 then (
		(* 6 bytes *)
		(uint_of_string !|(b land 0x03) (s,n) 5, 0x03FFFFFFFFFFL)
	) else if b >= 0x02 then (
		(* 7 bytes *)
		(uint_of_string !|(b land 0x01) (s,n) 6, 0x01FFFFFFFFFFFFL)
	) else if b >= 0x01 then (
		(* 8 bytes *)
		(uint_of_string !|(b land 0x00) (s,n) 7, 0x00FFFFFFFFFFFFFFL)
	) else (
		(* b = 0x00; INVALID *)
		raise Invalid_integer
	) in
	if len = mask then (ret, None) else (ret, Some len)
;;

let float_of_string got len =
	if len = 4 then (
		let (ret,f64) = sint_of_string 0L got 4 in
		(ret, Int32.float_of_bits (Int64.to_int32 f64))
	) else if len = 8 then (
		let (ret,f64) = sint_of_string 0L got 8 in
		(ret, Int64.float_of_bits f64)
	) else (
		raise (Invalid_float_length len)
	)
;;

let id_of_string (s,n) =
	let b = Char.code s.[n] in
	if b >= 0x80 then (
		(* 1 byte *)
		((s, n + 1), b land 0x7F)
	) else if b >= 0x40 then (
		(* 2 bytes *)
		let c = Char.code s.[n + 1] in
		((s, n + 2), ((b land 0x3F) lsl 8) lor c)
	) else if b >= 0x20 then (
		(* 3 bytes *)
		let c = Char.code s.[n + 1] in
		let d = Char.code s.[n + 2] in
		((s, n + 3), ((b land 0x1F) lsl 16) lor (c lsl 8) lor d)
	) else if b >= 0x10 then (
		(* 4 bytes *)
		let c = Char.code s.[n + 1] in
		let d = Char.code s.[n + 2] in
		let e = Char.code s.[n + 3] in
		((s, n + 4), ((b land 0x0F) lsl 24) lor (c lsl 16) lor (d lsl 8) lor e)
	) else (
		raise (Invalid_ID !|n)
	)
;;

let id_and_length_of_string n0 =
	let (n1,id) = id_of_string n0 in
	let (n2,len) = len_of_string n1 in
	(n2,id,len)
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

(*****************)
(* BUFFER OUTPUT *)
(*****************)

let buffer_output_uint =
	let u1_int b n = (
		Buffer.add_char b (Char.chr (n land 0xFF));
	) in
	let u2_int b n = (
		Buffer.add_char b (Char.chr ((n lsr 8) land 0xFF));
		u1_int b (n land 0xFF)
	) in
	let u3_int b n = (
		Buffer.add_char b (Char.chr ((n lsr 16) land 0xFF));
		u2_int b (n land 0xFFFF)
	) in
	let u4_64 b n = (
		Buffer.add_char b (Char.chr (!-(n >| 24) land 0xFF));
		u3_int b (!- (n &&| 0xFFFFFFL))
	) in
	let u5_64 b n = (
		Buffer.add_char b (Char.chr (!-(n >| 32) land 0xFF));
		u4_64 b (n &&| 0xFFFFFFFFL)
	) in
	let u6_64 b n = (
		Buffer.add_char b (Char.chr (!-(n >| 40) land 0xFF));
		u5_64 b (n &&| 0xFFFFFFFFFFL)
	) in
	let u7_64 b n = (
		Buffer.add_char b (Char.chr (!-(n >| 48) land 0xFF));
		u6_64 b (n &&| 0xFFFFFFFFFFFFL)
	) in
	let u8_64 b n = (
		Buffer.add_char b (Char.chr (!-(n >| 56) land 0xFF));
		u7_64 b (n &&| 0xFFFFFFFFFFFFFFL)
	) in
	fun b n -> (
		if n <= 0xFFL then (
			u1_int b !-n
		) else if n <= 0xFFFFL then (
			u2_int b !-n
		) else if n <= 0xFFFFFFL then (
			u3_int b !-n
		) else if n <= 0xFFFFFFFFL then (
			u4_64 b n
		) else if n <= 0xFFFFFFFFFFL then (
			u5_64 b n
		) else if n <= 0xFFFFFFFFFFFFL then (
			u6_64 b n
		) else if n <= 0xFFFFFFFFFFFFFFL then (
			u7_64 b n
		) else (
			u8_64 b n
		)
	)
;;
(*
	if n < 0L then (
		raise (Matroska_error "signed integer passed to buffer_output_uint")
	) else (
		let 
*)
let buffer_output_id b id =
	if id < 0x80 then (
		(* 1 *)
		Buffer.add_char b (Char.chr (0x80 lor id))
	) else if id < 0x4000 then (
		(* 2 *)
		Buffer.add_char b (Char.chr (0x40 lor (id lsr 8)));
		Buffer.add_char b (Char.chr (id land 0xFF))
	) else if id < 0x200000 then (
		(* 3 *)
		Buffer.add_char b (Char.chr (0x20 lor (id lsr 16)));
		Buffer.add_char b (Char.chr ((id lsr 8) land 0xFF));
		Buffer.add_char b (Char.chr (id land 0xFF));
	) else if id < 0x10000000 then (
		Buffer.add_char b (Char.chr (0x10 lor (id lsr 24)));
		Buffer.add_char b (Char.chr ((id lsr 16) land 0xFF));
		Buffer.add_char b (Char.chr ((id lsr 8) land 0xFF));
		Buffer.add_char b (Char.chr (id land 0xFF));
	) else (
		raise (Matroska_error "ID too high")
	)
;;

(**********************)
(* GENERALIZED OUTPUT *)
(**********************)
type binary_t = B_b of (int64 * binary_t list) | B_s of (int64 * string list);;
type elt_t = {
	id : int;
	inn : innards_t;
} and innards_t =
	| Sint of int64
	| Uint of int64
	| Float of float
	| String of string
	| Elts of elt_t list
	| Binary of binary_t
;;

let rec render_element elt = (
	let id = string_of_id elt.id in
	let id_len = String.length id in
	let binary = (match elt.inn with
		| Sint i -> (
			let s = string_of_sint i in
			let l = string_of_size (Some !|(String.length s)) in
			B_s (!|(id_len + String.length l + String.length s), [id;l;s])
		)
		| Uint i -> (
			let s = string_of_uint i in
			let l = string_of_size (Some !|(String.length s)) in
			B_s (!|(id_len + String.length l + String.length s), [id;l;s])
		)
		| Float f -> (
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
		| String s -> (
			let len_s = string_of_size (Some !|(String.length s)) in
			B_s (!|(id_len + String.length len_s + String.length s), [id;len_s;s])
		)
		| Elts el -> (
			let rendered = List.map render_element el in
			let size = List.fold_left (fun so_far -> function
				| B_s (g,_) | B_b (g,_) -> so_far +| g
			) 0L rendered in
			let len_s = string_of_size (Some size) in
			B_b (!|(id_len + String.length len_s) +| size, (B_s (!|id_len, [id])) :: (B_s (!|(String.length len_s), [len_s])) :: rendered)
		)
		| Binary (B_s (b_l, b)) -> (
			let l = string_of_size (Some b_l) in
			B_s (!|id_len +| !|(String.length l) +| b_l, id :: l :: b)
		)
		| Binary (B_b (b_l, b)) -> (
			let l = string_of_size (Some b_l) in
			let id_b = B_s (!|id_len, [id]) in
			let l_b = B_s (!|(String.length l), [l]) in
			B_b (!|id_len +| !|(String.length l) +| b_l, id_b :: l_b :: b)
		)
	) in
	binary
);;

let rec output_binary h = function
	| B_b (_, b_list) -> (
		List.iter (fun x -> output_binary h x) b_list
	)
	| B_s (_, s_list) -> (
		List.iter (fun x -> output h x 0 (String.length x)) s_list
	)
;;

let rec output_binary_unix h = function
	| B_b (_, b_list) -> (
		List.iter (fun x -> output_binary_unix h x) b_list
	)
	| B_s (_, s_list) -> (
		List.iter (fun x -> ignore (Unix.write h x 0 (String.length x))) s_list
	)
;;





(********)
(* TEST *)
(********)



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

						Parse_ctp_tcs (ctp,tcs)
					)
				)
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

let h = open_in_bin "test.mkv";;

let (ctp,tcs) = matroska_read_to_frame h 10L 24000L 1001L None 1000000L;;
matroska_read_to_frame h 10L 24000L 1001L ctp tcs;;
matroska_read_to_frame h 20L 24000L 1001L ctp tcs;;
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

