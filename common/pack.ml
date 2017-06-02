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

(* 32-bit:
 * UNSAFE: 30.360000
 * SAFE:   33.141000
 * SORTOF: 31.250000
 * 64-bit:
 * UNSAFE: 1.984000
 * SAFE:   4.031000
 * SORTOF: 2.657000 *)
let pack64 str at n =
	if at + 8 > String.length str || at < 0 then (
		invalid_arg "index out of bounds"
	) else (
		String.unsafe_set str (at    ) (Char.unsafe_chr ((Int64.to_int (Int64.shift_right_logical n 56)) land 0xFF));
		String.unsafe_set str (at + 1) (Char.unsafe_chr ((Int64.to_int (Int64.shift_right_logical n 48)) land 0xFF));
		String.unsafe_set str (at + 2) (Char.unsafe_chr ((Int64.to_int (Int64.shift_right_logical n 40)) land 0xFF));
		String.unsafe_set str (at + 3) (Char.unsafe_chr ((Int64.to_int (Int64.shift_right_logical n 32)) land 0xFF));
		String.unsafe_set str (at + 4) (Char.unsafe_chr ((Int64.to_int (Int64.shift_right_logical n 24)) land 0xFF));
		String.unsafe_set str (at + 5) (Char.unsafe_chr ((Int64.to_int (Int64.shift_right_logical n 16)) land 0xFF));
		String.unsafe_set str (at + 6) (Char.unsafe_chr ((Int64.to_int (Int64.shift_right_logical n  8)) land 0xFF));
		String.unsafe_set str (at + 7) (Char.unsafe_chr ((Int64.to_int (                          n   )) land 0xFF));
	)
;;
(* 32-bit:
 * MIN:   7.609000  (minimizes 64-bit operations; makes 3 native ints and puts them together as a 64-bit int)
 * EVEN:  23.782000 (converts each byte into 64-bit int, shifts to right place, then "or" all 8)
 * 64BIT: 4.703000  (Same as MIN, but uses 2 ints and does one 64-bit "or"; only works properly in 64-bit mode, so this is technically a worthless result)
 * 64-bit:
 * MIN:   1.453000
 * EVEN:  3.594000
 * 64BIT: 1.094000 *)
let unpack64 =
(*	let ( +|  ) = Int64.add in*)
(*	let ( -|  ) = Int64.sub in*)
(*	let ( *|  ) = Int64.mul in*)
(*	let ( /|  ) = Int64.div in*)
	let ( <|  ) = Int64.shift_left in
(*	let ( >|  ) = Int64.shift_right_logical in*)
(*	let ( >|- ) = Int64.shift_right in*)
	let ( ||| ) = Int64.logor in
(*	let ( &&| ) = Int64.logand in*)
	let ( !|  ) = Int64.of_int in
(*	let ( !-  ) = Int64.to_int in*)
	if Sys.word_size = 32 then (
		fun str at -> (
			if at + 8 > String.length str || at < 0 then (
				invalid_arg "index out of bounds"
			) else (
				(* 32-bit *)
				(* Split the string into 3 native ints, turn into int64, shift to the right place, and "or" them *)
				(* This minimizes the number of 64-bit operations on 32-bit systems *)
				(( !| ((
					Char.code (String.unsafe_get str (at    )) lsl 8
				) lor (
					Char.code (String.unsafe_get str (at + 1))
				)) ) <| 48) ||| (( !| ((
					Char.code (String.unsafe_get str (at + 2)) lsl 16
				) lor (
					Char.code (String.unsafe_get str (at + 3)) lsl 8
				) lor (
					Char.code (String.unsafe_get str (at + 4))
				)) ) <| 24) ||| ( !| ((
					Char.code (String.unsafe_get str (at + 5)) lsl 16
				) lor (
					Char.code (String.unsafe_get str (at + 6)) lsl 8
				) lor (
					Char.code (String.unsafe_get str (at + 7))
				)))
			)
		)
	) else (
		(* 64-bit *)
		(* Splits the string into 2 native ints, with 4 bytes going to each, then converts them to int64s and does one 64-bit "or" *)
		fun str at -> (
			if at + 8 > String.length str || at < 0 then (
				invalid_arg "index out of bounds"
			) else (
				(!|(
					(
						Char.code (String.unsafe_get str (at    )) lsl 24
					) lor (
						Char.code (String.unsafe_get str (at + 1)) lsl 16
					) lor (
						Char.code (String.unsafe_get str (at + 2)) lsl 8
					) lor (
						Char.code (String.unsafe_get str (at + 3))
					)
				) <| 32) ||| (!|(
					(
						Char.code (String.unsafe_get str (at + 4)) lsl 24
					) lor (
						Char.code (String.unsafe_get str (at + 5)) lsl 16
					) lor (
						Char.code (String.unsafe_get str (at + 6)) lsl 8
					) lor (
						Char.code (String.unsafe_get str (at + 7))
					)
				))
			)
		)
	)
;;




(* Packs an OCaml native int (31 or 63 bits) into 4 bytes.
 * For 64-bit packing, the argument is taken modulo 2^31, for compatability with 31-bit ints *)
(* UNSAFE: 9.578000
 * SAFE:   28.641000
 * SORTOF: 18.859000 (Char.unsafe_chr, String.set) *)
let packN =
	if Sys.word_size = 32 then (
		(* 32-bit - normal *)
		fun str at n -> (
			if at + 4 > String.length str || at < 0 then (
				raise (Invalid_argument "index out of bounds")
			) else (
				(* String.unsafe_set is safe since the bounds were checked above *)
				(* Char.unsafe_chr is safe since the operand is taken land 0xFF in each case *)
				String.unsafe_set str (at    ) (Char.unsafe_chr ((n lsr 24) land 0xFF));
				String.unsafe_set str (at + 1) (Char.unsafe_chr ((n lsr 16) land 0xFF));
				String.unsafe_set str (at + 2) (Char.unsafe_chr ((n lsr  8) land 0xFF));
				String.unsafe_set str (at + 3) (Char.unsafe_chr ((n       ) land 0xFF));
			)
		)
	) else (
		(* 64-bit - argument n is stored as (n land 0x7FFFFFFF) for compatability *)
		fun str at n -> (
			if at + 4 > String.length str || at < 0 then (
				raise (Invalid_argument "index out of bounds")
			) else (
				(* String.unsafe_set is safe since the bounds were checked above *)
				(* Char.unsafe_chr is safe since the operand is taken land 0xFF in each case *)
				String.unsafe_set str (at    ) (Char.unsafe_chr ((n lsr 24) land 0x7F));
				String.unsafe_set str (at + 1) (Char.unsafe_chr ((n lsr 16) land 0xFF));
				String.unsafe_set str (at + 2) (Char.unsafe_chr ((n lsr  8) land 0xFF));
				String.unsafe_set str (at + 3) (Char.unsafe_chr ((n       ) land 0xFF));
			)
		)
	)
;;

(* Unpacks a 4-byte substring to an OCaml native int
 * The split here is so 64-bit compilations return the same sign as 32-bit *)
(* UNSAFE: 10.110000
 * SAFE:   17.688000 *)
let unpackN =
	if Sys.word_size = 32 then (
		(* 32-bit *)
		fun str at -> (
			if at + 4 > String.length str || at < 0 then (
				raise (Invalid_argument "index out of bounds")
			) else (
				(* String.unsafe_get is safe here since the bounds were checked above *)
				(
					Char.code (String.unsafe_get str (at    )) lsl 24
				) lor (
					Char.code (String.unsafe_get str (at + 1)) lsl 16
				) lor (
					Char.code (String.unsafe_get str (at + 2)) lsl  8
				) lor (
					Char.code (String.unsafe_get str (at + 3))
				)
			)
		)
	) else (
		(* 64-bit *)
		fun str at -> (
			if at + 4 > String.length str || at < 0 then (
				raise (Invalid_argument "index out of bounds")
			) else (
				(* String.unsafe_get is safe here since the bounds were checked above *)
				(
					(* The asr at the end here ensures that the sign is transferred properly *)
					(Char.code (String.unsafe_get str (at    )) lsl 56) asr 32
				) lor (
					 Char.code (String.unsafe_get str (at + 1)) lsl 16
				) lor (
					 Char.code (String.unsafe_get str (at + 2)) lsl  8
				) lor (
					 Char.code (String.unsafe_get str (at + 3))
				)
			)
		)
	)
;;



(* Same as packN, but use a 16-bit field *)
(* UNSAFE: 7.703000
 * SAFE:   16.218000
 * SORTOF: 8.328000 *)
let packn str at n =
	if at + 2 > String.length str || at < 0 then (
		raise (Invalid_argument "index out of bounds")
	) else (
		(* Safeness of these functions is described in packN *)
		String.unsafe_set str (at    ) (Char.unsafe_chr ((n lsr 8) land 0xFF));
		String.unsafe_set str (at + 1) (Char.unsafe_chr ((n      ) land 0xFF));
	)
;;
(* UNSAFE: 7.438000
 * SAFE:   9.578000 *)
let unpackn str at =
	if at + 2 > String.length str || at < 0 then (
		raise (Invalid_argument "index out of bounds")
	) else (
		(
			Char.code (String.unsafe_get str (at    )) lsl 8
		) lor (
			Char.code (String.unsafe_get str (at + 1))
		)
	)
;;



(* Packs an 8-bit field *)
(* UNSAFE: 5.297000
 * SAFE:   8.547000
 * SORTOF: 3.797000 *)
let packC str at n =
	String.set str at (Char.unsafe_chr (n land 0xFF))
;;

(* UNSAFE: 5.297000
 * SAFE:   3.844000 *)
let unpackC str at =
	Char.code (String.get str at)
;;

