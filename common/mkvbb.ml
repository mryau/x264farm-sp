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

let u1_int b n =
	Bigbuffer.add_char b (Char.chr (n land 0xFF));
;;
let u2_int b n =
	Bigbuffer.add_char b (Char.chr ((n lsr 8) land 0xFF));
	u1_int b (n land 0xFF)
;;
let u3_int b n =
	Bigbuffer.add_char b (Char.chr ((n lsr 16) land 0xFF));
	u2_int b (n land 0xFFFF)
;;
let u4_int b n =
	Bigbuffer.add_char b (Char.chr ((n lsr 24) land 0xFF));
	u3_int b (n land 0xFFFFFF)
;;
let u4_64 b n =
	Bigbuffer.add_char b (Char.chr (!-(n >| 24) land 0xFF));
	u3_int b (!- (n &&| 0xFFFFFFL))
;;
let u5_64 b n =
	Bigbuffer.add_char b (Char.chr (!-(n >| 32) land 0xFF));
	u4_64 b (n &&| 0xFFFFFFFFL)
;;
let u6_64 b n =
	Bigbuffer.add_char b (Char.chr (!-(n >| 40) land 0xFF));
	u5_64 b (n &&| 0xFFFFFFFFFFL)
;;
let u7_64 b n =
	Bigbuffer.add_char b (Char.chr (!-(n >| 48) land 0xFF));
	u6_64 b (n &&| 0xFFFFFFFFFFFFL)
;;
let u8_64 b n =
	Bigbuffer.add_char b (Char.chr (!-(n >| 56) land 0xFF));
	u7_64 b (n &&| 0xFFFFFFFFFFFFFFL)
;;


let output_uint b n =
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
;;

let output_sint b n =
	if n < 128L && n >= -128L then (
		u1_int b !-n
	) else if n < 32768L && n >= -32768L then (
		u2_int b !-n
	) else if n < 8388608L && n >= -8388608L then (
		u3_int b !-n
	) else if n < 2147483648L && n >= -2147483648L then (
		u4_64 b n
	) else if n < 549755813888L && n >= -549755813888L then (
		u5_64 b n
	) else if n < 140737488355328L && n >= -140737488355328L then (
		u6_64 b n
	) else if n < 36028797018963968L && n >= -36028797018963968L then (
		u7_64 b n
	) else (
		u8_64 b n
	)
;;

let output_id b id =
	if id < 0x80 then (
		u1_int b (id lor 0x80)
	) else if id < 0x4000 then (
		u2_int b (id lor 0x4000)
	) else if id < 0x200000 then (
		u3_int b (id lor 0x200000)
	) else if id < 0x10000000 then (
		u4_int b (id lor 0x10000000)
	) else (
		failwith "stuff"
	)
;;

let output_size b = function
	| None -> Bigbuffer.add_char b '\xFF'
	| Some s -> (
		if s < 0x7FL then (
			u1_int b (0x80 lor !-s)
		) else if s < 0x3FFFL then (
			u2_int b (0x4000 lor !-s)
		) else if s < 0x1FFFFFL then (
			u3_int b (0x200000 lor !-s)
		) else if s < 0x0FFFFFFFL then (
			u4_int b (0x10000000 lor !-s)
		) else if s < 0x07FFFFFFFFL then (
			u5_64 b (0x0800000000L ||| s)
		) else if s < 0x03FFFFFFFFFFL then (
			u6_64 b (0x040000000000L ||| s)
		) else if s < 0x01FFFFFFFFFFFFL then (
			u7_64 b (0x02000000000000L ||| s)
		) else if s < 0x00FFFFFFFFFFFFFFL then (
			u8_64 b (0x0100000000000000L ||| s)
		) else (
			failwith "other stuff"
		)
	)
;;


