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

type t
val max_len : int
val resize : t -> int -> unit
val create : int -> t
val sub : t -> int -> int -> string
val contents : t -> string
val nth : t -> int -> char
val length : t -> int
val clear : t -> unit
val add_char : t -> char -> unit
val add_substring : t -> string -> int -> int -> unit
val add_string : t -> string -> unit
val add_bigbuffer : t -> t -> unit
val add_buffer : t -> Buffer.t -> unit
val add_channel : t -> in_channel -> int -> unit
val output_bigbuffer : out_channel -> t -> unit
val output_bigbuffer_unix : Unix.file_descr -> t -> unit
val shorten : t -> int -> unit
val iter : t -> (int -> string -> int -> unit) -> unit

