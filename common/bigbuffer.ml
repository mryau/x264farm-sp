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
type s = {
	s : string;
	mutable pos : int;
};;
*)
type t = {
	mutable b : string array;
	mutable i : int; (* Which array in b has the next letter *)
	mutable n : int; (* The index in b.[i] which contains the next letter *)
	mutable l : int; (* Total length of the filled area *)
	mutable b_l : int; (* Total length of all the strings in the buffer *)
};;

let max_len = Sys.max_string_length;;


let resize t n =
	(* add n bytes to t *)
	(* Do it a slightly different way from Buffer.resize - if the buffer has more than max_len bytes in it or more than max_len bytes should be added, this will add a multiple of max_len bytes *)
	let add_bytes = max t.b_l n in
	if add_bytes < max_len then (
		(* Add a single string with all the bytes *)
		let new_array = Array.init (Array.length t.b + 1) (fun i ->
			if i < Array.length t.b then (
				t.b.(i)
			) else (
				String.create add_bytes
			)
		) in
		t.b <- new_array;
		t.b_l <- t.b_l + add_bytes;
	) else (
		(* Add a multiple of max_len bytes *)
		let add_strings = (add_bytes - 1) / max_len + 1 in
		let new_array = Array.init (Array.length t.b + add_strings) (fun i ->
			if i < Array.length t.b then (
				t.b.(i)
			) else (
				String.create max_len
			)
		) in
		t.b <- new_array;
		t.b_l <- t.b_l + max_len * add_strings;
	)
;;


let create n =
	let n2 = max 1 n in
	let num_strings = (n2 - 1) / max_len + 1 in
	let b = Array.init num_strings (fun i ->
		if i < num_strings - 1 then (
			String.create max_len
		) else (
			String.create (n2 - (num_strings - 1) * max_len)
		)
	) in
	{
		b = b;
		i = 0;
		n = 0;
		l = 0;
		b_l = n2;
	}
;;

let sub t offset length =
	if length > Sys.max_string_length || offset < 0 || length < 0 || offset + length > t.l then (
		invalid_arg "Bigbuffer.sub"
	) else (
		let s = String.create length in
		let rec copy current_i input_offset output_offset current_length = (
			if current_length = 0 then () else (
				if input_offset >= String.length t.b.(current_i) then (
					(* NEXT! *)
					copy (succ current_i) (input_offset - String.length t.b.(current_i)) output_offset current_length
				) else if input_offset + current_length > String.length t.b.(current_i) then (
					(* Copy this whole string and keep going *)
					let copy_this_much = (String.length t.b.(current_i) - input_offset) in
					String.blit t.b.(current_i) input_offset s output_offset copy_this_much;
					copy (succ current_i) 0 (output_offset + copy_this_much) (current_length - copy_this_much)
				) else (
					(* The rest of the string is in this one *)
					String.blit t.b.(current_i) input_offset s output_offset current_length
				)
			)
		) in
		copy 0 offset 0 length;
		s
	)
;;

let contents t = 
	if t.l > Sys.max_string_length then (
		failwith "Bigbuffer.contents: buffer too long to store as a single string"
	) else (
		sub t 0 t.l
	)
;;

let nth t n =
	if n >= t.l || n < 0 then (
		invalid_arg "Bigbuffer.nth"
	) else (
		let rec goto current_i q = (
			if q >= String.length t.b.(current_i) then (
				goto (succ current_i) (q - String.length t.b.(current_i))
			) else (
				t.b.(current_i).[q]
			)
		) in
		goto 0 n
	)
;;

let length t = t.l;;

let clear t =
	t.i <- 0;
	t.n <- 0;
	t.l <- 0;
;;

let add_char t c =
	if t.l >= t.b_l then resize t 1;
	t.b.(t.i).[t.n] <- c;
	t.l <- succ t.l;
	if succ t.n = String.length t.b.(t.i) then (
		(* Increment i! *)
		t.n <- 0;
		t.i <- succ t.i;
	) else (
		(* Increment n *)
		t.n <- succ t.n
	)
;;

let add_substring t s offset length =
	if offset < 0 || length < 0 || offset + length > String.length s then (
		invalid_arg "Bigbuffer.add_substring"
	) else (
		if t.l + length > t.b_l then resize t length;
		let rec add_sub off len = (
			if len = 0 then () else (
				let max_add_bytes = String.length t.b.(t.i) - t.n in
				if len >= max_add_bytes then (
					(* Add max_add_bytes and re-run *)
					String.blit s off t.b.(t.i) t.n max_add_bytes;
					t.i <- succ t.i;
					t.n <- 0;
					t.l <- t.l + max_add_bytes;
					add_sub (off + max_add_bytes) (len - max_add_bytes)
				) else (
					(* Add the whole substring *)
					String.blit s off t.b.(t.i) t.n len;
					t.n <- t.n + len;
					t.l <- t.l + len;
				)
			)
		) in
		add_sub offset length
	)
;;

let add_string t s = add_substring t s 0 (String.length s);;

let add_bigbuffer t u =
	(* Adds u to the end of t *)
	if t.l + u.l > t.b_l then resize t u.l;
	for i = 0 to u.i - 1 do
		add_string t u.b.(i)
	done;
	add_substring t u.b.(u.i) 0 u.n
;;

let add_buffer t b = add_string t (Buffer.contents b);;

(* Not really checked; just added as a replacement for Buffer.add_channel *)
let add_channel t c length =
	if length < 0 then (
		invalid_arg "Bigbuffer.add_channel"
	) else (
		if t.l + length > t.b_l then resize t length;
		let rec add_more new_i new_n len = (
			if len = 0 then (new_i, new_n) else (
				let max_add_bytes = String.length t.b.(new_i) - new_n in
				if len >= max_add_bytes then (
					(* Add max_add_bytes and re-run *)
					really_input c t.b.(new_i) new_n max_add_bytes;
					add_more (succ new_i) 0 (len - max_add_bytes)
				) else (
					(* Just add the stuff *)
					really_input c t.b.(new_i) new_n len;
					(new_i, new_n + len)
				)
			)
		) in
		let (i,n) = add_more t.i t.n length in
		t.i <- i;
		t.n <- n;
		t.l <- t.l + length;
	)
;;

let output_bigbuffer c t =
	for i = 0 to t.i - 1 do
		output c t.b.(i) 0 (String.length t.b.(i))
	done;
	output c t.b.(t.i) 0 t.n
;;

let output_bigbuffer_unix fd t =
	for i = 0 to t.i - 1 do
		ignore (Unix.write fd t.b.(i) 0 (String.length t.b.(i)))
	done;
	ignore (Unix.write fd t.b.(t.i) 0 t.n)
;;

let shorten t by =
	if t.l >= by then (
		(* OK *)
		t.l <- t.l - by;
		if t.n >= by then (
			(* Good! Just change the index *)
			t.n <- t.n - by
		) else (
			(* Have to re-count the values *)
			let rec check_index left i = (
				let len = String.length t.b.(i) in
				if left > len then (
					check_index (left - len) (i + 1)
				) else (
					(i,left)
				)
			) in
			let (new_i, new_n) = check_index t.l 0 in
			t.i <- new_i;
			t.n <- new_n;
		)
	) else (
		invalid_arg "Bigbuffer.shorten"
	)
;;

let iter t (f : int -> string -> int -> unit) =
	let last_i = (if t.n = 0 then pred t.i else t.i) in
	let rec do_iter i start_char = (
		if i > last_i then (
			()
		) else (
			let str_now = t.b.(i) in
			let valid_len = (if i = last_i then t.l - start_char else String.length str_now) in
			f start_char str_now valid_len;
			do_iter (i + 1) (start_char + valid_len)
		)
	) in
	do_iter 0 0
;;

