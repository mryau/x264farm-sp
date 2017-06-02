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

type return_t = String of string | Int of int;;
type char_t = Char of char | Range of (char * char);;


type parser_t = (string -> int -> 'a list -> (return_t list * int) option as 'a) list -> (return_t list * int) option;;

(************)
(*   /.*/   *)
(************)
let dot_star keep =
	if keep then (
		fun s at next -> (
			let rec try_to x = (
				match next with
				| [] -> Some (String (String.sub s at (x - at)) :: [], x) (* Nothing comes later, so the full match succeeded *)
				| hd :: tl -> (
					match hd s x tl with
					| Some (ret,last) -> Some ((String (String.sub s at (x - at)) :: ret), last) (* Everything else matched here, so it's fine *)
					| None when x = at -> None (* Nothing matched, and the string is too small *)
					| None -> try_to (pred x) (* Decrease the match length *)
				)
			) in
			try_to (String.length s)
		)
	) else (
		fun s at next -> (
			let rec try_to x = (
				match next with
				| [] -> Some ([],x) (* Nothing comes later, so the full match succeeded *)
				| hd :: tl -> (
					match hd s x tl with
					| Some (ret,last) -> Some (ret,last) (* Everything else matched here, so it's fine *)
					| None when x = at -> None (* Nothing matched, and the string is too small *)
					| None -> try_to (pred x) (* Decrease the match length *)
				)
			) in
			try_to (String.length s)
		)
	)
;;

(*************)
(*   /.*?/   *)
(*************)
let dot_star_hook keep =
	if keep then (
		fun s at next -> (
			let len = String.length s in
			let rec try_to x = (
				match next with
				| [] -> Some (String (String.sub s at (x - at)) :: [], x) (* Nothing comes later, so the full match succeeded *)
				| hd :: tl -> (
					match hd s x tl with
					| Some (ret,last) -> Some (String (String.sub s at (x - at)) :: ret, last) (* Everything else matched here, so it's fine *)
					| None when x = len -> None (* Nothing matched, and the string is too small *)
					| None -> try_to (succ x) (* Decrease the match length *)
				)
			) in
			try_to at
		)
	) else (
		fun s at next -> (
			let len = String.length s in
			let rec try_to x = (
				match next with
				| [] -> Some ([],x) (* Nothing comes later, so the full match succeeded *)
				| hd :: tl -> (
					match hd s x tl with
					| Some (ret,last) -> Some (ret,last) (* Everything else matched here, so it's fine *)
					| None when x = len -> None (* Nothing matched, and the string is too big *)
					| None -> try_to (succ x) (* Decrease the match length *)
				)
			) in
			try_to at
		)
	)
;;

(******************)
(*   /constant/   *)
(******************)
let constant keep findme =
	let findme_len = String.length findme in
	if keep then (
		fun s at next -> (
			let s_len = String.length s in
			if at + findme_len > s_len then (
				(* There is not enough in the string to match *)
				None
			) else (
				let rec iter i = (
					if findme.[i] = s.[at + i] then (
						(* So far, so good *)
						if i = 0 then true else iter (pred i)
					) else (
						(* Not the same *)
						false
					)
				) in
				if iter (pred findme_len) then (
					(* Matches here; try the rest *)
					match next with
					| [] -> Some (String (String.copy findme) :: [], at + findme_len)
					| hd :: tl -> (
						(* Return whatever the rest returns *)
						hd s (at + findme_len) tl
					)
				) else (
					(* Did not match *)
					None
				)
			)
		)
	) else (
		fun s at next -> (
			let s_len = String.length s in
			if at + findme_len > s_len then (
				(* There is not enough in the string to match *)
				None
			) else (
				let rec iter i = (
					if findme.[i] = s.[at + i] then (
						(* So far, so good *)
						if i = 0 then true else iter (pred i)
					) else (
						(* Not the same *)
						false
					)
				) in
				if iter (pred findme_len) then (
					(* Matches here; try the rest *)
					match next with
					| [] -> Some ([], at + findme_len)
					| hd :: tl -> (
						(* Return whatever the rest returns *)
						hd s (at + findme_len) tl
					)
				) else (
					(* Did not match *)
					None
				)
			)
		)
	)
;;

let rec match_character c l =
	match l with
	| [] -> false
	| Char x :: _ when x = c -> true
	| Range (a,b) :: _ when a <= c && c <= b && a <= b -> true
	| Range (a,b) :: _ when b <= a && b <= c && c <= a -> true
	| _ :: tl -> match_character c tl
;;

(***********************)
(*   /[characters]+/   *)
(***********************)
(*
let characters keep c_list =
	if keep then (
		fun s at next -> (
			let s_len = String.length s in
			let rec keep_going matching i = (
				if i >= s_len then (
					(* off the end of the string *)
					(matching,i)
				) else if match_character s.[i] c_list then (
					(* A character matches *)
					keep_going (succ matching) (succ i)
				) else (
					(* Nothing matches *)
					(matching,i)
				)
			) in
			match keep_going 0 at with
			| (0,_) -> None
			| (i,next_at) -> (
				match next with
				| [] -> Some ([String (String.sub s at (next_at - at))], next_at)
				| hd :: tl -> (
					match hd s next_at tl with
					| None -> None
					| Some (ret,next) -> Some ((String (String.sub s at (next_at - at)) :: ret), next)
				)
			)
		)
	) else (
		fun s at next -> (
			let s_len = String.length s in
			let rec keep_going matching i = (
				if i >= s_len then (
					(* off the end of the string *)
					(matching,i)
				) else if match_character s.[i] c_list then (
					(* A character matches *)
					keep_going (succ matching) (succ i)
				) else (
					(* Nothing matches *)
					(matching,i)
				)
			) in
			match keep_going 0 at with
			| (0,_) -> None
			| (i,next_at) -> (
				match next with
				| [] -> Some ([],next_at)
				| hd :: tl -> hd s next_at tl
			)
		)
	)
;;
*)

(***************************)
(*   /[characters]{a,b}/   *)
(***************************)
let characters_min_max keep c_list min_len max_len =
	if keep then (
		fun s at next -> (
			let s_len = String.length s in
			let rec keep_going matching i = (
				if matching = max_len then (
					(* The matching length is the largest that it can be; end matching *)
					(matching,i)
				) else if i >= s_len then (
					(* off the end of the string *)
					(matching,i)
				) else if match_character s.[i] c_list then (
					(* A character matches *)
					keep_going (succ matching) (succ i)
				) else (
					(* Nothing matches *)
					(matching,i)
				)
			) in
			match keep_going 0 at with
			| (i,_) when i < min_len -> None
			| (i,next_at) -> (
				match next with
				| [] -> Some ([String (String.sub s at (next_at - at))], next_at)
				| hd :: tl -> (
					let rec try_next_at n = (
						(* Backtracks down to min_len to see if the next part matches *)
						if n < min_len then (
							(* Too small... *)
							None
						) else (
							match hd s n tl with
							| None -> try_next_at (pred n)
							| Some (ret,next) -> (
(*								Printf.printf "Characters_min_max String.sub %S %d %d\n%!" s at (n - at);*)
								Some ((String (String.sub s at (n - at)) :: ret), next)
							)
						)
					) in
					try_next_at next_at
				)
			)
		)
	) else (
		fun s at next -> (
			let s_len = String.length s in
			let rec keep_going matching i = (
				if matching = max_len then (
					(* The matching length is the largest that it can be; end matching *)
					(matching,i)
				) else if i >= s_len then (
					(* off the end of the string *)
					(matching,i)
				) else if match_character s.[i] c_list then (
					(* A character matches *)
					keep_going (succ matching) (succ i)
				) else (
					(* Nothing matches *)
					(matching,i)
				)
			) in
			match keep_going 0 at with
			| (i,_) when i < min_len -> None
			| (i,next_at) -> (
				match next with
				| [] -> Some ([],next_at)
				| hd :: tl -> (
					let rec try_next_at n = (
						(* Backtracks down to min_len to see if the next part matches *)
						if n < min_len then (
							(* Too small... *)
							None
						) else (
							match hd s n tl with
							| None -> try_next_at (pred n)
							| x -> x
						)
					) in
					try_next_at next_at
				)
			)
		)
	)
;;

(**************************)
(*   /[characters]{,b}/   *)
(**************************)
let characters_max keep c_list max_len =
	if keep then (
		fun s at next -> (
			let s_len = String.length s in
			let rec keep_going matching i = (
				if matching = max_len then (
					(* The matching length is the largest that it can be; end matching *)
					(matching,i)
				) else if i >= s_len then (
					(* off the end of the string *)
					(matching,i)
				) else if match_character s.[i] c_list then (
					(* A character matches *)
					keep_going (succ matching) (succ i)
				) else (
					(* Nothing matches *)
					(matching,i)
				)
			) in
			let (i,next_at) = keep_going 0 at in
			match next with
			| [] -> Some ([String (String.sub s at (next_at - at))], next_at)
			| hd :: tl -> (
				let rec try_next_at n = (
					(* Backtracks down to 0 to see if the next part matches *)
					if n < 0 then (
						(* Way too small... *)
						None
					) else (
						match hd s n tl with
						| None -> try_next_at (pred n)
						| Some (ret,next) -> (
(*							Printf.printf "Characters_max String.sub %S %d %d\n%!" s at (n - at);*)
							Some ((String (String.sub s at (n - at)) :: ret), next)
						)
					)
				) in
				try_next_at next_at
			)
		)
	) else (
		fun s at next -> (
			let s_len = String.length s in
			let rec keep_going matching i = (
				if matching = max_len then (
					(* The matching length is the largest that it can be; end matching *)
					(matching,i)
				) else if i >= s_len then (
					(* off the end of the string *)
					(matching,i)
				) else if match_character s.[i] c_list then (
					(* A character matches *)
					keep_going (succ matching) (succ i)
				) else (
					(* Nothing matches *)
					(matching,i)
				)
			) in
			let (i,next_at) = keep_going 0 at in
			match next with
			| [] -> Some ([],next_at)
			| hd :: tl -> (
				let rec try_next_at n = (
					(* Backtracks down to 0 to see if the next part matches *)
					if n < 0 then (
						(* Way too small... *)
						None
					) else (
						match hd s n tl with
						| None -> try_next_at (pred n)
						| x -> x
					)
				) in
				try_next_at next_at
			)
		)
	)
;;

(**************************)
(*   /[characters]{a,}/   *)
(**************************)
let characters_min keep c_list min_len =
	if keep then (
		fun s at next -> (
			let s_len = String.length s in
			let rec keep_going matching i = (
				if i >= s_len then (
					(* off the end of the string *)
					(matching,i)
				) else if match_character s.[i] c_list then (
					(* A character matches *)
					keep_going (succ matching) (succ i)
				) else (
					(* Nothing matches *)
					(matching,i)
				)
			) in
			match keep_going 0 at with
			| (i,_) when i < min_len -> None
			| (i,next_at) -> (
				match next with
				| [] -> Some ([String (String.sub s at (next_at - at))], next_at)
				| hd :: tl -> (
					let rec try_next_at n = (
						(* Backtracks down to min_len to see if the next part matches *)
						if n < min_len then (
							(* Too small... *)
							None
						) else (
							match hd s n tl with
							| None -> try_next_at (pred n)
							| Some (ret,next) -> (
(*								Printf.printf "Characters_min String.sub %S %d %d\n%!" s at (n - at);*)
								Some ((String (String.sub s at (n - at)) :: ret), next)
							)
						)
					) in
					try_next_at next_at
				)
			)
		)
	) else (
		fun s at next -> (
			let s_len = String.length s in
			let rec keep_going matching i = (
				if i >= s_len then (
					(* off the end of the string *)
					(matching,i)
				) else if match_character s.[i] c_list then (
					(* A character matches *)
					keep_going (succ matching) (succ i)
				) else (
					(* Nothing matches *)
					(matching,i)
				)
			) in
			match keep_going 0 at with
			| (i,_) when i < min_len -> None
			| (i,next_at) -> (
				match next with
				| [] -> Some ([],next_at)
				| hd :: tl -> (
					let rec try_next_at n = (
						(* Backtracks down to min_len to see if the next part matches *)
						if n < min_len then (
							(* Too small... *)
							None
						) else (
							match hd s n tl with
							| None -> try_next_at (pred n)
							| x -> x
						)
					) in
					try_next_at next_at
				)
			)
		)
	)
;;

(*************************)
(*   /[characters]{n}/   *)
(*************************)
let characters_exact keep c_list n =
	if keep then (
		fun s at next -> (
			let s_len = String.length s in
			let rec keep_going matching i = (
				if matching = n then (
					(* The matching length is the largest that it can be; end matching *)
					(matching,i)
				) else if i >= s_len then (
					(* off the end of the string *)
					(matching,i)
				) else if match_character s.[i] c_list then (
					(* A character matches *)
					keep_going (succ matching) (succ i)
				) else (
					(* Nothing matches *)
					(matching,i)
				)
			) in
			match keep_going 0 at with
			| (i,_) when i <> n -> None
			| (i,next_at) -> (
				match next with
				| [] -> Some ([String (String.sub s at (next_at - at))], next_at)
				| hd :: tl -> (
					(* Don't bother backtracking since only one length can match *)
					match hd s next_at tl with
					| None -> None
					| Some (ret,next) -> Some ((String (String.sub s at (next_at - at)) :: ret), next)
				)
			)
		)
	) else (
		fun s at next -> (
			let s_len = String.length s in
			let rec keep_going matching i = (
				if matching = n then (
					(* The matching length is the largest that it can be; end matching *)
					(matching,i)
				) else if i >= s_len then (
					(* off the end of the string *)
					(matching,i)
				) else if match_character s.[i] c_list then (
					(* A character matches *)
					keep_going (succ matching) (succ i)
				) else (
					(* Nothing matches *)
					(matching,i)
				)
			) in
			match keep_going 0 at with
			| (i,_) when i <> n -> None
			| (i,next_at) -> (
				match next with
				| [] -> Some ([],next_at)
				| hd :: tl -> (
					hd s next_at tl
				)
			)
		)
	)
;;

(***********************)
(*   /[characters]*/   *)
(***********************)
let characters_star keep c_list = characters_min keep c_list 0;;

(***********************)
(*   /[characters]+/   *)
(***********************)
let characters_plus keep c_list = characters_min keep c_list 1;;

(**********************)
(*   /[characters]/   *)
(**********************)
let character keep c_list = characters_exact keep c_list 1;;

(***********************)
(*   /[characters]?/   *)
(***********************)
let character_hook keep c_list = characters_max keep c_list 1;;




(*****************)
(*   /(a|b|c)/   *)
(*****************)
(* Alternates do not work properly - they will not backtrack with this implementation *)
(*
let alternate keep parser_list =
	if keep then (
		failwith "adfskldfsjkldfgsjkdfgsjkfes";
	) else (
		fun s at next -> (
			match parser_list with
			| [] -> ( (* Why did the user even bother?! *)
				match next with
				| [] -> Some ([],at)
				| hd :: tl -> hd s at tl (* Parse like normal *)
			)
			| _ -> (
				(* Iterate over the parsers until something returns Some (...) *)
				let iterate_parsers = function
					| [] -> None
					| (parse_hd :: parse_tl) :: alt_tl -> (
						(* Do stuff *)
						match parse_hd s at parse_tl with
						| None -> iterate_parsers alt_tl (* Try the next alternate *)
						| Some (found, next_at) -> (
							(* This alternate succeeded; *)
							match ;
						)
					)
					| ([]) :: alt_tl -> (
						(* This alternate has no parsers, for some reason *)
						iterate_parses alt_tl
					)
				in
			)
		)
	)
;;
*)







let rx parse_list str =
	match parse_list with
	| [] -> Some []
	| hd :: tl -> (
		match hd str 0 tl with
		| None -> None
		| Some (a,b) -> Some a
	)
;;


(* Same as above, but takes a starting offset and returns the offset of the byte after the last one that matched *)
let rx_offset offset parse_list str =
	match parse_list with
	| [] -> Some ([],offset)
	| hd :: tl -> hd str offset tl
;;

