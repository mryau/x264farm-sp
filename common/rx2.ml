type char_t = Char of char | Range of (char * char);;
type internal_t = Istring of string (*| Ilist of internal_t list*) | Inone;;
type found_t = Found of (internal_t * int) list | Not_found | Overflow;;
(*type return_t = String of string | List of return_t list;;*)


let rec list_split l at =
	if at = 0 then (
		(* SPLIT HERE! *)
		Some ([], l)
	) else (
		match l with
		| hd :: tl -> (
			match list_split tl (pred at) with
			| Some (first_part, last_part) -> Some (hd :: first_part, last_part)
			| None -> None
		)
		| _ -> (
			(* Not enough list elements to split there *)
			None
		)
	)
;;

(* Tail-recursive but reverses the first list returned *)
let list_rev_split l at =
	let rec helper list_from list_to more = (
		if more = 0 then (
			Some (list_to, list_from)
		) else (
			match list_from with
			| hd :: tl -> helper tl (hd :: list_to) (pred more)
			| [] -> None
		)
	) in
	helper l [] at
;;


let constant findme =
	let findme_len = String.length findme in
	fun s at next_rx -> (
		let s_len = String.length s in
		if at + findme_len > s_len then (
			(* Not enough room here *)
			Overflow
		) else (
			let rec iter i = (
				if findme.[i] = s.[i + at] then (
					if i = 0 then true else iter (pred i)
				) else (
					false
				)
			) in
			if iter (pred findme_len) then (
				(* This part matches; try the rest *)
				match next_rx with
				| [] -> Found ((Inone, at + findme_len) :: [])
				| hd :: tl -> (
					match hd s (at + findme_len) tl with
					| Found new_tl -> Found ((Inone, at + findme_len) :: new_tl)
					| x -> x (* Something else doesn't match *)
				)
			) else (
				(* Didn't match *)
				Not_found
			)
		)
	)
;;

let dot =
	fun s at next_rx -> (
		if at >= String.length s then (
			Overflow
		) else (
			match next_rx with
			| [] -> Found ((Inone, succ at) :: [])
			| hd :: tl -> (
				match hd s (succ at) tl with
				| Found new_tl -> Found ((Inone, succ at) :: new_tl)
				| x -> x
			)
		)
	)
;;

(* Mostly useless... *)
let void =
	fun (s:string) at next_rx -> (
		match next_rx with
		| [] -> Found ((Inone, at) :: [])
		| hd :: tl -> (
			match hd s at tl with
			| Found new_tl -> Found ((Inone, at) :: new_tl)
			| x -> x
		)
	)
;;

let character char_list =
	let rec match_character c l = (
		match l with
		| [] -> false
		| Char x :: _ when x = c -> true
		| Range (a,b) :: _ when a <= c && c <= b (*&& a <= b*) -> true
		| Range (a,b) :: _ when (*b <= a &&*) b <= c && c <= a -> true
		| _ :: tl -> match_character c tl
	) in
	fun s at next_rx -> (
		if at >= String.length s then (
			Overflow
		) else (
			(* Test the current character *)
			if match_character s.[at] char_list then (
				match next_rx with
				| [] -> Found ((Inone, succ at) :: [])
				| hd :: tl -> (
					match hd s (succ at) tl with
					| Found new_tl -> Found ((Inone, succ at) :: new_tl)
					| x -> x
				)
			) else (
				(* Didn't match *)
				Not_found
			)
		)
	)
;;

let match_at n =
	fun s at next_rx -> (
		if at = n then (
			match next_rx with
			| [] -> Found ((Inone, at) :: [])
			| hd :: tl -> (
				match hd s at tl with
				| Found new_tl -> Found ((Inone, at) :: new_tl)
				| x -> x
			)
		) else (
			(* Nope *)
			Not_found
		)
	)
;;

let match_at_backwards n =
	fun s at next_rx -> (
		if String.length s - at = n then (
			match next_rx with
			| [] -> Found ((Inone, at) :: [])
			| hd :: tl -> (
				match hd s at tl with
				| Found new_tl -> Found ((Inone, at) :: new_tl)
				| x -> x
			)
		) else (
			(* Nope *)
			Not_found
		)
	)
;;



(*************)
(* MODIFIERS *)
(*************)

let group rx_list =
	let rx_list_reversed = List.rev rx_list in
	let rx_list_length = List.length rx_list in
	fun (s:string) at next_rx -> (
		let total_rx_list = List.rev_append rx_list_reversed next_rx in
		match total_rx_list with
		| hd :: tl -> (
			match hd s at tl with
			| Found x -> (
				match list_rev_split x rx_list_length with
				| Some ((_,goto) :: _, rest) -> (
					(* Found enough results to group *)
					Found ((Inone, goto) :: rest)
				)
				| Some ([], rest) -> (
					(* This happens when there are 0 things in the group; just ignore the match and keep going *)
					Found rest
				)
				| _ -> Not_found
			)
			| x -> x
		)
		| [] -> (
			(* Nothing in either the current list or the next one *)
			Found ((Inone, at) :: [])
		)
	)
;;

let keep keep_rx =
	fun s at next_rx -> (
		match keep_rx s at next_rx with
		| Found ((_, goto) :: tl) -> Found ((Istring (String.sub s at (goto - at)), goto) :: tl)
		| x -> x (* Nothing matched (also handle the hopefully impossible case of (Found []) *)
	)
;;

let min_max_hook min_val max_val current_rx =
	let real_min = max 0 min_val in
	let real_max = max real_min max_val in
	let rec make_list so_far more_times = (
		if more_times = 0 then (
			so_far
		) else (
			make_list (current_rx :: so_far) (pred more_times)
		)
	) in
	let first_current_list = make_list [] real_min in
	fun (s:string) at next_rx -> (
		let rec try_or_add num current_list = (
			if num > real_max then (
				Not_found
			) else (
				let group_rx = group current_list in
				match group_rx s at next_rx with
				| Found x -> Found x (* yay. *)
				| Not_found -> (
					(* See if the inside matches at all *)
					match group_rx s at [] with
					| Found x -> (
						(* Try again *)
						try_or_add (succ num) (current_rx :: current_list)
					)
					| _ -> Not_found (* 'num' inside copies didn't match, so 'num+1' inside copies can't either *)
				)
				| Overflow -> Overflow (* oops. *)
			)
		) in
		try_or_add real_min first_current_list
	)
;;

let min_hook min_val current_rx =
	let real_min = max 0 min_val in
	let rec make_list so_far more_times = (
		if more_times = 0 then (
			so_far
		) else (
			make_list (current_rx :: so_far) (pred more_times)
		)
	) in
	let first_current_list = make_list [] real_min in
	fun (s:string) at next_rx -> (
		let rec try_or_add current_list = (
			let group_rx = group current_list in
			match group_rx s at next_rx with
			| Found x -> Found x (* yay. *)
			| Not_found -> (
				(* See if the inside matches at all *)
				match group_rx s at [] with
				| Found x -> (
					(* Try again *)
					try_or_add (current_rx :: current_list)
				)
				| _ -> Not_found
			)
			| Overflow -> Overflow (* oops. *)
		) in
		try_or_add first_current_list
	)
;;

let  max_hook max_val = min_max_hook 0 max_val;;
let star_hook = min_hook 0;;
let plus_hook = min_hook 1;;

let min_max min_val max_val current_rx =
	let real_min = max 0 min_val in
	let real_max = max real_min max_val in
	let rec make_list so_far more_times = (
		if more_times = 0 then (
			so_far
		) else (
			make_list (current_rx :: so_far) (pred more_times)
		)
	) in
	let first_current_list = make_list [] real_min in
	fun (s:string) at next_rx -> (
		let rec find_largest_current_match num so_far current_list = (
			if num > real_max then (
				(* That's all! *)
				(so_far, false) (* The second part of the tuple indicates that no overflow was encountered *)
			) else (
				let group_rx = group current_list in
				match group_rx s at [] with
				| Found _ -> find_largest_current_match (succ num) (Some (current_list, num)) (current_rx :: current_list)
(*				| Not_found -> find_largest_current_match (succ num) so_far (current_rx :: current_list)*)
				| Not_found -> (so_far, false)
				| Overflow -> (so_far, true)
			)
		) in
		(* This is the most copies of the current rx that is able to match *)
		match find_largest_current_match real_min None first_current_list with
		| (None, true) -> (
			(* OVERFLOW *)
			Overflow
		)
		| (None, false) -> (
			(* Found no matches! *)
			Not_found
		)
		| (Some (largest_current_list, largest_current_len), _) -> (
			let rec match_all current_list len = (
				if len < real_min then (
					Not_found
				) else (
					match (current_list, (group current_list) s at next_rx) with
					| (_, Found x) -> Found x
					| (hd :: tl, _) -> match_all tl (pred len)
					| ([], _) -> Not_found
				)
			) in
			match_all largest_current_list largest_current_len
		)
	)
;;

let min_rx min_val current_rx =
	let real_min = max 0 min_val in
	let rec make_list so_far more_times = (
		if more_times = 0 then (
			so_far
		) else (
			make_list (current_rx :: so_far) (pred more_times)
		)
	) in
	let first_current_list = make_list [] real_min in
	fun (s:string) at next_rx -> (
		let rec find_largest_current_match num so_far current_list = (
			let group_rx = group current_list in
			match group_rx s at [] with
			| Found _ -> find_largest_current_match (succ num) (Some (current_list, num)) (current_rx :: current_list)
(*			| Not_found -> find_largest_current_match (succ num) so_far (current_rx :: current_list)*)
			| Not_found -> (so_far, false)
			| Overflow -> (so_far, true)
		) in
		(* This is the most copies of the current rx that is able to match *)
		match find_largest_current_match real_min None first_current_list with
		| (None, false) -> (
			(* Found no matches! *)
			Not_found
		)
		| (None, true) -> (
			(* Overflow *)
			Overflow
		)
		| (Some (largest_current_list, largest_current_len), _)-> (
			let rec match_all current_list len = (
				if len < real_min then (
					Not_found
				) else (
					match (current_list, (group current_list) s at next_rx) with
					| (_, Found x) -> Found x
					| (hd :: tl, _) -> match_all tl (pred len)
					| ([], _) -> Not_found
				)
			) in
			match_all largest_current_list largest_current_len
		)
	)
;;

let max_rx max_val = min_max 0 max_val;;
let star = min_rx 0;;
let plus = min_rx 1;;

let alternate inside_rx =
	fun s at next_rx -> (
		let rec helper = function
			| hd :: tl -> (
				match hd s at next_rx with
				| Found x -> Found x
				| _ -> helper tl
			)
			| [] -> Not_found
		in
		helper inside_rx
	)
;;


(*****************)
(* OPTIMIZATIONS *)
(*****************)
let dot_star =
	fun s at next_rx -> (
		match next_rx with
		| [] -> Found ((Inone, String.length s) :: [])
		| hd :: tl -> (
			let rec helper n = (
				if n < at then (
					Not_found
				) else (
					match hd s n tl with
					| Found new_tl -> Found ((Inone, n) :: new_tl)
					| x -> helper (pred n)
				)
			) in
			helper (String.length s)
		)
	)
;;

let dot_plus =
	fun s at next_rx -> (
		if at >= String.length s then (
			Overflow
		) else (
			match next_rx with
			| [] -> Found ((Inone, String.length s) :: [])
			| hd :: tl -> (
				let rec helper n = (
					if n <= at then (
						Not_found
					) else (
						match hd s n tl with
						| Found new_tl -> Found ((Inone, n) :: new_tl)
						| x -> helper (pred n)
					)
				) in
				helper (String.length s)
			)
		)
	)
;;

(*****************)
(* MAIN FUNCTION *)
(*****************)

let rx parse_list (str:string) =
	match parse_list with
	| [] -> Some []
	| hd :: tl -> (
		match hd str 0 tl with
		| Not_found -> None
		| Overflow -> None
		| Found x -> (
			let rec fix_list l = (
				match l with
				| (Istring x,_) :: tl -> x :: fix_list tl
				| hd :: tl -> fix_list tl
				| [] -> []
			) in
			Some (fix_list x)
		)
	)
;;

let rx_offset offset parse_list (str:string) =
	match parse_list with
	| [] -> Some ([],offset)
	| hd :: tl -> (
		match hd str offset tl with
		| Not_found -> None
		| Overflow -> None
		| Found x -> (
			let rec get_last_pos l = (
				match l with
				| (_,pos) :: [] -> pos
				| hd :: tl -> get_last_pos tl
				| [] -> offset
			) in
			let last_pos = get_last_pos x in
			let rec fix_list l = (
				match l with
				| (Istring x,_) :: tl -> x :: fix_list tl
				| hd :: tl -> fix_list tl
				| [] -> []
			) in
			Some (fix_list x, last_pos)
		)
	)
;;



(* MO BETTAH *)
type fancy_t =
	| Constant of string
	| Dot
	| Void
	| Character of char_t list
	| Match_at of int
	| Match_at_backwards of int
(* Modifiers *)
	| Group of fancy_t list
	| Keep of fancy_t
	| Min_max_hook of fancy_t * int * int
	| Min_hook of fancy_t * int
	| Max_hook of fancy_t * int
	| Hook of fancy_t
	| Star_hook of fancy_t
	| Plus_hook of fancy_t
	| Min_max of fancy_t * int * int
	| Min of fancy_t * int
	| Max of fancy_t * int
	| Star of fancy_t
	| Plus of fancy_t
	| Alternate of fancy_t list

	| Dot_star
	| Dot_plus
;;


(*
let rec optimize_list f =
	match f with
	| [] -> []
	| Character [Char c] :: tl -> optimize_list (Constant (String.make 1 c) :: tl)
	| Constant x :: Constant y :: tl when String.length x + String.length y <= Sys.max_string_length -> optimize_list (Constant (x ^ y) :: tl)
	| Star Dot :: tl -> optimize_list (Dot_star :: tl)
	| Plus Dot :: tl -> optimize_list (Dot_plus :: tl)

	| Group rx_list :: tl -> Group (optimize_list rx_list) :: optimize_list tl

	| hd :: tl -> hd :: optimize_list tl
;;
*)
let rec render_element = function
	| Constant s -> constant s
	| Dot -> dot
	| Void -> void
	| Character c -> character c
	| Match_at n -> match_at n
	| Match_at_backwards n -> match_at_backwards n
	| Group fancy_l -> group (render_list fancy_l)
	| Keep fancy -> keep (render_element fancy)
	| Min_max_hook (fancy,a,b) -> min_max_hook a b (render_element fancy)
	| Min_hook (fancy,a) -> min_hook a (render_element fancy)
	| Max_hook (fancy,b) -> min_max_hook 0 b (render_element fancy)
	| Hook fancy -> min_max 0 1 (render_element fancy)
	| Star_hook fancy -> min_hook 0 (render_element fancy)
	| Plus_hook fancy -> min_hook 1 (render_element fancy)
	| Min_max (fancy,a,b) -> min_max a b (render_element fancy)
	| Min (fancy,a) -> min_rx a (render_element fancy)
	| Max (fancy,b) -> min_max 0 b (render_element fancy)
	| Star fancy -> min_rx 0 (render_element fancy)
	| Plus fancy -> min_rx 1 (render_element fancy)
	| Alternate fancy_l -> alternate (List.map render_element fancy_l) (* Can't run render_list on this since the list is an alternation, not a succession *)
	| Dot_star -> dot_star
	| Dot_plus -> dot_plus
and render_list = function
	| [] -> []

	| Character [Char c] :: tl -> render_list (Constant (String.make 1 c) :: tl)
	| Constant x :: Constant y :: tl when String.length x + String.length y <= Sys.max_string_length -> render_list (Constant (x ^ y) :: tl)
	| Star Dot :: tl -> render_list (Dot_star :: tl)
	| Plus Dot :: tl -> render_list (Dot_plus :: tl)

	| hd :: tl -> render_element hd :: render_list tl
;;

let make_rx fancy_list =
	render_list fancy_list
;;

