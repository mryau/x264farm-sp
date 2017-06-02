let time a b n name =
	let t1 = Unix.gettimeofday () in
	for i = 1 to n do
		a b
	done;
	let t2 = Unix.gettimeofday () in
	Printf.printf "%s: %g\n%!" name (t2 -. t1)
;;

if false then (

	let s = String.create 262144 in

	let s_ocaml s a b = (
		for i = 0 to String.length s - 1 do
			if s.[i] = a then (
				s.[i] <- b
			)
		done
	) in
	let s_c s a b = Opt.string_replace_c s a b in

	let s_base = String.create 256 in
	for i = 0 to 255 do s_base.[i] <- Char.chr i done;
	let a = String.copy s_base in
	let b = String.copy s_base in
	s_ocaml a '\x1A' '\x1B';
	s_c     b '\x1A' '\x1B';
	Printf.printf "%S\n" a;
	Printf.printf "%S\n" b;
	Printf.printf "%B\n" (a = b);

	time (s_ocaml s '\x00') '\x00' 1000 "OCaml string";
	time (s_c     s '\x00') '\x00' 1000 "C     string";
);;


(
	let a = 9000 in
	let b = 255 in
	time (fun () -> a mod b) () 1000000000 "a mod b";
	time (fun () -> a land 255) () 1000000000 "a land 255";
);;


if false then (
	let s = String.make 16777216 '\x01' in
	let so = String.copy s in
	let sc = String.copy s in
	let sd = String.copy s in
	let c = '\x01' in
	let m = 1000786403 in
	let a = 656228603 in

	let string_replace_ocaml_random s c n m a = (
		let c1 = if c = '\x00' then '\x01' else Char.chr (Char.code c - 1) in
		let c2 = if c = '\xFF' then '\xFE' else Char.chr (Char.code c + 1) in
		let n_ref = ref n in
		for i = 0 to String.length s - 1 do
			if s.[i] = c then (
				let q = !n_ref * m + a in
				n_ref := q;
				s.[i] <- if q land 0x40000000 = 0 then c2 else c1;
			)
		done;
		!n_ref
	) in

	let o_ref = ref 28 in
	let c_ref = ref 28 in
	let d_ref = ref 28 in

(*
	time (fun () ->
		o_ref := string_replace_ocaml_random so c !o_ref m a
	) () 100 "OCaml replace";
*)
	time (fun () ->
		c_ref := Opt.string_replace_c_random2 sd c !d_ref m a
	) () 100 "C2    replace";
	time (fun () ->
		c_ref := Opt.string_replace_c_random sc c !c_ref m a
	) () 100 "C     replace";
	time (fun () ->
		c_ref := Opt.string_replace_c_random2 sd c !d_ref m a
	) () 100 "C2    replace";
	time (fun () ->
		c_ref := Opt.string_replace_c_random sc c !c_ref m a
	) () 100 "C     replace";
	Printf.printf "%B\n%!" (sc = sd);
(*
	let o_out = open_out_bin "o.raw" in
	output o_out so 0 16777216;
	close_out o_out;
	Printf.printf "Wrote O\n%!";
*)
);;

