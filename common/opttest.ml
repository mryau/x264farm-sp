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

Printf.printf "Computer has %d processors\n" (Opt.num_processors ());;


(* N:\ is a truecrypt FAT32 drive now (cuz I don't have any physical FAT32 drives *)
(*
let a = Unix.openfile "test.txt" [Unix.O_RDWR] 0o640;;

let ok = Opt.set_sparse a;;
Printf.printf "Set sparse OK? %B\n" ok;;

let str = String.make 1048576 '#';;
for i = 1 to 200 do
	ignore (Unix.write a str 0 1048576);
	Printf.printf "%dMiB\t\t\r%!" i
done;;


let ok2 = Opt.set_zero_data a 0L 4294969000L;;
Printf.printf "Set zero data OK? %B\n" ok2;;
*)

(*
let a = Unix.openfile "C:\\Documents and Settings\\Omion\\Local Settings\\Temp\\opttest.mkv" [Unix.O_RDWR] 0o600;;

ignore (Opt.set_sparse a);;
*)

(*
let (a,b,c) = Unix.open_process_full "x264 --crf 26 --quiet -b 8 --analyse all --trellis 2 -o out.mkv d:\\documents\\dvd\\mkv\\amelie\\amelie.avs" env;;
*)
(*
let a = Opt.create_process_win_c "D:\\Programs\\Multimedia\\x264\\720_omion.exe --crf 26 --quiet -b 8 --analyse all --trellis 2 -o out.mkv d:\\documents\\dvd\\mkv\\amelie\\amelie.avs" Opt.Idle;;
*)

let a = Opt.create_process_win "D:\\Programs\\Multimedia\\x264\\720_omion.exe" [|
	"--crf";
	"26";
	"--quiet";
	"-b";
	"8";
	"--analyse";
	"all";
	"--trellis";
	"2";
	"-o";
	"out.mkv";
	"d:\\documents\\dvd\\mkv\\amelie\\amelie.avs";
|] Opt.Idle;;

match a with
| None -> Printf.printf "FAILED\n%!";
| Some x -> (
	Printf.printf "MADE %d\n%!" (Obj.magic x);
	let (ok,exit) = Opt.get_exit_code_win_c x in
	Printf.printf "EXIT? %B,%d\n%!" ok exit;
	for i = 5 downto 1 do
		Printf.printf "  Killing in %d\n%!" i;
		Unix.sleep 1;
	done;
	let y = Opt.kill_process_win x in
	Printf.printf "Killed? %B\n%!" y;
	let y = Opt.kill_process_win x in
	Printf.printf "Killed? %B\n%!" y;
	let (ok,exit) = Opt.get_exit_code_win_c x in
	Printf.printf "EXIT? %B,%d\n%!" ok exit;
	Unix.sleep 10;
	let (ok,exit) = Opt.get_exit_code_win_c x in
	Printf.printf "EXIT? %B,%d\n%!" ok exit;
	let closed = Opt.close_handle x in
	Printf.printf "CLOSED? %B\n%!" closed;
	let (ok,exit) = Opt.get_exit_code_win_c x in
	Printf.printf "EXIT? %B,%d\n%!" ok exit;
(*
	match Opt.get_process_id_win x with
	| None -> Printf.printf "Does not exist\n%!";
	| Some x -> Printf.printf "Process still has ID %d\n%!" x
*)
	);;

(*
Printf.printf "Made process %d\n%!" a;;
if a >= 0 then (
	for i = 5 downto 1 do
		Printf.printf "  Killing in %d\n%!" i
		Unix.sleep 1;
	done;
*)
Unix.sleep 1000;;

