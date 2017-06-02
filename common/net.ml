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

class net ?print (sin,sout) =
	let buffer_length = 8 in
	let start_string_length = 4 in
	let timeout_length = 60.0 in
	let indy_frame = "INDY\x00\x00\x00\x00" in
	let indy_frame_length = String.length indy_frame in

	object (o)
		method private print = (match print with
			| None -> ignore
			| Some f -> f
		)

		method really_recv d s off len = (
			if len <= 0 then () else (
				let r = try
					Unix.recv d s off len []
				with
					e -> (
						(match e with
							| Unix.Unix_error (x,y,z) -> (o#print (Printf.sprintf "really_recv got Unix error (%s,%s,%s)" (unix_error_string x) y z))
							| _ -> o#print (Printf.sprintf "really_recv got error %S" (Printexc.to_string e))
						);
						Thread.delay 0.1;
						Unix.recv d s off len []
					)
				in
				if r = 0 then (
					raise End_of_file
				) else if r = len then (
					()
				) else (
					o#really_recv d s (off + r) (len - r)
				)
			)
		)

		method really_send d s off len = (
			if len <= 0 then () else (
				let r = try
					Unix.send d s off len []
				with
					e -> (
						(match e with
							| Unix.Unix_error (x,y,z) -> (o#print (Printf.sprintf "really_send got Unix error (%s,%s,%s)" (unix_error_string x) y z))
							| _ -> o#print (Printf.sprintf "really_send got error %S" (Printexc.to_string e))
						);
						Thread.delay 0.1;
						Unix.send d s off len []
					)
				in
				if r = 0 then (
					raise End_of_file
				) else if r = len then (
					()
				) else (
					o#really_send d s (off + r) (len - r)
				)
			)
		)

		val put_out_e : buffer_t option Event.channel = Event.new_channel ()
		val get_out_e : buffer_t option Event.channel = Event.new_channel ()
		val put_in_e  : buffer_t        Event.channel = Event.new_channel ()
		val get_in_e  : buffer_t        Event.channel = Event.new_channel ()

		val out_buffer = (
			Thread.create (fun () ->
				try
					let q = Queue.create () in
					let rec keep_going () = (
						if Queue.is_empty q then (
							(* Only wait to have things put into it *)
							let put_this_in = Event.sync (Event.receive put_out_e) in
