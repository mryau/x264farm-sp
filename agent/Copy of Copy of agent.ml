
			let bs = new buffered_send ~print:p s "GOPF" in
			let mkv_read_guts () = (
				let h_unix = (
					let rec helper tries = (
						let stuff = (try
(*							Some (open_in_bin temp_name)*)
							if (Unix.LargeFile.stat temp_name).Unix.LargeFile.st_size > 0L then (
								Some (Unix.openfile temp_name [Unix.O_RDWR] 0o600)
							) else (
								p "temp file has nothing in it yet; waiting...";
								None
							)
						with
							_	-> None
						) in
						match stuff with
						| Some x -> x
						| None when tries = 0 -> (p "giving up trying to find temp file"; raise Not_found) (* Give up *)
						| None -> (Thread.delay 1.0; helper (pred tries))
					) in
					helper 60
				) in
				let is_sparse = Opt.set_sparse h_unix in
				p // sprintf "temp file sparse? %B" is_sparse;
				let set_zero_data = (if is_sparse then Opt.set_zero_data h_unix else fun a b -> false) in
				let h = Unix.in_channel_of_descr h_unix in
				let is_exception = (try
					let rec reader ct tcs nbp duration_location next_frame = (
						let before_pos = LargeFile.pos_in h in
						let before_len = LargeFile.in_channel_length h in
						let status = try
							match Matroska.input_id_and_length h with
							| Some (0x08538067, _) | Some (0x0549A966, _) | Some (0x0F43B675, _) -> (
								(* SEGMENT             INFO                   CLUSTER *)
								MKV_keep_going
							)
							| Some (0x0654AE6B, Some l) -> (
								(* TRACKS *)
								(* Just give the whole thing to the controller *)
								let q = String.create !-l in
								really_input h q 0 (String.length q);
								s#send "TRAX" q;
								MKV_keep_going
							)
							| Some (0x0AD7B1, Some l) when l <= 8L -> (
								(* TIMECODE SCALE *)
								let new_tcs = Matroska.input_uint 0L h !-l in
								MKV_new_tcs new_tcs
							)
							| Some (0x0489, Some l) when l <= 8L -> (
								let current_pos = LargeFile.pos_in h in
								p // sprintf "duration location is at %Ld" current_pos;
								seek_add h l;
								MKV_duration_location current_pos
							)
							| Some (0x67, Some l) when l <= 8L -> (
								(* CLUSTER TIMECODE *)

								(* Let's use this element to set the file sparse *)
								if before_pos >= 2097152L then (
									let floored = before_pos &&| 0xFFFFFFFFFFF00000L in (* Floors to nearest MB *)
									let zeroed = set_zero_data (floored -| 0x100000L) floored in
									if zeroed then (
										p // sprintf "zeroed from %Ld to %Ld (pos is %Ld)" (floored -| 0x100000L) floored before_pos
									) else (
										p // sprintf "failed to zero data (pos:%Ld, len:%Ld)" before_pos before_len
									)
								);

(*
											let s = Gc.stat () in
											p "GC stuff:";
											p // sprintf "  Minor words:       %.0f" s.Gc.minor_words;
											p // sprintf "  Promoted words:    %.0f" s.Gc.promoted_words;
											p // sprintf "  Major words:       %.0f" s.Gc.major_words;
											p // sprintf "  Minor collections: %d" s.Gc.minor_collections;
											p // sprintf "  Major collections: %d" s.Gc.major_collections;
											p // sprintf "  Heap words:        %d" s.Gc.heap_words;
											p // sprintf "  Heap chunks:       %d" s.Gc.heap_chunks;
											p // sprintf "  Live words:        %d" s.Gc.live_words;
											p // sprintf "  Live blocks:       %d" s.Gc.live_blocks;
											p // sprintf "  Free words:        %d" s.Gc.free_words;
											p // sprintf "  Free blocks:       %d" s.Gc.free_blocks;
											p // sprintf "  Largest free:      %d" s.Gc.largest_free;
											p // sprintf "  Fragments:         %d" s.Gc.fragments;
											p // sprintf "  Compactions:       %d" s.Gc.compactions;
											p // sprintf "  Top heap words:    %d" s.Gc.top_heap_words;
*)

								let new_ct = Matroska.input_uint 0L h !-l in
								MKV_new_ct new_ct
							)
							| Some (0x20, Some l) -> (
								(* BLOCK GROUP *)
								(* Save the location of the next block group (or whatever should be after this *)
								MKV_new_nbp (LargeFile.pos_in h +| l)
							)
							| Some (0x21, Some l) -> (
								(* XXX BLOCK XXX *)

								if nbp > before_len then (
									(* There can't be a frame here, since the location of the next block is after the end of the file *)
									p // sprintf "EOF: %Ld > %Ld" nbp before_len;
									MKV_EOF_in
								) else (
									(* The file should be large enough to contain the entire frame *)

									let b = Matroska.parse_block_header h l in

									let frame_guts = String.create !-(b.Matroska.block_frame_bytes) in
									really_input h frame_guts 0 (String.length frame_guts);

									let total_timecode = !|(b.Matroska.block_timecode) +| ct in
									let total_time_n = total_timecode *| tcs *| !|(job.job_fps_n) in
									let total_time_d = 1000000000L *| !|(job.job_fps_d) in
									let frame_num_64 = i64_div_round total_time_n total_time_d +| !|(job.job_seek) in

									(* If the current position is the calculated end of the block group,
									 * then there is no reference block, and therefore this frame is an I frame *)
									let i_frame = (LargeFile.pos_in h = nbp) in
									let keep_going = if i_frame then (
										(* Output the frame number *)
										let frame_num = !-frame_num_64 in
										let frame_num_string = String.create 4 in
										Pack.packN frame_num_string 0 frame_num;
										p // sprintf "MKV read thread sending I frame %d" frame_num;
										if bs#flush_req then (
											s#send "IFRM" frame_num_string;
											match s#recv with
											| ("CONT","") -> (p "keep going"; true)
											| ("STOP","") -> (p "controller says stop"; false)
											| _ -> (p "got something weird; better stop"; false)
										) else (
											(* Controller said stop when flushed *)
											false
										)
									) else (
										true
									) in

									if keep_going then (

										let send_frame_guts = if frame_num_64 = 0L && (job.job_version_major >= 2 || job.job_version_minor >= 1) then (
											(* Get rid of SEI on the first frame of the encode *)
											(* But only if controller sent version 1.1 *)
											if (
												String.length frame_guts > 6 &&
												String.sub frame_guts 0 2 = "\x00\x00" &&
												(frame_guts.[2] = '\x01' || frame_guts.[2] = '\x02') && (* I don't think this can be 3... although I don't know why it can also be 2 *)
												String.sub frame_guts 4 2 = "\x06\x05"
											) then (
												(* The beginning is OK; check the length and name *)
												match trap_exception2 read_xiph_from_string frame_guts 6 with
												| Exception _ -> frame_guts
												| Normal (sei_len, len_len) -> (
													if String.length frame_guts > 6 + sei_len + len_len && sei_len > 16 && String.sub frame_guts (6 + len_len) 16 = "\xDC\x45\xE9\xBD\xE6\xD9\x48\xB7\x96\x2C\xD8\x20\xD9\x23\xEE\xEF" then (
														let real_frame_start = 6 + len_len + sei_len + 1 in
														(* Send the stuff here *)
														s#send "USEI" (String.sub frame_guts 0 real_frame_start);
														String.sub frame_guts real_frame_start (String.length frame_guts - real_frame_start)
													) else (
														frame_guts
													)
												)
											) else (
												frame_guts
											)
										) else (
											frame_guts
										) in


										(* Now send the frame *)
										p // sprintf "MKV read thread sending frame data %Ld (pos %d)" frame_num_64 next_frame;

										ignore // bs#string_req (Matroska.string_of_id 0x0F43B675); (* Segment ID *)
										ignore // bs#string_req (Matroska.string_of_size None); (* Segment size *)

										let frame_num_matroska = Matroska.string_of_uint frame_num_64 in
										ignore // bs#string_req (Matroska.string_of_id 0x67); (* Timecode ID *)
										ignore // bs#string_req (Matroska.string_of_size (Some !|(String.length frame_num_matroska))); (* Timecode size *)
										ignore // bs#string_req frame_num_matroska; (* Timecode *)
										ignore // bs#string_req (Matroska.string_of_id 0x23);
										ignore // bs#string_req (Matroska.string_of_size (Some !|(4 + String.length send_frame_guts)));
										if i_frame then (
											ignore // bs#string_req "\x81\x00\x00\x80"; (* Simple block header (timecode=0 thanks to the new segment for each frame) *)
										) else (
											ignore // bs#string_req "\x81\x00\x00\x00"; (* Not an I frame *)
										);
										let req = bs#string_req send_frame_guts in
										if req then (
											MKV_new_frame
										) else (
											MKV_stop
										)
									) else (
										MKV_stop
									)
								)
							)
							| Some (x, Some l) -> (
								(* Unimportant block of known length *)
								seek_add h l;
								MKV_keep_going
							)
							| Some (x, None) -> (
								(* Unimportant block of unknown length (shouldn't happen) *)
								MKV_keep_going
							)
							| None -> (
								MKV_EOF_out
							)
						with
							End_of_file -> MKV_EOF_in
						in


						match status with
						| MKV_keep_going -> reader ct tcs nbp duration_location next_frame
						| MKV_new_frame -> reader ct tcs nbp duration_location (succ next_frame)
						| MKV_new_ct  new_ct  -> reader new_ct tcs nbp duration_location next_frame
						| MKV_new_tcs new_tcs -> reader ct new_tcs nbp duration_location next_frame
						| MKV_new_nbp new_nbp -> reader ct tcs new_nbp duration_location next_frame
						| MKV_duration_location new_dl -> reader ct tcs nbp (Some new_dl) next_frame
						| MKV_EOF_in -> (
							(* There was an EOF in the middle of an element; this means that this can't be the end *)
							LargeFile.seek_in h before_pos;
							let rec check_length times = (
								if times = 0 then (
									(* Give up waiting *)
									p "MKV read thread timed out reading MKV file in the middle of an element; raising EOF";
									raise End_of_file
								) else (
									let new_len = LargeFile.in_channel_length h in
									if new_len > before_len then (
										(* Something was added to the file since this iteration started *)
										reader ct tcs nbp duration_location next_frame
									) else (
										p "MKV read thread waiting (in)...";
										Thread.delay 1.0;
										check_length (pred times)
									)
								)
							) in
							check_length 240 (* Do it a bunch since there REALLY should be more *)
						)
						| MKV_EOF_out -> (
							(* This may be the end of the file, so check the duration *)
							match duration_location with
							| None -> (
								p "MKV read thread waiting for more, but there is no duration location";
								LargeFile.seek_in h before_pos;
								let rec check_length times = (
									if times = 0 then (
										(* Give up waiting *)
										p "MKV read thread timed out reading MKV file outside an element (no DL); raising EOF";
										raise End_of_file
									) else (
										let new_len = LargeFile.in_channel_length h in
										if new_len > before_len then (
											(* Something was added to the file since this iteration started *)
											reader ct tcs nbp duration_location next_frame
										) else (
											p "MKV read thread waiting (no DL)...";
											Thread.delay 1.0;
											check_length (pred times)
										)
									)
								) in
								check_length 240 (* Do it a bunch since there should at least be a duration *)
							)
							| Some dl -> (
								p // sprintf "MKV read thread waiting for more, and checking the 4 bytes at %Ld to see if anything's there" dl;
								let str = String.copy "\x00\x00\x00\x00" in
								let rec check_stuff times = (
									if times = 0 then (
										(* Give up *)
										p "MKV read thread timed out waiting for either the duration or a file extension; raising EOF";
										raise End_of_file
									) else (
										LargeFile.seek_in h dl;
										really_input h str 0 4;
										if str = "\x00\x00\x00\x00" then (
											(* No duration yet; check the length *)
											let new_len = LargeFile.in_channel_length h in
											if new_len > before_len then (
												(* At least there's more to the file *)
												LargeFile.seek_in h before_pos;
												reader ct tcs nbp duration_location next_frame
											) else (
												p "MKV read thread waiting (DL)...";
												Thread.delay 1.0;
												check_stuff (pred times)
											)
										) else (
											(* The duration changed, but now check to see that the length did NOT change *)
											let new_len = LargeFile.in_channel_length h in
											if new_len = before_len then (
												(* HUZZAH! *)
												p "MKV read thread got a duration, and the last length was the final one; exit";
												next_frame
											) else (
												(* Something else may have been written to the file right before the duration was checked *)
												p "MKV read thread got a duration, but the length changed since it was checked; re-run";
												LargeFile.seek_in h before_pos;
												reader ct tcs nbp duration_location next_frame
											)
										)
									)
								) in
								check_stuff 60
							)
						)
						| MKV_stop -> (
							p "stopping encode";
							next_frame
						)
					) in
					let total_frames = reader 0L 1000000L 0L None 0 in
					Normal total_frames
				with
					e -> Exception e
				) in

				(* Tell the video thread to give up *)
				Mutex.lock exit_x264_mutex;
				exit_x264_ref := true;
				Mutex.unlock exit_x264_mutex;
				Unix.close h_unix;

				match is_exception with
				| Normal total_frames -> (
					let total_frame_string = String.create 4 in
					Pack.packN total_frame_string 0 (total_frames + job.job_seek);
					bs#flush;
					s#send "EEND" total_frame_string
				)
				| Exception e -> raise e
			) in
			let mkv_read_thread = Thread.create (fun () ->
				try
					mkv_read_guts ()
				with
					e -> p // sprintf "MKV read thread died with %S" (Printexc.to_string e)
			) () in
			
			(* After everything's done, then join the video thread *)
			Thread.join video_thread;
			p "joined video thread";
			Thread.join mkv_read_thread;
			p "joined MKV read thread";

