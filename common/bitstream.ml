(*type nal_ref_idc_type = Nal_ref_idc_0 | Nal_ref_idc_1 | Nal_ref_idc_2 | Nal_ref_idc_3;;*)
(*
type nal_unit = {
	nal_ref_idc : int;
	nal_unit_type : int;
};;

type sps_set = {
	profile_idc : int;
	constraint_set0_flag : bool;
	constraint_set1_flag : bool;
	constraint_set2_flag : bool;
	constraint_set3_flag : bool;
	level_idc : int;
	seq_parameter_set_id
*)

let (//) a b = a b;;

let ( +|  ) = Int64.add;;
let ( -|  ) = Int64.sub;;
let ( *|  ) = Int64.mul;;
let ( !|  ) = Int64.of_int;;
let ( !-  ) = Int64.to_int;;
let ( >|- ) = Int64.shift_right;;
let ( >|  ) = Int64.shift_right_logical;;
let ( <|  ) = Int64.shift_left;;
let ( /|  ) = Int64.div;;
let ( ||| ) = Int64.logor;;
let ( &&| ) = Int64.logand;;
let ( ^^| ) = Int64.logxor;;


(* Used for signed Golomb codes *)
let sign c =
	if c = 0 then (
		0
	) else if c mod 2 = 0 then (
		-(c lsr 1)
	) else (
		(succ c) lsr 1
	)
;;



(* Returns true if the trailing bits are correct, false otherwise *)
let rbsp_trailing_bits s =
	if s#b 1 <> 1 then (
		false
	) else (
		let rec helper () = (
			if s#pos &&| 7L <> 0L then (
				if s#b 1 <> 0 then (
					false
				) else (
					helper ()
				)
			) else (
				true
			)
		) in
		helper ()
	)
;;

(***********************************)
(* SPS SPS SPS SPS SPS SPS SPS SPS *)
(***********************************)
type chroma_format_idc =
	| Grayscale
	| Subsample_hv
	| Subsample_h
	| Full_res of bool (* residual_color_transform_flag *)
;;

type chroma_format = {
	chroma_format_idc : chroma_format_idc;
	bit_depth_luma : int;
	bit_depth_chroma : int;
	qpprime_y_zero_transform_bypass_flag : bool;
	seq_scaling_matrix_present_flag : bool;
};;

(*                 66         77     88         100    110       122        144 *)
type profile_idc =
	| Profile_baseline                  (*  66 *)
	| Profile_main                      (*  77 *)
	| Profile_extended                  (*  88 *)
	| Profile_high     of chroma_format (* 100 *)
	| Profile_high_10  of chroma_format (* 110 *)
	| Profile_high_422 of chroma_format (* 122 *)
	| Profile_high_444 of chroma_format (* 144 *)
;;

type pic_order_cnt_type_0 = {
	log2_max_pic_order_cnt_lsb : int
};;
type pic_order_cnt_type =
	| Pic_order_cnt_type_0 of pic_order_cnt_type_0
	| Pic_order_cnt_type_other
;;
type frame_mbs_only =
	| Frame_mb_adaptive_frame_field_flag of bool
	| Frame_mbs_only
;;
type frame_crop = {
	frame_crop_left : int;
	frame_crop_right : int;
	frame_crop_top : int;
	frame_crop_bottom : int;
};;
type seq_parameter_set = {
	profile_idc : profile_idc;
	constraint_set0_flag : bool;
	constraint_set1_flag : bool;
	constraint_set2_flag : bool;
	constraint_set3_flag : bool;
	level_idc : int;
	seq_parameter_set_id : int;
	log2_max_frame_num : int;
	pic_order_cnt_type : pic_order_cnt_type;
	num_ref_frames : int;
	gaps_in_frame_num_value_allowed_flag : bool;
	pic_width_in_mbs : int;
	pic_height_in_map_units : int;
	frame_mbs_only : frame_mbs_only;
	direct_8x8_inference_flag : bool;
	frame_crop : frame_crop option;
};;

exception Invalid_sps of string;;
let get_sps s =
	let profile_idc_num = s#b 8 in (* Temporarily a number *)
	let constraint_set0_flag = s#b 1 = 1 in
	let constraint_set1_flag = s#b 1 = 1 in
	let constraint_set2_flag = s#b 1 = 1 in
	let constraint_set3_flag = s#b 1 = 1 in
	if s#b 4 <> 0 then raise (Invalid_sps "4 bits are not 0");
	let level_idc = s#b 8 in
	let seq_parameter_set_id = s#g in
	let profile_idc = match profile_idc_num with
		| 66 -> Profile_baseline
		| 77 -> Profile_main
		| 88 -> Profile_extended
		| pi -> (
			let innards = (
				let chroma_format_idc = match s#g with
					| 0 -> Grayscale
					| 2 -> Subsample_h
					| 3 -> Full_res (s#b 1 = 1)
					| x -> Subsample_hv
				in
				let bit_depth_luma = 8 + s#g in
				let bit_depth_chroma = 8 + s#g in
				let qpprime_y_zero_transform_bypass_flag = s#b 1 = 1 in
				let seq_scaling_matrix_present_flag = s#b 1 = 1 in
				{
					chroma_format_idc = chroma_format_idc;
					bit_depth_luma = bit_depth_luma;
					bit_depth_chroma = bit_depth_chroma;
					qpprime_y_zero_transform_bypass_flag = qpprime_y_zero_transform_bypass_flag;
					seq_scaling_matrix_present_flag = seq_scaling_matrix_present_flag;
				}
			) in
			match pi with
			| 100 -> Profile_high     innards
			| 110 -> Profile_high_10  innards
			| 122 -> Profile_high_422 innards
			| 144 -> Profile_high_444 innards
			|  x  -> raise (Invalid_sps "profile type invalid")
		)
	in
	let log2_max_frame_num = 4 + s#g in
	let pic_order_cnt_type = match s#g with
		| 0 -> Pic_order_cnt_type_0 {log2_max_pic_order_cnt_lsb = 4 + s#g}
		| _ -> Pic_order_cnt_type_other
	in
	let num_ref_frames = s#g in
	let gaps_in_frame_num_value_allowed_flag = s#b 1 = 1 in
	let pic_width_in_mbs = 1 + s#g in
	let pic_height_in_map_units = 1 + s#g in
	let frame_mbs_only = if s#b 1 = 0 then (
		Frame_mb_adaptive_frame_field_flag (s#b 1 = 1)
	) else (
		Frame_mbs_only
	) in
	let direct_8x8_inference_flag = s#b 1 = 1 in
	let frame_crop = if s#b 1 = 1 then (
		let fcl = s#g in
		let fcr = s#g in
		let fct = s#g in
		let fcb = s#g in
		Some {
			frame_crop_left = fcl;
			frame_crop_right = fcr;
			frame_crop_top = fct;
			frame_crop_bottom = fcb;
		}
	) else (
		None
	) in
	{
		profile_idc = profile_idc;
		constraint_set0_flag = constraint_set0_flag;
		constraint_set1_flag = constraint_set1_flag;
		constraint_set2_flag = constraint_set2_flag;
		constraint_set3_flag = constraint_set3_flag;
		level_idc = level_idc;
		seq_parameter_set_id = seq_parameter_set_id;
		log2_max_frame_num = log2_max_frame_num;
		pic_order_cnt_type = pic_order_cnt_type;
		num_ref_frames = num_ref_frames;
		gaps_in_frame_num_value_allowed_flag = gaps_in_frame_num_value_allowed_flag;
		pic_width_in_mbs = pic_width_in_mbs;
		pic_height_in_map_units = pic_height_in_map_units;
		frame_mbs_only = frame_mbs_only;
		direct_8x8_inference_flag = direct_8x8_inference_flag;
		frame_crop = frame_crop;
	}
;;

(*******)
(* PPS *)
(*******)
exception Invalid_pps of string;;
type pic_parameter_set = {
	pic_parameter_set_id : int;
	seq_parameter_set : seq_parameter_set;
	entropy_coding_mode_flag : bool;
	pic_order_present_flag : bool;
	num_slice_groups : int;
};; (* I think that's all I need *)
let get_pps s sps_array =
	let pps_id = s#g in
	let sps_id = s#g in
	if sps_id >= Array.length sps_array then raise (Invalid_pps "SPS index out of bounds");
	let sps = sps_array.(sps_id) in
	let pic_order_present_flag = s#b 1 = 1 in
	let entropy_coding_mode_flag = s#b 1 = 1 in
	let num_slice_groups = 1 + s#g in
	{
		pic_parameter_set_id = pps_id;
		seq_parameter_set = sps;
		entropy_coding_mode_flag = entropy_coding_mode_flag;
		pic_order_present_flag = pic_order_present_flag;
		num_slice_groups = num_slice_groups;
	}
;;



(****************)
(* SLICE HEADER *)
(****************)
type slice_type =
	| Slice_P      (* 0 *)
	| Slice_B      (* 1 *)
	| Slice_I      (* 2 *)
	| Slice_SP     (* 3 *)
	| Slice_SI     (* 4 *)
	| Slice_P_all  (* 5 *)
	| Slice_B_all  (* 6 *)
	| Slice_I_all  (* 7 *)
	| Slice_SP_all (* 8 *)
	| Slice_SI_all (* 9 *)
;;
type field_pic_flag =
	| Field_pic_frame_mbs_only    (* if "frame_mbs_only_flag" *)
	| Field_pic_flag_false        (* if no "frame_mbs_only_flag", but "field_pic_flag" is false *)
	| Field_pic_flag_bottom_false (* no "frame_mbs_only_flag", "field_pic_flag" true, "bottom field flag" false *)
	| Field_pic_flag_bottom_true  (* no "frame_mbs_only_flag", "field_pic_flag" true, "bottom field flag" true *)
;;
type slice_header_pic_order =
	| Slice_header_pic_order_cnt_type_0 of int
	| Slice_header_pic_order_cnt_type_other
;;
type slice_header = {
	first_mb_in_slice : int;
	slice_type : slice_type;
	pps : pic_parameter_set;
	frame_num : int;
	field_pic_flag : field_pic_flag;
	idr_pic_id : int option; (* Only set if NAL type is 5=Nal_type_idr *)
	pic_order_cnt : slice_header_pic_order;
};;
exception Invalid_slice_header of string;;
let get_slice_header s nal_type_idr pps_array =
	let first_mb_in_slice = s#g in
	let slice_type = match s#g with
		| 0 -> Slice_P
		| 1 -> Slice_B
		| 2 -> Slice_I
		| 3 -> Slice_SP
		| 4 -> Slice_SI
		| 5 -> Slice_P_all
		| 6 -> Slice_B_all
		| 7 -> Slice_I_all
		| 8 -> Slice_SP_all
		| 9 -> Slice_SI_all
		| _ -> raise (Invalid_slice_header "invalid slice type")
	in
	let pps_id = s#g in
	if pps_id >= Array.length pps_array then raise (Invalid_slice_header "PPS index out of bounds");
	let pps = pps_array.(pps_id) in
	let frame_num = s#b pps.seq_parameter_set.log2_max_frame_num in
	let field_pic_flag = if pps.seq_parameter_set.frame_mbs_only = Frame_mbs_only then (
		Field_pic_frame_mbs_only
	) else (
		if s#b 1 = 1 then (
			if s#b 1 = 1 then (
				Field_pic_flag_bottom_true
			) else (
				Field_pic_flag_bottom_false
			)
		) else (
			Field_pic_flag_false
		)
	) in
	let idr_pic_id = if nal_type_idr then (
		Some s#g
	) else (
		None
	) in
	let pic_order_cnt = match pps.seq_parameter_set.pic_order_cnt_type with
		| Pic_order_cnt_type_0 {log2_max_pic_order_cnt_lsb = x} -> Slice_header_pic_order_cnt_type_0 (s#b x)
		| _ -> Slice_header_pic_order_cnt_type_other
	in
	{
		first_mb_in_slice = first_mb_in_slice;
		slice_type = slice_type;
		pps = pps;
		frame_num = frame_num;
		field_pic_flag = field_pic_flag;
		idr_pic_id = idr_pic_id;
		pic_order_cnt = pic_order_cnt;
	}
;;

(***********************************)
(* SEI SEI SEI SEI SEI SEI SEI SEI *)
(***********************************)
type sei_unregistered = {
	sei_unregistered_uuid : string;
	sei_unregistered_data : string;
}
type payload_type =
	| Sei_buffering_period
	| Sei_pic_timing
	| Sei_pan_scan_rect
	| Sei_filler
	| Sei_user_data_registered
	| Sei_user_data_unregistered of sei_unregistered
	| Sei_recovery_point
	| Sei_dec_ref_pic_marking_repetition
	| Sei_spare_pic
	| Sei_scene_info
	| Sei_sub_seq_info
	| Sei_sub_seq_layer_characteristics
	| Sei_sub_seq_characteristics
	| Sei_full_frame_freeze
	| Sei_full_frame_freeze_release
	| Sei_full_frame_snapshot
	| Sei_progressive_refinement_segment_start
	| Sei_progressive_refinement_segment_end
	| Sei_motion_constrained_slice_group_set
	| Sei_film_grain_characteristics
	| Sei_deblocking_filter_display_preference
	| Sei_stereo_video_info
	| Sei_reserved_sei_message
;;
type sei_type = {
	payload_type : payload_type;
	payload_size : int;
};;
let get_sei_unregistered bytes s =
	let uuid_string = String.create 16 in
	let data_string = String.create (bytes - 16) in
	for i = 0 to 15 do
		uuid_string.[i] <- Char.chr (s#b 8)
	done;
	for i = 0 to (bytes - 17) do
		data_string.[i] <- Char.chr (s#b 8) (* Horribly inefficient! Yay! *)
	done;
	{
		sei_unregistered_uuid = uuid_string;
		sei_unregistered_data = data_string;
	}
;;
exception Invalid_sei of string;;
let get_sei s =
	let rec xiph so_far = (
		match s#b 8 with
		| 255 -> xiph (so_far + 255)
		| x -> so_far + x
	) in
	let type_int = xiph 0 in
	let size_int = xiph 0 in
	let payload_type = match type_int with
		|  0 -> Sei_buffering_period
		|  1 -> Sei_pic_timing
		|  2 -> Sei_pan_scan_rect
		|  3 -> Sei_filler
		|  4 -> Sei_user_data_registered
		|  5 -> Sei_user_data_unregistered (get_sei_unregistered size_int s)
		|  6 -> Sei_recovery_point
		|  7 -> Sei_dec_ref_pic_marking_repetition
		|  8 -> Sei_spare_pic
		|  9 -> Sei_scene_info
		| 10 -> Sei_sub_seq_info
		| 11 -> Sei_sub_seq_layer_characteristics
		| 12 -> Sei_sub_seq_characteristics
		| 13 -> Sei_full_frame_freeze
		| 14 -> Sei_full_frame_freeze_release
		| 15 -> Sei_full_frame_snapshot
		| 16 -> Sei_progressive_refinement_segment_start
		| 17 -> Sei_progressive_refinement_segment_end
		| 18 -> Sei_motion_constrained_slice_group_set
		| 19 -> Sei_film_grain_characteristics
		| 20 -> Sei_deblocking_filter_display_preference
		| 21 -> Sei_stereo_video_info
		|  _ -> Sei_reserved_sei_message
	in
	(* Byte-alignment *)
	if not (rbsp_trailing_bits s) then raise (Invalid_sei "rbsp_trailing_bits incorrect");
	{
		payload_type = payload_type;
		payload_size = size_int;
	}
;;


(***********************************)
(* NAL NAL NAL NAL NAL NAL NAL NAL *)
(***********************************)
type nal_type =
	| Nal_type_unspecified              (*  0, 24-31 *)
	| Nal_type_non_idr of slice_header  (*  1 - frame *)
	| Nal_type_partition_a              (*  2 *)
	| Nal_type_partition_b              (*  3 *)
	| Nal_type_partition_c              (*  4 *)
	| Nal_type_idr of slice_header      (*  5 - frame *)
	| Nal_type_sei of sei_type          (*  6 *)
	| Nal_type_sps of seq_parameter_set (*  7 *)
	| Nal_type_pps of pic_parameter_set (*  8 *)
	| Nal_type_aud                      (*  9 *)
	| Nal_type_end_of_sequence          (* 10 *)
	| Nal_type_end_of_stream            (* 11 *)
	| Nal_type_filler                   (* 12 *)
	| Nal_type_spse                     (* 13 *)
	| Nal_type_reserved                 (* 14-18, 20-23 *)
	| Nal_type_aux                      (* 19 *)
;;
type nal_ref_idc =
	| Nal_ref_disposable (* 0 *)
	| Nal_ref_low        (* 1 *)
	| Nal_ref_high       (* 2 *)
	| Nal_ref_highest    (* 3 *)
;;

type nal_id =
	| Nal_id_unspecified     (*  0, 24-31 *)
	| Nal_id_non_idr         (*  1 - frame *)
	| Nal_id_partition_a     (*  2 *)
	| Nal_id_partition_b     (*  3 *)
	| Nal_id_partition_c     (*  4 *)
	| Nal_id_idr             (*  5 - frame *)
	| Nal_id_sei             (*  6 *)
	| Nal_id_sps             (*  7 *)
	| Nal_id_pps             (*  8 *)
	| Nal_id_aud             (*  9 *)
	| Nal_id_end_of_sequence (* 10 *)
	| Nal_id_end_of_stream   (* 11 *)
	| Nal_id_filler          (* 12 *)
	| Nal_id_spse            (* 13 *)
	| Nal_id_reserved        (* 14-18, 20-23 *)
	| Nal_id_aux             (* 19 *)
;;
let string_of_nal_type = function
	| Nal_type_unspecified -> "unspecified"
	| Nal_type_non_idr _ -> "non-IDR"
	| Nal_type_partition_a -> "partition A"
	| Nal_type_partition_b -> "partition B"
	| Nal_type_partition_c -> "partition C"
	| Nal_type_idr _ -> "IDR"
	| Nal_type_sei _ -> "SEI"
	| Nal_type_sps _ -> "SPS"
	| Nal_type_pps _ -> "PPS"
	| Nal_type_aud -> "AUD"
	| Nal_type_end_of_sequence -> "end of sequence"
	| Nal_type_end_of_stream -> "end of stream"
	| Nal_type_filler -> "filler"
	| Nal_type_spse -> "SPSE"
	| Nal_type_reserved -> "reserved"
	| Nal_type_aux -> "aux"
;;
let string_of_nal_id = function
	| Nal_id_unspecified -> "unspecified"
	| Nal_id_non_idr -> "non-IDR"
	| Nal_id_partition_a -> "partition A"
	| Nal_id_partition_b -> "partition B"
	| Nal_id_partition_c -> "partition C"
	| Nal_id_idr -> "IDR"
	| Nal_id_sei -> "SEI"
	| Nal_id_sps -> "SPS"
	| Nal_id_pps -> "PPS"
	| Nal_id_aud -> "AUD"
	| Nal_id_end_of_sequence -> "end of sequence"
	| Nal_id_end_of_stream -> "end of stream"
	| Nal_id_filler -> "filler"
	| Nal_id_spse -> "SPSE"
	| Nal_id_reserved -> "reserved"
	| Nal_id_aux -> "aux"
;;

type nal_unit = {
	nal_ref_idc : nal_ref_idc;
	nal_unit_type : nal_type;
};;
(*
class nal_reader s =
	object(o)
		val mutable bit = 8
		val mutable byte = -1
		val s = s

		method 

	end
;;
*)
exception Invalid_nal of string;;
let get_nal len sps_perhaps pps_perhaps s =
	let pos_start = s#pos in
	let real_start = (pos_start +| 7L) &&| 0xFFFFFFFFFFFFFFF8L in
	s#seek real_start;
	if s#b 1 <> 0 then raise (Invalid_nal "first bit not 0");
	let nal_ref_idc = match s#b 2 with
		| 0 -> Nal_ref_disposable
		| 1 -> Nal_ref_low
		| 2 -> Nal_ref_high
		| _ -> Nal_ref_highest
	in
	let nal_unit_type_int = s#b 5 in
	let nal_unit_type = match (nal_unit_type_int, sps_perhaps, pps_perhaps) with
		| ( 1, _, Some pps) -> Nal_type_non_idr (get_slice_header s false pps)
		| ( 2, _, _       ) -> Nal_type_partition_a
		| ( 3, _, _       ) -> Nal_type_partition_b
		| ( 4, _, _       ) -> Nal_type_partition_c
		| ( 5, _, Some pps) -> Nal_type_idr (get_slice_header s true pps)
		| ( 6, _, _       ) -> Nal_type_sei (get_sei s)
		| ( 7, _, _       ) -> Nal_type_sps (get_sps s)
		| ( 8, Some sps, _) -> Nal_type_pps (get_pps s sps)
		| ( 9, _, _       ) -> Nal_type_aud
		| (10, _, _       ) -> Nal_type_end_of_sequence
		| (11, _, _       ) -> Nal_type_end_of_stream
		| (12, _, _       ) -> Nal_type_filler
		| (13, _, _       ) -> Nal_type_spse
		| (19, _, _       ) -> Nal_type_aux
		| ((0|24|25|26|27|28|29|30|31), _, _) -> Nal_type_unspecified
		| ((14|15|16|17|18|20|21|22|23), _, _) -> Nal_type_reserved
		| _ -> raise (Invalid_nal "NAL type not supported")
	in
	s#seek (real_start +| (len <| 3));
	{
		nal_ref_idc = nal_ref_idc;
		nal_unit_type = nal_unit_type;
	}
;;

(* An easy way to check the NAL type without actually reading it *)
let nal_id_of_char c =
	let b = Char.code c in
	let zero = b lsr 7 in
	if zero <> 0 then (
		None
	) else (
		let idc = (b lsr 5) land 0x03 in
		let t = b land 0x1F in
		let idc_type = [| Nal_ref_disposable; Nal_ref_low; Nal_ref_high; Nal_ref_highest |].(idc) in
		let nt = [|
			Nal_id_unspecified    ;
			Nal_id_non_idr        ;
			Nal_id_partition_a    ;
			Nal_id_partition_b    ;
			Nal_id_partition_c    ;
			Nal_id_idr            ;
			Nal_id_sei            ;
			Nal_id_sps            ;
			Nal_id_pps            ;
			Nal_id_aud            ;
			Nal_id_end_of_sequence;
			Nal_id_end_of_stream  ;
			Nal_id_filler         ;
			Nal_id_spse           ;
			Nal_id_reserved       ;
			Nal_id_reserved       ;
			Nal_id_reserved       ;
			Nal_id_reserved       ;
			Nal_id_reserved       ;
			Nal_id_aux            ;
			Nal_id_reserved       ;
			Nal_id_reserved       ;
			Nal_id_reserved       ;
			Nal_id_reserved       ;
			Nal_id_unspecified    ;
			Nal_id_unspecified    ;
			Nal_id_unspecified    ;
			Nal_id_unspecified    ;
			Nal_id_unspecified    ;
			Nal_id_unspecified    ;
			Nal_id_unspecified    ;
			Nal_id_unspecified    ;
		|].(t) in
		Some (idc_type, nt)
	)
;;

(*******************************************************************)
(* PRIVATE PRIVATE PRIVATE PRIVATE PRIVATE PRIVATE PRIVATE PRIVATE *)
(*******************************************************************)
type private_data = {
	private_profile : int;
	private_level : int;
	private_nalu_size : int;
	private_sps : seq_parameter_set array;
	private_pps : pic_parameter_set array;
};;

exception Invalid_private of string;;
let get_private s =
	ignore // s#b 8;
	let profile = s#b 8 in
	ignore // s#b 8;
	let level = s#b 8 in
	ignore // s#b 6;
	let nal_unit_size = 1 + s#b 2 in
	ignore // s#b 3;
	let num_sps = s#b 5 in

	(* Do it imperatively(sp?) to ensure the proper reading order *)
	let sps_list_ref = ref [] in
	for i = 1 to num_sps do
		let this_sps_length = !|(s#b 16) in
		let sps_start_pos = s#pos in
		let this_sps = get_nal this_sps_length None None s in
		(match this_sps.nal_unit_type with
			| Nal_type_sps x -> sps_list_ref := !sps_list_ref @ [x] (* Inefficient, but is done at most 31 times *)
			| _ -> raise (Invalid_private "other NAL type where there should be SPS")
		);
		s#seek (sps_start_pos +| (this_sps_length <| 3))
	done;
	let sps_array = Array.of_list !sps_list_ref in

	let num_pps = s#b 8 in
	let pps_list_ref = ref [] in
	for i = 1 to num_pps do
		let this_pps_length = !|(s#b 16) in
		let pps_start_pos = s#pos in
		let this_pps = get_nal this_pps_length (Some sps_array) None s in
		(match this_pps.nal_unit_type with
			| Nal_type_pps x -> pps_list_ref := !pps_list_ref @ [x] (* Also inefficient, but is done at most 255 times *)
			| _ -> raise (Invalid_private "other NAL type where there should be PPS")
		);
		s#seek (pps_start_pos +| this_pps_length <| 3)
	done;
	let pps_array = Array.of_list !pps_list_ref in

	{
		private_profile = profile;
		private_level = level;
		private_nalu_size = nal_unit_size;
		private_sps = sps_array;
		private_pps = pps_array;
	}
;;



(********)
(* TEST *)
(********)
(*                                         v NAL starts at offset 8, SPS starts at 9 *)
let s1 = "\x01\x4d\x40\x33\xff\xe1\x00\x16\x67\x4d\x40\x33\x9a\x72\x01\x60\x5b\x42\x00\x00\x07\xd2\x00\x01\x77\x01\x1e\x30\x63\x24\x01\x00\x04\x68\xee\x04\x72";;
(* I guess this was main profile *)
let s2 = "\x01\x90\x00\x33\xff\xe1\x00\x16\x67\x90\x00\x33\xae\x34\xe8\x0b\x02\xda\x10\x00\x00\x3e\x90\x00\x0b\xb8\x08\xf1\x83\x2a\x01\x00\x05\x68\xee\x01\xaf\x20";;

let frame2_1 = "\x65\x88\x80\x20\x01\x5f\xe8\xd6\x6a\xe4";;
let frame2_2 = "\x41\x9a\x01\x00\xb3\x5f\xba\xb9\x2a\x49";;

(*
let get_bg s start =
	let byte = ref (pred start) in
	let data = ref 0 in
	let bit = ref 8 in
	let rec get_bits so_far n = (
		if n = 0 then (
			so_far
		) else if !bit > 7 then (
			incr byte;
			bit := 0;
			data := Char.code s.[!byte];
			get_bits so_far n
		) else (
			let now = (so_far lsl 1) lor ((!data lsr (7 - !bit)) land 1) in
			incr bit;
			get_bits now (pred n)
		)
	) in
	let golomb () = (
		let rec count_zeros so_far = (
			match get_bits 0 1 with
			| 0 -> count_zeros (succ so_far)
			| _ -> so_far
		) in
		let zeros = count_zeros 0 in
		let num_plus_one = get_bits 1 zeros in
		pred num_plus_one
	) in
	(get_bits 0, golomb)
;;

let (b1,g1) = get_bg s1 0;;
let (b2,g2) = get_bg s2 0;;
*)

class bg_string s =
	object(o)
		val s = s
		val mutable bit = 8
		val mutable byte = -1
		val mutable data = 0
		
		method b n = (
			let rec h so_far n = (
				if n = 0 then (
					so_far
				) else if bit > 7 then (
					byte <- succ byte;
					bit <- bit - 8;
					data <- Char.code s.[byte];
					h so_far n
				) else (
					let now = (so_far lsl 1) lor ((data lsr (7 - bit)) land 1) in
					bit <- succ bit;
					h now (pred n)
				)
			) in
			h 0 n
		)

		method g = (
			let rec h so_far n = (
				if n = 0 then (
					so_far
				) else if bit > 7 then (
					byte <- succ byte;
					bit <- 0;
					data <- Char.code s.[byte];
					h so_far n
				) else (
					let now = (so_far lsl 1) lor ((data lsr (7 - bit)) land 1) in
					bit <- succ bit;
					h now (pred n)
				)
			) in
			let rec count_zeros so_far = (
				match h 0 1 with
				| 0 -> count_zeros (succ so_far)
				| _ -> so_far
			) in
			let zeros = count_zeros 0 in
			pred (h 1 zeros)
		)
		
		method pos = !|(byte lsl 3 + bit)

		method seek t64 = (
			let t = !-t64 in
			if t land 3 = 0 then (
				(* Bit is 0 *)
				byte <- pred (t lsr 3);
				bit <- 8;
				data <- 0;
			) else (
				byte <- t lsr 3;
				bit <- t land 7;
				data <- Char.code s.[byte];
			)
		)

		method seek_byte t = (
			bit <- 8;
			byte <- t - 1;
		)

		method pos_byte = if bit = 8 then byte + 1 else byte

	end
;;

(*
class bg_fd fdi =
	object(o)
		val fd = fdi
		val mutable start_byte = (Unix.LargeFile.lseek fdi 0L Unix.SEEK_CUR)
		val mutable bit = 8
		val mutable data = 0
		val temp_char = String.copy "\x00"

		method private bs s n = (
			let rec h so_far n = (
				if n = 0 then (
					so_far
				) else if bit > 7 then (
					bit <- bit - 8;
					let read_bytes = Unix.read fd temp_char 0 1 in
					if read_bytes <> 1 then raise End_of_file;
					data <- Char.code temp_char.[0];
					h so_far n
				) else (
					let now = (so_far lsl 1) lor ((data lsr (7 - bit)) land 1) in
					bit <- succ bit;
					h now (pred n)
				)
			) in
			h s n
		)

		method b n = o#bs 0 n

		method g = (
			let rec count_zeros so_far = (
				match o#bs 0 1 with
				| 0 -> count_zeros (succ so_far)
				| _ -> so_far
			) in
			let zeros = count_zeros 0 in
			pred (o#bs 1 zeros)
		)

		method pos = (
			let file_pos = Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR in
			let file_offset = Int64.to_int (Int64.sub file_pos start_byte) in
			let bit_offset = (file_offset - 1) * 8 + bit in
			bit_offset
		)

		method seek t = (
			let new_seek = Int64.of_int (t lsr 3) in
			let seek_here = Int64.add start_byte new_seek in
			ignore // Unix.LargeFile.lseek fd seek_here Unix.SEEK_SET;
			if t land 3 = 0 then (
				(* Bit is 0 *)
				bit <- 8;
			) else (
				(* Read a byte *)
				bit <- t land 7;
				let read_bytes = Unix.read fd temp_char 0 1 in
				if read_bytes <> 1 then raise End_of_file;
				data <- Char.code temp_char.[0];
			)
		)

		method seek_byte t = (
			let new_seek = Int64.add start_byte t in
			ignore // Unix.LargeFile.lseek fd new_seek Unix.SEEK_SET;
			bit <- 8;
		)

		method reset_start t = (
			ignore // Unix.LargeFile.lseek fd t Unix.SEEK_SET;
			bit <- 8;
			start_byte <- t;
		)

		method pos_byte_in_file = (
			if bit = 8 then (
				Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR
			) else (
				Int64.pred (Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR)
			)
		)
	end
;;
*)


class bg_fd fdi =
	object(o)
		val fd = fdi
		val mutable bit = 8
		val mutable data = 0
		val temp_char = String.copy "\x00"

		method private bs s n = (
			let rec h so_far n = (
				if n = 0 then (
					so_far
				) else if bit > 7 then (
					bit <- bit - 8;
					let read_bytes = Unix.read fd temp_char 0 1 in
					if read_bytes <> 1 then raise End_of_file;
					data <- Char.code temp_char.[0];
					h so_far n
(*
				) else if bit = 0 && n >= 8 then (
					(* Read a whole byte *)
					let now = (so_far lsl 8) lor data in
					h now (n - 8)
(*
					let read_bytes = Unix.read fd temp_char 0 1 in
					if read_bytes <> 1 then raise End_of_file;
					data <- Char.code temp_char.[0];
*)
*)
				) else (
					let now = (so_far lsl 1) lor ((data lsr (7 - bit)) land 1) in
					bit <- succ bit;
					h now (pred n)
				)
			) in
			h s n
		)

		method b n = o#bs 0 n

		method g = (
			let rec count_zeros so_far = (
				match o#bs 0 1 with
				| 0 -> count_zeros (succ so_far)
				| _ -> so_far
			) in
			let zeros = count_zeros 0 in
			pred (o#bs 1 zeros)
		)

		method pos = (
			let file_pos = Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR in
			((file_pos -| 1L) <| 3) +| !|bit
		)

		method seek t = (
			let seek_byte = t >| 3 in
			let seek_bit = !-(t &&| 7L) in
			ignore // Unix.LargeFile.lseek fd seek_byte Unix.SEEK_SET;
			if seek_bit = 0 then (
				(* Force a read next time *)
				bit <- 8;
			) else (
				(* Read a bit *)
				bit <- seek_bit;
				let read_bytes = Unix.read fd temp_char 0 1 in
				if read_bytes <> 1 then raise End_of_file;
				data <- Char.code temp_char.[0];
			)
		)

		method seek_byte t = (
			ignore // Unix.LargeFile.lseek fd t Unix.SEEK_SET;
			bit <- 8
		)

		method pos_byte = (
			if bit = 8 then (
				Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR
			) else (
				Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR -| 1L
			)
		)

		method pos_bit = bit

		method read_bytes s from len = (
			if bit = 8 then (
				(* Read like normal *)
				Unix.read fd s from len
			) else if bit = 0 && len > 0 then (
				(* Read a single byte *)
				s.[from] <- Char.chr data;
				bit <- 8;
				1
			) else (
				(* No can do, cap'n - not byte-aligned *)
				failwith "bg_fd#read_bytes needs a byte-aligned file position"
			)
		)

		method really_read_bytes s from len = (
			let rec helper f l = (
				if l = 0 then (
					()
				) else (
					let read_bytes = Unix.read fd s f l in
					if read_bytes = 0 then (
						raise End_of_file
					) else if read_bytes = l then (
						()
					) else (
						helper (f + read_bytes) (l - read_bytes)
					)
				)
			) in
			if bit = 8 then (
				helper from len
			) else if bit = 0 && len > 0 then (
				s.[from] <- Char.chr data;
				bit <- 8;
				helper (from + 1) (len - 1)
			) else (
				failwith "bg_fd#really_read_bytes needs a byte-aligned file position"
			)
		)

		method debug = (bit, data, Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR)

	end
;;
(*
class bg_buffered_fd ?(buf_bits=14) fdi =
	let buf_len = 1 lsl buf_bits in
	let buf_len_64 = Int64.of_int buf_len in
	object(o)
		val fd = fdi
		val mutable bit = 0
		val mutable valid_len = 0
		val mutable byte = 0
		val buffer = String.create buf_len
		val mutable last_read_location = 0L

		method private bs s n = (
			let rec h so_far n = (
				if n = 0 then (
					so_far
				) else if byte >= valid_len then (
					(* Over the end of the valid part; read some more *)
					last_read_location <- Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR;
					let got_bytes = Unix.read fd buffer 0 buf_len in
					if got_bytes = 0 then (
						raise End_of_file
					) else (
						byte <- byte - valid_len;
						valid_len <- got_bytes;
						h so_far n
					)
				) else if bit > 7 then (
					(* Increment the byte count *)
					bit <- bit - 8;
					byte <- byte + 1;
					h so_far n
				) else if bit = 0 && n >= 8 then (
					(* Read a whole byte *)
					let now = (so_far lsl 8) lor (Char.code buffer.[byte]) in
					byte <- byte + 1;
					h now (n - 8)
				) else (
					(* Just read a bit *)
					let now = (so_far lsl 1) lor ((Char.code buffer.[byte] lsr (7 - bit)) land 1) in
					bit <- succ bit;
					h now (pred n)
				)
			) in
			h s n
		)

		method b n = o#bs 0 n

		method g = (
			let rec count_zeros so_far = (
				match o#bs 0 1 with
				| 0 -> count_zeros (succ so_far)
				| _ -> so_far
			) in
			let zeros = count_zeros 0 in
			pred (o#bs 1 zeros)
		)

		method pos = (
			let file_pos = Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR in
			((file_pos -| buf_len_64 +| !|byte) <| 3) +| !|bit
		)

		method seek t = (
			let seek_byte = t >| 3 in
			let seek_bit = !-(t &&| 7L) in
			bit <- seek_bit;
			let first_unread_byte = last_read_location +| !|valid_len in
			if seek_byte >= last_read_location && seek_byte < first_unread_byte then (
				(* Don't bother actually seeking *)
				byte <- !-(seek_byte -| last_read_location);
			) else (
				(* READ! *)
				let next_read_location = (seek_byte >| buf_bits) <| buf_bits in
				ignore // Unix.LargeFile.lseek fd next_read_location Unix.SEEK_SET;
				byte <- !-(next_read_location ^^| seek_byte);
				valid_len <- 0; (* Invalidate buffer to force a read next time *)
			)
		)

		method pos_byte = (
			let file_pos = Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR in
			file_pos -| buf_len_64 +| !|byte
		)

		method seek_byte t = (
			bit <- 0;
			let first_unread_byte = last_read_location +| !|valid_len in
			if t >= last_read_location && t < first_unread_byte then (
				(* Don't bother actually seeking *)
				byte <- !-(t -| last_read_location);
			) else (
				(* READ! *)
				let next_read_location = (t >| buf_bits) <| buf_bits in
				ignore // Unix.LargeFile.lseek fd next_read_location Unix.SEEK_SET;
				byte <- !-(next_read_location ^^| t);
				valid_len <- 0; (* Invalidate buffer to force a read next time *)
			)
		)


		(* Throws out the current pos and refreshes the whole buffer *)
		method get_more_bytes = (
			last_read_location <= Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR;
			let got_bytes = Unix.read fd buffer 0 buf_len in
			if got_bytes = 0 then (
				raise End_of_file
			) else (
				byte <- 0;
				bit <- 0;
				valid_len <- got_bytes;
			)
		)

		method get_buffer = (buffer, (byte,bit), valid_len)


		(* Byte-wise reading *)
(*
		method input s from len = (
			if from < 0 || len < 0 || from + len > String.length s then (
				invalid_arg "bg_buffered_fd#input";
			) else if bit <> 0 then (
				(* Can't do dat *)
				failwith "bg_buffered_fd#input needs a byte-aligned input position"
			) else (
				if
				let bytes = min l (valid_len - byte) in
*)

		method debug = (bit, valid_len, byte, last_read_location, buffer, Unix.LargeFile.lseek fd 0L Unix.SEEK_CUR)

	end
;;
*)
