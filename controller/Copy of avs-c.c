#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/unixsupport.h>


#ifdef _WIN32
// # i n c l u d e <windows.h>
// # i n c l u d e <winioctl.h>
// # i n c l u d e <fcntl.h>
//#include <vfw.h>
#else
#error "Not win32? AVS hooks don't work"
#endif

#include "avisynth_c.h"





/*
CAMLprim value info_vfw(value file_val) {
	CAMLparam1(file_val);
	CAMLlocal2(output,outinfo);

	PAVIFILE avi;
	AVIFileInit();
	if(AVIFileOpen(&avi, String_val(file_val), OF_PARSE, NULL)) {
		output = Int_val(0);
	} else {
		AVIFILEINFO info;
		AVIFileInfo(avi, &info, sizeof(info));
/ *
		printf("MaxBytesPerSec      = %d\n", info.dwMaxBytesPerSec     );
		printf("Flags               = %d\n", info.dwFlags              );
		printf("Caps                = %d\n", info.dwCaps               );
		printf("Streams             = %d\n", info.dwStreams            );
		printf("SuggestedBufferSize = %d\n", info.dwSuggestedBufferSize);
		printf("Width               = %d\n", info.dwWidth              );
		printf("Height              = %d\n", info.dwHeight             );
		printf("Scale               = %d\n", info.dwScale              );
		printf("Rate                = %d\n", info.dwRate               );
		printf("Length              = %d\n", info.dwLength             );
		printf("EditCount           = %d\n", info.dwEditCount          );
* /
		AVIFileRelease(avi);

		outinfo = caml_alloc_tuple(5);
		Store_field(outinfo,0,Val_int(info.dwWidth));
		Store_field(outinfo,1,Val_int(info.dwHeight));
		Store_field(outinfo,2,Val_int(info.dwRate));
		Store_field(outinfo,3,Val_int(info.dwScale));
		Store_field(outinfo,4,Val_int(info.dwLength));
		output = caml_alloc_tuple(1);
		Store_field(output,0,outinfo);
	}

	CAMLreturn(output);
}
*/

CAMLprim value test(value file_val) {
	CAMLparam1(file_val);
	AVS_ScriptEnvironment *script = avs_create_script_environment(2);
	AVS_Value str = avs_new_value_string(String_val(file_val));
	AVS_Value throwaway1 = avs_invoke(script, "SetMemoryMax", avs_new_value_int(16), NULL);
	AVS_Value throwaway2 = avs_invoke(script, "SetMemoryMax", avs_new_value_int(32), NULL);
	AVS_Value ta3 = avs_invoke(script, "Cos", avs_new_value_float(1.0), NULL);
	AVS_Value ret = avs_invoke(script, "Import", str, NULL);
	CAMLlocal1(string_val);
	if(avs_is_int(throwaway1)) {
		printf("Had used %d,%d memory - %f\n", avs_as_int(throwaway1), avs_as_int(throwaway2), avs_as_float(ta3));
	}
	if(!avs_is_clip(ret)) {
		printf("AVS file does not produce a clip\n");
		CAMLreturn(file_val);
	} else {
		// everything worked so far
		AVS_Clip *clip;
		const AVS_VideoInfo *info;
		int bytes_per_frame;
		char *outstr;
		AVS_VideoFrame *frame;
		int f;
		int offset;
		static const int planes[] = {AVS_PLANAR_Y, AVS_PLANAR_U, AVS_PLANAR_V};
		// In one frame
		int c;
		clip = avs_take_clip(ret, script);
		info = avs_get_video_info(clip);
		
		bytes_per_frame = info->width * info->height * 3 / 2;
		string_val = caml_alloc_string(bytes_per_frame);
		outstr = String_val(string_val);
		frame = avs_get_frame(clip, 1000);

			for(c = 0; c < 3; c++) {
				// In one plane
				int w = info->width >> (c ? 1 : 0);
				int h = info->height >> (c ? 1 : 0);
				int pitch = avs_get_pitch_p(frame, planes[c]);
				const BYTE* data = avs_get_read_ptr_p(frame, planes[c]);
				int y;
				for(y = 0; y < h; y++) {
					memcpy(outstr, data, w);
					outstr += w;
					data += pitch;
				}
			}

		avs_release_video_frame(frame);

		avs_release_clip(clip);
		CAMLreturn(string_val);
	}
}

CAMLprim value open_avs(value filename_val, value mem_val) {
	CAMLparam2(filename_val, mem_val);
	AVS_ScriptEnvironment *script = avs_create_script_environment(2);
	AVS_Value str = avs_new_value_string(String_val(filename_val));
	AVS_Value ret = avs_invoke(script, "Import", str, NULL);
	AVS_Value throwaway1 = avs_invoke(script, "SetMemoryMax", avs_new_value_int(32), NULL);

	CAMLlocal2(ret_val, tuple_val);

	if(avs_is_clip(ret)) {
		AVS_Clip *clip = avs_take_clip(ret, script);

		// Check to see if the video is in YV12 format
		const AVS_VideoInfo *info = avs_get_video_info(clip);
		if(!avs_is_yv12(info)) {
			avs_release_clip(clip); // I think I need to do this...
			ret = avs_invoke(script, "ConvertToYV12", ret, NULL);
			clip = avs_take_clip(ret, script);
//			printf("converting to YV12\n");
		}

		tuple_val = caml_alloc(8,0);
		ret_val = caml_alloc(1,0);
		Store_field(tuple_val, 0, (value)script);
		Store_field(tuple_val, 1, (value)clip);
		Store_field(tuple_val, 2, Val_int(info->width));
		Store_field(tuple_val, 3, Val_int(info->height));
		Store_field(tuple_val, 4, Val_int(info->fps_numerator));
		Store_field(tuple_val, 5, Val_int(info->fps_denominator));
		Store_field(tuple_val, 6, Val_int(info->num_frames));
		Store_field(tuple_val, 7, Val_int(info->width * info->height / 2 * 3));
		Store_field(ret_val, 0, tuple_val);
	} else if(avs_is_error(ret) || avs_is_string(ret)) {
		ret_val = caml_alloc(1,1);
		Store_field(ret_val, 0, caml_copy_string(avs_as_string(ret)));
	} else {
		ret_val = caml_alloc(1,1);
		Store_field(ret_val, 0, caml_copy_string("AVS file did not result in a video clip"));
	}

	CAMLreturn(ret_val);
}

CAMLprim void close_avs(value script_val) {
	CAMLparam1(script_val);
	AVS_ScriptEnvironment *script = (AVS_ScriptEnvironment *)Field(script_val, 0);
	AVS_Clip *clip = (AVS_Clip *)Field(script_val, 1);

	avs_release_clip(clip);
	avs_delete_script_environment(script);

	CAMLreturn0;
}

CAMLprim value get_info(value script_val) {
	CAMLparam1(script_val);
	const AVS_VideoInfo *info = avs_get_video_info((AVS_Clip *)Field(script_val, 1));
	CAMLlocal1(out_val);

	out_val = caml_alloc_tuple(5);
	Store_field(out_val,0,Val_int(info->width));
	Store_field(out_val,1,Val_int(info->height));
	Store_field(out_val,2,Val_int(info->fps_numerator));
	Store_field(out_val,3,Val_int(info->fps_denominator));
	Store_field(out_val,4,Val_int(info->num_frames));

	CAMLreturn(out_val);
}


CAMLprim value info(value file_val) {
	CAMLparam1(file_val);
	AVS_ScriptEnvironment *script = avs_create_script_environment(2);
	AVS_Value str = avs_new_value_string(String_val(file_val));
	AVS_Value ret = avs_invoke(script, "Import", str, NULL);
	CAMLlocal2(ret_val, tuple_val);

	if(avs_is_clip(ret)) {
		AVS_Clip *clip = avs_take_clip(ret, script);

		// Don't bother making sure it's YV12, since we'll just close it anyway
		const AVS_VideoInfo *info = avs_get_video_info(clip);

		tuple_val = caml_alloc(5,0);
		ret_val = caml_alloc(1,0);
		Store_field(tuple_val,0,Val_int(info->width));
		Store_field(tuple_val,1,Val_int(info->height));
		Store_field(tuple_val,2,Val_int(info->fps_numerator));
		Store_field(tuple_val,3,Val_int(info->fps_denominator));
		Store_field(tuple_val,4,Val_int(info->num_frames));
		Store_field(ret_val, 0, tuple_val);

		avs_release_clip(clip);
	} else if(avs_is_error(ret) || avs_is_string(ret)) {
		ret_val = caml_alloc(1,1);
		Store_field(ret_val, 0, caml_copy_string(avs_as_string(ret)));
	} else {
		ret_val = caml_alloc(1,1);
		Store_field(ret_val, 0, caml_copy_string("AVS file did not result in a video clip"));
	}

	avs_release_value(ret);
	avs_release_value(str);

	avs_delete_script_environment(script);

	CAMLreturn(ret_val);
}



/*
CAMLprim value get_frame(value script_val, value frame_val) {
	CAMLparam2(script_val, frame_val);
	char *str;
	int len;
	int h = Int_val(Field(script_val, 3));
	int w = Int_val(Field(script_val, 2));
	int i;
	CAMLlocal1(str_val);

	AVS_Clip *clip = (AVS_Clip *)Field(script_val, 1);
	AVS_VideoFrame *frame = avs_get_frame(clip, Int_val(frame_val));
	int pitch;
	const BYTE* data;

	len = Int_val(Field(script_val, 7));

	str_val = caml_alloc_string(len);
	str = String_val(str_val);

	// Y
	pitch = avs_get_pitch_p(frame, AVS_PLANAR_Y);
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_Y);
	for(i = 0; i < h; i++) {
		memcpy(str, data, w);
		str += w;
		data += pitch;
	}

	// The U and V planes are 1/2 the width and height of the Y plane
	h >>= 1;
	w >>= 1;

	// U
	pitch = avs_get_pitch_p(frame, AVS_PLANAR_U);
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_U);
	for(i = 0; i < h; i++) {
		memcpy(str, data, w);
		str += w;
		data += pitch;
	}

	// V
	// pitch is not needed here, since it is the same as the U pitch
//	pitch = avs_get_pitch_p(frame, AVS_PLANAR_V);
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_V);
	for(i = 0; i < h; i++) {
		memcpy(str, data, w);
		str += w;
		data += pitch;
	}

	avs_release_video_frame(frame);
	CAMLreturn(str_val);
}
*/

// BLIT BLIT BLIT BLIT BLIT BLIT BLIT BLIT //

CAMLprim void blit_frames_unsafe(value script_val, value start_frame_val, value length_val, value str_val, value off_val) {
	CAMLparam5(script_val, start_frame_val, length_val, str_val, off_val);
	char *str = String_val(str_val) + Int_val(off_val);
	int h = Int_val(Field(script_val, 3));
	int w = Int_val(Field(script_val, 2));
	int f;
	int i;
	int pitch;
	const byte *data;

	AVS_Clip *clip = (AVS_Clip *)Field(script_val, 1);
	AVS_VideoFrame *frame;

	for(f = Int_val(start_frame_val); f < Int_val(start_frame_val) + Int_val(length_val); f++) {
//		enter_blocking_section();
		frame = avs_get_frame(clip, f);
//		leave_blocking_section();

		// Y
		pitch = avs_get_pitch_p(frame, AVS_PLANAR_Y);
		data = avs_get_read_ptr_p(frame, AVS_PLANAR_Y);
		for(i = 0; i < h; i++) {
			memcpy(str, data, w);
			str += w;
			data += pitch;
		}

		h >>= 1;
		w >>= 1;

		// U
		pitch = avs_get_pitch_p(frame, AVS_PLANAR_U);
		data = avs_get_read_ptr_p(frame, AVS_PLANAR_U);
		for(i = 0; i < h; i++) {
			memcpy(str, data, w);
			str += w;
			data += pitch;
		}

		// V
		data = avs_get_read_ptr_p(frame, AVS_PLANAR_V);
		for(i = 0; i < h; i++) {
			memcpy(str, data, w);
			str += w;
			data += pitch;
		}

		h <<= 1;
		w <<= 1;

		avs_release_video_frame(frame);
	}

	CAMLreturn0;
}

CAMLprim void blit_frame_unsafe(value script_val, value frame_val, value str_val, value off_val) {
	CAMLparam4(script_val, frame_val, str_val, off_val);
	char *str = String_val(str_val) + Int_val(off_val);
	int h = Int_val(Field(script_val, 3));
	int w = Int_val(Field(script_val, 2));
	int i;
	int f = Int_val(frame_val);

	AVS_Clip *clip = (AVS_Clip *)Field(script_val, 1);
	AVS_VideoFrame *frame;
	int pitch;
	const BYTE* data;

//	enter_blocking_section();
	frame = avs_get_frame(clip, f);
//	leave_blocking_section();

	// Y
	pitch = avs_get_pitch_p(frame, AVS_PLANAR_Y);
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_Y);
	for(i = 0; i < h; i++) {
		memcpy(str, data, w);
		str += w;
		data += pitch;
	}

	// The U and V planes are 1/2 the width and height of the Y plane
	h >>= 1;
	w >>= 1;

	// U
	pitch = avs_get_pitch_p(frame, AVS_PLANAR_U);
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_U);
	for(i = 0; i < h; i++) {
		memcpy(str, data, w);
		str += w;
		data += pitch;
	}

	// V
	// pitch is not needed here, since it is the same as the U pitch
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_V);
	for(i = 0; i < h; i++) {
		memcpy(str, data, w);
		str += w;
		data += pitch;
	}

	avs_release_video_frame(frame);
	CAMLreturn0;
}

CAMLprim void blit_y_unsafe(value script_val, value frame_val, value str_val, value off_val) {
	CAMLparam4(script_val, frame_val, str_val, off_val);
	char *str = String_val(str_val) + Int_val(off_val);
	int h = Int_val(Field(script_val, 3));
	int w = Int_val(Field(script_val, 2));
	int i;
	int f = Int_val(frame_val);

	AVS_Clip *clip = (AVS_Clip *)Field(script_val, 1);
	AVS_VideoFrame *frame;
	int pitch;
	const BYTE* data;

//	enter_blocking_section();
	frame = avs_get_frame(clip, f);
//	leave_blocking_section();

	// Y
	pitch = avs_get_pitch_p(frame, AVS_PLANAR_Y);
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_Y);
	for(i = 0; i < h; i++) {
		memcpy(str, data, w);
		str += w;
		data += pitch;
	}

	avs_release_video_frame(frame);
	CAMLreturn0;
}

CAMLprim void blit_uv_unsafe(value script_val, value frame_val, value str_val, value off_val) {
	CAMLparam4(script_val, frame_val, str_val, off_val);
	char *str = String_val(str_val) + Int_val(off_val);
	// The U and V planes are 1/2 the width and height of the Y plane
	int h = Int_val(Field(script_val, 3)) >> 1;
	int w = Int_val(Field(script_val, 2)) >> 1;
	int i;
	int f = Int_val(frame_val);

	AVS_Clip *clip = (AVS_Clip *)Field(script_val, 1);
	AVS_VideoFrame *frame;
	int pitch;
	const BYTE* data;

	frame = avs_get_frame(clip, f);
	
	// U
	pitch = avs_get_pitch_p(frame, AVS_PLANAR_U);
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_U);
	for(i = 0; i < h; i++) {
		memcpy(str, data, w);
		str += w;
		data += pitch;
	}

	// V
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_V);
	for(i = 0; i < h; i++) {
		memcpy(str, data, w);
		str += w;
		data += pitch;
	}

	avs_release_video_frame(frame);
	CAMLreturn0;
}

CAMLprim void blit_y_line_unsafe(value script_val, value frame_val, value line_val, value str_val, value off_val) {
	CAMLparam5(script_val, frame_val, line_val, str_val, off_val);
	char *str = String_val(str_val) + Int_val(off_val);
	int w = Int_val(Field(script_val, 2));
	int f = Int_val(frame_val);

	AVS_Clip *clip = (AVS_Clip *)Field(script_val, 1);

	AVS_VideoFrame *frame;
	int pitch;
	const BYTE *data;

	frame = avs_get_frame(clip, f);

	pitch = avs_get_pitch_p(frame, AVS_PLANAR_Y);
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_Y) + pitch * Int_val(line_val);

	memcpy(str, data, w);

	avs_release_video_frame(frame);
	CAMLreturn0;
}

CAMLprim void blit_u_line_unsafe(value script_val, value frame_val, value line_val, value str_val, value off_val) {
	CAMLparam5(script_val, frame_val, line_val, str_val, off_val);
	char *str = String_val(str_val) + Int_val(off_val);
	int w = Int_val(Field(script_val, 2)) >> 1;
	int f = Int_val(frame_val);

	AVS_Clip *clip = (AVS_Clip *)Field(script_val, 1);
	AVS_VideoFrame *frame;
	int pitch;
	const BYTE *data;

	frame = avs_get_frame(clip, f);

	pitch = avs_get_pitch_p(frame, AVS_PLANAR_U);
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_U) + pitch * Int_val(line_val);

	memcpy(str, data, w);

	avs_release_video_frame(frame);
	CAMLreturn0;
}

CAMLprim void blit_v_line_unsafe(value script_val, value frame_val, value line_val, value str_val, value off_val) {
	CAMLparam5(script_val, frame_val, line_val, str_val, off_val);
	char *str = String_val(str_val) + Int_val(off_val);
	int w = Int_val(Field(script_val, 2)) >> 1;
	int f = Int_val(frame_val);

	AVS_Clip *clip = (AVS_Clip *)Field(script_val, 1);
	AVS_VideoFrame *frame;
	int pitch;
	const BYTE *data;

//	enter_blocking_section();
	frame = avs_get_frame(clip, f);
//	leave_blocking_section();

	pitch = avs_get_pitch_p(frame, AVS_PLANAR_V);
	data = avs_get_read_ptr_p(frame, AVS_PLANAR_V) + pitch * Int_val(line_val);

	memcpy(str, data, w);

	avs_release_video_frame(frame);
	CAMLreturn0;
}

/*
CAMLprim value dither(value input_val) {
	CAMLparam1(input_val);
	int i = 0;
	int n = Int_val(input_val);
	float temp = 1.0;

//	enter_blocking_section();
	for(i = 0; i < n; i++) {
		temp = cos(temp);
	}
//	leave_blocking_section();

	CAMLreturn(caml_copy_double(temp));
}
*/
