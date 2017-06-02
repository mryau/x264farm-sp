#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/unixsupport.h>
#include <caml/custom.h>

#include <winioctl.h>
#include <fcntl.h>

#ifdef _WIN32
#include <windows.h>
#include <winioctl.h>
#include <fcntl.h>
#include <vfw.h>
#else
#error "Not win32? AVS hooks don't work"
#endif

/////////////////////
//////// PTR ////////
/////////////////////
/*
#define ptr_custom(x) (*(char **)Data_custom_val(x))
#define ptr_value(x) ptr_custom(Field((x),1))
#define ptr_size(x) Field((x),0)
static void ptr_finalize(value v) {
	free(ptr_custom(v));
}

static struct custom_operations generic_ptr_opts = {
	"c_ptr",
	ptr_finalize,
	custom_compare_default,
	custom_hash_default,
	custom_serialize_default,
	custom_deserialize_default
};

value make_ptr(value size_val) {
	CAMLparam1(size_val);
	char *p;
	int deleteme;
	CAMLlocal2(cust,tuple);
	p = (char *)malloc(Int_val(size_val));
	if(p == NULL) raise_out_of_memory();
	for(deleteme = 0; deleteme < Int_val(size_val); deleteme++) {
		p[deleteme] = 37;
	}
	cust = caml_alloc_custom(&generic_ptr_opts, sizeof(char *), Int_val(size_val), 256*1024*1024);
	ptr_custom(cust) = p;
	tuple = caml_alloc_tuple(2);
	Store_field(tuple,0,size_val);
	Store_field(tuple,1,cust);
	CAMLreturn(tuple);
}

CAMLprim value length_ptr(value ptr_val) {
	CAMLparam1(ptr_val);
	CAMLreturn(ptr_size(ptr_val));
}

CAMLprim value send_all_ptr(value sock, value ptr_val) {
	CAMLparam2(sock, ptr_val);
	SOCKET s = Socket_val(sock);
	int ret;
	intnat numbytes = Int_val(ptr_size(ptr_val));
	intnat sent_so_far = 0;
	intnat sent_this_time = 0;
	DWORD err = 0;
	char *buff = ptr_value(ptr_val);

	enter_blocking_section();
	while(err == 0 && numbytes > 0) {
		sent_this_time = send(s, buff, numbytes, 0);
		if(sent_this_time == SOCKET_ERROR) err = WSAGetLastError();
	}
	leave_blocking_section();

	if(err != 0) {
		win32_maperr(err);
		uerror("send_all_ptr", Nothing);
	}
	CAMLreturn(Val_int(0));
}

CAMLprim value send_ptr(value sock, value ptr_val, value ofs_val, value len_val) {
	CAMLparam4(sock, ptr_val, ofs_val, len_val);
	SOCKET s = Socket_val(sock);
	int ofs = Int_val(ofs_val);
	int len = Int_val(len_val);
	int ret;
	char *buff = ptr_value(ptr_val) + ofs;
	DWORD err = 0;

//	printf("send_ptr %p\n", buff);

	enter_blocking_section();
	// This usually sends the requested amount of data
	ret = send(s, buff, len, 0);
	if(ret == SOCKET_ERROR) err = WSAGetLastError();
	leave_blocking_section();

	if(ret == SOCKET_ERROR) {
		win32_maperr(err);
//		printf("ERROR %d\n", err);
		uerror("send_ptr", Nothing);
	}
	CAMLreturn(Val_int(ret));
}

CAMLprim value write_ptr(value hand, value ptr_val) {
	CAMLparam2(hand, ptr_val);
	HANDLE h = Handle_val(hand);
	intnat len = Int_val(ptr_size(ptr_val));
	char *buff = ptr_value(ptr_val);
	DWORD numbytes = len;
	DWORD numwritten = 0;
	DWORD err = 0;

//	printf("WRITING %d BYTES\n", numbytes);

	enter_blocking_section();
	while(numbytes > 0 && err == 0) {
		if(!WriteFile(h, (LPVOID)buff, numbytes, &numwritten, NULL)) {
			err = GetLastError();
		} else {
			buff += numwritten;
			numbytes -= numwritten;
		}
	}
	leave_blocking_section();

	if(err) {
		win32_maperr(err);
		printf("ERROR %d\n", err);
		uerror("output_ptr", Nothing);
	}
	CAMLreturn(Val_int(0));
}
*/
/////////////
// END PTR //
/////////////


























static struct custom_operations ops = {
	"AVISTREAMINFO",
	custom_finalize_default,
	custom_compare_default,
	custom_hash_default,
	custom_serialize_default,
	custom_deserialize_default
};

#define Avi_value(x)  (*((PAVISTREAM *)Data_custom_val(x)))
#define Pavi_value(x) ((PAVISTREAM *)Data_custom_val(x))
#define Avi_of_t(x)   Avi_value(Field(x,0))

//#define Avi_of_t(x) (PAVISTREAM)Field((x),0)


CAMLprim value info_avi(value file_val) {
	CAMLparam1(file_val);
	PAVISTREAM avi;
	CAMLlocal2(option_val,tuple_val);

	AVISTREAMINFO info;

//	AVIFileInit();
	if(AVIStreamOpenFromFile(&avi, String_val(file_val), streamtypeVIDEO, 0, OF_PARSE, NULL)) {
//		AVIFileExit();
		option_val = Val_int(0);
	} else if(AVIStreamInfo(avi, &info, sizeof(AVISTREAMINFO))) {
		// Error getting info
		AVIStreamRelease(avi);
//		AVIFileExit();
		option_val = Val_int(0);
	} else if(info.fccHandler != MAKEFOURCC('Y','V','1','2')) {
		AVIStreamRelease(avi);
//		AVIFileExit();
		option_val = Val_int(0);
	} else {
		int width = info.rcFrame.right - info.rcFrame.left;
		int height = info.rcFrame.bottom - info.rcFrame.top;
		int num = info.dwRate;
		int den = info.dwScale;
		int len = info.dwLength;
//	printf("``````````````AVI F\n");

		tuple_val = caml_alloc_tuple(5);
		Store_field(tuple_val,0,Val_int(width));
		Store_field(tuple_val,1,Val_int(height));
		Store_field(tuple_val,2,Val_int(num));
		Store_field(tuple_val,3,Val_int(den));
		Store_field(tuple_val,4,Val_int(len));
//		Store_field(tuple_val,5,Val_int(width * height * 3 / 2));

		option_val = caml_alloc_tuple(1);
		Store_field(option_val,0,tuple_val);

		AVIStreamRelease(avi);
//		AVIFileExit();
	}

	CAMLreturn(option_val);
}


CAMLprim void init_avi(value unit) {
	CAMLparam1(unit);
	AVIFileInit();
	CAMLreturn0;
}

CAMLprim void exit_avi(value unit) {
	CAMLparam1(unit);
	AVIFileExit();
	CAMLreturn0;
}

CAMLprim value open_avi(value file_val) {
	CAMLparam1(file_val);
	int bad_stuff_happened;
//	PAVISTREAM avi_out;
	CAMLlocal3(option_val,tuple_val,avi_val);

	AVISTREAMINFO info;

//	printf("``````````````AVI A %p\n", Pavi_value(avi_val));

	avi_val = caml_alloc_custom(&ops,sizeof(PAVISTREAM),1,1000);

//	printf("``````````````AVI B %p %s\n", Pavi_value(avi_val), String_val(file_val));
	bad_stuff_happened = AVIStreamOpenFromFile(Pavi_value(avi_val), String_val(file_val), streamtypeVIDEO, 0, OF_READ, NULL);
//	printf("``````````````AVI B.5 %p\n", Pavi_value(avi_val));
	if(bad_stuff_happened) {
		// ERROR
//	printf("``````````````AVI C\n");
		option_val = Val_int(0);
	} else if(AVIStreamInfo(Avi_value(avi_val), &info, sizeof(AVISTREAMINFO))) {
//	printf("``````````````AVI D\n");
		AVIStreamRelease(Avi_value(avi_val));
		option_val = Val_int(0);
	} else if(info.fccHandler != MAKEFOURCC('Y','V','1','2')) {
//	printf("``````````````AVI E\n");
		AVIStreamRelease(Avi_value(avi_val));
		option_val = Val_int(0);
	} else {
		int width = info.rcFrame.right - info.rcFrame.left;
		int height = info.rcFrame.bottom - info.rcFrame.top;
		int num = info.dwRate;
		int den = info.dwScale;
		int len = info.dwLength;
//	printf("``````````````AVI F\n");

		tuple_val = caml_alloc_tuple(7);
//		Store_field(tuple_val,0,avi_val);
		Store_field(tuple_val,0,avi_val);
		Store_field(tuple_val,1,Val_int(width));
		Store_field(tuple_val,2,Val_int(height));
		Store_field(tuple_val,3,Val_int(num));
		Store_field(tuple_val,4,Val_int(den));
		Store_field(tuple_val,5,Val_int(len));
		Store_field(tuple_val,6,Val_int(width * height * 3 / 2));

		option_val = caml_alloc_tuple(1);
		Store_field(option_val,0,tuple_val);
	}

//	printf("``````````````AVI G\n");
	CAMLreturn(option_val);
}

CAMLprim void close_avi(value tuple_val) {
	CAMLparam1(tuple_val);

	AVIStreamRelease(Avi_of_t(tuple_val));
//	AVIFileExit();

	CAMLreturn0;
}

CAMLprim value blit_avi_frame_unsafe(value tuple_val, value frame_val, value string_val, value pos_val) {
	CAMLparam4(tuple_val, frame_val, string_val, pos_val);
	char *str = String_val(string_val) + Int_val(pos_val);
	int ret;
	int got_frames;

//	printf("````````````````AVI doing frame %d\n", Int_val(frame_val));
	// Can't do any of these inside blocking_section()!
	PAVISTREAM avis = Avi_of_t(tuple_val);
	int frame = Int_val(frame_val);
	int bytes = Int_val(Field(tuple_val,6));

//	enter_blocking_section();
	ret = AVIStreamRead(avis, frame, 1, str, bytes, NULL, &got_frames);
//	leave_blocking_section();

	if(ret) {
		CAMLreturn(Val_int(0));
	} else {
		CAMLreturn(Val_int(got_frames));
	}
}

CAMLprim value blit_avi_frames_unsafe(value tuple_val, value frame_val, value iter_val, value string_val, value pos_val) {
	CAMLparam5(tuple_val, frame_val, iter_val, string_val, pos_val);
	char *str = String_val(string_val) + Int_val(pos_val);
	int frame_from = Int_val(frame_val);
	int frame_to = frame_from + Int_val(iter_val);
	int i;
	int bytes_per_frame = Int_val(Field(tuple_val,6));
	int read_return;
	int got_frames;
	int frames_so_far = 0;
	PAVISTREAM avi = Avi_of_t(tuple_val);

//	enter_blocking_section();
	for(i = frame_from; i < frame_to; i++, str += bytes_per_frame, frames_so_far ++) {
		read_return = AVIStreamRead(avi, i, 1, str, bytes_per_frame, NULL, &got_frames);

		if(read_return || got_frames != 1) {
			break;
		}
	}
//	leave_blocking_section();

	CAMLreturn(Val_int(frames_so_far));
}




/////////////
// AVI PTR //
/////////////
#define ptr_custom(x) (*(char **)Data_custom_val(x))
#define ptr_value(x) ptr_custom(Field((x),1))
#define ptr_size(x) Field((x),0)
CAMLprim value ptr_avi_frame_unsafe(value tuple_val, value frame_val, value ptr_val) {
	CAMLparam3(tuple_val, frame_val, ptr_val);
	char *str = ptr_value(ptr_val);
	int ret;
	int got_frames;

	// Can't do any of these inside blocking_section()!
	PAVISTREAM avis = Avi_of_t(tuple_val);
	int frame = Int_val(frame_val);
	int bytes = Int_val(Field(tuple_val,6));

	enter_blocking_section();
	ret = AVIStreamRead(avis, frame, 1, str, bytes, NULL, &got_frames);
	leave_blocking_section();

	if(ret) {
		CAMLreturn(Val_int(0));
	} else {
		CAMLreturn(Val_int(got_frames));
	}
}

