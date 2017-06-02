/*******************************************************************************
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
*******************************************************************************/

#include <errno.h>
#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/fail.h>
#include <caml/memory.h>
#include <caml/unixsupport.h>
#include <caml/compatibility.h>
#include <caml/custom.h>
#include <caml/signals.h>

#ifdef _WIN32
#include <windows.h>
#include <winioctl.h>
#include <fcntl.h>
#else
#include <string.h>
#include <sys/socket.h>
#endif




value set_sparse(value fd)
{
	CAMLparam1(fd);
#ifdef _WIN32
	CAMLlocal1(retval);
	int worked;
	DWORD junk;
	if (Descr_kind_val(fd) == KIND_SOCKET) {
		worked = 0;
	} else {
		HANDLE h = Handle_val(fd);
		enter_blocking_section();
		worked = DeviceIoControl(
			h,
			FSCTL_SET_SPARSE,
			NULL,
			0,
			NULL,
			0,
			&junk,
			(LPOVERLAPPED)NULL
		);
		leave_blocking_section();
	}
	retval = Val_bool(worked);
	CAMLreturn(retval);
#else
	CAMLreturn(Val_bool(0));
#endif
}

value set_zero_data(value fd, value bigfrom, value bigto)
{
	CAMLparam3(fd, bigfrom, bigto);
#ifdef _WIN32
	CAMLlocal1(retval);
	int worked;
	DWORD junk;
	if (Descr_kind_val(fd) == KIND_SOCKET) {
		worked = 0;
	} else {
		HANDLE h = Handle_val(fd);
		FILE_ZERO_DATA_INFORMATION fz;
		fz.FileOffset.QuadPart = (LONGLONG)Int64_val(bigfrom);
		fz.BeyondFinalZero.QuadPart = (LONGLONG)Int64_val(bigto);
		enter_blocking_section();
		worked = DeviceIoControl(
			h,
			FSCTL_SET_ZERO_DATA,
			(LPVOID)&fz,
			sizeof(fz),
			NULL,
			0,
			&junk,
			(LPOVERLAPPED)NULL
		);
		leave_blocking_section();
	}
	retval = Val_bool(worked);
	CAMLreturn(retval);
#else
	CAMLreturn(Val_bool(0));
#endif
}


#ifdef _WIN32
/* Windowsy */
value num_processors(value null)
{
	CAMLparam1(null);
	SYSTEM_INFO sysinfo;
	GetSystemInfo(&sysinfo);
	CAMLreturn(Val_int(sysinfo.dwNumberOfProcessors));
}

#elif __APPLE__
/* Appley */
#include <sys/types.h>
#include <sys/sysctl.h>
value num_processors(value null)
{
	CAMLparam1(null);
	int num;
	size_t size = sizeof(num);
	if(sysctlbyname("hw.physicalcpu",&num,&size,NULL,0)) {
		num = 0;
	}
	CAMLreturn(Val_int(num));
}

#else
/* Generic (ignore) */
value num_processors(value null)
{
	CAMLparam1(null);
	CAMLreturn(Val_int(0));
}
#endif


#ifdef _WIN32
static DWORD priority_array[6] = {
	ABOVE_NORMAL_PRIORITY_CLASS,
	BELOW_NORMAL_PRIORITY_CLASS,
	HIGH_PRIORITY_CLASS,
	IDLE_PRIORITY_CLASS,
	NORMAL_PRIORITY_CLASS,
	REALTIME_PRIORITY_CLASS
};
static DWORD process_creation_flags[15] = {
	CREATE_BREAKAWAY_FROM_JOB,
	CREATE_DEFAULT_ERROR_MODE,
	CREATE_NEW_CONSOLE,
	CREATE_NEW_PROCESS_GROUP,
	CREATE_NO_WINDOW,
	CREATE_PROTECTED_PROCESS,
	CREATE_PRESERVE_CODE_AUTHZ_LEVEL,
	CREATE_SEPARATE_WOW_VDM,
	CREATE_SHARED_WOW_VDM,
	CREATE_SUSPENDED,
	CREATE_UNICODE_ENVIRONMENT,
	DEBUG_ONLY_THIS_PROCESS,
	DEBUG_PROCESS,
	DETACHED_PROCESS,
	EXTENDED_STARTUPINFO_PRESENT
};
#endif



value create_process_win(value proc_val, value priority_val)
{
	CAMLparam2(proc_val, priority_val);
#ifdef _WIN32
	CAMLlocal1(ret);
	DWORD priority;
	STARTUPINFO si;
	PROCESS_INFORMATION pi;
	ZeroMemory(&si, sizeof(si));
	ZeroMemory(&pi, sizeof(pi));

	priority = priority_array[Int_val(priority_val)];
	if (
		CreateProcess(
			NULL,
			String_val(proc_val),
			NULL,
			NULL,
			FALSE,
			priority,
			NULL,
			NULL,
			&si,
			&pi
		)
	) {
		if(CloseHandle(pi.hThread) == 0) {
			printf("create_process_win CloseHandle failed with %d\n",GetLastError());
		}
		ret = caml_alloc_tuple(1);
		Store_field(ret,0,Val_int(pi.hProcess));
	} else {
		ret = Val_int(0);
	}
	CAMLreturn(ret);
#else
	CAMLreturn(Val_int(0));
#endif
}


value create_process_flags_win(value proc_val, value priority_val, value flags_val)
{
	CAMLparam3(proc_val, priority_val, flags_val);
#ifdef _WIN32
	CAMLlocal2(ret,handles_ret);
	DWORD priority;
	DWORD flags;
	STARTUPINFO si;
	PROCESS_INFORMATION pi;
	ZeroMemory(&si, sizeof(si));
	ZeroMemory(&pi, sizeof(pi));

	priority = priority_array[Int_val(priority_val)];
	flags = caml_convert_flag_list(flags_val, process_creation_flags);
	if (
		CreateProcess(
			NULL,
			String_val(proc_val),
			NULL,
			NULL,
			FALSE,
			priority | flags,
			NULL,
			NULL,
			&si,
			&pi
		)
	) {
		ret = caml_alloc_tuple(1);
		handles_ret = caml_alloc_tuple(2);
		Store_field(handles_ret,0,Val_int(pi.hProcess));
		Store_field(handles_ret,1,Val_int(pi.hThread));
		Store_field(ret,0,handles_ret);
	} else {
		ret = Val_int(0);
	}
	CAMLreturn(ret);
#else
	CAMLreturn(Val_int(0));
#endif
}

value resume_thread(value thread_val)
{
	CAMLparam1(thread_val);
#ifdef _WIN32
	CAMLreturn((ResumeThread((HANDLE)Int_val(thread_val))) ? Val_int(1) : Val_int(0));
#else
	CAMLreturn(Val_int(0));
#endif
}



value kill_process_win(value handle_val)
{
	CAMLparam1(handle_val);
#ifdef _WIN32
	int ret;
//	printf("kill_process_win got handle %08X\n", Int_val(handle_val));
/*
	ret = SetPriorityClass((HANDLE)Int_val(handle_val), NORMAL_PRIORITY_CLASS);
	printf("TerminateProcess changed priority %d\n",ret);
*/
	ret = TerminateProcess((HANDLE)Int_val(handle_val), 1);
	if(ret == 0) {
		printf("TerminateProcess failed with %d\n",GetLastError());
	}
	CloseHandle((HANDLE)Int_val(handle_val)); // Just in case
//	if(ch == 0) {
//		printf("CloseHandle failed with %d\n",GetLastError());
//	}
	CAMLreturn(ret == 0 ? Val_int(0) : Val_int(1));
#else
	CAMLreturn(Val_int(0));
#endif
}

value get_exit_code(value handle_val)
{
	CAMLparam1(handle_val);
	CAMLlocal1(tuple);
#ifdef _WIN32
	DWORD code;
	tuple = caml_alloc_tuple(2);
	if(GetExitCodeProcess((HANDLE)Int_val(handle_val), &code)) {
		Store_field(tuple,0,Val_int(1));
		Store_field(tuple,1,Val_int(code));
	} else {
		Store_field(tuple,0,Val_int(0));
		Store_field(tuple,1,Val_int(0));
	}
	CAMLreturn(tuple);
#else
	tuple = caml_alloc_tuple(2);
	Store_field(tuple,0,Val_int(0));
	Store_field(tuple,1,Val_int(0));
	CAMLreturn(tuple);
#endif
}

value close_handle(value handle_val)
{
	CAMLparam1(handle_val);
#ifdef _WIN32
	int ret;
	ret = CloseHandle((HANDLE)Int_val(handle_val));
	if(ret == 0) printf("Close handle failed with %d\n", GetLastError());
	CAMLreturn(ret == 0 ? Val_int(0) : Val_int(1));
#else
	CAMLreturn(Val_int(0));
#endif
}

value wait_for_process(value handle_val, value ms_val)
{
	CAMLparam2(handle_val, ms_val);
#ifdef _WIN32
	HANDLE process = (HANDLE)Int_val(handle_val);
	DWORD ms;
	DWORD ret;
	CAMLlocal1(ret_val);

	if(ms_val == Val_int(0)) {
		// I guess this is how you check for None
		ms = INFINITE;
	} else {
		ms = Int_val(Field(ms_val, 0));
	}
//	printf("Using timeout %d\n", ms);

	enter_blocking_section();
	ret = WaitForSingleObject(process, ms);
	leave_blocking_section();

	if(ret == WAIT_OBJECT_0) {
		CAMLreturn(Val_int(0));
	} else if(ret == WAIT_TIMEOUT) {
		CAMLreturn(Val_int(1));
	} else {
		CAMLreturn(Val_int(2));
	}
#else
	CAMLreturn(Val_int(2));
#endif
}

void exit_process(value exit_with)
{
	CAMLparam1(exit_with);
#ifdef _WIN32
	ExitProcess(Int_val(exit_with));
#endif
	CAMLreturn0;
}

value get_current_process(value nuffin) {
	CAMLparam1(nuffin);
#ifdef _WIN32
	CAMLreturn(Val_int(GetCurrentProcess()));
#else
	CAMLreturn(Val_int(-1)); // It's usually -1 anyway
#endif
}

value create_process_win_full(value proc_val, value priority_val, value stdin_val, value stdout_val, value stderr_val)
{
	CAMLparam5(proc_val, priority_val, stdin_val, stdout_val, stderr_val);
#ifdef _WIN32
	CAMLlocal1(ret);
	DWORD priority;
	STARTUPINFO si;
	PROCESS_INFORMATION pi;
	ZeroMemory(&si, sizeof(si));
	ZeroMemory(&pi, sizeof(pi));

	si.dwFlags = STARTF_USESTDHANDLES;
	si.hStdInput = Handle_val(stdin_val);
	si.hStdOutput = Handle_val(stdout_val);
	si.hStdError = Handle_val(stderr_val);

	priority = priority_array[Int_val(priority_val)];
	if (
		CreateProcess(
			NULL,
			String_val(proc_val),
			NULL,
			NULL,
			TRUE, // TRUE does bad things!
			priority,
			NULL,
			NULL,
			&si,
			&pi
		)
	) {
		// OK //
		CloseHandle(pi.hThread);
		ret = caml_alloc_tuple(1);
		Store_field(ret,0,Val_int(pi.hProcess));
	} else {
		// NOTOK //
		printf("CreateProcess failed %s (%d)\n", String_val(proc_val), GetLastError());
		ret = Val_int(0);
	}
	CAMLreturn(ret);
#else
	CAMLreturn(Val_int(0));
#endif
}

value create_process_flags_win_full(value proc_val, value priority_val, value flags_val, value all_pipes)
{
	CAMLparam4(proc_val, priority_val, flags_val, all_pipes);
#ifdef _WIN32
	CAMLlocal2(ret, handles_ret);
	DWORD priority;
	DWORD flags;
	STARTUPINFO si;
	PROCESS_INFORMATION pi;
	ZeroMemory(&si, sizeof(si));
	ZeroMemory(&pi, sizeof(pi));

	si.dwFlags = STARTF_USESTDHANDLES;
	si.hStdInput = Handle_val(Field(all_pipes,0));
	si.hStdOutput = Handle_val(Field(all_pipes,1));
	si.hStdError = Handle_val(Field(all_pipes,2));

	priority = priority_array[Int_val(priority_val)];
	flags = caml_convert_flag_list(flags_val, process_creation_flags);

	if (
		CreateProcess(
			NULL,
			String_val(proc_val),
			NULL,
			NULL,
			TRUE, // TRUE does bad things!
			priority | flags,
			NULL,
			NULL,
			&si,
			&pi
		)
	) {
		// OK //
		ret = caml_alloc_tuple(1);
		handles_ret = caml_alloc_tuple(2);
		Store_field(handles_ret,0,Val_int(pi.hProcess));
		Store_field(handles_ret,1,Val_int(pi.hThread));
		Store_field(ret,0,handles_ret);
	} else {
		// NOTOK //
		printf("CreateProcess failed %s (%d)\n", String_val(proc_val), GetLastError());
		ret = Val_int(0);
	}
	CAMLreturn(ret);
#else
	CAMLreturn(Val_int(0));
#endif
}






CAMLprim value get_process_priority(value handle_val) {
	CAMLparam1(handle_val);
	CAMLlocal1(return_this);
#ifdef _WIN32
	HANDLE process = (HANDLE)Int_val(handle_val);
	int got = 0;
	got = GetPriorityClass(process);
	switch(got) {
		case ABOVE_NORMAL_PRIORITY_CLASS:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(0));
			break;
		case BELOW_NORMAL_PRIORITY_CLASS:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(1));
			break;
		case HIGH_PRIORITY_CLASS:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(2));
			break;
		case IDLE_PRIORITY_CLASS:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(3));
			break;
		case NORMAL_PRIORITY_CLASS:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(4));
			break;
		case REALTIME_PRIORITY_CLASS:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(5));
			break;
		default:
			return_this = Val_int(0);
			break;
	}
#else
	return_this = Val_int(0);
#endif
	CAMLreturn(return_this);
}

CAMLprim value set_process_priority(value process_val, value index) {
	CAMLparam2(process_val, index);
	int ret = 0;
#ifdef _WIN32
	HANDLE process = (HANDLE)Int_val(process_val);
	switch(Int_val(index)) {
		case 0:
			ret = SetPriorityClass(process, ABOVE_NORMAL_PRIORITY_CLASS);
			break;
		case 1:
			ret = SetPriorityClass(process, BELOW_NORMAL_PRIORITY_CLASS);
			break;
		case 2:
			ret = SetPriorityClass(process, HIGH_PRIORITY_CLASS);
			break;
		case 3:
			ret = SetPriorityClass(process, IDLE_PRIORITY_CLASS);
			break;
		case 4:
			ret = SetPriorityClass(process, NORMAL_PRIORITY_CLASS);
			break;
		case 5:
			ret = SetPriorityClass(process, REALTIME_PRIORITY_CLASS);
			break;
	}
#endif
	CAMLreturn(Val_int(ret ? 1 : 0));
}

value get_thread_priority(value nuffin) {
	CAMLparam1(nuffin);
	CAMLlocal1(return_this);
#ifdef _WIN32
	int ret = 0;
	ret = GetThreadPriority(GetCurrentThread());
	switch(ret) {
		case THREAD_PRIORITY_ABOVE_NORMAL:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(0));
			break;
		case THREAD_PRIORITY_BELOW_NORMAL:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(1));
			break;
		case THREAD_PRIORITY_HIGHEST:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(2));
			break;
		case THREAD_PRIORITY_IDLE:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(3));
			break;
		case THREAD_PRIORITY_LOWEST:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(4));
			break;
		case THREAD_PRIORITY_TIME_CRITICAL:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(5));
			break;
		case THREAD_PRIORITY_NORMAL:
			return_this = caml_alloc_tuple(1);
			Store_field(return_this,0,Val_int(6));
			break;
		default:
			return_this = Val_int(0);
			break;
	}
#else
	return_this = Val_int(0);
#endif
	CAMLreturn(return_this);
}

value set_thread_priority(value index) {
	CAMLparam1(index);
	int ret = 0;
#ifdef _WIN32
	switch(Int_val(index)) {
		case 0: // THREAD_PRIORITY_ABOVE_NORMAL
			ret = SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_ABOVE_NORMAL);
			break;
		case 1: // THREAD_PRIORITY_BELOW_NORMAL
			ret = SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_BELOW_NORMAL);
			break;
		case 2: // THREAD_PRIORITY_HIGHEST
			ret = SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_HIGHEST);
			break;
		case 3: // THREAD_PRIORITY_IDLE
			ret = SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_IDLE);
			break;
		case 4: // THREAD_PRIORITY_LOWEST
			ret = SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_LOWEST);
			break;
		case 5: // THREAD_PRIORITY_TIME_CRITICAL
			ret = SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
			break;
		default: // THREAD_PRIORITY_NORMAL
			ret = SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_NORMAL);
			break;
	}
#endif
	CAMLreturn(Val_int(ret ? 1 : 0));
}

value get_process_affinity(value handle_val) {
	CAMLparam1(handle_val);
#ifdef _WIN32
	HANDLE h = (HANDLE)Int_val(handle_val);
	DWORD_PTR proc_mask;
	DWORD_PTR sys_mask;
	CAMLlocal2(ret_val, tuple_val);
	if(GetProcessAffinityMask(h, &proc_mask, &sys_mask)) {
		// OK
		ret_val = caml_alloc_tuple(1);
		tuple_val = caml_alloc_tuple(2);
		Store_field(tuple_val, 0, Val_int(proc_mask));
		Store_field(tuple_val, 1, Val_int(sys_mask));
		Store_field(ret_val, 0, tuple_val);
	} else {
		ret_val = Val_int(0);
	}
	CAMLreturn(ret_val);
#else
	CAMLreturn(Val_int(0));
#endif
}

value set_process_affinity(value handle_val, value mask_val) {
	CAMLparam2(handle_val, mask_val);
#ifdef _WIN32
	HANDLE h = (HANDLE)Int_val(handle_val);
	DWORD m = (DWORD)Int_val(mask_val);
	BOOL ret = SetProcessAffinityMask(h, m);
	CAMLreturn(ret ? Val_int(1) : Val_int(0));
#else
	CAMLreturn(Val_int(0));
#endif
}


void print_stuff(value a) {
	CAMLparam1(a);
	intnat nata = (intnat)a;
	int i;
	unsigned char *h = (unsigned char *)(&(((header_t *)a)[-1]));

	if(nata & 1) {
		printf("  It's an int!\n");
		printf("    \"%016p\"\n",(void *)a);
	} else {
		int go;
		printf("  It's a pointer!\n");
		printf("    \"%016p\"\n",(void *)a);
		printf("    Size: %d\n", Wosize_val(a));
		printf("    Tag:  %d\n", Tag_val(a));
		if(Tag_val(a) == Custom_tag) {
			go = sizeof(intnat) * (Wosize_val(a) + 2) - 1;
		} else {
			go = sizeof(intnat) * (Wosize_val(a) + 1) - 1;
		}
		printf("    \"");
		for(i = 0; i < go; i++) {
			printf("%02X",h[i]);
		}
		printf("\"\n");
	}

	CAMLreturn0;
}

/*
void string_replace_c(value a) {
	CAMLparam1(a);
	printf("An int is %d bytes long\n", sizeof(int));
	printf("An intnat is %d bytes long\n", sizeof(intnat));
	printf("A float is %d bytes long\n", sizeof(float));
	printf("A value is %d bytes long\n", sizeof(value));
}
*/

void string_replace_c(value s_val, value c1_val, value c2_val) {
	CAMLparam3(s_val, c1_val, c2_val);
	unsigned char c1 = Int_val(c1_val);
	unsigned char c2 = Int_val(c2_val);
	intnat l = string_length(s_val);
	intnat i;
	unsigned char *s = (unsigned char *)String_val(s_val);
	for(i = 0; i < l; i++, s++) {
		if(*s == c1) {
			*s = c2;
		}
	}
	CAMLreturn0;
}


// This searches through "s" and replaces all instances of "c" with either "c+1" or "c-1",
// depending on the value of a pseudo-random number
// It returns the new seed for the random number generator
// This function uses "int" rather than "intnat" since the bits higher than the chosen one don't matter
value string_replace_c_random(value s_val, value c_val, value nm1_val, value mul_val, value add_val) {
	CAMLparam5(s_val, c_val, nm1_val, mul_val, add_val);
	unsigned char *s = (unsigned char *)String_val(s_val);
	intnat l = string_length(s_val);
	unsigned char c = Int_val(c_val);
	int n = Int_val(nm1_val);
	int mul = Int_val(mul_val);
	int add = Int_val(add_val);
	intnat i;
	unsigned char c1 = (c == 0 ? 1 : c);
	unsigned char c2 = (c == 255 ? 254 : c);

	for(i = 0; i < l; i++, s++) {
		if(*s == c) {
			n = n * mul + add;
			*s = (n & 0x40000000 ? c1 : c2);
		}
	}

	CAMLreturn(Val_int(n));
}

// Hard-code (n-1) * 1000786403 + 656228603
// This also fixes 0x0D0A translation
// NOTE: Change 0x0D if at the end of the string too, just in case
value string_sanitize_text_mode(value s_val, value from_val, value num_val, value nm1_val) {
	CAMLparam4(s_val, from_val, num_val, nm1_val);
	char *s = String_val(s_val) + Int_val(from_val);
	intnat l = Int_val(num_val);
	intnat i;
	int n = Int_val(nm1_val);

	enter_blocking_section(); // For better multi-threaded powaz
	for(i = 0; i < l; i++, s++) {
		if(*s == 0x1A) {
			// Fix EOF
			n = n * 1000786403 + 656228603;
			*s = (n & 0x40000000 ? 0x19 : 0x1B);
		} else if(*s == 0x0D) {
			if(i == l - 1) {
				// End of the string
				// Change it just in case it is concatenated with a string that starts with 0x0A
				n = n * 1000786403 + 656228603;
				*s = (n & 0x40000000 ? 0x0C : 0x0E);
			} else if(*(s + 1) == 0x0A) {
				// CRLF!
				n = n * 1000786403 + 656228603;
				if(n & 0x20000000) {
					// Change the CR
					*s = (n & 0x40000000 ? 0x0C : 0x0E);
				} else {
					// Change the LF
					*(s + 1) = (n & 0x40000000 ? 0x09 : 0x0B);
				}
				i++; s++; // Skip over one character since we know it's not 0x0D or 0x1A
			} // else we don't need to change - 0x0D is OK by itself
		}
	}
	leave_blocking_section();

	CAMLreturn(Val_int(n));
}


// Sync to 0x00000001, since "we always use long ones" according to x264/common/common.c
CAMLprim value fetch_0001(value str_val, value from_val, value num_val, value state_val)
{
	CAMLparam4(str_val, from_val, num_val, state_val);
	char *str = String_val(str_val) + Int_val(from_val);
	int i;
	int num = Int_val(num_val);
	int state = Int_val(state_val);
	char c;
	int found = 0;
	CAMLlocal1(ret);
	ret = caml_alloc_tuple(3);

	for(i = 0; i < num; i++, str++) {
		c = *str;
		if(state != 3) {
			if(c == 0) {
				// One more zero
				state++;
			} else {
				// Reset and try again
				state = 0;
			}
		} else {
			if(c == 1) {
				// HERE'S ONE!
				state = 0;
				found = 1;
				break;
			} else if (c == 0) {
				// This means there are 4 0x00 bytes in a row
				// Although this shouldn't happen, if it does a 0x01 at the end should be found
				// Basically, we keep the state at 3 and keep going
			} else {
				// Keep looking (reset state)
				state = 0;
			}
		}
	}

	Store_field(ret,0,Val_bool(found));
	Store_field(ret,1,Val_int(Int_val(from_val) + i + 1)); // add 1 to i to get the byte after the 0x01
	Store_field(ret,2,Val_int(state));
	CAMLreturn(ret);
}

/////////////////////
//////// PTR ////////
/////////////////////
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

CAMLprim value make_ptr(value size_val) {
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

void ptr_of_string(value str, value ptr) {
	CAMLparam2(str, ptr);
	memmove(ptr_value(ptr), String_val(str), Int_val(ptr_size(ptr)));
	CAMLreturn0;
}

void string_of_ptr(value ptr, value str) {
	CAMLparam2(ptr, str);
	memmove(String_val(str), ptr_value(ptr), string_length(str));
	CAMLreturn0;
}

static int msg_flag_table[] = {
  MSG_OOB, MSG_DONTROUTE, MSG_PEEK
};

//////////////
// SEND ALL //
//////////////
/*
 # ifdef _WIN32
CAMLprim value send_all_ptr(value sock, value ptr, value flags) {
	CAMLparam3(sock, ptr, flags);
	SOCKET s = Socket_val(sock);
	int flg = caml_convert_flag_list(flags, msg_flag_table);
	int ret;
	intnat numbytes = Int_val(ptr_size(ptr));
	intnat sent_so_far = 0;
	intnat sent_this_time = 0;
	DWORD err = 0;
	char *buff = ptr_value(ptr);

	enter_blocking_section();
	leave_blocking_section();
*/
//////////
// SEND //
//////////
#ifdef _WIN32
CAMLprim value send_ptr(value sock, value ptr_val, value ofs, value len, value flags) {
	CAMLparam5(sock, ptr_val, ofs, len, flags);
	SOCKET s = Socket_val(sock);
	int flg = caml_convert_flag_list(flags, msg_flag_table);
	int ret;
	intnat numbytes = Long_val(len);
	char *buff = ptr_value(ptr_val) + Int_val(ofs);
	DWORD err = 0;

	enter_blocking_section();
	ret = send(s, buff, numbytes, flg); // This usually sends the requested amount of data
	if(ret == SOCKET_ERROR) err = WSAGetLastError();
	leave_blocking_section();

	if(ret == SOCKET_ERROR) {
		win32_maperr(err);
		uerror("send_ptr", Nothing);
	}
	CAMLreturn(Val_int(ret));
}
#else
CAMLprim value send_ptr(value sock, value ptr_val, value ofs, value len, value flags) {
	CAMLparam5(sock, ptr_val, ofs, len, flags);
	int flg = caml_convert_flag_list(flags, msg_flag_table);
	int ret;
	intnat numbytes = Long_val(len);
	char *buff = ptr_value(ptr_val) + Int_val(ofs);

	enter_blocking_section();
	ret = send(Int_val(sock), buff, (int)numbytes, flg);
	leave_blocking_section();

	if(ret == -1) uerror("send_ptr", Nothing);
	CAMLreturn(Val_int(ret));
}
#endif


//////////
// RECV //
//////////
#ifdef _WIN32
CAMLprim value recv_ptr(value sock, value ptr_val, value ofs, value len, value flags) {
	CAMLparam5(sock, ptr_val, ofs, len, flags);
	SOCKET s = Socket_val(sock);
	int flg = caml_convert_flag_list(flags, msg_flag_table);
	int ret;
	intnat numbytes = Long_val(len);
	char *buff = ptr_value(ptr_val) + Int_val(ofs);
	DWORD err = 0;

	enter_blocking_section();
	ret = recv(s, buff, (int)numbytes, flg);
	if(ret == SOCKET_ERROR) err = WSAGetLastError();
	leave_blocking_section();

	if(ret == SOCKET_ERROR) {
		win32_maperr(err);
		uerror("recv_ptr", Nothing);
	}
	CAMLreturn(Val_int(ret));
}
#else
CAMLprim value recv_ptr(value sock, value ptr_val, value ofs, value len, value flags) {
	CAMLparam5(sock, ptr_val, ofs, len, flags);
	int flg = caml_convert_flag_list(flags, msg_flag_table);
	int ret;
	intnat numbytes = Long_val(len);
	char *buff = ptr_value(ptr_val) + Int_val(ofs);

	enter_blocking_section();
	ret = recv(Int_val(sock), buff, numbytes, flg);
	leave_blocking_section();

	if(ret == -1) uerror("recv_ptr", Nothing);
	CAMLreturn(Val_int(ret));
}
#endif

/////////////////////
// DESTUPIDIFY PTR //
/////////////////////
CAMLprim value ptr_sanitize_text_mode(value ptr, value from_val, value num_val, value nm1_val) {
	CAMLparam4(ptr, from_val, num_val, nm1_val);
	char *s = ptr_value(ptr) + Int_val(from_val);
	intnat l = Int_val(num_val);
	intnat i;
	int n = Int_val(nm1_val);

	enter_blocking_section();
	for(i = 0; i < l; i++, s++) {
		if(*s == 0x1A) {
			// Fix EOF
			n = n * 1000786403 + 656228603;
			*s = (n & 0x40000000 ? 0x19 : 0x1B);
		} else if(*s == 0x0D) {
			if(i == l - 1) {
				// End of the string
				// Change it just in case it is concatenated with a string that starts with 0x0A
				n = n * 1000786403 + 656228603;
				*s = (n & 0x40000000 ? 0x0C : 0x0E);
			} else if(*(s + 1) == 0x0A) {
				// CRLF!
				n = n * 1000786403 + 656228603;
				if(n & 0x20000000) {
					// Change the CR
					*s = (n & 0x40000000 ? 0x0C : 0x0E);
				} else {
					// Change the LF
					*(s + 1) = (n & 0x40000000 ? 0x09 : 0x0B);
				}
				i++; s++; // Skip over one character since we know it's not 0x0D or 0x1A
			} // else we don't need to change - 0x0D is OK by itself
		}
	}
	leave_blocking_section();

	CAMLreturn(Val_int(n));
}



///////////////
// WRITE PTR //
///////////////
#ifdef _WIN32
CAMLprim value write_ptr(value fd, value ptr, value vofs, value vlen) {
	CAMLparam4(fd,ptr,vofs,vlen);
	char *buf = ptr_value(ptr) + Long_val(vofs);
	intnat len = Long_val(vlen);
	intnat written = 0;
	DWORD numwritten;
	DWORD err = 0;
	while(len > 0) {
		HANDLE h = Handle_val(fd);

		enter_blocking_section();
		if(!WriteFile(h, buf, len, &numwritten, NULL)) err = GetLastError();
		leave_blocking_section();

		if(err) {
			win32_maperr(err);
			uerror("write_ptr", Nothing);
		}
		buf += numwritten;
		written += numwritten;
		len -= numwritten;
	}
	CAMLreturn(Val_long(written));
}
#else
CAMLprim value write_ptr(value fd, value ptr, value vofs, value vlen) {
	CAMLparam4(fd,ptr,vofs,vlen);
	char *buf = ptr_value(ptr) + Long_val(vofs);
	intnat len = Long_val(vlen);
	intnat written = 0;
	int ret;
	while(len > 0) {

		enter_blocking_section();
		ret = write(Int_val(fd), buf, len);
		leave_blocking_section();

		if(ret == -1) {
			if((errno == EAGAIN || errno == EWOULDBLOCK) && written > 0) break;
			uerror("write_ptr", Nothing);
		}
		buf += ret;
		written += ret;
		len -= ret;
	}
	CAMLreturn(Val_long(written));
}
#endif


//////////////////////////
// WINDOWS PROCESS JOBS //
//////////////////////////

// Returns (error_code, handle)
CAMLprim value create_job_object(value sec, value name) {
	CAMLparam2(sec, name);
	CAMLlocal1(tuple);
#ifdef _WIN32
	HANDLE hjob;
	tuple = caml_alloc_tuple(2);
	if(string_length(name) == 0) {
		hjob = CreateJobObject(NULL, NULL);
	} else {
		hjob = CreateJobObject(NULL, String_val(name));
	}
	if(hjob == NULL) {
		// ERROR!
		Store_field(tuple,0,Val_int(GetLastError()));
		Store_field(tuple,1,Val_int(0));
	} else {
		Store_field(tuple,0,Val_int(0));
		Store_field(tuple,1,Val_int(hjob));
	}
	CAMLreturn(tuple);
#else
	tuple = caml_alloc_tuple(2);
	Store_field(tuple,0,Val_int(1));
	Store_field(tuple,1,Val_int(0));
	CAMLreturn(tuple);
#endif
}

CAMLprim value assign_process_to_job_object(value hjob, value hprocess) {
	CAMLparam2(hjob, hprocess);
#ifdef _WIN32
//	CAMLreturn((AssignProcessToJobObject((HANDLE)Int_val(hjob),(HANDLE)Int_val(hprocess))) ? Val_int(1) : Val_int(0));
	if(AssignProcessToJobObject((HANDLE)Int_val(hjob),(HANDLE)Int_val(hprocess))) {
		CAMLreturn(Val_int(1));
	} else {
		printf("ASSIGN FAILWITH %d\n", GetLastError());
		CAMLreturn(Val_int(0));
	}
//	CAMLreturn(() ? Val_int(1) : Val_int(0));
#else
	CAMLreturn(Val_int(0));
#endif
}

CAMLprim value terminate_job_object(value hjob, value exitcode) {
	CAMLparam2(hjob, exitcode);
#ifdef _WIN32
	CAMLreturn((TerminateJobObject((HANDLE)Int_val(hjob),Int_val(exitcode))) ? Val_int(1) : Val_int(0));
#else
	CAMLreturn(Val_int(0));
#endif
}


CAMLprim value set_job_extended_information(value hjob, value limits_val) {
	CAMLparam2(hjob, limits_val);
#ifdef _WIN32
	int ret;
	JOBOBJECT_EXTENDED_LIMIT_INFORMATION ext;
	JOBOBJECT_BASIC_LIMIT_INFORMATION basic;
	basic.LimitFlags = 0;

	if(Val_int(0) != Field(limits_val,0)) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
		basic.ActiveProcessLimit = (DWORD)Int_val(Field(Field(limits_val,0),0));
	}
	if(Val_int(0) != Field(limits_val,1)) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_AFFINITY;
		basic.Affinity = (DWORD)Int_val(Field(Field(limits_val,1),0));
	}
	if(Int_val(Field(limits_val,2))) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_BREAKAWAY_OK;
	}
	if(Int_val(Field(limits_val,3))) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_DIE_ON_UNHANDLED_EXCEPTION;
	}
	if(Val_int(0) != Field(limits_val,4)) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_JOB_MEMORY;
		ext.JobMemoryLimit = (SIZE_T)Long_val(Field(Field(limits_val,4),0));
	}
	if(Val_int(0) != Field(limits_val,5)) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_JOB_TIME;
		// These fields are crazy
		basic.PerJobUserTimeLimit.QuadPart = (LONGLONG)(10000000.0 * Double_field(Field(Field(limits_val,5),0),0));
	}
	if(Int_val(Field(limits_val,6))) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
	}
	if(Int_val(Field(limits_val,7))) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_PRESERVE_JOB_TIME;
	}
	if(Val_int(0) != Field(limits_val,8)) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_PRIORITY_CLASS;
		basic.PriorityClass = priority_array[Int_val(Field(Field(limits_val,8),0))];
	}
	if(Val_int(0) != Field(limits_val,9)) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_PROCESS_MEMORY;
		ext.ProcessMemoryLimit = (SIZE_T)Long_val(Field(Field(limits_val,9),0));
	}
	if(Val_int(0) != Field(limits_val,10)) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_PROCESS_TIME;
		basic.PerProcessUserTimeLimit.QuadPart = (LONGLONG)(10000000.0 * Double_field(Field(Field(limits_val,10),0),0));
	}
	if(Val_int(0) != Field(limits_val,11)) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_SCHEDULING_CLASS;
		basic.SchedulingClass = (DWORD)Int_val(Field(Field(limits_val,11),0));
	}
	if(Int_val(Field(limits_val,12))) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_SILENT_BREAKAWAY_OK;
	}
	if(Val_int(0) != Field(limits_val,13)) {
		basic.LimitFlags |= JOB_OBJECT_LIMIT_WORKINGSET;
		basic.MinimumWorkingSetSize = (SIZE_T)Long_val(Field(Field(Field(limits_val,13),0),0));
		basic.MaximumWorkingSetSize = (SIZE_T)Long_val(Field(Field(Field(limits_val,13),0),1));
//		printf("Using a working set from %d to %d\n",basic.MinimumWorkingSetSize,basic.MaximumWorkingSetSize);
	}

	ext.BasicLimitInformation = basic;
//	printf("Ext flags: %d\n", ext.BasicLimitInformation.LimitFlags);
//	printf("Processes: %d\n", ext.BasicLimitInformation.ActiveProcessLimit);


	ret = SetInformationJobObject(
		(HANDLE)Int_val(hjob),
		JobObjectExtendedLimitInformation,
		&ext,
		sizeof(JOBOBJECT_EXTENDED_LIMIT_INFORMATION)
	);
//	printf("Ext flags: %d\n", ext.BasicLimitInformation.LimitFlags);
//	printf("Processes: %d\n", ext.BasicLimitInformation.ActiveProcessLimit);
	CAMLreturn(ret ? Val_int(1) : Val_int(0));
#else
	CAMLreturn(Val_int(0));
#endif
}

