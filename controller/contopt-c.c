
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
//#include <caml/unixsupport.h>
#include <caml/compatibility.h>



CAMLprim value sock_it_to_me(value null) {
	CAMLparam1(null);
#ifdef _WIN32
	CAMLlocal2(out_array,out_option);
	SOCKET sd = WSASocket(AF_INET, SOCK_DGRAM, 0, 0, 0, 0);
	if(sd == SOCKET_ERROR) {
		// FAIL
//		printf("FAIL1\n");
		out_option = Val_int(0);
	} else {
		INTERFACE_INFO if_list[20];
		unsigned long n_bytes_returned;
		if(WSAIoctl(sd, SIO_GET_INTERFACE_LIST, 0, 0, &if_list, sizeof(if_list), &n_bytes_returned, 0, 0) == SOCKET_ERROR) {
			// ALSO FAIL
//			printf("FAIL2\n");
			out_option = Val_int(0);
		} else {
			int i;
			int n_if = n_bytes_returned / sizeof(INTERFACE_INFO);
			out_array = caml_alloc_tuple(n_if);
			out_option = caml_alloc_tuple(1);
			Store_field(out_option, 0, out_array);
//			printf("Got %d interfaces\n",n_if);
			for(i = 0; i < n_if; i++) {
				struct sockaddr_in *p_address;
				struct sockaddr_in *p_mask;
				unsigned char *s;
				value s_val;
//				Store_field(out_array, i, caml_alloc_tuple(4));
				p_address = (struct sockaddr_in *) &(if_list[i].iiAddress);
				p_mask    = (struct sockaddr_in *) &(if_list[i].iiNetmask);
//				printf("ADDR %s\n",inet_ntoa(p_address->sin_addr));
//				printf("MASK %s\n",inet_ntoa(p_address->sin_addr));
//				printf(">%d",p_address->sin_addr.S_un.S_un_b.s_b1 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b1));
//				printf(">%d",p_address->sin_addr.S_un.S_un_b.s_b2 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b2));
//				printf(">%d",p_address->sin_addr.S_un.S_un_b.s_b3 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b3));
//				printf(">%d",p_address->sin_addr.S_un.S_un_b.s_b4 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b4));
//				printf("\n");

//				Store_field(Field(out_array, i), 0, Val_int(p_address->sin_addr.S_un.S_un_b.s_b1 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b1)));
//				Store_field(Field(out_array, i), 1, Val_int(p_address->sin_addr.S_un.S_un_b.s_b2 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b2)));
//				Store_field(Field(out_array, i), 2, Val_int(p_address->sin_addr.S_un.S_un_b.s_b3 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b3)));
//				Store_field(Field(out_array, i), 3, Val_int(p_address->sin_addr.S_un.S_un_b.s_b4 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b4)));

				s_val = caml_alloc_string(4);
				s = String_val(s_val);
				s[0] = p_address->sin_addr.S_un.S_un_b.s_b1 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b1);
				s[1] = p_address->sin_addr.S_un.S_un_b.s_b2 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b2);
				s[2] = p_address->sin_addr.S_un.S_un_b.s_b3 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b3);
				s[3] = p_address->sin_addr.S_un.S_un_b.s_b4 | (255 ^ p_mask->sin_addr.S_un.S_un_b.s_b4);
				Store_field(out_array, i, s_val);

			}
		}
	}
	closesocket(sd);
	CAMLreturn(out_option);
#else
	CAMLreturn(Val_int(0));
#endif
}

