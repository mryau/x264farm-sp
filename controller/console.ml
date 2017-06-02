type console_handle

type console_screen_buffer_info = {
	size_x : int;
	size_y : int;
	cursor_x : int;
	cursor_y : int;
	attributes : int;
	window_left : int;
	window_top : int;
	window_right : int;
	window_bottom : int;
	max_size_x : int;
	max_size_y : int;
}

type 'a console_return = Normal of 'a | Exception of (string * int)
(* Hooray for monads! *)
let (%%) a b = match a with
	| Normal x -> b x
	| Exception x -> Exception x
;;
(* This doesn't work! OCaml evaluates the right side first! *)
(* Turns out (;) will work fine... *)
(*
let (%) a b = match a with
	| Normal x -> b
	| Exception x -> Exception x
;;
*)

external caml_std_input_handle  : unit -> int = "caml_STD_INPUT_HANDLE"
external caml_std_output_handle : unit -> int = "caml_STD_OUTPUT_HANDLE"
external caml_std_error_handle  : unit -> int = "caml_STD_ERROR_HANDLE"

external caml_fg_lightblack   : unit -> int = "caml_FG_LIGHTBLACK"
external caml_fg_red          : unit -> int = "caml_FG_RED"
external caml_fg_green        : unit -> int = "caml_FG_GREEN"
external caml_fg_blue         : unit -> int = "caml_FG_BLUE"

external caml_bg_lightblack   : unit -> int = "caml_BG_LIGHTBLACK"
external caml_bg_red          : unit -> int = "caml_BG_RED"
external caml_bg_green        : unit -> int = "caml_BG_GREEN"
external caml_bg_blue         : unit -> int = "caml_BG_BLUE"

external caml_get_std_handle : int -> console_handle = "caml_GetStdHandle"
external caml_set_console_text_attribute : console_handle -> int -> bool = "caml_SetConsoleTextAttribute"
external caml_get_console_screen_buffer_info : console_handle -> bool * console_screen_buffer_info = "caml_GetConsoleScreenBufferInfo"
external caml_get_console_text_attribute : console_handle -> bool * int = "caml_GetConsoleTextAttribute"
external caml_set_console_cursor_position : console_handle -> (int * int) -> bool = "caml_SetConsoleCursorPosition"

external caml_write : console_handle -> string -> bool * int = "caml_WriteConsole"

external caml_get_last_error : unit -> int = "caml_GetLastError"



(* Now set stuff *)
let std_input_handle  = caml_get_std_handle (caml_std_input_handle  ())
let std_output_handle = caml_get_std_handle (caml_std_output_handle ())
let std_error_handle  = caml_get_std_handle (caml_std_error_handle  ())

(** Foreground colors *)
let fg_black          = 0
let fg_lightblack     = caml_fg_lightblack     ()
let fg_red            = caml_fg_red            ()
let fg_lightred       = fg_red lor fg_lightblack
let fg_green          = caml_fg_green          ()
let fg_lightgreen     = fg_green lor fg_lightblack
let fg_blue           = caml_fg_blue           ()
let fg_lightblue      = fg_blue lor fg_lightblack
let fg_magenta        = fg_red lor fg_blue
let fg_lightmagenta   = fg_red lor fg_blue lor fg_lightblack
let fg_cyan           = fg_green lor fg_blue
let fg_lightcyan      = fg_green lor fg_blue lor fg_lightblack
let fg_brown          = fg_red lor fg_green
let fg_yellow         = fg_red lor fg_green lor fg_lightblack
let fg_gray           = fg_red lor fg_green lor fg_blue
let fg_white          = fg_red lor fg_green lor fg_blue lor fg_lightblack

(** Background colors *)
let bg_black          = 0
let bg_lightblack     = caml_bg_lightblack     ()
let bg_red            = caml_bg_red            ()
let bg_lightred       = bg_red lor bg_lightblack
let bg_green          = caml_bg_green          ()
let bg_lightgreen     = bg_green lor bg_lightblack
let bg_blue           = caml_bg_blue           ()
let bg_lightblue      = bg_blue lor bg_lightblack
let bg_magenta        = bg_red lor bg_blue
let bg_lightmagenta   = bg_red lor bg_blue lor bg_lightblack
let bg_cyan           = bg_green lor bg_blue
let bg_lightcyan      = bg_green lor bg_blue lor bg_lightblack
let bg_brown          = bg_red lor bg_green
let bg_yellow         = bg_red lor bg_green lor bg_lightblack
let bg_gray           = bg_red lor bg_green lor bg_blue
let bg_white          = bg_red lor bg_green lor bg_blue lor bg_lightblack

(** Sets the current color of the console text *)
let set_console_text_attribute handle attrib =
	if caml_set_console_text_attribute handle attrib then (
		Normal ()
	) else (
		Exception ("set_console_text_attribute", caml_get_last_error ())
	)

(** Info about the console *)
let get_console_screen_buffer_info handle =
	let (a,b) = caml_get_console_screen_buffer_info handle in
	if a then (
		Normal b
	) else (
		Exception ("get_console_screen_buffer_info", caml_get_last_error ())
	)

(** Gets the current color of the console text *)
let get_console_text_attribute handle =
	let (a,b) = caml_get_console_text_attribute handle in
	if a then (
		Normal b
	) else (
		Exception ("get_console_text_attribute", caml_get_last_error ())
	)

(** Sets the current position of the cursor in the console. If you go out of bounds you will get a Console_error ("set_console_cursor_position",87) *)
let set_console_cursor_position handle xy =
	if caml_set_console_cursor_position handle xy then (
		Normal ()
	) else (
		Exception ("set_console_cursor_position", caml_get_last_error ())
	)

(** Writes the specified string to the console *)
let write handle str =
	let (a,b) = caml_write handle str in
	if a then (
		Normal b
	) else (
		Exception ("write", caml_get_last_error ())
	)

let rec really_write handle str =
	let written = write handle str in
	match written with
	| Normal x when x < String.length str -> really_write handle (String.sub str x (String.length str - x))
	| Normal x -> Normal ()
	| Exception (a,b) -> Exception ("really_write",b)



(* Higher level functions *)
(* These make heavy use of Haskell-style monads for transmitting exceptions *)
(* The (%%) function is like Haskell's (>>=), (%) is like (>>) *)
class console handle =
	object (o)

		val blank_line = (
			match get_console_screen_buffer_info handle with
			| Normal {size_x = x} -> String.make x ' '
			| Exception _ -> String.make 80 ' ' (* Default to 80 - I don't want to make this a (string console_return) type *)
		)

		method up n = (
			(get_console_screen_buffer_info handle) %%
			(fun i -> set_console_cursor_position handle (i.cursor_x, i.cursor_y - n))
		)
		method down n = (
			(get_console_screen_buffer_info handle) %%
			(fun i -> set_console_cursor_position handle (i.cursor_x, i.cursor_y + n))
		)
		method left n = (
			(get_console_screen_buffer_info handle) %%
			(fun i -> set_console_cursor_position handle (i.cursor_x - n, i.cursor_y))
		)
		method right n = (
			(get_console_screen_buffer_info handle) %%
			(fun i -> set_console_cursor_position handle (i.cursor_x + n, i.cursor_y))
		)

		method left_justify = (
			(get_console_screen_buffer_info handle) %%
			(fun i -> set_console_cursor_position handle (0, i.cursor_y))
		)

		method print_line s = (
			(get_console_screen_buffer_info handle) %%
			(fun i ->
				(set_console_cursor_position handle (0, i.cursor_y)) %%
				(fun () ->
					let str = String.make (i.size_x) ' ' in
					String.blit s 0 str 0 (min (String.length s) (String.length str));
					really_write handle str
				)
			)
		)

		method print_blank_line = (
			(get_console_screen_buffer_info handle) %%
			(fun i ->
				(set_console_cursor_position handle (0, i.cursor_y)) %%
				(fun () -> really_write handle blank_line)
			)
		)

		method info = get_console_screen_buffer_info handle

		method print_line_noenter s = (o#print_line s) %% (fun () -> o#up 1)

		method print_char c = really_write handle (String.make 1 c)

	end
;;

