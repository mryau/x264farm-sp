#include <caml/mlvalues.h>
//#include <caml/alloc.h>
#include <caml/memory.h>
//#include <caml/unixsupport.h>
//#include <caml/compatibility.h>


#ifdef _WIN32
#include <windows.h>


value caml_STD_INPUT_HANDLE(unit) {CAMLparam1 (unit);CAMLreturn (Val_int(STD_INPUT_HANDLE));}
value caml_STD_OUTPUT_HANDLE(unit) {CAMLparam1 (unit); CAMLreturn (Val_int(STD_OUTPUT_HANDLE));}
value caml_STD_ERROR_HANDLE(unit) {CAMLparam1 (unit); CAMLreturn (Val_int(STD_ERROR_HANDLE));}

value caml_FG_LIGHTBLACK(unit)   {CAMLparam1 (unit); CAMLreturn (Val_int(FOREGROUND_INTENSITY));} /* Does this exist? Who cares. */
value caml_FG_BLUE(unit)         {CAMLparam1 (unit); CAMLreturn (Val_int(FOREGROUND_BLUE));}
value caml_FG_RED(unit)          {CAMLparam1 (unit); CAMLreturn (Val_int(FOREGROUND_RED));}
value caml_FG_GREEN(unit)        {CAMLparam1 (unit); CAMLreturn (Val_int(FOREGROUND_GREEN));}

value caml_BG_LIGHTBLACK(unit)   {CAMLparam1 (unit); CAMLreturn (Val_int(BACKGROUND_INTENSITY));} /* Hooray for light black */
value caml_BG_BLUE(unit)         {CAMLparam1 (unit); CAMLreturn (Val_int(BACKGROUND_BLUE));}
value caml_BG_RED(unit)          {CAMLparam1 (unit); CAMLreturn (Val_int(BACKGROUND_RED));}
value caml_BG_GREEN(unit)        {CAMLparam1 (unit); CAMLreturn (Val_int(BACKGROUND_GREEN));}


value caml_GetStdHandle(value val_nStdHandle) {
	CAMLparam1 (val_nStdHandle);
	CAMLreturn (Val_int(GetStdHandle(Int_val(val_nStdHandle))));
}

value caml_SetConsoleTextAttribute(value val_hConsoleOutput, value val_wAttributes) {
	CAMLparam2 (val_hConsoleOutput, val_wAttributes);
	BOOL success = SetConsoleTextAttribute((HANDLE) Int_val(val_hConsoleOutput), Int_val(val_wAttributes));
//	printf("From C: %d\n", success);
	CAMLreturn (Val_bool(success));
}

value caml_GetLastError(value unit) {
	CAMLparam1 (unit);
	CAMLreturn (Val_int(GetLastError()));
}



value caml_WriteConsole(value val_hConsoleOutput, value val_outString) {
	CAMLparam2(val_hConsoleOutput, val_outString);
	int actuallyWritten;
	CAMLlocal1(outputThis);
	BOOL success = WriteConsole((HANDLE) Int_val(val_hConsoleOutput), String_val(val_outString), string_length(val_outString), &actuallyWritten, NULL);
	outputThis = alloc_tuple(2);
	Store_field(outputThis,0,Val_bool((int) success));
	Store_field(outputThis,1,Val_int(actuallyWritten));
	CAMLreturn(outputThis);
}


value caml_GetConsoleScreenBufferInfo(value val_hConsoleOutput) {
	CAMLparam1(val_hConsoleOutput);
	CONSOLE_SCREEN_BUFFER_INFO outputInfo;
	BOOL success;
	CAMLlocal2(outputInfoValue, outputThis);
	outputInfoValue = alloc_tuple(11);
	outputThis = alloc_tuple(2);
	success = GetConsoleScreenBufferInfo((HANDLE) Int_val(val_hConsoleOutput), &outputInfo);

	Store_field(outputInfoValue,0, Val_int(outputInfo.dwSize.X));
	Store_field(outputInfoValue,1, Val_int(outputInfo.dwSize.Y));
	Store_field(outputInfoValue,2, Val_int(outputInfo.dwCursorPosition.X));
	Store_field(outputInfoValue,3, Val_int(outputInfo.dwCursorPosition.Y));
	Store_field(outputInfoValue,4, Val_int(outputInfo.wAttributes));
	Store_field(outputInfoValue,5, Val_int(outputInfo.srWindow.Left));
	Store_field(outputInfoValue,6, Val_int(outputInfo.srWindow.Top));
	Store_field(outputInfoValue,7, Val_int(outputInfo.srWindow.Right));
	Store_field(outputInfoValue,8, Val_int(outputInfo.srWindow.Bottom));
	Store_field(outputInfoValue,9, Val_int(outputInfo.dwMaximumWindowSize.X));
	Store_field(outputInfoValue,10,Val_int(outputInfo.dwMaximumWindowSize.Y));

	Store_field(outputThis,0,Val_bool((int) success));
	Store_field(outputThis,1,outputInfoValue);
	CAMLreturn(outputThis);
}

value caml_GetConsoleTextAttribute(value val_hConsoleOutput) {
	CAMLparam1(val_hConsoleOutput);
	CONSOLE_SCREEN_BUFFER_INFO outputInfo;
	BOOL success;
	CAMLlocal1(outputThis);
	outputThis = alloc_tuple(2);
	success = GetConsoleScreenBufferInfo((HANDLE) Int_val(val_hConsoleOutput), &outputInfo);

	Store_field(outputThis,0,Val_bool((int) success));
	Store_field(outputThis,1,Val_int((int) (outputInfo.wAttributes)));
	CAMLreturn(outputThis);
}

value caml_SetConsoleCursorPosition(value val_hConsoleOutput, value val_coord) {
	CAMLparam2(val_hConsoleOutput, val_coord);
	COORD passme;
	int x = Int_val(Field(val_coord, 0));
	int y = Int_val(Field(val_coord, 1));
//printf("Handle number %d %d %d %d\n", val_hConsoleOutput, val_hConsoleOutput, Int_val(val_hConsoleOutput), Int_val(val_hConsoleOutput));
	passme.X = x;
	passme.Y = y;
	CAMLreturn(Val_bool(SetConsoleCursorPosition((HANDLE) Int_val(val_hConsoleOutput), passme)));
}






#endif
