type return_t = String of string | Int of int
type char_t = Char of char | Range of (char * char)
type parser_t
val dot_star :
  bool ->
  string ->
  int ->
  parser_t
val dot_star_hook :
  bool ->
  string ->
  int ->
  parser_t
val constant :
  bool ->
  string ->
  string ->
  int ->
  parser_t
val characters :
  bool ->
  char_t list ->
  string ->
  int ->
  parser_t
val rx :
  ('b -> int -> 'a list -> ('c list * 'd) option as 'a) list ->
  'b -> 'c list option


