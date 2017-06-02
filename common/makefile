OBJ_EXT=.obj


all_opt: bigbuffer.cmx matroska.cmx mkvbb.cmx pack.cmx rx.cmx rx2.cmx bigbuffer.cmx bitstream.cmx rbtree.cmx opt.cmx opt-c$(OBJ_EXT)

%.cmx: %.ml
	ocamlopt.opt -c -thread $*.ml

.PRECIOUS: %.cmi
%.cmi:
	ocamlopt.opt -c -thread $*.mli

rx.cmx: rx.ml
	ocamlopt.opt -rectypes -c -thread rx.ml

rx2.cmx: rx2.ml
	ocamlopt.opt -rectypes -c -thread rx2.ml

rbtree.cmx: rbtree.cmi

bigbuffer.cmx: bigbuffer.cmi

mkvbb.cmx: matroska.cmx bigbuffer.cmx

opt-c$(OBJ_EXT): opt-c.c
	ocamlopt.opt -c opt-c.c


test: all_opt test.ml
	ocamlopt.opt -o test unix.cmxa opt-c$(OBJ_EXT) opt.cmx test.ml
	./test


clean:
	rm -f ./*.exe ./*.obj ./*.cmi ./*.cmx ./*.cmo ./*.cma ./*.cmxa ./*.asm
#	rm -rf ./*.exe
#	rm -rf ./*.obj
#	rm -rf ./*.cmi
#	rm -rf ./*.cmx
#	rm -rf ./*.cmo
#	rm -rf ./*.cma
#	rm -rf ./*.cmxa
#	rm -rf ./*.lib
#	rm -rf ./*.asm


