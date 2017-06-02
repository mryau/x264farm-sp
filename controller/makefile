# Change this if compiling on Unixy OS (or run "make OBJ_EXT=.o")
OBJ_EXT=.obj

# avs.cmx avs-c$(OBJ_EXT)
controller: .common avi.cmx avi-c$(OBJ_EXT) console.cmx console-c$(OBJ_EXT) contopt.cmx contopt-c$(OBJ_EXT)
	ocamlopt.opt -o controller -cclib vfw32.lib -I ../common -rectypes -thread unix.cmxa threads.cmxa ws2_32.lib xml-light.cmxa ..\common\opt-c$(OBJ_EXT) opt.cmx pack.cmx rx.cmx rx2.cmx rbtree.cmx bigbuffer.cmx matroska.cmx mkvbb.cmx avi.cmx avi-c$(OBJ_EXT) console.cmx console-c$(OBJ_EXT) contopt.cmx contopt-c$(OBJ_EXT) controller.ml
#	ocamlopt.opt -o controller -cclib vfw32.lib -cclib avisynth.lib -I ../common -rectypes -thread unix.cmxa threads.cmxa xml-light.cmxa ..\common\opt-c$(OBJ_EXT) opt.cmx pack.cmx rx.cmx rbtree.cmx bigbuffer.cmx matroska.cmx mkvbb.cmx avi.cmx avi-c$(OBJ_EXT) controller.ml

i: .common avi.cmx avi-c$(OBJ_EXT) console.cmx console-c$(OBJ_EXT) contopt.cmx contopt-c$(OBJ_EXT)
	ocamlopt.opt -i -cclib vfw32.lib -I ../common -rectypes -thread unix.cmxa threads.cmxa ws2_32.lib xml-light.cmxa ..\common\opt-c$(OBJ_EXT) opt.cmx pack.cmx rx.cmx rx2.cmx rbtree.cmx bigbuffer.cmx matroska.cmx mkvbb.cmx avi.cmx avi-c$(OBJ_EXT) console.cmx console-c$(OBJ_EXT) contopt.cmx contopt-c$(OBJ_EXT) controller.ml


fakecontroller: .common avi.cmx avi-c$(OBJ_EXT) console.cmx console-c$(OBJ_EXT) contopt.cmx contopt-c$(OBJ_EXT)
	ocamlopt.opt -o fakecontroller -cclib vfw32.lib -I ../common -rectypes -thread unix.cmxa threads.cmxa ws2_32.lib xml-light.cmxa ..\common\opt-c$(OBJ_EXT) opt.cmx pack.cmx rx.cmx rx2.cmx rbtree.cmx bigbuffer.cmx matroska.cmx mkvbb.cmx avi.cmx avi-c$(OBJ_EXT) console.cmx console-c$(OBJ_EXT) contopt.cmx contopt-c$(OBJ_EXT) fakecontroller.ml
#fakecontroller: .common
#	ocamlopt.opt -o fakecontroller -I ../common -rectypes -thread unix.cmxa threads.cmxa xml-light.cmxa ..\common\opt-c$(OBJ_EXT) opt.cmx pack.cmx rx.cmx rx2.cmx rbtree.cmx bigbuffer.cmx matroska.cmx mkvbb.cmx fakecontroller.ml

.common:
	make -C ../common OBJ_EXT=$(OBJ_EXT)


#avs.cmx: avs.ml
#	ocamlopt.opt -c -thread unix.cmxa threads.cmxa avs.ml
#
#avs-c$(OBJ_EXT): avs-c.c
#	ocamlopt.opt -c avs-c.c


avi.cmx: avi.ml
	ocamlopt.opt -I ../common -c -thread unix.cmxa threads.cmxa opt.cmx avi.ml

avi-c$(OBJ_EXT): avi-c.c
	ocamlopt.opt -c avi-c.c

console.cmx: console.ml
	ocamlopt.opt -c console.ml

console-c$(OBJ_EXT): console-c.c
	ocamlopt.opt -c console-c.c

contopt.cmx: contopt.ml
	ocamlopt.opt -c -thread unix.cmxa threads.cmxa contopt.ml

contopt-c$(OBJ_EXT): contopt-c.c
	ocamlopt.opt -c contopt-c.c



clean:
	make -C ../common clean
	rm -f ./*.exe ./*.obj ./*.cmi ./*.cmx ./*.cmo ./*.cma ./*.cmxa ./*.asm

#	rm -f ./*.exe
#	rm -f ./*.obj
#	rm -f ./*.cmi
#	rm -f ./*.cmx
#	rm -f ./*.cmo
#	rm -f ./*.cma
#	rm -f ./*.cmxa
#	rm -f ./*.lib
#	rm -f ./*.asm

