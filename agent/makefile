# Change this if compiling on Unixy OS (or run "make OBJ_EXT=.o")
OBJ_EXT=.obj

agent: .common
	ocamlopt.opt -o agent -I ../common -rectypes -thread unix.cmxa threads.cmxa xml-light.cmxa ../common/opt-c$(OBJ_EXT) opt.cmx pack.cmx rx2.cmx bigbuffer.cmx bitstream.cmx matroska.cmx rbtree.cmx agent.ml

fakeagent: .common
	ocamlopt.opt -o fakeagent -I ../common -rectypes -thread unix.cmxa threads.cmxa xml-light.cmxa ../common/opt-c$(OBJ_EXT) opt.cmx pack.cmx rx2.cmx bigbuffer.cmx bitstream.cmx matroska.cmx rbtree.cmx fakeagent.ml

i: .common
	ocamlopt.opt -i -I ../common -rectypes -thread unix.cmxa threads.cmxa xml-light.cmxa ../common/opt-c$(OBJ_EXT) opt.cmx pack.cmx rx2.cmx bigbuffer.cmx bitstream.cmx matroska.cmx rbtree.cmx agent.ml


handletest: .common
	ocamlopt.opt -o handletest -I ../common -thread unix.cmxa threads.cmxa ../common/opt-c$(OBJ_EXT) opt.cmx handletest.ml


.common:
	make -C ../common OBJ_EXT=$(OBJ_EXT)

clean:
	rm -f ./*.exe ./*.obj ./*.cmi ./*.cmx ./*.cmo ./*.cma ./*.cmxa ./*.lib ./*.asm

cleanall:
	make -C ../common clean
	make -C . clean

