all:
	gcc main.c -o test -I/usr/local/include -L/usr/local/lib -lzookeeper_mt

	mkdir -p build
	gcc src/module.c \
	-shared -o build/distman.so \
	-fPIC \
	-I/usr/include/glib-2.0 \
	-I/usr/lib64/glib-2.0/include \
	-I/usr/local/include \
	-L/usr/lib64 \
	-L/usr/local/lib \
	-lzookeeper_mt \
	-pedantic \
	-std=c11

install: all
	mkdir -p /usr/lib64/distman
	mkdir -p /usr/etc/distman
	mkdir -p /usr/etc/naemon/module-conf.d
	cp build/distman.so /usr/lib64/distman/
	cp build/distman.conf /usr/etc/distman/
	cp build/distman.cfg /usr/etc/naemon/module-conf.d/
