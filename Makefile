# Top level makefile, the real shit is at src/Makefile

default: all

.DEFAULT:
	cd src && $(MAKE)  $@
indexer:
	cd src && make -f makefile.indexer

install:
	cd src && $(MAKE) $@

.PHONY: install
