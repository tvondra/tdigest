MODULE_big = tdigest
OBJS = tdigest.o

EXTENSION = tdigest
EXTVERSIONS = 1.0.1
DATA_built = $(foreach v,$(EXTVERSIONS),$(EXTENSION)--$(v).sql)
DATA = $(wildcard $(EXTENSION)--*--*.sql)
MODULES = tdigest

CFLAGS=`pg_config --includedir-server`

TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

$(EXTENSION)--1.0.1.sql: $(EXTENSION)--1.0.0.sql $(EXTENSION)--1.0.0--1.0.1.sql
	cat $^ > $@
