MODULE_big = tdigest
OBJS = tdigest.o

EXTENSION = tdigest
DATA = tdigest--1.0.0.sql tdigest--1.0.0--1.0.1.sql tdigest--1.0.1--1.2.0.sql  tdigest--1.2.0--1.3.0.sql tdigest--1.3.0--1.4.0.sql tdigest--1.4.0--1.4.1.sql tdigest--1.4.1--1.4.2.sql tdigest--1.4.2--1.4.3.sql
MODULES = tdigest

CFLAGS=`pg_config --includedir-server`

REGRESS      = basic cast conversions incremental parallel_query value_count_api trimmed_aggregates
REGRESS_OPTS = --inputdir=test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
