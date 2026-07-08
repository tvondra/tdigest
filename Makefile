EXTENSION    = $(shell grep -m 1 '"name":' META.json | \
               sed -e 's/[[:space:]]*"name":[[:space:]]*"\([^"]*\)",/\1/')
EXTVERSION   = $(shell grep -m 1 '[[:space:]]\{8\}"version":' META.json | \
               sed -e 's/[[:space:]]*"version":[[:space:]]*"\([^"]*\)",\{0,1\}/\1/')
DISTVERSION  = $(shell grep -m 1 '[[:space:]]\{3\}"version":' META.json | \
               sed -e 's/[[:space:]]*"version":[[:space:]]*"\([^"]*\)",\{0,1\}/\1/')

MODULE_big = tdigest
OBJS = tdigest.o

EXTENSION = tdigest
DATA = tdigest--1.0.0.sql tdigest--1.0.0--1.0.1.sql tdigest--1.0.1--1.2.0.sql \
	tdigest--1.2.0--1.3.0.sql tdigest--1.3.0--1.4.0.sql tdigest--1.4.0--1.4.1.sql \
	tdigest--1.4.1--1.4.2.sql tdigest--1.4.2--1.4.3.sql tdigest--1.4.3--1.4.4.sql
MODULES = tdigest

CFLAGS=`pg_config --includedir-server`

REGRESS      = basic copy cast conversions incremental parallel_query value_count_api trimmed_aggregates combine_crash combine
REGRESS_OPTS = --inputdir=test


# The data type whose I/O functions we fuzz, and its source module.  Extend the
# TYPES/<type>_* variables below to cover additional types.

# For each type: the input function, the receive function and the send
# function.  Used by the per-target rules generated below.
TYPES = tdigest

tdigest_IN = tdigest_in
tdigest_RECV = tdigest_recv
tdigest_SEND = tdigest_send
tdigest_OBJS = $(OBJS)
tdigest_INC =



FUZZ_TARGETS = \
	$(foreach t,$(TYPES),fuzz_$(t)_in fuzz_$(t)_recv)

top_builddir=/home/tomas/postgres

# The backend objects to link against.  The per-directory objfiles.txt files
# are produced by an ordinary backend build and list all backend object files;
# the timezone objects live in a separate tree.  This mirrors what the meson
# build links via postgres_lib, and what the "postgres" executable itself links.
BACKEND_OBJFILES = \
	$(wildcard /home/tomas/postgres/src/backend/*/objfiles.txt) \
	/home/tomas/postgres/src/timezone/objfiles.txt
BACKEND_OBJS = $(addprefix /home/tomas/postgres/,$(shell cat $(BACKEND_OBJFILES) 2>/dev/null))

SRV_LIBS = /home/tomas/postgres/src/common/libpgcommon_srv.a \
	/home/tomas/postgres/src/port/libpgport_srv.a


# The backend needs a few libraries that ordinary programs don't, and does not
# need the client-only line-editing libraries.  Mirror src/backend/Makefile.
FUZZ_LIBS := $(filter-out -lpgport -lpgcommon, $(LIBS))
FUZZ_LIBS += $(LDAP_LIBS_BE) $(ICU_LIBS) $(LIBURING_LIBS)
FUZZ_LIBS := $(filter-out -lreadline -ledit -ltermcap -lncurses -lcurses, $(FUZZ_LIBS))

# The backend static library also contains the server's main(); our own main()
# comes first on the link line, so let the linker keep it.
FUZZ_LDFLAGS = -Wl,--allow-multiple-definition


PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: all fuzz
fuzz: $(FUZZ_TARGETS)

# Compile the type's sources into a private subdirectory so we don't collide
# with contrib/ltree's own (PIC, shared-module) objects.
type_tdigest/%.o: tdigest.c
	@mkdir -p type_tdigest
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $<

# Generate, for every type, the per-harness driver object and the executable.
# $(1) = type name, $(2) = harness ("in" or "recv"), $(3) = driver source,
# $(4) = -D macros selecting the function(s) to exercise.
define fuzz_target
fuzz_$(1)_$(2).o: $(3)
	$$(CC) $$(CFLAGS) $$(CPPFLAGS) $$($(1)_INC) $(4) -c -o $$@ $$<

fuzz_$(1)_$(2): fuzz_$(1)_$(2).o $$($(1)_OBJS) $$(BACKEND_OBJS) $$(SRV_LIBS)
	$$(CC) $$(CFLAGS) $$(FUZZ_LDFLAGS) $$(LDFLAGS) $$^ $$(FUZZ_LIBS) -o $$@
endef

$(foreach t,$(TYPES),\
  $(eval $(call fuzz_target,$(t),in,fuzz_in.c,-DFUZZ_IN_SYMBOL=$($(t)_IN))))
$(foreach t,$(TYPES),\
  $(eval $(call fuzz_target,$(t),recv,fuzz_recv.c,-DFUZZ_RECV_SYMBOL=$($(t)_RECV) -DFUZZ_SEND_SYMBOL=$($(t)_SEND))))


dist:
	git archive --format zip --prefix=$(EXTENSION)-$(DISTVERSION)/ -o $(EXTENSION)-$(DISTVERSION).zip HEAD

latest-changes.md: Changes
	perl -e 'while (<>) {last if /^(v?\Q${DISTVERSION}\E)/; } print "Changes for v${DISTVERSION}:\n"; while (<>) { last if /^\s*$$/; s/^\s+//; print }' Changes > $@
