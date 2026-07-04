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

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

FUZZ_CFLAGS = -fsanitize=fuzzer-no-link
FUZZ_LDFLAGS = -fsanitize=fuzzer -Wl,--allow-multiple-definition

FUZZ_RECV_TARGETS = fuzz_tdigest_recv
FUZZ_IN_TARGETS = fuzz_tdigest_in
fuzz_tdigest_recv_SYMBOL = tdigest_recv
fuzz_tdigest_in_SYMBOL = tdigest_in
FUZZ_TARGETS = $(FUZZ_RECV_TARGETS) $(FUZZ_IN_TARGETS)

FUZZ_RECV_OBJS = $(FUZZ_RECV_TARGETS:%=%.o)
FUZZ_IN_OBJS = $(FUZZ_IN_TARGETS:%=%.o)
FUZZ_OBJS = $(FUZZ_RECV_OBJS) $(FUZZ_IN_OBJS)

# FIXME hardcoded location for the static .a libraries etc.
top_builddir=/home/user/work/postgres

# The static server-side archives, linked as plain objects (not via -lpg*).
SRV_LIBS = \
	$(top_builddir)/src/common/libpgcommon_srv.a \
	$(top_builddir)/src/port/libpgport_srv.a

BACKEND_OBJFILES = \
	$(top_builddir)/src/backend/*/objfiles.txt \
	$(top_builddir)/src/timezone/objfiles.txt

BACKEND_ARCHIVE = libpostgres_fuzz.a

# Backend link libraries, mirroring src/backend/Makefile: libpgport and
# libpgcommon come in as the _srv.a archives above, and the backend needs a
# few libraries that ordinary frontend programs don't (and none of the
# line-editing ones).
BE_LIBS := $(filter-out -lpgport -lpgcommon, $(LIBS))
BE_LIBS += $(LDAP_LIBS_BE) $(ICU_LIBS) $(LIBURING_LIBS)
BE_LIBS := $(filter-out -lreadline -ledit -ltermcap -lncurses -lcurses, $(BE_LIBS))
ifeq ($(with_systemd),yes)
BE_LIBS += -lsystemd
endif

dist:
	git archive --format zip --prefix=$(EXTENSION)-$(DISTVERSION)/ -o $(EXTENSION)-$(DISTVERSION).zip HEAD

latest-changes.md: Changes
	perl -e 'while (<>) {last if /^(v?\Q${DISTVERSION}\E)/; } print "Changes for v${DISTVERSION}:\n"; while (<>) { last if /^\s*$$/; s/^\s+//; print }' Changes > $@

fuzz: $(FUZZ_TARGETS)

# Compile the harness once per target, selecting the receive function.
# Compile the receive harness once per target, selecting the receive function.
$(FUZZ_RECV_OBJS): fuzz_%.o: fuzz_recv.c
	$(CC) $(CFLAGS) $(FUZZ_CFLAGS) $(CPPFLAGS) \
		-DFUZZ_RECV_SYMBOL=$(fuzz_$*_SYMBOL) -c -o $@ $<

# Compile the input harness once per target, selecting the input function.
$(FUZZ_IN_OBJS): fuzz_%.o: fuzz_in.c
	$(CC) $(CFLAGS) $(FUZZ_CFLAGS) $(CPPFLAGS) \
		-DFUZZ_IN_SYMBOL=$(fuzz_$*_SYMBOL) -c -o $@ $<

$(FUZZ_TARGETS): fuzz_%: fuzz_%.o $(OBJS) $(BACKEND_ARCHIVE) $(SRV_LIBS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LDFLAGS_EX_BE) \
		$< $(OBJS) $(BACKEND_ARCHIVE) $(SRV_LIBS) $(BE_LIBS) \
		$(FUZZ_LDFLAGS) -o $@

%.o: %.c
	$(CC) $(CFLAGS) $(FUZZ_CFLAGS) $(CPPFLAGS) -c -o $@ $<

$(BACKEND_ARCHIVE): | submake-backend
	rm -f $@
	cat $(BACKEND_OBJFILES) | xargs -n1 | sed 's,^,$(top_builddir)/,' \
		| xargs $(AR) $(AROPT) $@

# The instrumented objects need the generated backend headers; the final link
# needs the whole backend built.
$(OBJS) $(FUZZ_OBJS): | submake-generated-headers

.PHONY: submake-backend
submake-backend: | submake-generated-headers
	$(MAKE) -C $(top_builddir)/src/backend
