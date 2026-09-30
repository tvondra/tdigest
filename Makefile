# The version the distribution archive and the release notes are built for.
# The extension name is not read from META.json - the name there is the PGXN
# distribution name ("t-digest"), which is not the name of the extension.
DISTVERSION  = $(shell grep -m 1 '[[:space:]]\{3\}"version":' META.json | \
               sed -e 's/[[:space:]]*"version":[[:space:]]*"\([^"]*\)",\{0,1\}/\1/')

MODULE_big = tdigest
OBJS = tdigest.o

EXTENSION = tdigest
DATA = tdigest--1.0.0.sql tdigest--1.0.0--1.0.1.sql tdigest--1.0.1--1.2.0.sql \
	tdigest--1.2.0--1.3.0.sql tdigest--1.3.0--1.4.0.sql tdigest--1.4.0--1.4.1.sql \
	tdigest--1.4.1--1.4.2.sql tdigest--1.4.2--1.4.3.sql tdigest--1.4.3--1.4.4.sql \
	tdigest--1.4.4--1.4.5.sql tdigest--1.4.5--1.4.6.sql tdigest--1.4.6--1.4.7.sql

REGRESS      = --schedule=$(srcdir)/test/parallel_schedule
REGRESS_OPTS = --inputdir=test

# AFL++ fuzzing harnesses, see FUZZING.md
FUZZ_TARGETS = fuzz_tdigest_in fuzz_tdigest_recv
EXTRA_CLEAN = $(FUZZ_TARGETS)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Disable FP contraction, if supported, for native code and LLVM bitcode.
# Append after PGXS so this overrides any earlier -ffp-contract option.
FP_CONTRACT := $(shell $(CC) -ffp-contract=off -xc -E /dev/null > /dev/null 2>&1 \
                       && echo -ffp-contract=off)
override CFLAGS += $(FP_CONTRACT)
override BITCODE_CFLAGS += $(FP_CONTRACT)

# The PGXS "uninstall" target only removes the files currently listed in DATA,
# so scripts installed by an older build (e.g. upgrade scripts that were since
# renamed or removed) would be left behind. Match the control file and versioned
# SQL scripts, not just the name prefix, which other extensions may share.
#
# A recipe can't be appended to the PGXS "uninstall" rule, so this is hooked in
# as a prerequisite (which means it runs before the PGXS part - that's fine,
# the two steps are independent).
uninstall: uninstall-stale

.PHONY: uninstall-stale
uninstall-stale:
	rm -f '$(DESTDIR)$(datadir)/extension/$(EXTENSION).control' \
		'$(DESTDIR)$(datadir)/extension/$(EXTENSION)--'*.sql

# The fuzzing harnesses are standalone executables: the harness and tdigest.c,
# compiled with FUZZ_CC, linked with the backend objects of the PostgreSQL
# build tree in the same way src/backend/Makefile links the postgres binary.
# This requires a configure/make build (meson does not produce objfiles.txt).
# PostgreSQL itself does not need to be built with AFL++ instrumentation.
#
# PGXS does not set abs_top_builddir, but the installed Makefile.global still
# records the build tree. Override PG_BUILD if the build tree was moved.
#
# FUZZ_BACKEND and FUZZ_LIBS are expanded only when building a harness, so the
# other targets don't need the build tree.
FUZZ_CC = afl-clang-fast
PG_BUILD := $(shell sed -n 's/^abs_top_builddir = //p' $(top_builddir)/src/Makefile.global)

FUZZ_BACKEND = \
	$(addprefix $(PG_BUILD)/,$(shell cat $(PG_BUILD)/src/backend/*/objfiles.txt \
		$(PG_BUILD)/src/timezone/objfiles.txt)) \
	$(if $(filter yes,$(enable_dtrace)),$(PG_BUILD)/src/backend/utils/probes.o) \
	$(PG_BUILD)/src/common/libpgcommon_srv.a \
	$(PG_BUILD)/src/port/libpgport_srv.a

FUZZ_LIBS = \
	$(filter-out -lpgport -lpgcommon -lreadline -ledit -ltermcap -lncurses -lcurses,$(LIBS)) \
	$(LDAP_LIBS_BE) $(ICU_LIBS) $(LIBURING_LIBS) \
	$(if $(filter yes,$(with_systemd)),-lsystemd)

# init_database_collation_standalone() is added by postgres-collation.patch.
# tdigest does not use collations, so call it only if it's available.
FUZZ_CPPFLAGS = \
	$(if $(shell grep -s init_database_collation_standalone \
		$(includedir_server)/utils/pg_locale.h),-DHAVE_INIT_DATABASE_COLLATION_STANDALONE)

fuzz_tdigest_in: FUZZ_DEFINES = -DFUZZ_IN_SYMBOL=tdigest_in
fuzz_tdigest_recv: FUZZ_DEFINES = -DFUZZ_RECV_SYMBOL=tdigest_recv -DFUZZ_SEND_SYMBOL=tdigest_send

.PHONY: fuzz
fuzz: $(FUZZ_TARGETS)

# The backend's main.o defines main(), but can't be left out because it also
# defines other symbols the backend needs. The harness is linked first, so its
# main() is the one that's used.
$(FUZZ_TARGETS): fuzz.c tdigest.c $(PG_BUILD)/src/backend/postgres
	$(FUZZ_CC) $(CFLAGS) $(CPPFLAGS) $(FUZZ_CPPFLAGS) $(FUZZ_DEFINES) \
		fuzz.c tdigest.c $(FUZZ_BACKEND) \
		$(LDFLAGS) $(LDFLAGS_EX) $(LDFLAGS_EX_BE) -Wl,--allow-multiple-definition \
		$(FUZZ_LIBS) -o $@

dist:
	git archive --format zip --prefix=$(EXTENSION)-$(DISTVERSION)/ -o $(EXTENSION)-$(DISTVERSION).zip HEAD

# The output is tracked, so checkout timestamps do not establish whether its
# contents match the release selected by META.json.
.PHONY: latest-changes.md
latest-changes.md: Changes META.json
	perl -e 'while (<>) {last if /^(v?\Q${DISTVERSION}\E)/; } print "Changes for v${DISTVERSION}:\n"; while (<>) { last if /^\s*$$/; s/^\s+//; print }' Changes > $@
