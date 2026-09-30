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

# libFuzzer harnesses (see FUZZING.md), built by "make fuzz" from instrumented
# copies of $(OBJS). Defined before including PGXS, which only looks at
# EXTRA_CLEAN when it is included.
FUZZ_TARGETS = fuzz_tdigest_in fuzz_tdigest_recv
FUZZ_OBJS    = $(OBJS:.o=_fuzz.o)
EXTRA_CLEAN  = $(FUZZ_TARGETS) $(FUZZ_TARGETS:=.o) $(FUZZ_OBJS) fuzz_backend.rsp

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

dist:
	git archive --format zip --prefix=$(EXTENSION)-$(DISTVERSION)/ -o $(EXTENSION)-$(DISTVERSION).zip HEAD

# The output is tracked, so checkout timestamps do not establish whether its
# contents match the release selected by META.json.
.PHONY: latest-changes.md
latest-changes.md: Changes META.json
	perl -e 'while (<>) {last if /^(v?\Q${DISTVERSION}\E)/; } print "Changes for v${DISTVERSION}:\n"; while (<>) { last if /^\s*$$/; s/^\s+//; print }' Changes > $@

# The fuzzers link the backend objects from the build tree of the PostgreSQL
# installation, which must be kept after "make install". Its location is
# recorded in the installed Makefile.global, but PGXS does not set
# abs_top_builddir, so read it from the file. Set PG_BUILDDIR to override it.
ifndef PG_BUILDDIR
PG_BUILDDIR := $(shell sed -n 's/^abs_top_builddir = //p' '$(top_builddir)/src/Makefile.global' 2>/dev/null)
endif

# FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION is the usual macro for fuzzing
# builds, it enables the wrappers called by the harnesses in tdigest.c.
FUZZ_CFLAGS  = -fsanitize=fuzzer-no-link -DFUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION
# The backend objects include a main(), but libFuzzer's (linked first) wins.
FUZZ_LDFLAGS = -fsanitize=fuzzer -Wl,--allow-multiple-definition

# The objfiles.txt lists are touched whenever the backend objects change.
FUZZ_OBJFILES = $(wildcard $(PG_BUILDDIR)/src/backend/*/objfiles.txt) \
	$(PG_BUILDDIR)/src/timezone/objfiles.txt
FUZZ_SRV_LIBS = $(PG_BUILDDIR)/src/common/libpgcommon_srv.a \
	$(PG_BUILDDIR)/src/port/libpgport_srv.a

# The backend's libraries, as in src/backend/Makefile.
FUZZ_LIBS = $(filter-out -lpgport -lpgcommon -lreadline -ledit -ltermcap -lncurses -lcurses, \
	$(LIBS) $(LDAP_LIBS_BE) $(ICU_LIBS) $(LIBURING_LIBS)) \
	$(if $(filter yes,$(with_systemd)),-lsystemd)

.PHONY: fuzz
fuzz: $(FUZZ_TARGETS)

fuzz_tdigest_in.o: fuzz_in.c
	$(CC) $(CFLAGS) $(CPPFLAGS) $(FUZZ_CFLAGS) -DFUZZ_IN_SYMBOL=tdigest_in_fuzz -c -o $@ $<

fuzz_tdigest_recv.o: fuzz_recv.c
	$(CC) $(CFLAGS) $(CPPFLAGS) $(FUZZ_CFLAGS) -DFUZZ_RECV_SYMBOL=tdigest_recv_fuzz -c -o $@ $<

%_fuzz.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) $(FUZZ_CFLAGS) -c -o $@ $<

# Hundreds of absolute paths may exceed the command line length limit, so the
# backend objects are passed to the linker in a response file.
fuzz_backend.rsp: $(FUZZ_OBJFILES)
	sed 's,[^ ][^ ]*,$(PG_BUILDDIR)/&,g' $^ > $@

$(FUZZ_TARGETS): %: %.o $(FUZZ_OBJS) fuzz_backend.rsp $(FUZZ_SRV_LIBS)
	$(CC) $(CFLAGS) $< $(FUZZ_OBJS) @fuzz_backend.rsp $(FUZZ_SRV_LIBS) \
		$(LDFLAGS) $(LDFLAGS_EX_BE) $(FUZZ_LIBS) $(FUZZ_LDFLAGS) -o $@

$(FUZZ_OBJFILES) $(FUZZ_SRV_LIBS):
	$(error $@ not found, set PG_BUILDDIR to the PostgreSQL build tree (see FUZZING.md))
