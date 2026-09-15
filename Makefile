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
	tdigest--1.4.1--1.4.2.sql tdigest--1.4.2--1.4.3.sql tdigest--1.4.3--1.4.4.sql \
	tdigest--1.4.4--1.4.5.sql tdigest--1.4.5--1.4.6.sql tdigest--1.4.6--1.4.7.sql
MODULES = tdigest

CFLAGS=`pg_config --includedir-server`

REGRESS      = --schedule=$(srcdir)/test/parallel_schedule
REGRESS_OPTS = --inputdir=test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Disable the FP contraction, if the compiler understands the option. It's added
# after including PGXS, so that it overrides any earlier -ffp-contract value.
FP_CONTRACT := $(shell $(CC) -ffp-contract=off -xc -E /dev/null > /dev/null 2>&1 \
                       && echo -ffp-contract=off)
override CFLAGS += $(FP_CONTRACT)

# The PGXS "uninstall" target only removes the files currently listed in DATA,
# so scripts installed by an older build (e.g. upgrade scripts that were since
# renamed or removed) would be left behind. Delete everything matching the
# extension name, to get rid of those stale files too.
#
# A recipe can't be appended to the PGXS "uninstall" rule, so this is hooked in
# as a prerequisite (which means it runs before the PGXS part - that's fine,
# the two steps are independent).
uninstall: uninstall-stale

.PHONY: uninstall-stale
uninstall-stale:
	rm -f '$(DESTDIR)$(datadir)/extension'/$(EXTENSION)*

dist:
	git archive --format zip --prefix=$(EXTENSION)-$(DISTVERSION)/ -o $(EXTENSION)-$(DISTVERSION).zip HEAD

latest-changes.md: Changes
	perl -e 'while (<>) {last if /^(v?\Q${DISTVERSION}\E)/; } print "Changes for v${DISTVERSION}:\n"; while (<>) { last if /^\s*$$/; s/^\s+//; print }' Changes > $@
