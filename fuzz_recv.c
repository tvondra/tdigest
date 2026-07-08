/*-------------------------------------------------------------------------
 *
 * fuzz_recv.c
 *	  Generic fuzzing harness for the binary receive (and send) functions
 *	  of a user-defined data type.
 *
 * This driver is meant to be compiled once per target type together with the
 * type's implementation and the whole backend (see the meson.build in this
 * directory).  The receive function to exercise is selected at compile time
 * through the FUZZ_RECV_SYMBOL macro; optionally FUZZ_SEND_SYMBOL names the
 * matching send function, which is then called on every value that the
 * receive function accepts, so that a round-trip through send is fuzzed too.
 *
 * The harness supports two modes:
 *
 *	1. When built with an AFL++ instrumenting compiler (afl-clang-fast or
 *	   afl-clang-lto), it uses AFL++ "persistent mode": a single process
 *	   handles many test cases in a tight loop, which is dramatically faster
 *	   than fork()+exec() per input.
 *
 *	2. When built with a plain compiler, it reads a single test case from the
 *	   file named on the command line (or from stdin) and runs it once.  This
 *	   is handy for reproducing crashes found by AFL++ and for quickly
 *	   checking that the harness itself builds and works.
 *
 * A malformed input is expected to make the receive function raise an error
 * via ereport(ERROR); such errors are caught and treated as a normal (non
 * crashing) outcome.  Only genuine memory-safety problems - reads/writes out
 * of bounds, assertion failures, etc. - are reported as crashes by the fuzzer.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/test/fuzz/fuzz_recv.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "fmgr.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/pg_locale.h"

#ifndef FUZZ_RECV_SYMBOL
#error "FUZZ_RECV_SYMBOL must be defined to the name of the receive function"
#endif

/* The receive (and optional send) function are ordinary fmgr functions. */
extern PGDLLIMPORT Datum FUZZ_RECV_SYMBOL(PG_FUNCTION_ARGS);

#ifdef FUZZ_SEND_SYMBOL
extern PGDLLIMPORT Datum FUZZ_SEND_SYMBOL(PG_FUNCTION_ARGS);
#endif

/* A long-lived context that everything the harness produces is allocated in. */
static MemoryContext fuzz_ctx = NULL;

/*
 * One-time set up of the minimal backend environment needed to run type
 * input/output code: process id, memory contexts and the stack-depth base.
 */
static void
fuzz_setup(void)
{
	MyProcPid = getpid();
	MemoryContextInit();
	(void) set_stack_base();

	/*
	 * Type input/output code may classify characters using the default
	 * collation, which normally comes from the catalogs.  Since we run without
	 * a live catalog, install a plain C locale instead.
	 */
	init_database_collation_standalone();

	fuzz_ctx = AllocSetContextCreate(TopMemoryContext,
									 "fuzz",
									 ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(fuzz_ctx);
}

/*
 * Run the receive (and optionally send) function on a single test case.
 *
 * All work happens inside fuzz_ctx, which is reset afterwards so that memory
 * usage stays bounded across the many iterations of persistent mode.  Errors
 * raised by the type functions are caught and ignored - they simply mean the
 * input was rejected as invalid, which is not a bug.
 */
static void
fuzz_one(const char *data, size_t len)
{
	sigjmp_buf	local_sigjmp_buf;

	MemoryContextSwitchTo(fuzz_ctx);

	if (sigsetjmp(local_sigjmp_buf, 1) == 0)
	{
		StringInfoData buf;
		Datum		result;

		PG_exception_stack = &local_sigjmp_buf;

		/* Wrap the fuzzer-supplied bytes in a StringInfo message buffer. */
		initStringInfo(&buf);
		appendBinaryStringInfo(&buf, data, (int) len);

		/*
		 * Call the receive function.  We pass a valid-looking type OID and
		 * typmod so that receive functions which read those arguments (for
		 * example array or record receive) behave sanely; scalar receive
		 * functions such as ltree_recv simply ignore them.
		 */
		result = DirectFunctionCall3(FUZZ_RECV_SYMBOL,
									 PointerGetDatum(&buf),
									 ObjectIdGetDatum(InvalidOid),
									 Int32GetDatum(-1));

		/* Reject inputs that left unconsumed trailing bytes, like the server. */
		pq_getmsgend(&buf);

#ifdef FUZZ_SEND_SYMBOL
		/* Round-trip the accepted value back out through the send function. */
		(void) DirectFunctionCall1(FUZZ_SEND_SYMBOL, result);
#else
		(void) result;
#endif
	}
	else
	{
		/* The input was rejected via ereport(ERROR); this is expected. */
		FlushErrorState();
	}

	PG_exception_stack = NULL;
	error_context_stack = NULL;

	/* Free everything the iteration allocated. */
	MemoryContextSwitchTo(fuzz_ctx);
	MemoryContextReset(fuzz_ctx);
}

#ifdef __AFL_FUZZ_TESTCASE_LEN

/* Reserve space for the shared test-case buffer used by persistent mode. */
__AFL_FUZZ_INIT();

int
main(int argc, char **argv)
{
	unsigned char *buf;

	fuzz_setup();

#ifdef __AFL_HAVE_MANUAL_CONTROL
	/* Defer forkserver start-up until after the (cheap) set up above. */
	__AFL_INIT();
#endif

	buf = __AFL_FUZZ_TESTCASE_BUF;

	while (__AFL_LOOP(100000))
	{
		size_t		len = __AFL_FUZZ_TESTCASE_LEN;

		fuzz_one((const char *) buf, len);
	}

	return 0;
}

#else							/* !__AFL_FUZZ_TESTCASE_LEN */

/*
 * Non-instrumented build: read one test case from the file named on the
 * command line, or from stdin when no file is given, and run it once.
 */
int
main(int argc, char **argv)
{
	char	   *data;
	size_t		len = 0;
	size_t		cap = 8192;
	FILE	   *fp = stdin;

	fuzz_setup();

	if (argc > 1)
	{
		fp = fopen(argv[1], "rb");
		if (fp == NULL)
		{
			fprintf(stderr, "could not open \"%s\"\n", argv[1]);
			return 1;
		}
	}

	data = malloc(cap);
	if (data == NULL)
	{
		fprintf(stderr, "out of memory\n");
		return 1;
	}

	for (;;)
	{
		size_t		got;

		if (len == cap)
		{
			cap *= 2;
			data = realloc(data, cap);
			if (data == NULL)
			{
				fprintf(stderr, "out of memory\n");
				return 1;
			}
		}

		got = fread(data + len, 1, cap - len, fp);
		len += got;
		if (got == 0)
			break;
	}

	if (fp != stdin)
		fclose(fp);

	fuzz_one(data, len);

	free(data);

	return 0;
}

#endif							/* __AFL_FUZZ_TESTCASE_LEN */
