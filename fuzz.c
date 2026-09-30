/*-------------------------------------------------------------------------
 *
 * fuzz.c
 *	  Generic fuzzing harness for the text input or binary receive function
 *	  of a user-defined data type.
 *
 * Input and receive functions parse untrusted data, which makes them a natural
 * fuzzing target.  The harness is compiled together with the type's
 * implementation and the whole backend (see the "fuzz" target in the
 * Makefile).  The function to exercise is selected at compile time, by
 * defining exactly one of these macros:
 *
 *	FUZZ_IN_SYMBOL - the input function, which gets the test case as a
 *	NUL-terminated string
 *
 *	FUZZ_RECV_SYMBOL - the receive function, which gets the test case in a
 *	message buffer and has to consume all of it
 *
 * With FUZZ_RECV_SYMBOL, FUZZ_SEND_SYMBOL may name the matching send function,
 * which is then called on every value that the receive function accepts, so
 * that a round-trip through send is fuzzed too.
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
 * A malformed input is expected to make the type function raise an error via
 * ereport(ERROR); such errors are caught and treated as a normal (non
 * crashing) outcome.  Only genuine memory-safety problems - reads/writes out
 * of bounds, assertion failures, etc. - are reported as crashes by the fuzzer.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  fuzz.c
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

#if defined(FUZZ_IN_SYMBOL) == defined(FUZZ_RECV_SYMBOL)
#error "exactly one of FUZZ_IN_SYMBOL and FUZZ_RECV_SYMBOL must be defined"
#endif

#if defined(FUZZ_SEND_SYMBOL) && !defined(FUZZ_RECV_SYMBOL)
#error "FUZZ_SEND_SYMBOL requires FUZZ_RECV_SYMBOL"
#endif

/* The type functions are ordinary fmgr functions. */
#ifdef FUZZ_IN_SYMBOL
extern PGDLLIMPORT Datum FUZZ_IN_SYMBOL(PG_FUNCTION_ARGS);
#else
extern PGDLLIMPORT Datum FUZZ_RECV_SYMBOL(PG_FUNCTION_ARGS);
#endif

#ifdef FUZZ_SEND_SYMBOL
extern PGDLLIMPORT Datum FUZZ_SEND_SYMBOL(PG_FUNCTION_ARGS);
#endif

/* A long-lived context that everything the harness produces is allocated in. */
static MemoryContext fuzz_ctx = NULL;

/*
 * The type functions are not expected to emit warnings, but the memory context
 * checks in assert-enabled builds report problems like writes past the end of
 * a chunk only as a WARNING, which the fuzzer would not notice.  Report those
 * (and anything more severe that is not caught as an ERROR) as crashes.
 */
static void
fuzz_emit_log_hook(ErrorData *edata)
{
	if (edata->elevel >= WARNING)
	{
		fprintf(stderr, "unexpected message: %s\n", edata->message);
		abort();
	}
}

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
	emit_log_hook = fuzz_emit_log_hook;

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

#ifdef FUZZ_IN_SYMBOL

/*
 * Parse a test case with the input function.
 */
static Datum
fuzz_input(const char *data, size_t len)
{
	char	   *str;

	/*
	 * Input functions take a NUL-terminated C string.  The fuzzer supplies
	 * arbitrary bytes with an explicit length, so copy them into a freshly
	 * allocated, NUL-terminated buffer.  Any embedded NUL simply truncates the
	 * string, exactly as it would for a client-supplied value.
	 */
	str = palloc(len + 1);
	if (len > 0)
		memcpy(str, data, len);
	str[len] = '\0';

	/*
	 * The type OID is not known, and there is no typmod.  Input functions
	 * that don't need them simply ignore these arguments.
	 */
	return DirectFunctionCall3(FUZZ_IN_SYMBOL,
							   CStringGetDatum(str),
							   ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(-1));
}

#else							/* FUZZ_RECV_SYMBOL */

/*
 * Parse a test case with the receive function.
 */
static Datum
fuzz_input(const char *data, size_t len)
{
	StringInfoData buf;
	Datum		result;

	/* Wrap the fuzzer-supplied bytes in a StringInfo message buffer. */
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, data, (int) len);

	/*
	 * The type OID is not known, and there is no typmod.  Receive functions
	 * that don't need them simply ignore these arguments.
	 */
	result = DirectFunctionCall3(FUZZ_RECV_SYMBOL,
								 PointerGetDatum(&buf),
								 ObjectIdGetDatum(InvalidOid),
								 Int32GetDatum(-1));

	/* Reject inputs that left unconsumed trailing bytes, like the server. */
	pq_getmsgend(&buf);

	return result;
}

#endif							/* FUZZ_IN_SYMBOL */

/*
 * Run a single test case.
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
		Datum		value;

		PG_exception_stack = &local_sigjmp_buf;

		value = fuzz_input(data, len);

#ifdef FUZZ_SEND_SYMBOL
		/* Round-trip the accepted value back out through the send function. */
		(void) DirectFunctionCall1(FUZZ_SEND_SYMBOL, value);
#else
		(void) value;
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
