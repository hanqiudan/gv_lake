/*
 * gv_lake.c - Main entry point for gv_lake extension
 */

#include "postgres.h"
#include "fmgr.h"
#include <time.h>

PG_MODULE_MAGIC;

void
_PG_init(void)
{
    srand((unsigned int)time(NULL));
}

void
_PG_fini(void)
{
}
