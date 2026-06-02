EXTENSION = gv_lake
MODULE_big = gv_lake
OBJS = src/gv_lake.o \
       src/commit_table.o \
       src/create_table.o \
       src/load_table.o \
       src/namespace.o \
       src/table_ops.o \
       src/utils.o

DATA = sql/gv_lake--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
