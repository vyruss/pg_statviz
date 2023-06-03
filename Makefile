EXTENSION = pg_statviz
DATA = pg_statviz--0.1.sql pg_statviz--0.1--0.2.sql
DOCS = README.md
REGRESS = pg_statviz_test

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
