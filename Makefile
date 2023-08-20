EXTENSION   = $(shell grep -m 1 '"name":' META.json | sed -e 's/[[:space:]]*"name":[[:space:]]*"\([^"]*\)",/\1/')
DISTVERSION  = $(shell grep -m 1 '[[:space:]]\{3\}"version":' META.json | sed -e 's/[[:space:]]*"version":[[:space:]]*"\([^"]*\)",\{0,1\}/\1/')
DATA = $(wildcard *--*.sql)
DOCS = README.md
REGRESS = pg_statviz_test
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

dist:
	git archive --format zip --prefix=$(EXTENSION)-$(DISTVERSION)/ -o $(EXTENSION)-$(DISTVERSION).zip HEAD *.sql *.control META.json README.md
