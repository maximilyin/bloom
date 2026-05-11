PROJECT = bloom
PROJECT_DESCRIPTION = Wrapper over gun http client
PROJECT_VERSION = $(PROJECT_BUILD_TAG)

DIALYZER_OPTS = -Wunmatched_returns

DEPS = gun
dep_gun = git https://github.com/erlangbureau/gun.git 0.1.1

# Common Test: erlang.mk runs `ct_run` only when CT_SUITES is non-empty. If auto-discovery
# fails, `make ct` would not compile test/*.erl or execute suites. Set suites explicitly
# from test/*_SUITE.erl (same names erlang.mk would use), with fallback to `bloom`.
TEST_DIR ?= $(CURDIR)/test
_test_suite_erls := $(wildcard $(TEST_DIR)/*_SUITE.erl)
CT_SUITES ?= $(if $(_test_suite_erls),$(sort $(patsubst %_SUITE.erl,%,$(notdir $(_test_suite_erls)))),bloom)

include erlang.mk
