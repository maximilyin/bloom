PROJECT = bloom
PROJECT_DESCRIPTION = Wrapper over gun http client
PROJECT_VERSION = $(PROJECT_BUILD_TAG)

DIALYZER_OPTS = -Wunmatched_returns

DEPS = gun
dep_gun = git https://github.com/ninenines/gun.git 2.6.0

TEST_DIR ?= $(CURDIR)/test
_test_suite_erls := $(wildcard $(TEST_DIR)/*_SUITE.erl)
CT_SUITES ?= $(if $(_test_suite_erls),$(sort $(patsubst %_SUITE.erl,%,$(notdir $(_test_suite_erls)))),bloom)

include erlang.mk
