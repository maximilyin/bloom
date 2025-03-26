PROJECT = bloom
PROJECT_DESCRIPTION = Wrapper over gun http client
PROJECT_VERSION = $(PROJECT_BUILD_TAG)

DIALYZER_OPTS = -Wunmatched_returns

DEPS = gun
dep_gun = git https://github.com/ninenines/gun.git  2.1.0

include erlang.mk
