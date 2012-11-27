DEPS=$(CURDIR)/deps

# The release branch should have a file named USE_REBAR_LOCKED
use_locked_config = $(wildcard USE_REBAR_LOCKED)
ifeq ($(use_locked_config),USE_REBAR_LOCKED)
  rebar_config = rebar.config.lock
else
  rebar_config = rebar.config
endif
REBAR = rebar -C $(rebar_config)

all: compile

compile: $(DEPS)
	@$(REBAR) compile

compile_skip:
	@$(REBAR) compile skip_deps=true

test:
	@$(REBAR) eunit skip_deps=true

# For a release-only project, this won't make much sense, but could be
# useful for release projects that have their own code
clean:
	@$(REBAR) clean skip_deps=true

allclean:
	@$(REBAR) clean

update: compile
	@cd rel/estatsd;bin/estatsd restart

distclean: relclean
	@rm -rf deps
	@$(REBAR) clean
	@rm -rf rel/apps rel/rebar.config

tags: TAGS

TAGS:
	find deps -name "*.[he]rl" -print | etags -

# Only do munge_apps if we have files in src/
all_src_files = $(wildcard src/*)
ifeq ($(strip $(all_src_files)),)
munge_apps:
	@true
else
munge_apps:
	@mkdir -p rel/apps/estatsd
	@ln -sf `pwd`/ebin rel/apps/estatsd
	@ln -sf `pwd`/priv rel/apps/estatsd
	@cp rebar.config rel
	@echo '{deps_dir, ["../deps"]}.' >> rel/rebar.config
endif

generate: munge_apps
	@/bin/echo 'building OTP release package for estatsd'
	@/bin/echo "using rebar as: $(REBAR)"
	@cd rel;$(REBAR) generate
	@rm -rf rel/apps rel/rebar.config

rel: rel/estatsd

devrel: rel
	@/bin/echo -n Symlinking deps and apps into release
	@$(foreach dep,$(wildcard deps/*), /bin/echo -n .;rm -rf rel/estatsd/lib/$(shell basename $(dep))-* \
	   && ln -sf $(abspath $(dep)) rel/estatsd/lib;)
	@rm -rf rel/estatsd/lib/estatsd-*;mkdir -p rel/estatsd/lib/estatsd
	@ln -sf `pwd`/ebin rel/estatsd/lib/estatsd
	@ln -sf `pwd`/priv rel/estatsd/lib/estatsd
	@/bin/echo done.
	@/bin/echo  Run \'make update\' to pick up changes in a running VM.

rel/estatsd: compile generate

relclean:
	@rm -rf rel/estatsd

$(DEPS):
	@echo "Fetching deps as: $(REBAR)"
	@$(REBAR) get-deps

prepare_release: distclean unlocked_deps rel lock_deps
	@echo 'release prepared, bumping version'
	@touch USE_REBAR_LOCKED
	@$(REBAR) bump-rel-version

unlocked_deps:
	@echo 'Fetching deps as: rebar -C rebar.config'
	@rebar -C rebar.config get-deps

lock_deps:
	@rebar lock-deps skip_deps=true

.PHONY: distclean prepare_release lock_deps unlocked_deps update clean compile compile_skip allclean tags relclean devrel rel relclean generate munge_apps test
