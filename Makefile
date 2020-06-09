.PHONY: all compile clean test ut ct xref dialyzer elvis cover coverview edoc

REBAR ?= rebar3

all: compile xref dialyzer elvis

compile:
	@$(REBAR) compile

clean:
	@$(REBAR) clean
	@rm -rf _build

test: ut ct

ut:
	@$(REBAR) eunit -v --cover_export_name ut

ct:
	@$(REBAR) ct -v --cover_export_name ct

xref:
	@$(REBAR) xref

dialyzer:
	@$(REBAR) dialyzer

elvis:
	@elvis rock

cover:
	@$(REBAR) cover -v

coverview: cover
	xdg-open _build/test/cover/index.html

edoc:
	@$(REBAR) skip_deps=true doc
