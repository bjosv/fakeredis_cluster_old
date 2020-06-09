-module(fakeredis_cluster_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , all/0
        , suite/0
        ]).

%% Test cases
-export([ t_cluster_slots/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(NL, "\r\n").

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(fakeredis_cluster),
    Config.

end_per_suite(_Config) ->
    application:stop(fakeredis_cluster),
    ok.

all() -> [F || {F, _A} <- module_info(exports),
               case atom_to_list(F) of
                   "t_" ++ _ -> true;
                   _         -> false
               end].

suite() -> [{timetrap, {minutes, 5}}].

%% Test

t_cluster_slots(Config) when is_list(Config) ->
    fakeredis_cluster:start_link([30001, 30002, 30003, 30004, 30005, 30006]),
    {ok, Sock} = gen_tcp:connect("localhost", 30001,
                                 [binary, {active , false}, {packet, 0}]),
    ok = gen_tcp:send(Sock, ["*2", ?NL, "$7", ?NL, "CLUSTER", ?NL, "$5", ?NL, "SLOTS", ?NL]),
    {ok, _Data} = gen_tcp:recv(Sock, 0),
    ok = gen_tcp:close(Sock).
