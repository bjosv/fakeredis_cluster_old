-module(fakeredis_instance_sup).
-behaviour(supervisor).

-include("fakeredis_common.hrl").

%% API
-export([start_link/3]).

%% Supervisor callback
-export([init/1]).

-define(SEND_TIMEOUT, 5000).
-define(TCP_OPTIONS, [binary,
                      {active, once},
                      {packet, raw},
                      {reuseaddr, true},
                      {keepalive, false},
                      {send_timeout, ?SEND_TIMEOUT}]).

-define(TLS_OPTIONS, [{fail_if_no_peer_cert, true},
                      {verify, verify_peer},
                      {server_name_indication, "CHANGE ME"}]).


-define(SERVER(Port), {via, gproc, {n, l, {?MODULE, Port}}}).

start_link(Port, Options, MaxClients) ->
    supervisor:start_link(?SERVER(Port), ?MODULE, [Port, Options, MaxClients]).

init([Port, Options, MaxClients]) ->
    ?LOG("Start listening on port=~p [Options=~p MaxClients=~p]", [Port, Options, MaxClients]),
    {ok, ListenSocket} = gen_tcp:listen(Port, ?TCP_OPTIONS),

    spawn_link(fun() -> start_listeners(Port, MaxClients) end),

    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [{fakeredis_instance,
                   {fakeredis_instance, start_link, [ListenSocket, Options]},
                   temporary, infinity, worker, [fakeredis_instance]}],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions

start_listener(Port) ->
    supervisor:start_child(?SERVER(Port), []).

start_listeners(Port, N) ->
  [start_listener(Port) || _ <- lists:seq(1, N)],
  ok.
