-module(fakeredis_instance_sup).
-behaviour(supervisor).

-include("fakeredis_common.hrl").

%% API
-export([start_link/3]).
-export([start_acceptors/2]).

%% Supervisor callback
-export([init/1]).

-define(SEND_TIMEOUT, 5000).
-define(TCP_OPTIONS, [binary,
                      {active, once},
                      {packet, raw},
                      {reuseaddr, true},
                      {keepalive, false},
                      {send_timeout, ?SEND_TIMEOUT}]).

-define(SERVER(Port), {via, gproc, {n, l, {?MODULE, Port}}}).

start_link(Port, Options, MaxClients) ->
    supervisor:start_link(?SERVER(Port), ?MODULE, [Port, Options, MaxClients]).

init([Port, Options, MaxClients]) ->
    ?LOG("Start listening on port=~p [Options=~p MaxClients=~p]", [Port, Options, MaxClients]),
    ListenSocket = start_listen_socket(Port, ?TCP_OPTIONS),

    %% Make sure the listen socket is properly closed when this
    %% supervisor exits. Otherwise, we may get {error,eaddrinuse} if
    %% we try listening on this port again later.
    spawn_link(fun() ->
                       process_flag(trap_exit, true),
                       receive {'EXIT', _, _} ->
                               gen_tcp:close(ListenSocket)
                       end
               end),

    spawn_link(fun() -> start_acceptors(Port, MaxClients) end),

    SupFlags = #{strategy => simple_one_for_one,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [{fakeredis_instance,
                   {fakeredis_instance, start_link, [ListenSocket, Options]},
                   temporary, infinity, worker, [fakeredis_instance]}],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
start_listen_socket(Port, Options) ->
    case gen_tcp:listen(Port, Options) of
        {ok, ListenSocket} -> ListenSocket;
        {error, Reason} -> exit({listen_err, Reason})
    end.

start_acceptor(Port) ->
    supervisor:start_child(?SERVER(Port), []).

start_acceptors(Port, N) ->
    case proplists:get_value(active, supervisor:count_children(?SERVER(Port))) of
        A when N>A ->
            ?DBG("Starting ~p acceptors on port ~p (N:~p, Active:~p)",
                 [Port, N-A, N, A]),
            [start_acceptor(Port) || _ <- lists:seq(1, N-A)];
        A ->
            ?ERR("Max amount of acceptors already started for port ~p (N:~p, Active:~p)",
                 [Port, N, A])
    end,
    ok.
