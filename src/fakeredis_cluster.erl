-module(fakeredis_cluster).
-behaviour(gen_server).

-include("fakeredis_common.hrl").

-export([start_link/1, start_link/2, start_link/3]).

-export([ start_instance/1
        , kill_instance/1
        ]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2, code_change/3]).

-record(state, { ports = []
               , max_clients = 0
               , options = []
               }).

-define(DEFAULT_MAX_CLIENTS, 10).

%% API

start_link(Ports) ->
    start_link(Ports, [], ?DEFAULT_MAX_CLIENTS).

start_link(Ports, Options) ->
    start_link(Ports, Options, ?DEFAULT_MAX_CLIENTS).

start_link(Ports, Options, MaxClients) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Ports, Options, MaxClients], []).

start_instance(Port) ->
    gen_server:call(?MODULE, {start_instance, Port}).

kill_instance(Port) ->
    gen_server:call(?MODULE, {kill_instance, Port}).

%% Internals

init([Ports, Options, MaxClients]) ->
    ets:new(?STORAGE, [public, set, named_table, {read_concurrency, true}]),
    [fakeredis_instance_sup:start_link(Port, Options, MaxClients) || Port <- Ports],
    {ok, #state{ports = Ports,
                max_clients = MaxClients,
                options = Options}}.

handle_cast(_, State) ->
    {noreply, State}.

handle_call(cluster_slots, _From, #state{ports = Ports} = State) ->
    ?LOG("Handling a CLUSTER SLOTS request"),
    M = [[    0,  5460, [<<"127.0.0.1">>, lists:nth(1, Ports),
                         <<"d761377cea3f01b5e6ff6e51fa02d96f5cacf674">>],
                        [<<"127.0.0.1">>, lists:nth(2, Ports),
                         <<"f6198a1311df6e5b64ddd0e80e465dfab43f0b21">>]],
         [ 5461, 10922, [<<"127.0.0.1">>, lists:nth(3, Ports),
                         <<"5aad0b87b6e8e4e0d5849da7d0d7d5b58554b9ab">>],
                        [<<"127.0.0.1">>, lists:nth(4, Ports),
                         <<"8ad9430f62e6a707c95ff7cc309d68a7a8f1cafa">>]],
         [10923, 16383, [<<"127.0.0.1">>, lists:nth(5, Ports),
                         <<"66d36985f1d25af2a0493e3161d312cecc174397">>],
                        [<<"127.0.0.1">>, lists:nth(6, Ports),
                         <<"83b210fd405fcd09098033b58524c91d9bbd51a8">>]]],
    Msg = fakeredis_encoder:encode(M),
    {reply, {ok, Msg}, State};
handle_call({start_instance, Port}, _From, #state{max_clients = MaxClients} = State) ->
    ?LOG("start_instance: ~p", [Port]),
    fakeredis_instance_sup:start_listeners(Port, MaxClients),
    {reply, ok, State};
handle_call({kill_instance, Port}, _From, State) ->
    ?LOG("kill_instance: ~p", [Port]),
    Pids = gproc:lookup_pids({p, l, {local, Port}}),
    ?LOG("Stop port=~p, pids=~p", [Port, Pids]),
    [exit(Pid, kill) || Pid <- Pids],
    {reply, ok, State};
handle_call(_E, _From, State) ->
    {noreply, State}.

handle_info(_E, State) -> {noreply, State}.

terminate(_Reason, _Tab) -> ok.

code_change(_OldVersion, Tab, _Extra) -> {ok, Tab}.
