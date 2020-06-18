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
               , slots_maps = []
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
                options = Options,
                slots_maps = create_slots_maps(Ports)}}.

handle_cast(_, State) ->
    {noreply, State}.

handle_call(cluster_slots, _From, #state{slots_maps = SlotsMaps} = State) ->
    ?LOG("Handling a CLUSTER SLOTS request"),
    M = create_cluster_slots_resp(SlotsMaps),
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

%% Internal

create_slots_maps(Ports) ->
    Ports2 = lists:flatten(to_2tuple(Ports)),
    SlotsMaps = [#slots_map{master = #node{id = generate_id(),
                                           address = <<"127.0.0.1">>,
                                           port = MasterPort},
                            slave = #node{id = generate_id(),
                                          address = <<"127.0.0.1">>,
                                          port = SlavePort}
                           } || {MasterPort, SlavePort} <- Ports2],
    distribute_slots(SlotsMaps).

to_2tuple([M, S | Rest]) ->
    [{M, S}, to_2tuple(Rest)];
to_2tuple([]) ->
    [].

distribute_slots(SlotsMaps) ->
    SlotsPerNode = ?HASH_SLOTS / length(SlotsMaps), %% Calc as redis-cli does, in floats
    update_slots(SlotsMaps, 0, SlotsPerNode, 0.0).

update_slots([H | []], First, _SlotsPerNode, _Cursor) ->
    %% Last master takes remaining slots
    [H#slots_map{start_slot = First, end_slot = ?HASH_SLOTS-1}];
update_slots([H | T], First, SlotsPerNode, Cursor) ->
    Last = round(Cursor + SlotsPerNode - 1),
    [H#slots_map{start_slot = First, end_slot = Last}]
        ++ update_slots(T, Last + 1, SlotsPerNode, Cursor + SlotsPerNode).

generate_id() ->
    Bits160 = crypto:hash(sha, integer_to_list(rand:uniform(20))),
    list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                      X <- binary_to_list(Bits160)]).

create_cluster_slots_resp(SlotsMaps) ->
    [[SlotsMap#slots_map.start_slot,
      SlotsMap#slots_map.end_slot,
      [SlotsMap#slots_map.master#node.address,
       SlotsMap#slots_map.master#node.port,
       SlotsMap#slots_map.master#node.id],
      [SlotsMap#slots_map.slave#node.address,
       SlotsMap#slots_map.slave#node.port,
       SlotsMap#slots_map.slave#node.id]] || SlotsMap <- SlotsMaps].
