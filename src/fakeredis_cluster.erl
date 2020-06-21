-module(fakeredis_cluster).
-behaviour(gen_server).

-include("fakeredis_common.hrl").

-export([start_link/1, start_link/2, start_link/3]).
-export([main/1]). %% escript

-export([ start_instance/1
        , kill_instance/1
        ]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2, code_change/3]).

-record(state, { max_clients = 0
               , options = []
               , node_map = #{}
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

%% escript Entry point
main([]) ->
    DefaultPorts = [{20010, 20011}, {20020, 20021}, {20030, 20031}],
    main_(DefaultPorts);

main(Args) ->
    main_(Args).

main_(Ports) ->
    application:ensure_all_started(?MODULE),
    fakeredis_cluster:start_link(Ports),
    timer:sleep(infinity).

%% Internals

init([Ports, Options, MaxClients]) ->
    ets:new(?STORAGE, [public, set, named_table, {read_concurrency, true}]),
    {NodeMap, SlotsMaps0} = parse_port_args(Ports),
    SlotsMaps = distribute_slots(SlotsMaps0),
    [fakeredis_instance_sup:start_link(Node#node.port, Options, MaxClients) ||
        Node <- maps:values(NodeMap)],
    {ok, #state{max_clients = MaxClients,
                options = Options,
                node_map = NodeMap,
                slots_maps = SlotsMaps}}.

handle_cast(_, State) ->
    {noreply, State}.

handle_call(cluster_slots, _From, State) ->
    ?LOG("Handling a CLUSTER SLOTS request"),
    Msg = fakeredis_encoder:encode(create_cluster_slots_resp(State)),
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

%% [M1, M2, M3]
%% [{M1, S1}, {M2, S2}]
%% [{M1, S1, S2}, {M2, S3, S4}]
%% -> {NodeMap, SlotsMaps}
parse_port_args(ArgList) when is_list(ArgList) ->
    Parse = [parse_slots_maps_and_nodes(PortElem) || PortElem <- ArgList],
    {NodeMapList, SlotsMaps} = lists:unzip(Parse),
    NodeMap = maps:from_list(lists:flatten(NodeMapList)),
    {NodeMap, SlotsMaps}.

parse_slots_maps_and_nodes(PortElem) ->
    Nodes = parse_nodes(PortElem),
    SlotsMaps = create_slots_maps(Nodes),
    {Nodes, SlotsMaps}.

%% Parse data, create [{id, node}, ...]
parse_nodes(PortElem) when is_tuple(PortElem) ->
    PortList = [element(Pos, PortElem) || Pos <- lists:seq(1, tuple_size(PortElem))],
    {[MasterPort], Replicas} = lists:split(1, PortList),
    lists:flatten([create_node(MasterPort),
     [create_node(Port) || Port <- Replicas]]);
parse_nodes(PortElem) when is_list(PortElem) ->
    {[MasterPort], Relicas} = lists:split(1, PortElem),
    lists:flatten([create_node(MasterPort),
     [create_node(Port) || Port <- Relicas]]);
parse_nodes(PortElem) ->
    create_node(PortElem).

create_node(Port) ->
    Id = generate_id(),
    {Id, #node{id = Id,
               address = <<"127.0.0.1">>,
               port = Port}}.

%% Create a #slots_map{} from node (master only) or nodes (master + replicas)
create_slots_maps([{Id, _Node} | Replicas] = Nodes) when is_list(Nodes)->
    ReplicaIds = [ReplicaNode#node.id || {_ReplicaId, ReplicaNode} <- Replicas],
    #slots_map{master_id = Id,
               slave_ids = ReplicaIds};
create_slots_maps({Id, _Node}) ->
    #slots_map{master_id = Id}.

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
    Bits160 = crypto:hash(sha, binary_to_list(crypto:strong_rand_bytes(20))),
    list_to_binary([io_lib:format("~2.16.0b", [X]) ||
                      X <- binary_to_list(Bits160)]).

create_cluster_slots_resp(State) ->
    [[SlotsMap#slots_map.start_slot,
      SlotsMap#slots_map.end_slot,
      get_cluster_nodes(SlotsMap,
                        State#state.node_map,
                        SlotsMap#slots_map.slave_ids)] || SlotsMap <- State#state.slots_maps].

get_cluster_nodes(SlotsMap, NodeMap, undefined) ->
    Master = maps:get(SlotsMap#slots_map.master_id, NodeMap),
    [Master#node.address,
      Master#node.port,
      Master#node.id];
get_cluster_nodes(SlotsMap, NodeMap, SlaveIds) ->
    Master = maps:get(SlotsMap#slots_map.master_id, NodeMap),
    Replicas = [maps:get(NodeId, NodeMap) || NodeId <- SlaveIds],
    [[Node#node.address,
      Node#node.port,
      Node#node.id] || Node <- [Master] ++ Replicas].
