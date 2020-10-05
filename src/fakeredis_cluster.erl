-module(fakeredis_cluster).
-behaviour(gen_server).

-include("fakeredis_common.hrl").

-export([start_link/1, start_link/2, start_link/3]).
-export([main/1]). %% escript

-export([ start_instance/1
        , kill_instance/1
        , get_redirect_by_key/1
        , get_node_by_slot/1
        , move_all_slots/0
        , set_ask_redirect/3
        , delete_ask_redirect/1
        , get_event_log/0
        , clear_event_log/0
        ]).

%% Internal, called by e.g. fakeredis_instance
-export([log_event/2]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2, code_change/3]).

-type event_log_entry() :: {ConnectionId :: binary(),
                            EventType    :: connect | command | reply,
                            Data         :: any()}.

-record(state, { max_clients = 0
               , options = []
               , node_map = #{}
               , slots_maps = []
               , ask_redirects = #{} :: #{Key :: binary() => {Host :: binary(),
                                                              Port :: inet:port_number()}}
               , event_log = []      :: [event_log_entry()]
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

%% @doc Returns the node which serves a particular key. If there is an
%% ASK redirect for the key, it is returned. Otherwise a MOVED
%% redirect to the node serving the key's slot is returned.
-spec get_redirect_by_key(Key :: binary()) ->
          {Redirect :: ask | moved,
           Slot     :: 0..16383,
           Address  :: binary(),
           Port     :: inet:port_number()}.
get_redirect_by_key(Key) ->
    gen_server:call(?MODULE, {get_redirect_by_key, Key}).

%% @doc Returns the node which serves a slot.
-spec get_node_by_slot(Slot :: 0..16383) -> {Address :: binary(),
                                             Port    :: inet:port_number()}.
get_node_by_slot(Slot) ->
    gen_server:call(?MODULE, {get_node_by_slot, Slot}).

%% @doc Shifts all slot mappings so that every slot range will be
%% serverd by the node which previously served the next slot range. In
%% practive, every slot will be served by a different node. Doesn't
%% affect ASK redirects.
-spec move_all_slots() -> ok.
move_all_slots() ->
    gen_server:call(?MODULE, move_all_slots).

%% @doc Creates an ASK redirect, i.e. assign key to a different node that what normally serves the key's slot.
set_ask_redirect(Key, Address, Port) ->
    gen_server:call(?MODULE, {set_ask_redirect, Key, Address, Port}).

%% @doc Deletes an ASK redirect. This assigns the key back to the node
%% which serves the slot of the key.
delete_ask_redirect(Key) ->
    gen_server:call(?MODULE, {delete_ask_redirect, Key}).

-spec get_event_log() -> [event_log_entry()].
get_event_log() ->
    gen_server:call(?MODULE, get_event_log).

-spec clear_event_log() -> ok.
clear_event_log() ->
    gen_server:call(?MODULE, clear_event_log).

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

%% Called by fakeredis_instance
-spec log_event(connect, map())                       -> ok;
               (disconnect, normal | {error, term()}) -> ok;
               (command, [binary()])                  -> ok;
               (reply, any())                         -> ok.
log_event(EventType, Data) ->
    %% A hash of the caller's pid uniquely represents a connection
    ConnectionId = binary:part(
                     base64:encode(
                       crypto:hash(sha, term_to_binary(self()))),
                     0, 6),
    gen_server:cast(?MODULE, {log_event, {ConnectionId, EventType, Data}}).

%% gen_server callbacks

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

handle_cast({log_event, Event}, #state{event_log = Log} = State) ->
    {noreply, State#state{event_log = [Event | Log]}};
handle_cast(_, State) ->
    {noreply, State}.

handle_call(cluster_slots, _From, State) ->
    ?LOG("Handling a CLUSTER SLOTS request"),
    {reply, {ok, create_cluster_slots_resp(State)}, State};
handle_call({start_instance, Port}, _From, #state{max_clients = MaxClients} = State) ->
    ?LOG("start_instance: ~p", [Port]),
    fakeredis_instance_sup:start_acceptors(Port, MaxClients),
    {reply, ok, State};
handle_call({kill_instance, Port}, _From, State) ->
    ?LOG("kill_instance: ~p", [Port]),
    Pids = gproc:lookup_pids({p, l, {local, Port}}),
    ?LOG("Stop port=~p, pids=~p", [Port, Pids]),
    [exit(Pid, kill) || Pid <- Pids],
    {reply, ok, State};
handle_call({get_redirect_by_key, Key}, _From, State) ->
    Slot = fakeredis_hash:hash(Key),
    case State#state.ask_redirects of
        #{Key := Id} ->
            %% ASK redirect exists for this key
            #node{address = Addr,
                  port    = Port} = maps:get(Id, State#state.node_map),
            {reply, {ask, Slot, Addr, Port}, State};
        _NoAskRedirect ->
            %% Use cluster slots map
            #node{address = Addr, port = Port} = lookup_slot(Slot, State),
            {reply, {moved, Slot, Addr, Port}, State}
    end;
handle_call({get_node_by_slot, Slot}, _From, State) when Slot >= 0,
                                                         Slot < 16384 ->
    #node{address = Addr,
          port    = Port} = lookup_slot(Slot, State),
    {reply, {Addr, Port}, State};
handle_call(move_all_slots, _From,
            #state{slots_maps = M} = State) ->
    {reply, ok, State#state{slots_maps = move_all_slots(M)}};
handle_call({set_ask_redirect, Key, Addr, Port}, _From, State) ->
    Slot = fakeredis_hash:hash(Key),
    case lookup_slot(Slot, State) of
        #node{address = Addr,
              port    = Port} ->
            {reply, {error, target_same_as_source}, State};
        _SourceNode ->
            Nodes = maps:values(State#state.node_map),
            case [Id || #node{address   = Addr0,
                              port      = Port0,
                              id        = Id} <- Nodes,
                        Addr0 =:= Addr,
                        Port0 =:= Port] of
                [TargetNodeId] ->
                    NewAskRedirects = (State#state.ask_redirects)#{Key => TargetNodeId},
                    {reply, ok, State#state{ask_redirects = NewAskRedirects}};
                [] ->
                    {reply, {error, node_not_found}, State}
            end
    end;
handle_call({delete_ask_redirect, Key}, _From,
            #state{ask_redirects = AskRedirects} = State) ->
    case maps:take(Key, AskRedirects) of
        {_Value, RemainingMap} ->
            {reply, ok, State#state{ask_redirects = RemainingMap}};
        error ->
            {reply, error, State}
    end;
handle_call(get_event_log, _From, State) ->
    {reply, lists:reverse(State#state.event_log), State};
handle_call(clear_event_log, _From, State) ->
    {reply, ok, State#state{event_log = []}};
handle_call(_Req, _From, State) ->
    {reply, {error, bad_call}, State}.

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

-spec lookup_slot(Slot :: 0..16383, #state{}) -> #node{}.
lookup_slot(Slot, #state{slots_maps = SlotsMaps,
                         node_map   = NodeMap}) ->
    [Id] = [MasterId || #slots_map{start_slot = Start,
                                   end_slot   = End,
                                   master_id  = MasterId} <- SlotsMaps,
                        Start =< Slot,
                        Slot =< End],
    maps:get(Id, NodeMap).

%% Shifts the master and slave ids so that each slot range will have
%% the master and slaves of the next range in a list of #slots_maps{}.
move_all_slots(SlotsMaps) ->
    move_all_slots(SlotsMaps, hd(SlotsMaps)).

move_all_slots([This | [#slots_map{master_id = NextMasterId,
                                   slave_ids = NextSlaveIds} | _] = Rest],
               First) ->
    [This#slots_map{master_id = NextMasterId,
                    slave_ids = NextSlaveIds}
    | move_all_slots(Rest, First)];
move_all_slots([Last], #slots_map{master_id = FirstMasterId,
                                  slave_ids = FirstSlaveIds}) ->
    [Last#slots_map{master_id = FirstMasterId,
                    slave_ids = FirstSlaveIds}].

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

move_all_slots_test() ->
    ?assertEqual(
       [#slots_map{start_slot = 0,
                   end_slot = 1000,
                   master_id = <<2>>,
                   slave_ids = [<<201>>, <<202>>]},
        #slots_map{start_slot = 1001,
                   end_slot = 2000,
                   master_id = <<3>>,
                   slave_ids = undefined},
        #slots_map{start_slot = 2001,
                   end_slot = 16383,
                   master_id = <<1>>,
                   slave_ids = undefined}],
       move_all_slots([#slots_map{start_slot = 0,
                                  end_slot = 1000,
                                  master_id = <<1>>,
                                  slave_ids = undefined},
                       #slots_map{start_slot = 1001,
                                  end_slot = 2000,
                                  master_id = <<2>>,
                                  slave_ids = [<<201>>, <<202>>]},
                       #slots_map{start_slot = 2001,
                                  end_slot = 16383,
                                  master_id = <<3>>,
                                  slave_ids = undefined}])).
-endif.
