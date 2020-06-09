-module(fakeredis_instance).
-behaviour(gen_server).

-include("fakeredis_common.hrl").

-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2, code_change/3]).

-record(state, { socket,
                 local_address,
                 local_port,
                 parser_state
               }).

start_link(ListenSocket) ->
    gen_server:start_link(?MODULE, ListenSocket, []).

init(ListenSocket) ->
    %% Start accepting requests, cast this as it blocks it.
    gen_server:cast(self(), accept),
    {ok, {Address, Port}} = inet:sockname(ListenSocket),
    logger:debug("Listening on ~p:~p", [Address, Port]),
    gproc:reg({p, l, {local, Port}}),
    {ok, #state{socket = ListenSocket,
                local_address = Address,
                local_port = Port,
                parser_state = eredis_parser:init()
               }}.

handle_cast(accept, State = #state{socket=ListenSocket}) ->
    logger:debug("Waiting for client to accept..."),
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    {ok, {Address, Port}} = inet:peername(AcceptSocket),
    logger:debug("Client ~p:~p accepted...", [Address, Port]),
    gproc:reg({n, l, {remote, Address, Port}}),
    {noreply, State#state{socket=AcceptSocket}};
handle_cast(_, State) ->
    {noreply, State}.

handle_call(exit, _From, State) ->
    {stop, "Exit called", State};
handle_call(_E, _From, State) -> {noreply, State}.

%% Connection calls
handle_info({tcp, Socket, Data}, #state{socket = Socket} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, parse_data(Data, State)};
handle_info({tcp_closed, _Socket}, State) -> {stop, normal, State};
handle_info({tcp_error, _Socket, _}, State) -> {stop, normal, State};
handle_info(E, State) ->
    logger:error("unexpected: ~p", [E]),
    {noreply, State}.

terminate(_Reason, _Tab) ->
    logger:debug("fakeredis_intance terminated"),
    ok.
code_change(_OldVersion, Tab, _Extra) -> {ok, Tab}.

parse_data(Data, #state{parser_state = ParserState} = State) ->
    case eredis_parser:parse(ParserState, Data) of
        %% Got complete request
        {ok, Value, NewParserState} ->
            handle_request(State, Value),
            State#state{parser_state = NewParserState};

        %% Got complete request, with extra data
        {ok, Value, Rest, NewParserState} ->
            handle_request(State, Value),
            parse_data(Rest, #state{parser_state = NewParserState});

        %% Parser needs more data
        {continue, NewParserState} ->
            State#state{parser_state = NewParserState};

        %% Error
        {error, unknown_response} ->
            logger:error("Unknown data: ~p", [Data]),
            {noreply, State}
    end.

handle_request(#state{socket = Socket}, [<<"CLUSTER">>, <<"SLOTS">>]) ->
    logger:debug("Requesting CLUSTER SLOTS"),
    {ok, Msg} = gen_server:call(fakeredis_cluster, cluster_slots),
    send(Socket, Msg);
handle_request(#state{socket = Socket}, [<<"SET">>, Key, Value | _Tail]) ->
    logger:debug("SET request for key: ~p and value: ~p", [Key, Value]),
    ets:insert(?STORAGE, {Key, Value}),
    Msg = fakeredis_encoder:encode(ok),
    send(Socket, Msg);
handle_request(#state{socket = Socket}, [<<"GET">>, Key | _Tail]) ->
    logger:debug("GET request for key: ~p", [Key]),
    Value = case ets:lookup(?STORAGE, Key) of
                [{Key, Val}] ->
                    Val;
                [] ->
                    null_bulkstring
            end,
    Msg = fakeredis_encoder:encode(Value),
    send(Socket, Msg).

send(Socket, Str) ->
    logger:debug("SEND: ~p", [Str]),
    ok = gen_tcp:send(Socket, Str),
    ok = inet:setopts(Socket, [{active, once}]),
    ok.




