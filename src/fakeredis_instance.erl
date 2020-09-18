-module(fakeredis_instance).
-behaviour(gen_server).

-include("fakeredis_common.hrl").

-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2, code_change/3]).

-record(state, { socket,
                 transport,
                 local_address,
                 local_port,
                 remote_address,
                 remote_port,
                 delay,                         % Option {delay, Milliseconds},
                                                % default 0, affects GET and SET
                 parser_state
               }).

start_link(ListenSocket, Options) ->
    proc_lib:start_link(?MODULE, init, [[ListenSocket, Options]]).

init([ListenSocket, Options]) ->
    {ok, {LocalAddress, LocalPort}} = inet:sockname(ListenSocket),
    gproc:reg({p, l, {local, LocalPort}}),
    ok = proc_lib:init_ack({ok, self()}), %% Avoid block in accept/1
    ?DBG("Listening on ~p:~p", [LocalAddress, LocalPort]),

    ?DBG("Waiting for client to accept..."),
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    {ok, {RemoteAddress, RemotePort}} = inet:peername(Socket),
    gproc:reg({n, l, {remote, RemoteAddress, RemotePort}}),

    %% TLS handshake if option given
    {Transport, ActiveSocket} = case proplists:get_value(tls, Options, []) of
                                    [] -> {tcp, Socket};
                                    TlsOptions ->
                                        inet:setopts(Socket, [{active, false}]),
                                        case ssl:handshake(Socket, TlsOptions) of
                                            {ok, TlsSocket} -> {tls, TlsSocket};
                                            {error, _} -> {tcp, Socket}
                                        end
                                end,
    setopts(ActiveSocket, Transport, [{active, once}]),
    ?DBG("Client ~p:~p using ~p accepted...", [RemoteAddress, RemotePort, Transport]),

    Delay = proplists:get_value(delay, Options, 0),
    gen_server:enter_loop(?MODULE, [], #state{socket = ActiveSocket,
                                              transport = Transport,
                                              local_address = LocalAddress,
                                              local_port = LocalPort,
                                              remote_address = RemoteAddress,
                                              remote_port = RemotePort,
                                              delay = Delay,
                                              parser_state = fakeredis_parser:init()
                                             }).

handle_cast(_, State) ->
    {noreply, State}.

handle_call(exit, _From, State) ->
    {stop, "Exit called", State};
handle_call(_E, _From, State) -> {noreply, State}.

%% TCP/TLS events
handle_info({_, Socket, Data}, #state{socket = Socket,
                                      transport = Transport} = State) ->
    ok = setopts(Socket, Transport, [{active, once}]),
    {noreply, parse_data(Data, State)};
handle_info({tcp_closed, _Socket}, State) -> {stop, normal, State};
handle_info({ssl_closed, _Socket}, State) -> {stop, normal, State};
handle_info({tcp_error, _Socket, _Reason}, State) -> {stop, normal, State};
handle_info({ssl_error, _Socket, _Reason}, State) -> {stop, normal, State};
handle_info(E, State) ->
    ?ERR("unexpected: ~p (State: ~p)", [E, State]),
    {noreply, State}.

terminate(Reason, _Tab) ->
    ?DBG("fakeredis_intance:terminate(Reason=~p)", [Reason]),
    ok.

code_change(_OldVersion, Tab, _Extra) -> {ok, Tab}.

parse_data(Data, #state{parser_state = ParserState} = State) ->
    case fakeredis_parser:parse(ParserState, Data) of
        %% Got complete request
        {ok, Value, NewParserState} ->
            handle_data(State, Value),
            State#state{parser_state = NewParserState};

        %% Got complete request, with extra data
        {ok, Value, Rest, NewParserState} ->
            handle_data(State, Value),
            parse_data(Rest, #state{parser_state = NewParserState});

        %% Parser needs more data
        {continue, NewParserState} ->
            State#state{parser_state = NewParserState};

        %% Error
        {error, unknown_response} ->
            ?ERR("Unknown data: ~p", [Data]),
            {noreply, State}
    end.

%% Make sure first command is in upper
handle_data(State, [Cmd | Data]) ->
    handle_request(State, lists:flatten([string:uppercase(Cmd), Data])).

handle_request(State, [<<"CLUSTER">> | [Type | _Rest]]) ->
    ?DBG("Requesting CLUSTER"),
    case string:uppercase(Type) of
        <<"SLOTS">> ->
            {ok, Msg} = gen_server:call(fakeredis_cluster, cluster_slots),
            send(State, Msg);
        _ ->
            ?ERR("Not handled cmd: CLUSTER ~p~n", [Type])
    end;

handle_request(State, [<<"SET">>, Key, Value | _Tail]) ->
    ?DBG("SET request for key: ~p and value: ~p", [Key, Value]),
    timer:sleep(State#state.delay),
    ets:insert(?STORAGE, {Key, Value}),
    Msg = fakeredis_encoder:encode(ok),
    send(State, Msg);

handle_request(State, [<<"GET">>, Key | _Tail]) ->
    ?DBG("GET request for key: ~p", [Key]),
    timer:sleep(State#state.delay),
    Value = case ets:lookup(?STORAGE, Key) of
                [{Key, Val}] ->
                    Val;
                [] ->
                    null_bulkstring
            end,
    Msg = fakeredis_encoder:encode(Value),
    send(State, Msg).

setopts(Socket, tcp, Opts) -> inet:setopts(Socket, Opts);
setopts(Socket, tls, Opts) ->  ssl:setopts(Socket, Opts).

send(State, Str) when State#state.transport == tls ->
    ?DBG("SEND: ~p", [Str]),
    ok = ssl:send(State#state.socket, Str),
    ssl:setopts(State#state.socket, [{active, once}]);

send(State, Str) ->
    ?DBG("SEND: ~p", [Str]),
    ok = gen_tcp:send(State#state.socket, Str),
    inet:setopts(State#state.socket, [{active, once}]).
