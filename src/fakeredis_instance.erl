
-module(fakeredis_instance).
-behaviour(gen_server).

-export([start_link/1]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2, code_change/3]).

-record(state, { socket,
                 parser_state
               }).

start_link(ListenSocket) ->
    gen_server:start_link(?MODULE, ListenSocket, []).

init(ListenSocket) ->
    %% Start accepting requests, cast this as it blocks it.
    gen_server:cast(self(), accept),
    {ok, {Address, Port}} = inet:sockname(ListenSocket),
    io:format(user, "Listening on ~p:~p~n", [Address, Port]),
    gproc:reg({p, l, {local, Port}}),
    {ok, #state{socket = ListenSocket,
                parser_state = eredis_parser:init()
               }}.

handle_cast(accept, State = #state{socket=ListenSocket}) ->
    io:format(user, "Waiting for client to accept...~n", []),
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    {ok, {Address, Port}} = inet:peername(AcceptSocket),
    io:format(user, "Client ~p:~p accepted...~n", [Address, Port]),
    gproc:reg({n, l, {remote, Address, Port}}),
    {noreply, State#state{socket=AcceptSocket}};
handle_cast(_, State) ->
    {noreply, State}.

handle_call(exit, _From, State) ->
    {stop, "Exit called", State};
handle_call(_E, _From, State) -> {noreply, State}.

handle_info({tcp, Socket, Data}, #state{socket = Socket} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, handle_data(Data, State)};
handle_info({tcp_closed, _Socket}, State) -> {stop, normal, State};
handle_info({tcp_error, _Socket, _}, State) -> {stop, normal, State};
handle_info(E, State) ->
    io:format(user, "unexpected: ~p~n", [E]),
    {noreply, State}.

terminate(_Reason, _Tab) ->
    io:format(user, "gen_server terminated~n", []),
    ok.
code_change(_OldVersion, Tab, _Extra) -> {ok, Tab}.


handle_data(Data, #state{parser_state = ParserState} = State) ->
    case eredis_parser:parse(ParserState, Data) of
        %% Got complete request
        {ok, Value, NewParserState} ->
            handle_request(State, Value),
            State#state{parser_state = NewParserState};

        %% Got complete request, with extra data
        {ok, Value, Rest, NewParserState} ->
            handle_request(State, Value),
            handle_data(Rest, #state{parser_state = NewParserState});

        %% Parser needs more data
        {continue, NewParserState} ->
            State#state{parser_state = NewParserState};

        %% Error
        {error, unknown_response} ->
            io:format(user, "Unknown data: ~p~n", [Data]),
            {noreply, State}
    end.

handle_request(#state{socket = Socket}, Value) ->
    io:format(user, "Got request: ~p~n", [Value]),
    {ok, Msg} = gen_server:call(fakeredis_cluster, cluster_slots),
    send(Socket, Msg).

send(Socket, Str) ->
    ok = gen_tcp:send(Socket, Str),
    ok = inet:setopts(Socket, [{active, once}]),
    ok.




