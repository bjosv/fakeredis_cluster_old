%%
%% Parser of the Redis protocol, see http://redis.io/topics/protocol
%%
%% From eredis:eredis_parser.erl
%%
-module(fakeredis_parser).

-export([init/0, parse/2]).

-type parser_state() :: status_continue | error_continue | bulk_continue | array_continue.

-record(state, {
                state = undefined :: parser_state() | undefined,
                continuation_data :: any() | undefined
               }).

-define(NL, "\r\n").

init() -> #state{}.

parse(#state{state = undefined} = State, NewData) ->
    %% First byte specifies type
    case NewData of
        %% Simple strings
        <<$+, Data/binary>> ->
            return_result(parse_simple(Data), State, status_continue);

        %% Errors
        <<$-, Data/binary>> ->
            return_error(parse_simple(Data), State, error_continue);

        %% Integers
        <<$:, Data/binary>> ->
            return_result(parse_simple(Data), State, status_continue);

        %% Bulk Strings
        <<$$, _Rest/binary>> ->
            return_result(parse_bulk(NewData), State, bulk_continue);

        %% Arrays
        <<$*, _Rest/binary>> ->
            return_result(parse_array(NewData), State, array_continue);

        _ ->
            {error, unknown_response}
    end;

%% Match continuation states

parse(#state{state = bulk_continue,
             continuation_data = ContinuationData} = State, NewData) ->
    return_result(parse_bulk(ContinuationData, NewData), State, bulk_continue);

parse(#state{state = array_continue,
             continuation_data = ContinuationData} = State, NewData) ->
    return_result(parse_array(ContinuationData, NewData), State, array_continue);

parse(#state{state = status_continue,
             continuation_data = ContinuationData} = State, NewData) ->
    return_result(parse_simple(ContinuationData, NewData), State, status_continue);

parse(#state{state = error_continue,
             continuation_data = ContinuationData} = State, NewData) ->
    return_error(parse_simple(ContinuationData, NewData), State, error_continue).

%%
%% Simple strings
%% Data is without type character, like '+', '-', ':'.
%% Data needs to be terminated by \r\n
%%
parse_simple(Data) when is_binary(Data) -> parse_simple(buffer_create(Data));

parse_simple(Buffer) ->
    case get_newline_pos(Buffer) of
        undefined ->
            {continue, {incomplete_simple, Buffer}};
        NewlinePos ->
            <<Value:NewlinePos/binary, ?NL, Rest/binary>> = buffer_to_binary(Buffer),
            {ok, Value, Rest}
    end.

parse_simple({incomplete_simple, Buffer}, NewData0) ->
    NewBuffer = buffer_append(Buffer, NewData0),
    parse_simple(NewBuffer).

%%
%% Bulk strings
%% Data needs to include the type,
%% like $ in: $<Bytes>CRLF<string>CRLF.
%%
parse_bulk(Data) when is_binary(Data) -> parse_bulk(buffer_create(Data));

parse_bulk(Buffer) ->
    case buffer_hd(Buffer) of
        [$*] -> parse_array(Buffer);
        [$+] -> parse_simple(buffer_tl(Buffer));
        [$-] -> parse_simple(buffer_tl(Buffer));
        [$:] -> parse_simple(buffer_tl(Buffer));
        [$$] -> do_parse_bulk(Buffer)
    end.

%% Bulk string
do_parse_bulk(Buffer) ->
    %% Find the position of the first terminator, everything up until
    %% this point contains the size specifier. If we cannot find it,
    %% we received a partial message and need more data
    case get_newline_pos(Buffer) of
        undefined ->
            {continue, {incomplete_size, Buffer}};
        NewlinePos ->
            OffsetNewlinePos = NewlinePos - 1, % Take into account the first $
            <<$$, Size:OffsetNewlinePos/binary, Bulk/binary>> = buffer_to_binary(Buffer),
            IntSize = list_to_integer(binary_to_list(Size)),

            if
                %% Nil response from redis
                IntSize =:= -1 ->
                    <<?NL, Rest/binary>> = Bulk,
                    {ok, undefined, Rest};
                %% We have enough data for the entire bulk
                size(Bulk) - (size(<<?NL>>) * 2) >= IntSize ->
                    <<?NL, Value:IntSize/binary, ?NL, Rest/binary>> = Bulk,
                    {ok, Value, Rest};
                true ->
                    %% Need more data, so we send the bulk without the
                    %% size specifier to our future self
                    {continue, {IntSize, buffer_create(Bulk)}}
            end
    end.

%% Bulk, continuation from partial bulk size
parse_bulk({incomplete_size, Buffer}, NewData) ->
    NewBuffer = buffer_append(Buffer, NewData),
    parse_bulk(NewBuffer);

%% Bulk, continuation from partial bulk value
parse_bulk({IntSize, Buffer0}, Data) ->
    Buffer = buffer_append(Buffer0, Data),

    case buffer_size(Buffer) - (size(<<?NL>>) * 2) >= IntSize of
        true ->
            <<?NL, Value:IntSize/binary, ?NL, Rest/binary>> = buffer_to_binary(Buffer),
            {ok, Value, Rest};
        false ->
            {continue, {IntSize, Buffer}}
    end.


%%
%% Arrays
%% Can contain multiple other types, like bulk strings, simple types..
%%
parse_array(Data) when is_binary(Data) -> parse_array(buffer_create(Data));

parse_array(Buffer) ->
    case get_newline_pos(Buffer) of
        undefined ->
            {continue, {incomplete_size, Buffer}};
        NewlinePos ->
            OffsetNewlinePos = NewlinePos - 1,
            <<$*, Size:OffsetNewlinePos/binary, ?NL, Bulk/binary>> = buffer_to_binary(Buffer),
            IntSize = list_to_integer(binary_to_list(Size)),

            do_parse_array(IntSize, buffer_create(Bulk))
    end.

%% Size of array was incomplete, try again
parse_array({incomplete_size, Buffer}, NewData0) ->
    NewBuffer = buffer_append(Buffer, NewData0),
    parse_array(NewBuffer);

%% Ran out of data inside do_parse_array in parse_bulk, must
%% continue traversing the bulks
parse_array({in_parsing_bulks, Count, Buffer, Acc},
            NewData0) ->
    NewBuffer = buffer_append(Buffer, NewData0),

    %% Continue where we left off
    do_parse_array(Count, NewBuffer, Acc).

%% @doc: Parses the given number of bulks from Data. If Data does not
%% contain enough bulks, {continue, ContinuationData} is returned with
%% enough information to start parsing with the correct count and
%% accumulated data.
do_parse_array(Count, Buffer) ->
    do_parse_array(Count, Buffer, []).

do_parse_array(-1, Buffer, []) ->
    {ok, undefined, buffer_to_binary(Buffer)};
do_parse_array(0, Buffer, Acc) ->
    {ok, lists:reverse(Acc), buffer_to_binary(Buffer)};
do_parse_array(Count, Buffer, Acc) ->
    case buffer_size(Buffer) == 0 of
        true -> {continue, {in_parsing_bulks, Count, buffer_create(), Acc}};
        false ->
            %% Try parsing the first bulk in Data, if it works, we get the
            %% extra data back that was not part of the bulk which we can
            %% recurse on.  If the bulk does not contain enough data, we
            %% return with a continuation and enough data to pick up where we
            %% left off. In the continuation we will get more data
            %% automagically in Data, so parsing the bulk might work.
            case parse_bulk(Buffer) of
                {ok, Value, Rest} ->
                    do_parse_array(Count - 1, buffer_create(Rest), [Value | Acc]);
                {continue, _} ->
                    {continue, {in_parsing_bulks, Count, Buffer, Acc}}
            end
    end.

%%
%% Helpers
%%
get_newline_pos({B, _}) ->
    case re:run(B, ?NL) of
        {match, [{Pos, _}]} -> Pos;
        nomatch -> undefined
    end.


%% Buffer

buffer_create() ->
    {[], 0}.

buffer_create(Data) ->
    {[Data], byte_size(Data)}.

buffer_append({List, Size}, Binary) ->
    NewList = case List of
                  [] -> [Binary];
                  [Head | Tail] -> [Head, Tail, Binary]
              end,
    {NewList, Size + byte_size(Binary)}.

buffer_hd({[<<Char, _/binary>> | _], _}) -> [Char].

buffer_tl({[<<_, RestBin/binary>> | Rest], Size}) -> {[RestBin | Rest], Size - 1}.

buffer_to_binary({List, _}) -> iolist_to_binary(List).

buffer_size({_, Size}) -> Size.


%% Result handling

return_result({ok, Value, <<>>}, _State, _StateName) ->
    {ok, Value, init()};
return_result({ok, Value, Rest}, _State, _StateName) ->
    {ok, Value, Rest, init()};
return_result({continue, ContinuationData}, State, StateName) ->
    {continue, State#state{state = StateName, continuation_data = ContinuationData}}.

return_error(Result, State, StateName) ->
    case return_result(Result, State, StateName) of
        {ok, Value, ParserState} ->
            {error, Value, ParserState};
        {ok, Value, Rest, ParserState} ->
            {error, Value, Rest, ParserState};
        Res ->
            Res
    end.
