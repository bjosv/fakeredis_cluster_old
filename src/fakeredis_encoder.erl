-module(fakeredis_encoder).

-define(NL, "\r\n").

-export([encode/1]).

encode(T) when is_list(T) ->
    ["*", integer_to_list(length(T)), ?NL,
     [encode(E) || E <- T]];

encode(T) when is_integer(T) ->
    [":", integer_to_list(T), ?NL];

%% This can probably be improved
encode(T) when is_bitstring(T) ->
    TL = binary_to_list(T),
    ["$", integer_to_list(length(TL)), ?NL, TL, ?NL].
