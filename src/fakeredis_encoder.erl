-module(fakeredis_encoder).

-export([encode/1]).
-define(NL, "\r\n").

%% See https://redis.io/topics/protocol

%% RESP Integers
encode(T) when is_integer(T) ->
    [":", integer_to_list(T), ?NL];

%% RESP Bulk Strings (This can probably be improved)
encode(T) when is_bitstring(T) ->
    TL = binary_to_list(T),
    ["$", integer_to_list(length(TL)), ?NL, TL, ?NL];

%% RESP Null Bulk Strings
encode(null_bulkstring) ->
    ["$-1", ?NL];

%% RESP Arrays
encode(T) when is_list(T) ->
    ["*", integer_to_list(length(T)), ?NL,
     [encode(E) || E <- T]];

%% RESP Null Arrays
encode(null_array) ->
    ["*-1", ?NL];

%% RESP Simple Strings
encode(T) when is_atom(T) ->
    ["+", string:uppercase(atom_to_list(T)), ?NL].

%% RESP Errors: TODO
