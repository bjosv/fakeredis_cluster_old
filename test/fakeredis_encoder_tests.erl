-module(fakeredis_encoder_tests).

-include_lib("eunit/include/eunit.hrl").

-import(fakeredis_encoder, [encode/1]).

encode_number_test() ->
    T = 5,
    Result = lists:flatten(encode(T)),
    ?assertEqual(":5\r\n", Result).

encode_array_of_number_test() ->
    T = [5],
    Result = lists:flatten(encode(T)),
    ?assertEqual("*1\r\n:5\r\n", Result).

encode_empty_array_test() ->
    T = [],
    Result = lists:flatten(encode(T)),
    ?assertEqual("*0\r\n", Result).

%% encode_nullarray_test() ->

encode_array_of_many_numbers_test() ->
    T = [5, 6, 7],
    Result = lists:flatten(encode(T)),
    ?assertEqual("*3\r\n:5\r\n:6\r\n:7\r\n", Result).

encode_array_of_array_test() ->
    T = [[5]],
    Result = lists:flatten(encode(T)),
    ?assertEqual("*1\r\n*1\r\n:5\r\n", Result).

encode_array_of_many_arrays_test() ->
    T = [[5], [6], [7]],
    Result = lists:flatten(encode(T)),
    ?assertEqual("*3\r\n*1\r\n:5\r\n*1\r\n:6\r\n*1\r\n:7\r\n", Result).

encode_bulkstring_test() ->
    T = <<"foobar">>,
    Result = lists:flatten(encode(T)),
    ?assertEqual("$6\r\nfoobar\r\n", Result).

encode_cluster_nodes_test() ->
    T = [[    0,  5460, [<<"127.0.0.1">>, 30004, <<"d761377cea3f01b5e6ff6e51fa02d96f5cacf674">>],
                        [<<"127.0.0.1">>, 30001, <<"f6198a1311df6e5b64ddd0e80e465dfab43f0b21">>]],
         [ 5461, 10922, [<<"127.0.0.1">>, 30005, <<"5aad0b87b6e8e4e0d5849da7d0d7d5b58554b9ab">>],
                        [<<"127.0.0.1">>, 30002, <<"8ad9430f62e6a707c95ff7cc309d68a7a8f1cafa">>]],
         [10923, 16383, [<<"127.0.0.1">>, 30006, <<"66d36985f1d25af2a0493e3161d312cecc174397">>],
                        [<<"127.0.0.1">>, 30003, <<"83b210fd405fcd09098033b58524c91d9bbd51a8">>]]],
    %% Message from a real Redis Cluster setup
    Msg = "*3\r\n*4\r\n:0\r\n:5460\r\n*3\r\n$9\r\n127.0.0.1\r\n:30004\r\n$40\r\nd761377cea3f01b5e6ff6e51fa02d96f5cacf674\r\n*3\r\n$9\r\n127.0.0.1\r\n:30001\r\n$40\r\nf6198a1311df6e5b64ddd0e80e465dfab43f0b21\r\n*4\r\n:5461\r\n:10922\r\n*3\r\n$9\r\n127.0.0.1\r\n:30005\r\n$40\r\n5aad0b87b6e8e4e0d5849da7d0d7d5b58554b9ab\r\n*3\r\n$9\r\n127.0.0.1\r\n:30002\r\n$40\r\n8ad9430f62e6a707c95ff7cc309d68a7a8f1cafa\r\n*4\r\n:10923\r\n:16383\r\n*3\r\n$9\r\n127.0.0.1\r\n:30006\r\n$40\r\n66d36985f1d25af2a0493e3161d312cecc174397\r\n*3\r\n$9\r\n127.0.0.1\r\n:30003\r\n$40\r\n83b210fd405fcd09098033b58524c91d9bbd51a8\r\n",
    Result = lists:flatten(encode(T)),
    ?assertEqual(Msg, Result).
