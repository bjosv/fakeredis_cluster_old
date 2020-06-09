%% ets table to use
-define(STORAGE, fakeredis_cluster_storage).

-ifdef(TEST).
%% Seems to give too long lines:
%% -define(LOG(X),    io:format(user, "[~p:L~p] " ++ X ++ "~n", [?MODULE, ?LINE])).
%% -define(LOG(X, V), io:format(user, "[~p:L~p] " ++ X ++ "~n", [?MODULE, ?LINE]++V)).
-define(DBG(X),    io:format(user, X ++ "~n", [])).
-define(DBG(X, V), io:format(user, X ++ "~n", V)).
-define(LOG(X),    io:format(user, X ++ "~n", [])).
-define(LOG(X, V), io:format(user, X ++ "~n", V)).
-define(ERR(X),    io:format(user, "Error:" ++ X ++ "~n", [])).
-define(ERR(X, V), io:format(user, "Error:" ++ X ++ "~n", V)).

-else.

-define(DBG(X),    logger:debug("[~p:L~p] " ++ X ++ "~n", [?MODULE, ?LINE])).
-define(DBG(X, V), logger:debug("[~p:L~p] " ++ X ++ "~n", [?MODULE, ?LINE]++V)).
-define(LOG(X),    logger:notice("[~p:L~p] " ++ X ++ "~n", [?MODULE, ?LINE])).
-define(LOG(X, V), logger:notice("[~p:L~p] " ++ X ++ "~n", [?MODULE, ?LINE]++V)).
-define(ERR(X),    logger:error(X ++ "~n", [])).
-define(ERR(X, V), logger:error(X ++ "~n", V)).
-endif.
