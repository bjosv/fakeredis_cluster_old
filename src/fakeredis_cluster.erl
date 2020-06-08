-module(fakeredis_cluster).
-behaviour(gen_server).

-export([start_link/1, start_link/2]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2, code_change/3]).

-record(state, { ports
               }).

-define(DEFAULT_MAX_CLIENTS, 10).

start_link(Ports) ->
    start_link(Ports, ?DEFAULT_MAX_CLIENTS).

start_link(Ports, MaxClients) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Ports, MaxClients], []).

init([Ports, MaxClients]) ->
    [fakeredis_instance_sup:start_link(Port, MaxClients) || Port <- Ports],
    {ok, #state{ports = Ports}}.

handle_cast(_, State) ->
    {noreply, State}.

handle_call(cluster_slots, _From, State) ->
    io:format(user, "## Got cluster slots call~n", []),
    Reply = ok,
    %% Msg = "*3\r\n*4\r\n:0\r\n:5460\r\n*3\r\n$9\r\n127.0.0.1\r\n:30004\r\n$40\r\nd761377cea3f01b5e6ff6e51fa02d96f5cacf674\r\n*3\r\n$9\r\n127.0.0.1\r\n:30001\r\n$40\r\nf6198a1311df6e5b64ddd0e80e465dfab43f0b21\r\n*4\r\n:5461\r\n:10922\r\n*3\r\n$9\r\n127.0.0.1\r\n:30005\r\n$40\r\n5aad0b87b6e8e4e0d5849da7d0d7d5b58554b9ab\r\n*3\r\n$9\r\n127.0.0.1\r\n:30002\r\n$40\r\n8ad9430f62e6a707c95ff7cc309d68a7a8f1cafa\r\n*4\r\n:10923\r\n:16383\r\n*3\r\n$9\r\n127.0.0.1\r\n:30006\r\n$40\r\n66d36985f1d25af2a0493e3161d312cecc174397\r\n*3\r\n$9\r\n127.0.0.1\r\n:30003\r\n$40\r\n83b210fd405fcd09098033b58524c91d9bbd51a8\r\n",
    Msg = "
*3\r\n
  *4\r\n
    :0\r\n
    :5460\r\n
    *3\r\n
      $9\r\n127.0.0.1\r\n
      :30004\r\n
      $40\r\nd761377cea3f01b5e6ff6e51fa02d96f5cacf674\r\n
    *3\r\n
      $9\r\n127.0.0.1\r\n
      :30001\r\n
      $40\r\nf6198a1311df6e5b64ddd0e80e465dfab43f0b21\r\n
  *4\r\n
    :5461\r\n
    :10922\r\n
    *3\r\n
      $9\r\n127.0.0.1\r\n
      :30005\r\n
      $40\r\n5aad0b87b6e8e4e0d5849da7d0d7d5b58554b9ab\r\n
    *3\r\n
      $9\r\n127.0.0.1\r\n
      :30002\r\n
      $40\r\n8ad9430f62e6a707c95ff7cc309d68a7a8f1cafa\r\n
  *4\r\n
    :10923\r\n
    :16383\r\n
    *3\r\n
       $9\r\n127.0.0.1\r\n
       :30006\r\n
       $40\r\n66d36985f1d25af2a0493e3161d312cecc174397\r\n
    *3\r\n
       $9\r\n127.0.0.1\r\n
       :30003\r\n
       $40\r\n83b210fd405fcd09098033b58524c91d9bbd51a8\r\n
",
    {reply, {Reply, Msg}, State};
handle_call(_E, _From, State) ->
    {noreply, State}.

handle_info(_E, State) ->
    {noreply, State}.

terminate(_Reason, _Tab) ->
    ok.

code_change(_OldVersion, Tab, _Extra) -> {ok, Tab}.
