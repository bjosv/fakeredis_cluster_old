# fakeredis_cluster

## Build

```
rebar3 compile
```

## Test
```
rebar3 shell --apps fakeredis_cluster
observer:start().
fakeredis_cluster:start_link([4000, 4001]).

%% Get pids for all fakeredis_instance from port 4001:
gproc:lookup_pids({p, l, {local, 4001}}).

gen_server:call(fakeredis_cluster, cluster_slots).

gproc:get_value({p, l, {local, 4001}}).

```
