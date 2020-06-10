# fakeredis_cluster

## Build

```shell
make
```

## Test

```shell
make test
```

## Examples

Start a redis cluster on localhost using port 4000-4005

```erlang
rebar3 shell --apps fakeredis_cluster

fakeredis_cluster:start_link([4000, 4001, 4002, 4003, 4004, 4005]).
```

Start a redis cluster using TLS on localhost using port 4000-4005

```erlang
rebar3 shell --apps fakeredis_cluster

Options = [{cacertfile, "ca.crt"},
           {certfile,   "redis.crt"},
           {keyfile,    "redis.key"}].

fakeredis_cluster:start_link([4000, 4001, 4002, 4003, 4004, 4005], Options).
```

## Other

```
rebar3 shell --apps fakeredis_cluster
observer:start().
fakeredis_cluster:start_link([4000, 4001]).

%% Get pids for all fakeredis_instance from port 4001:
gproc:lookup_pids({p, l, {local, 4001}}).

gen_server:call(fakeredis_cluster, cluster_slots).

gproc:get_value({p, l, {local, 4001}}).
```
