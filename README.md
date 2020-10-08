# DEPRECATED: fakeredis_cluster

This repo has been moved to https://github.com/Nordix/fakeredis_cluster

## Build

```shell
make
```

## Test

```shell
make test
```

## Examples

### Masters only

Start a redis cluster on localhost using port 4000-4005 as masters only.

```erlang
rebar3 shell --apps fakeredis_cluster

fakeredis_cluster:start_link([4000, 4001, 4002, 4003, 4004, 4005]).
```

### Masters with 1 replica each

Start a redis cluster on localhost with a master on port 4000 which has a replica on port 4001,
and a master on port 4002 with its replica on 4003, and so on..

```erlang
rebar3 shell --apps fakeredis_cluster

fakeredis_cluster:start_link([{4000, 4001}, {4002, 4003}, {4004, 4005}]).
```

### Masters with multiple replicas

Add more replicas in the tuple, in this example each master has 2 replicas.

```erlang
rebar3 shell --apps fakeredis_cluster

fakeredis_cluster:start_link([{4000, 4001, 4002}, {4010, 4011, 4012},  {4020, 4021, 4022}]).
```

### TLS

Start a redis cluster using TLS by adding the TLS options, like this example:

```erlang
rebar3 shell --apps fakeredis_cluster

Options = [{cacertfile, "ca.crt"},
           {certfile,   "redis.crt"},
           {keyfile,    "redis.key"}].

fakeredis_cluster:start_link([4000, 4001, 4002, 4003, 4004, 4005], Options).
```

### Run as standalone binary

The cluster can be started as a escript

```bash
make
_build/default/bin/fakeredis_cluster
```
