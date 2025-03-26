Wrapper over gun http client
====

## Usage examples

Start bloom first

```erlang
1> bloom:start().
{ok,[crypto,asn1,public_key,ssl,cowlib,gun,bloom]}
```

## Create pool connection

```erlang
2> Name = remote_service,
2> PoolOpts = #{
2>     pool_size => 10,
2>     pool_max_size => 50
2> },
2> ConnectionOpts = #{
2>     host             => "remotehost",
2>     port             => 8080,
2>     http_opts        => #{keepalive => 10000},
2>     connect_timeout  => 1000         % optional, 3000 by default
2> },
2> bloom:init(Name, PoolOpts, ConnectionOpts).
ok
```

## Do a request

```erlang
3> ServiceName = remote_service,
3> Method = post,
3> Path = <<"/confagent/get_all_partners">>,
3> Headers = #{<<"content-type">> => <<"application/json">>},
3> Body = jsx:encode([{<<"field">>, <<"value">>}]),
3> Opts = #{timeout => 500},
3> bloom:req(ServiceName, Method, Path, Headers, Body, Opts).
{ok,200,
    [{<<"content-type">>,<<"application/json">>},
     {<<"content-length">>,<<"12510">>},
     {<<"date">>,<<"Tue, 14 May 2024 06:15:51 GMT">>},
     {<<"server">>,<<"Cowboy">>}],
    <<"{\"status\":\"ok\",\"response\":[{\"id\":30194,\"name\":\"test158\",\"country\":null},{\"id\":30195,\"name\":\"test123 "...>>}
```

## Get statistics

```erlang
4> bloom:info().
[{remote_service,[{pool_config,[{pool_size,10},
                                {pool_max_size,50}]},
                  {connections,[{ready,10},
                                {busy,0},
                                {init,0},
                                {connected,10},
                                {max_connected,10},
                                {total_broken,0}]},
                  {requests,[{rps,0},
                             {total,1},
                             {succeed,1},
                             {failed,0},
                             {now_waiting,0},
                             {total_waiting,0},
                             {'top longest',[{<<"/remote_service/entity/action">>,
                                              82}]}]}]}]
```
