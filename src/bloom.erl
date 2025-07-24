-module(bloom).

-export([start/0]).
-export([stop/0]).
-export([init/3]).
-export([delete/1]).
-export([info/0]).
-export([req/4, req/5, req/6]).
-export([request/4, request/5]).

-type methods() :: get | post | put | patch | delete | head | options.

-type pool_opts() :: #{
    pool_size       => integer(),
    pool_max_size   => integer()
}.

-type connection_opts() :: #{
    host            => list(),
    port            => integer(),
    tls_opts        => list(),
    http_opts       => map(),
    connect_timeout => integer()
}.

-type req_opts() :: #{
    timeout => integer()
}.

-type code()    :: integer().
-type headers() :: list().
-type body()    :: binary().

start() ->
    application:ensure_all_started(?MODULE).

stop() ->
	ok = application:stop(?MODULE).

-spec init(Name, PoolOpts, ConnectOpts) -> ok
when
    Name        :: atom(),
    PoolOpts    :: pool_opts(),
    ConnectOpts :: connection_opts().
init(Name, PoolOpts, ConnectOpts) ->
    ok = bloom_stats:add(Name),
    bloom_sup:start_pool(Name, PoolOpts, ConnectOpts).

-spec delete(Name) -> ok
when
    Name :: atom().
delete(Name) ->
    ok = bloom_stats:delete(Name),
    bloom_sup:stop_pool(Name).

-spec request(Url, Method, Headers, Opts) -> Result
when
    Url     :: binary(),
    Method  :: methods(),
    Headers :: map() | list(),
    Opts    :: req_opts(),
    Result  :: {ok | error, code(), headers(), body()} | {error, atom()}.
request(Url, Method, Headers, Opts) ->
    request(Url, Method, Headers, <<>>, Opts).

-spec request(Url, Method, Headers, Body, Opts) -> Result
when
    Url     :: binary(),
    Method  :: methods(),
    Headers :: map() | list(),
    Body    :: binary(),
    Opts    :: req_opts(),
    Result  :: {ok | error, code(), headers(), body()} | {error, atom()}.
request(Url, Method, Headers, Body, Opts) ->
    bloom_worker:request(Url, Method, Headers, Body, Opts).

-spec req(ServiceName, Method, Path, Headers) -> Result
when
    ServiceName :: atom(),
    Method      :: methods(),
    Path        :: binary(),
    Headers     :: map() | list(),
    Result      :: {ok | error, code(), headers(), body()} | {error, atom()}.
req(ServiceName, Method, Path, Headers) ->
    req(ServiceName, Method, Path, Headers, <<>>, #{}).

-spec req(ServiceName, Method, Path, Headers, Body) -> Result
when
    ServiceName :: atom(),
    Method      :: methods(),
    Path        :: binary(),
    Headers     :: map() | list(),
    Body        :: binary(),
    Result      :: {ok | error, code(), headers(), body()} | {error, atom()}.
req(ServiceName, Method, Path, Headers, Body) ->
    req(ServiceName, Method, Path, Headers, Body, #{}).

-spec req(ServiceName, Method, Path, Headers, Body, Opts) -> Result
when
    ServiceName :: atom(),
    Method      :: methods(),
    Path        :: binary(),
    Headers     :: map() | list(),
    Body        :: binary(),
    Opts        :: req_opts(),
    Result      :: {ok | error, code(), headers(), body()} | {error, atom()}.
req(ServiceName, Method, Path, Headers, Body, Opts) when Method == get; Method ==  post;
        Method ==  put; Method == patch; Method == delete; Method == head; Method == options ->
    bloom_worker:req(ServiceName, Method, Path, Headers, Body, Opts);
req(ServiceName, Method, Path, _Headers, _Body, _Opts) ->
    Args = [Method, ServiceName, Path],
    ok = logger:error("Method isn't supported ~p; Service ~p; Path ~p", Args),
    {error, method_isnt_supported}.

info() ->
    bloom_stats:info().
