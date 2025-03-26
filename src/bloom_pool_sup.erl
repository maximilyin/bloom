-module(bloom_pool_sup).
-behaviour(supervisor).

%% API
-export([start_link/3]).

%% supervisor callbacks
-export([init/1]).

start_link(Name, PoolOpts, ConnectOpts) ->
    supervisor:start_link(?MODULE, [Name, PoolOpts, ConnectOpts]).

init([Name, PoolOpts, ConnectOpts]) ->
    SupFlags = #{
        strategy    => one_for_one,
        intensity   => 1000,
        period      => 1
    },
    Children = [
        #{
            id          => {bloom_pool_worker_sup, Name},
            start       => {bloom_pool_worker_sup, start_link, [Name, ConnectOpts]},
            restart     => transient,
            shutdown    => infinity,
            type        => supervisor,
            modules     => [bloom_pool_worker_sup]
        },
        #{
            id          => {bloom_pool_manager, Name},
            start       => {bloom_pool_manager, start_link, [Name, PoolOpts]},
            restart     => transient,
            shutdown    => infinity,
            type        => worker,
            modules     => [bloom_pool_manager]
        }
    ],
    {ok, {SupFlags, Children}}.
