-module(bloom_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).
-export([start_pool/3]).
-export([stop_pool/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_pool(Name, PoolOpts, ConnectOpts) ->
    Spec = #{
        id          => {bloom_pool_sup, Name},
        start       => {bloom_pool_sup, start_link, [Name, PoolOpts, ConnectOpts]},
        restart     => transient,
        shutdown    => infinity,
        type        => supervisor,
        modules     => [bloom_pool_sup]
    },
    case supervisor:start_child(?MODULE, Spec) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            Args = [Name, Reason],
            logger:error("Create service pool: ~p; reason: ~p", Args)
    end.

stop_pool(Name) ->
    Id = {bloom_pool_sup, Name},
    ok = supervisor:terminate_child(?MODULE, Id),
    ok = supervisor:delete_child(?MODULE, Id).

init([]) ->
    {ok, { {one_for_one, 5000, 10}, []} }.


