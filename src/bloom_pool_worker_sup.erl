-module(bloom_pool_worker_sup).
-behaviour(supervisor).

-export([start_link/2]).
-export([init/1]).
-export([start_child/3]).

start_link(Name, ConnectOpts) ->
    SupName = make_sup_name(Name),
	supervisor:start_link({local, SupName}, ?MODULE, [Name, ConnectOpts]).

start_child(Id, Name, Type) ->
    SupName = make_sup_name(Name),
    supervisor:start_child(SupName, [Id, Name, Type]).

init([Name, ConnectOpts]) ->
    SupFlags = #{
        strategy    => simple_one_for_one,
        intensity   => 1000,
        period      => 1
    },
    Child = #{
        id          => {bloom_worker, Name},
        start       => {bloom_worker, start_link, [ConnectOpts]},
        restart     => transient,
        shutdown    => infinity,
        type        => worker,
        modules     => [bloom_worker]
    },
    {ok, {SupFlags, [Child]}}.

make_sup_name(Name) ->
    BinaryName = atom_to_binary(Name, utf8),
    CompaundName = <<"bloom_", BinaryName/binary, "_", "worker_sup">>,
    binary_to_atom(CompaundName, utf8).

