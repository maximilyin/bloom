-module(bloom_pool_manager).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2]).
-export([lockin/2, lockout/1, lockout2/1]).
-export([add/4, initial/3]).
%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    name,
    pool_size,
    pool_max_size
}).

-define(SEND_INFO_TIME,     5000).   %% 5 seconds
-define(TTL_EXTRA_WORKERS,  600000). %% 10 minutes
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Name, Opts) ->
    PoolName = make_pool_name(Name),
    gen_server:start_link({local, PoolName}, ?MODULE, [Name, Opts], []).

lockout2(#{host := Host} = UriMap) ->
    Port = get_port(UriMap),
    Name = <<Host/binary,":", (integer_to_binary(Port))/binary>>,
    PoolName = make_pool_name(Name),
    case is_pool_exists(PoolName) of
        true -> ok;
        false -> create_pool(UriMap)
    end,
    gen_server:call(PoolName, {lockout, self()}, infinity).

lockin(Name, Id) ->
    PoolName = make_pool_name(Name),
    ok = gen_server:cast(PoolName, {lockin, Id}).

lockout(Name) ->
    PoolName = make_pool_name(Name),
    case is_pool_exists(PoolName) of
        false ->
            {error, service_not_exists};
        true ->
            gen_server:call(PoolName, {lockout, self()}, infinity)
    end.

add(Id, Connection, Name, Type) ->
    PoolName = make_pool_name(Name),
    ok = gen_server:cast(PoolName, {add, Id, Connection, Type}).

initial(Id, WorkerPid, Name) ->
    PoolName = make_pool_name(Name),
    ok = gen_server:cast(PoolName, {initial, Id, WorkerPid}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Name, Opts]) ->
    ok = insert(main, []),
    ok = insert(extra, []),
    ok = insert(busy, []),
    ok = insert(waiting, []),
    ok = insert(total_waiting, 0),
    PoolSize = maps:get(pool_size, Opts),
    State = #state{
        name = Name,
        pool_size = PoolSize,
        pool_max_size = maps:get(pool_max_size, Opts)
    },
    InitWorkers = [begin
        Id = erlang:unique_integer([positive, monotonic]),
        {ok, WorkerPid} = bloom_pool_worker_sup:start_child(Id, Name, main),
        {Id, WorkerPid}
    end || _ <- lists:seq(1, PoolSize)],
    ok = insert(init, InitWorkers),
    {ok, _} = timer:send_after(0, send_pool_info),
    {ok, _} = timer:send_after(?TTL_EXTRA_WORKERS, clear_extra_workers),
    {ok, State}.

handle_call({lockout, ReqPid}, From, State) ->
    Busy = lookup(busy),
    case lookup(main) of
        [] ->
            case lookup(extra) of
                [] ->
                    _ = gen_server:reply(From, {error, no_free_connections}),
                    ok = create_extra_worker(State),
                    Waiting = lookup(waiting),
                    TotalWaiting = lookup(total_waiting),
                    ok = insert(waiting, Waiting ++ [ReqPid]),
                    ok = insert(total_waiting, TotalWaiting+1);
                [{Id, Connection, WorkerPid, _LTU}|RestConnections] ->
                    _ = gen_server:reply(From, {ok, Id, Connection}),
                    Monitor = erlang:monitor(process, ReqPid),
                    ok = insert(extra, RestConnections),
                    ok = insert(busy, [{Id, ReqPid, Connection, WorkerPid, Monitor, extra}|Busy])
            end;
        [{Id, Connection, WorkerPid, _LTU}|RestConnections] ->
            _ = gen_server:reply(From, {ok, Id, Connection}),
            Monitor = erlang:monitor(process, ReqPid),
            ok = insert(main, RestConnections),
            ok = insert(busy, [{Id, ReqPid, Connection, WorkerPid, Monitor, main}|Busy])
    end,
    {noreply, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({lockin, Id}, State) ->
    Busy = lookup(busy),
    case lists:keytake(Id, 1, Busy) of
        {value, {Id, _ReqPid, Connection, WorkerPid, Monitor, Type}, RestBusy} ->
            true = erlang:demonitor(Monitor),
            ok = give_waiting_req(Id, Connection, RestBusy, Type, WorkerPid);
        false ->
            ok = logger:warning("Busy connection_id not found: ~p", [Id])
    end,
    {noreply, State};
handle_cast({add, Id, Connection, Type}, State) ->
    Init = lookup(init),
    case lists:keytake(Id, 1, Init) of
        {_, {Id, WorkerPid}, RestInit} ->
            Busy = lookup(busy),
            ok = give_waiting_req(Id, Connection, Busy, Type, WorkerPid),
            ok = insert(init, RestInit);
        false ->
            ok = logger:warning("Init connection: ~p not found", [Id])
    end,
    {noreply, State};
handle_cast({initial, Id, WorkerPid}, State) ->
    Busy = lookup(busy),
    Main = lookup(main),
    Extra = lookup(extra),
    Init = lookup(init),
    RestBusy = lists:keydelete(Id, 1, Busy),
    RestMain = lists:keydelete(Id, 1, Main),
    RestExtra = lists:keydelete(Id, 1, Extra),
    ok = insert(busy, RestBusy),
    ok = insert(main, RestMain),
    ok = insert(extra, RestExtra),
    ok = insert(init, [{Id, WorkerPid}|Init]),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(send_pool_info, #state{name = Name} = State) ->
    Info = #{
        ready => length(lookup(main))+length(lookup(extra)),
        busy => length(lookup(busy)),
        init => length(lookup(init)),
        now_waiting => length(lookup(waiting)),
        total_waiting => lookup(total_waiting),
        connected => length(lookup(main))+length(lookup(extra))+length(lookup(busy)),
        pool_size => State#state.pool_size,
        pool_max_size => State#state.pool_max_size
    },
    ok = bloom_stats:update(Name, conn_info, Info),
    {ok, _} = timer:send_after(?SEND_INFO_TIME, send_pool_info),
    {noreply, State};
handle_info(clear_extra_workers, #state{name = Name} = State) ->
    Extra = lookup(extra),
    ok = clear_workers(Extra, Name, []),
    {ok, _} = timer:send_after(?TTL_EXTRA_WORKERS, clear_extra_workers),
    {noreply, State};
handle_info({'DOWN', Monitor, process, ReqPid, _}, State) ->
    true = erlang:demonitor(Monitor),
    Busy = lookup(busy),
    case lists:keytake(ReqPid, 2, Busy) of
        {value, {Id, ReqPid, Connection, WorkerPid, _, Type}, RestBusy} ->
            ok = give_waiting_req(Id, Connection, RestBusy, Type, WorkerPid);
        false ->
            ok
    end,
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
lookup(Type) ->
    get(Type).

insert(Type, List) ->
    put(Type, List),
    ok.

clear_workers([], _ServiceName, UpdatedExtra) ->
    ok = insert(extra, UpdatedExtra);
clear_workers([{Id, Connection, WorkerPid, LTU}|Extra], ServiceName, Acc) ->
    ExpiredTime = os:system_time(millisecond) - ?TTL_EXTRA_WORKERS,
    case ExpiredTime > LTU of
        true ->
            ok = bloom_worker:stop(WorkerPid, Connection),
            clear_workers(Extra, ServiceName, Acc);
        false ->
            NewAcc = [{Id, Connection, WorkerPid, LTU}|Acc],
            clear_workers(Extra, ServiceName, NewAcc)
    end.

create_extra_worker(State) ->
    Init = lookup(init),
    Connected = length(lookup(main))+length(lookup(extra))+length(lookup(busy))+length(Init),
    PoolMaxSize = State#state.pool_max_size,
    case Connected < PoolMaxSize of
        true ->
            Name = State#state.name,
            Id = erlang:unique_integer([positive, monotonic]),
            {ok, WorkerPid} = bloom_pool_worker_sup:start_child(Id, Name, extra),
            ok = insert(init, [{Id, WorkerPid}|Init]);
        false ->
            ok
    end.

give_waiting_req(Id, Connection, Busy, Type, WorkerPid) ->
    Waiting = lookup(waiting),
    case get_alive_process(Waiting) of
        {} ->
            ok = insert(busy, Busy),
            LTU = os:system_time(millisecond),
            Ready = lookup(Type),
            ok = insert(Type, [{Id, Connection, WorkerPid, LTU}|Ready]);
        {ReqPid, RestWaiting} ->
            ReqPid ! {ok, Id, Connection},
            Monitor = erlang:monitor(process, ReqPid),
            ok = insert(waiting, RestWaiting),
            ok = insert(busy, [{Id, ReqPid, Connection, WorkerPid, Monitor, Type}|Busy])
    end.

get_alive_process([]) ->
    ok = insert(waiting, []),
    {};
get_alive_process([Pid|RestPids]) ->
    case is_process_alive(Pid) of
        true ->
            {Pid, RestPids};
        false ->
            get_alive_process(RestPids)
    end.

is_pool_exists(PoolName) ->
    RegisteredProcess = erlang:registered(),
    lists:member(PoolName, RegisteredProcess).

make_pool_name(Name) when is_atom(Name) ->
    StringName = atom_to_binary(Name, utf8),
    make_pool_name(StringName);
make_pool_name(Name) when is_binary(Name) ->
    CompaundName = <<"bloom_", Name/binary, "_pool_manager">>,
    binary_to_atom(CompaundName, utf8).

create_pool(#{host := Host} = UriMap) ->
    Port = get_port(UriMap),
    TlsOpts = get_tls_opts(UriMap),
    PoolOpts = #{
        pool_size => 1,
        pool_max_size => 5
    },
    Opts = #{
        host => binary_to_list(Host),
        port => Port,
        http_opts => #{keepalive => 10000}
    },
    ConnectionOpts = maps:merge(Opts, TlsOpts),
    Name = binary_to_atom(<<Host/binary, ":", (integer_to_binary(Port))/binary>>, utf8),
    bloom:init(Name, PoolOpts, ConnectionOpts).

get_port(UriMap) ->
    Port = case maps:get(scheme, UriMap) of
        <<"http">> -> 80;
        <<"https">> -> 443
    end,
    maps:get(port, UriMap, Port).

get_tls_opts(#{scheme := <<"http">>}) ->
    #{};
get_tls_opts(#{scheme := <<"https">>}) ->
    #{tls_opts => [{verify, verify_none}]}.
