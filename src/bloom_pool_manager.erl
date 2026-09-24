-module(bloom_pool_manager).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2]).
-export([lockin/2, lockout/2, lockout2/3, cancel_wait/2]).
-export([add/4, initial/3]).
-export([pool_counts/1]).
%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    name,
    pool_size,
    pool_max_size,
    main = #{},
    extra = #{},
    busy = #{},
    busy_mons = #{},
    busy_waitrefs = #{},
    init = #{},
    worker_by_id = #{},
    worker_mons = #{},
    waiting,
    waiters = #{},
    waiter_mons = #{},
    cancelled = #{},
    total_waiting = 0
}).

-define(SEND_INFO_TIME,     5000).   %% 5 seconds
-define(TTL_EXTRA_WORKERS,  600000). %% 10 minutes
%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Name, Opts) ->
    PoolName = make_pool_name(Name),
    gen_server:start_link({local, PoolName}, ?MODULE, [Name, Opts], []).

lockout2(#{host := Host} = UriMap, ConnectionOpts, Timeout) ->
    Port = get_port(UriMap),
    Name = <<Host/binary,":", (integer_to_binary(Port))/binary>>,
    PoolName = make_pool_name(Name),
    case whereis(PoolName) of
        undefined -> create_pool(UriMap, ConnectionOpts);
        _Pid -> ok
    end,
    lockout_call(PoolName, Name, Timeout).

lockin(Name, Id) ->
    PoolName = make_pool_name(Name),
    case whereis(PoolName) of
        undefined -> ok;
        Pid -> gen_server:cast(Pid, {lockin, Id})
    end,
    ok.

lockout(Name, Timeout) ->
    PoolName = make_pool_name(Name),
    lockout_call(PoolName, Name, Timeout).

cancel_wait(Name, WaitRef) ->
    PoolName = make_pool_name(Name),
    case whereis(PoolName) of
        undefined ->
            ok;
        _Pid ->
            case manager_call(PoolName, {cancel_wait, WaitRef}, 5000) of
                {ok, _} -> ok;
                {error, _} -> ok
            end
    end.

pool_counts(Name) ->
    PoolName = make_pool_name(Name),
    case whereis(PoolName) of
        undefined ->
            #{busy => 0, waiters => 0, connected => 0, init => 0};
        _Pid ->
            case manager_call(PoolName, pool_counts, 5000) of
                {ok, Counts} -> Counts;
                {error, _} -> #{busy => 0, waiters => 0, connected => 0, init => 0}
            end
    end.

add(Id, Connection, Name, Type) ->
    PoolName = make_pool_name(Name),
    case whereis(PoolName) of
        undefined -> ok;
        Pid -> gen_server:cast(Pid, {add, Id, Connection, Type, self()})
    end,
    ok.

initial(Id, WorkerPid, Name) ->
    PoolName = make_pool_name(Name),
    case whereis(PoolName) of
        undefined -> ok;
        Pid -> gen_server:cast(Pid, {initial, Id, WorkerPid})
    end,
    ok.

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Name, Opts]) ->
    ok = bloom_pool_worker_sup:terminate_all(Name),
    PoolSize = maps:get(pool_size, Opts),
    State0 = #state{
        name = Name,
        pool_size = PoolSize,
        pool_max_size = maps:get(pool_max_size, Opts),
        waiting = queue:new()
    },
    State1 = lists:foldl(fun(_, Acc) ->
        Id = erlang:unique_integer([positive, monotonic]),
        {ok, WorkerPid} = bloom_pool_worker_sup:start_child(Id, Name, main),
        track_new_worker(Id, WorkerPid, main, Acc)
    end, State0, lists:seq(1, PoolSize)),
    {ok, _} = timer:send_after(0, send_pool_info),
    {ok, _} = timer:send_after(?TTL_EXTRA_WORKERS, clear_extra_workers),
    {ok, State1}.

handle_call({lockout, ReqPid, WaitRef}, _From, State) ->
    case maps:take(WaitRef, State#state.cancelled) of
        {_, RestCancelled} ->
            {reply, {error, no_free_connections}, State#state{cancelled = RestCancelled}};
        error ->
            case take_free(State) of
                {Id, Connection, WorkerPid, Type, State1} ->
                    State2 = checkout_to_req(Id, Connection, WorkerPid, Type, ReqPid, WaitRef, State1),
                    {reply, {ok, Id, Connection}, State2};
                empty ->
                    State1 = enqueue_waiter(WaitRef, ReqPid, State),
                    ok = request_scale_up(),
                    {reply, {error, no_free_connections}, State1}
            end
    end;
handle_call({cancel_wait, WaitRef}, _From, State) ->
    {reply, ok, cancel_waiter(WaitRef, State)};
handle_call(pool_counts, _From, State) ->
    Counts = #{
        busy => map_size(State#state.busy),
        waiters => maps:size(State#state.waiters),
        connected => map_size(State#state.main) + map_size(State#state.extra) + map_size(State#state.busy),
        init => map_size(State#state.init)
    },
    {reply, Counts, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({lockin, Id}, State) ->
    {noreply, lockin_id(Id, State)};
handle_cast({add, Id, Connection, Type, WorkerPid}, State) ->
    case accept_add(Id, WorkerPid, State) of
        false ->
            {noreply, State};
        true ->
            State1 = drop_id(Id, State),
            State2 = retarget_worker(Id, WorkerPid, Type, State1),
            Init = maps:remove(Id, State2#state.init),
            State3 = give_waiting_req(Id, Connection, Type, WorkerPid,
                State2#state{init = Init}),
            {noreply, State3}
    end;
handle_cast({initial, Id, WorkerPid}, State) ->
    State1 = drop_id(Id, State),
    {noreply, State1#state{init = maps:put(Id, WorkerPid, State1#state.init)}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(send_pool_info, #state{name = Name} = State) ->
    Info = #{
        ready => map_size(State#state.main) + map_size(State#state.extra),
        busy => map_size(State#state.busy),
        init => map_size(State#state.init),
        now_waiting => maps:size(State#state.waiters),
        total_waiting => State#state.total_waiting,
        connected => map_size(State#state.main) + map_size(State#state.extra) + map_size(State#state.busy),
        pool_size => State#state.pool_size,
        pool_max_size => State#state.pool_max_size
    },
    ok = bloom_stats:update(Name, conn_info, Info),
    {ok, _} = timer:send_after(?SEND_INFO_TIME, send_pool_info),
    {noreply, State};
handle_info(clear_extra_workers, State) ->
    State1 = expire_extra_workers(State),
    {ok, _} = timer:send_after(?TTL_EXTRA_WORKERS, clear_extra_workers),
    {noreply, State1};
handle_info(scale_up, #state{name = Name, pool_max_size = Max} = State) ->
    NewState = case connected_and_init(State) < Max of
        true ->
            Id = erlang:unique_integer([positive, monotonic]),
            case bloom_pool_worker_sup:start_child(Id, Name, extra) of
                {ok, WorkerPid} ->
                    track_new_worker(Id, WorkerPid, extra, State);
                {ok, WorkerPid, _Info} ->
                    track_new_worker(Id, WorkerPid, extra, State);
                {error, Reason} ->
                    ok = logger:error("Start extra worker failed for ~p; reason: ~p",
                        [Name, Reason]),
                    State
            end;
        false ->
            State
    end,
    {noreply, NewState};
handle_info({'DOWN', Mon, process, _Pid, Reason}, State) ->
    case maps:take(Mon, State#state.worker_mons) of
        {Id, WorkerMons} ->
            {noreply, worker_down(Id, Reason, State#state{worker_mons = WorkerMons})};
        error ->
            case maps:take(Mon, State#state.waiter_mons) of
                {WaitRef, WaiterMons} ->
                    {noreply, waiter_down(WaitRef, State#state{waiter_mons = WaiterMons})};
                error ->
                    {noreply, req_down(Mon, State)}
            end
    end;
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
lockout_call(PoolName, Name, Timeout) ->
    case whereis(PoolName) of
        undefined ->
            {error, service_not_exists};
        _Pid ->
            WaitRef = make_ref(),
            case manager_call(PoolName, {lockout, self(), WaitRef}, Timeout) of
                {ok, {ok, Id, Connection}} ->
                    {ok, Id, Connection};
                {ok, {error, no_free_connections}} ->
                    {error, no_free_connections, WaitRef};
                {error, timeout} ->
                    _ = cancel_wait(Name, WaitRef),
                    ok = discard_late_conn(WaitRef),
                    {error, timeout_no_free_connections};
                {error, noproc} ->
                    {error, service_not_exists}
            end
    end.

manager_call(PoolName, Request, Timeout) ->
    try
        {ok, gen_server:call(PoolName, Request, Timeout)}
    catch
        exit:{timeout, {gen_server, call, _}} ->
            {error, timeout};
        exit:{noproc, {gen_server, call, _}} ->
            {error, noproc};
        exit:{normal, {gen_server, call, _}} ->
            {error, noproc};
        exit:{shutdown, {gen_server, call, _}} ->
            {error, noproc};
        exit:{{noproc, _}, {gen_server, call, _}} ->
            {error, noproc};
        exit:{{shutdown, _}, {gen_server, call, _}} ->
            {error, noproc}
    end.

take_free(#state{main = Main, extra = Extra} = State) ->
    case take_any(Main) of
        {Id, {Connection, WorkerPid, _LTU}, RestMain} ->
            {Id, Connection, WorkerPid, main, State#state{main = RestMain}};
        empty ->
            case take_any(Extra) of
                {Id, {Connection, WorkerPid, _LTU}, RestExtra} ->
                    {Id, Connection, WorkerPid, extra, State#state{extra = RestExtra}};
                empty ->
                    empty
            end
    end.

take_any(Map) ->
    case maps:next(maps:iterator(Map)) of
        none ->
            empty;
        {Id, Value, _Iter} ->
            {Id, Value, maps:remove(Id, Map)}
    end.

enqueue_waiter(WaitRef, Pid, #state{waiting = Q, waiters = Waiters,
        waiter_mons = WaiterMons, total_waiting = Total} = State) ->
    Mon = erlang:monitor(process, Pid),
    State#state{
        waiting = queue:in(WaitRef, Q),
        waiters = Waiters#{WaitRef => {Pid, Mon}},
        waiter_mons = WaiterMons#{Mon => WaitRef},
        total_waiting = Total + 1
    }.

cancel_waiter(WaitRef, State) ->
    case maps:take(WaitRef, State#state.waiters) of
        {{_Pid, Mon}, RestWaiters} ->
            erlang:demonitor(Mon, [flush]),
            State#state{
                waiters = RestWaiters,
                waiter_mons = maps:remove(Mon, State#state.waiter_mons)
            };
        error ->
            case maps:take(WaitRef, State#state.busy_waitrefs) of
                {Id, RestRefs} ->
                    reclaim_busy_id(Id, State#state{busy_waitrefs = RestRefs});
                error ->
                    State#state{cancelled = maps:put(WaitRef, true, State#state.cancelled)}
            end
    end.

discard_late_conn(WaitRef) ->
    receive
        {bloom_conn, WaitRef, _Id, _Connection} ->
            ok
    after 0 ->
        ok
    end.

accept_add(Id, WorkerPid, State) ->
    is_process_alive(WorkerPid) andalso
        (maps:is_key(Id, State#state.init) orelse maps:is_key(Id, State#state.worker_by_id)).

request_scale_up() ->
    self() ! scale_up,
    ok.

track_new_worker(Id, WorkerPid, Type, State) ->
    Mon = erlang:monitor(process, WorkerPid),
    State#state{
        init = maps:put(Id, WorkerPid, State#state.init),
        worker_by_id = maps:put(Id, {WorkerPid, Mon, Type}, State#state.worker_by_id),
        worker_mons = maps:put(Mon, Id, State#state.worker_mons)
    }.

retarget_worker(Id, WorkerPid, Type, #state{worker_by_id = ById, worker_mons = Mons} = State) ->
    case maps:get(Id, ById, undefined) of
        {WorkerPid, _Mon, _} ->
            State;
        {_OldPid, OldMon, _} ->
            erlang:demonitor(OldMon, [flush]),
            NewMon = erlang:monitor(process, WorkerPid),
            State#state{
                worker_by_id = maps:put(Id, {WorkerPid, NewMon, Type}, ById),
                worker_mons = maps:put(NewMon, Id, maps:remove(OldMon, Mons))
            };
        undefined ->
            NewMon = erlang:monitor(process, WorkerPid),
            State#state{
                worker_by_id = maps:put(Id, {WorkerPid, NewMon, Type}, ById),
                worker_mons = maps:put(NewMon, Id, Mons)
            }
    end.

checkout_to_req(Id, Connection, WorkerPid, Type, ReqPid, WaitRef, State) ->
    ReqMon = erlang:monitor(process, ReqPid),
    BusyItem = {ReqPid, Connection, WorkerPid, ReqMon, Type, WaitRef},
    State#state{
        busy = maps:put(Id, BusyItem, State#state.busy),
        busy_mons = maps:put(ReqMon, Id, State#state.busy_mons),
        busy_waitrefs = maps:put(WaitRef, Id, State#state.busy_waitrefs)
    }.

give_waiting_req(Id, Connection, Type, WorkerPid, #state{waiting = Q} = State) ->
    case queue:out(Q) of
        {empty, _} ->
            LTU = os:system_time(millisecond),
            Conn = {Connection, WorkerPid, LTU},
            case Type of
                main ->
                    State#state{main = maps:put(Id, Conn, State#state.main)};
                extra ->
                    State#state{extra = maps:put(Id, Conn, State#state.extra)}
            end;
        {{value, WaitRef}, RestQ} ->
            State1 = State#state{waiting = RestQ},
            case maps:take(WaitRef, State1#state.waiters) of
                error ->
                    give_waiting_req(Id, Connection, Type, WorkerPid, State1);
                {{Pid, Mon}, RestWaiters} ->
                    erlang:demonitor(Mon, [flush]),
                    WaiterMons = maps:remove(Mon, State1#state.waiter_mons),
                    Pid ! {bloom_conn, WaitRef, Id, Connection},
                    checkout_to_req(Id, Connection, WorkerPid, Type, Pid, WaitRef,
                        State1#state{waiters = RestWaiters, waiter_mons = WaiterMons})
            end
    end.

drop_id(Id, State) ->
    Main = maps:remove(Id, State#state.main),
    Extra = maps:remove(Id, State#state.extra),
    case maps:take(Id, State#state.busy) of
        {{_ReqPid, _Conn, _WPid, ReqMon, _Type, WaitRef}, RestBusy} ->
            erlang:demonitor(ReqMon, [flush]),
            BusyMons = maps:remove(ReqMon, State#state.busy_mons),
            WaitRefs = maps:remove(WaitRef, State#state.busy_waitrefs),
            State#state{
                main = Main,
                extra = Extra,
                busy = RestBusy,
                busy_mons = BusyMons,
                busy_waitrefs = WaitRefs
            };
        error ->
            State#state{main = Main, extra = Extra}
    end.

lockin_id(Id, State) ->
    case maps:take(Id, State#state.busy) of
        {{_ReqPid, Connection, WorkerPid, ReqMon, Type, WaitRef}, RestBusy} ->
            erlang:demonitor(ReqMon, [flush]),
            BusyMons = maps:remove(ReqMon, State#state.busy_mons),
            WaitRefs = maps:remove(WaitRef, State#state.busy_waitrefs),
            give_waiting_req(Id, Connection, Type, WorkerPid,
                State#state{busy = RestBusy, busy_mons = BusyMons, busy_waitrefs = WaitRefs});
        error ->
            ok = logger:warning("Busy connection_id not found: ~p", [Id]),
            State
    end.

reclaim_busy_id(Id, State) ->
    case maps:take(Id, State#state.busy) of
        {{_ReqPid, Connection, WorkerPid, ReqMon, Type, _WaitRef}, RestBusy} ->
            erlang:demonitor(ReqMon, [flush]),
            BusyMons = maps:remove(ReqMon, State#state.busy_mons),
            give_waiting_req(Id, Connection, Type, WorkerPid,
                State#state{busy = RestBusy, busy_mons = BusyMons});
        error ->
            State
    end.

worker_down(Id, Reason, State) ->
    State1 = drop_id(Id, State),
    ById = maps:remove(Id, State1#state.worker_by_id),
    State2 = State1#state{worker_by_id = ById},
    case Reason of
        normal ->
            State2#state{init = maps:remove(Id, State2#state.init)};
        shutdown ->
            State2#state{init = maps:remove(Id, State2#state.init)};
        {shutdown, _} ->
            State2#state{init = maps:remove(Id, State2#state.init)};
        _ ->
            State2#state{init = maps:put(Id, undefined, State2#state.init)}
    end.

waiter_down(WaitRef, State) ->
    Waiters = maps:remove(WaitRef, State#state.waiters),
    State#state{waiters = Waiters}.

req_down(Mon, State) ->
    case maps:take(Mon, State#state.busy_mons) of
        {Id, BusyMons} ->
            case maps:take(Id, State#state.busy) of
                {{_ReqPid, Connection, WorkerPid, Mon, Type, WaitRef}, RestBusy} ->
                    WaitRefs = maps:remove(WaitRef, State#state.busy_waitrefs),
                    give_waiting_req(Id, Connection, Type, WorkerPid,
                        State#state{busy = RestBusy, busy_mons = BusyMons, busy_waitrefs = WaitRefs});
                error ->
                    State#state{busy_mons = BusyMons}
            end;
        error ->
            State
    end.

connected_and_init(#state{main = M, extra = E, busy = B, init = I}) ->
    map_size(M) + map_size(E) + map_size(B) + map_size(I).

expire_extra_workers(#state{extra = Extra} = State) ->
    ExpiredTime = os:system_time(millisecond) - ?TTL_EXTRA_WORKERS,
    maps:fold(fun(Id, {Connection, WorkerPid, LTU}, Acc) ->
        case ExpiredTime > LTU of
            true ->
                ok = bloom_worker:stop(WorkerPid, Connection),
                Acc#state{extra = maps:remove(Id, Acc#state.extra)};
            false ->
                Acc
        end
    end, State, Extra).

make_pool_name(Name) when is_atom(Name) ->
    StringName = atom_to_binary(Name, utf8),
    make_pool_name(StringName);
make_pool_name(Name) when is_binary(Name) ->
    CompaundName = <<"bloom_", Name/binary, "_pool_manager">>,
    binary_to_atom(CompaundName, utf8).

create_pool(#{host := Host} = UriMap, ConnOpts) ->
    Port = get_port(UriMap),
    TlsOpts = get_tls_opts(UriMap),
    PoolOpts = #{
        pool_size => 1,
        pool_max_size => 5
    },
    Opts = ConnOpts#{
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
