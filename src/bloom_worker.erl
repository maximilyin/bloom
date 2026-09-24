-module(bloom_worker).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------
-export([start_link/4]).
-export([req/6, request/5]).
-export([stop/2]).
%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {id, connection, service_name, type, opts}).

-define(RECONNECT_ATTEMPT, 3).
-define(REQ_TIMEOUT, 60000).
-define(RECONECT_TIMEOUT, 5000).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------
start_link(Opts, Id, ServiceName, Type) ->
    gen_server:start_link(?MODULE, [Id, ServiceName, Type, Opts], []).

request(Uri, Method, Headers, Body, Opts) ->
    UriMap = uri_string:parse(Uri),
    Path = get_request_path(UriMap),
    ServiceName = make_service_name(UriMap),
    ConnOpts = maps:get(connection_opts, Opts, #{}),
    Timeout = maps:get(timeout, Opts, ?REQ_TIMEOUT),
    Start = erlang:monotonic_time(millisecond),
    case bloom_pool_manager:lockout2(UriMap, ConnOpts, Timeout) of
        {ok, Id, Connection} ->
            call(ServiceName, Id, Connection, Method, Path, Headers, Body, Opts);
        {error, no_free_connections, WaitRef} ->
            Remain = remaining_timeout(Start, Timeout),
            wait_for_conn(ServiceName, WaitRef, Remain, Method, Path, Headers, Body, Opts);
        {error, Reason} ->
            {error, Reason}
    end.

req(ServiceName, Method, Path, Headers, Body, Opts) ->
    Timeout = maps:get(timeout, Opts, ?REQ_TIMEOUT),
    Start = erlang:monotonic_time(millisecond),
    case bloom_pool_manager:lockout(ServiceName, Timeout) of
        {ok, Id, Connection} ->
            call(ServiceName, Id, Connection, Method, Path, Headers, Body, Opts);
        {error, no_free_connections, WaitRef} ->
            Remain = remaining_timeout(Start, Timeout),
            wait_for_conn(ServiceName, WaitRef, Remain, Method, Path, Headers, Body, Opts);
        {error, Reason} ->
            {error, Reason}
    end.

wait_for_conn(ServiceName, WaitRef, Timeout, Method, Path, Headers, Body, Opts) ->
    receive
        {bloom_conn, WaitRef, Id, Connection} ->
            call(ServiceName, Id, Connection, Method, Path, Headers, Body, Opts)
    after Timeout ->
        _ = bloom_pool_manager:cancel_wait(ServiceName, WaitRef),
        ok = return_late_conn(WaitRef),
        ok = bloom_stats:update_counter(ServiceName, req_total),
        ok = bloom_stats:update_counter(ServiceName, req_failed),
        {error, timeout_no_free_connections}
    end.

return_late_conn(WaitRef) ->
    receive
        {bloom_conn, WaitRef, _Id, _Connection} ->
            ok
    after 0 ->
        ok
    end.

remaining_timeout(Start, Timeout) ->
    Elapsed = erlang:monotonic_time(millisecond) - Start,
    case Timeout - Elapsed of
        Remain when Remain > 0 -> Remain;
        _ -> 0
    end.

call(ServiceName, Id, Connection, Method, Path, Headers, Body, Opts) ->
    ok = bloom_stats:update_counter(ServiceName, req_total),
    Timeout = maps:get(timeout, Opts, ?REQ_TIMEOUT),
    StartTime = os:system_time(millisecond),
    StreamRef = get_stream_ref(Connection, Method, Path, Headers, Body, Opts),
    Response = gun:await(Connection, StreamRef, Timeout),
    case await_response(Connection, StreamRef, Response, Timeout) of
        {Status, Code, ResponseHeaders, ResponseBody} ->
            ok = bloom_pool_manager:lockin(ServiceName, Id),
            EndTime = os:system_time(millisecond),
            TimeDiff = EndTime - StartTime,
            ok = bloom_stats:update(ServiceName, execution_time, {Path, TimeDiff}),
            ok = bloom_stats:update_counter(ServiceName, req_succeed),
            {Status, Code, ResponseHeaders, ResponseBody};
        {error, Reason} ->
            ok = close_connection(Connection),
            ok = bloom_stats:update_counter(ServiceName, req_failed),
            {error, Reason}
    end.

await_response(Connection, StreamRef, {response, nofin, Code, Headers}, Timeout) ->
    Response = gun:await_body(Connection, StreamRef, Timeout),
    format_response(Response, Code, Headers);
await_response(_Connection, _StreamRef, {response, fin, Code, Headers}, _Timeout) ->
    format_response({ok, <<>>}, Code, Headers);
await_response(_Connection, _StreamRef, {error, Reason}, _Timeout) ->
    {error, Reason}.

format_response({error, Reason}, _Code, _Headers) ->
    {error, Reason};
format_response({Status, ResponseBody}, Code, Headers) ->
    {Status, Code, Headers, ResponseBody}.

stop(WorkerPid, Connection) ->
    ok = gen_server:cast(WorkerPid, stop_worker),
    ok = close_connection(Connection).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------
init([Id, Name, Type, Opts]) ->
    self() ! connect,
    {ok, #state{id = Id, connection = init, service_name = Name, type = Type, opts = Opts}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(stop_worker, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(connect, State) ->
    {noreply, do_connect(State)};
handle_info(reconnect, State) ->
    {noreply, do_connect(State)};
handle_info({'DOWN', Ref, process, Pid, R}, #state{id = Id} = State) ->
    ok = logger:warning("Connection is down: ~p; reason: ~p~n", [Pid, R]),
    Name = State#state.service_name,
    ok = bloom_pool_manager:initial(Id, self(), Name),
    ok = bloom_stats:update_counter(Name, conn_broken),
    _ = erlang:demonitor(Ref, [flush]),
    ok = close_connection(Pid),
    self() ! connect,
    {noreply, State#state{connection = init}};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
do_connect(#state{connection = Pid} = State) when is_pid(Pid) ->
    State;
do_connect(#state{id = Id, type = Type, opts = Opts} = State) ->
    case connect(Opts) of
        {ok, NewConnection} ->
            _Ref = erlang:monitor(process, NewConnection),
            Name = State#state.service_name,
            ok = bloom_pool_manager:add(Id, NewConnection, Name, Type),
            State#state{connection = NewConnection};
        {error, _Reason} ->
            {ok, _} = timer:send_after(?RECONECT_TIMEOUT, reconnect),
            State#state{connection = fail}
    end.

connect(Opts) ->
    {Host, Opts2} = maps:take(host, Opts),
    {Port, Opts3} = maps:take(port, Opts2),
    Retry = maps:get(retry, Opts, ?RECONNECT_ATTEMPT),
    RetryTimeout = maps:get(retry_timeout, Opts, 1000),
    ConnectTimeout = maps:get(connect_timeout, Opts, 3000),
    ConnectionOpts = Opts3#{
        retry => Retry,
        retry_timeout => RetryTimeout,
        connect_timeout => ConnectTimeout
    },
    case gun:open(Host, Port, ConnectionOpts) of
        {ok, Pid} ->
            case gun:await_up(Pid, 5000) of
                {ok, _Protocol} ->
                    {ok, Pid};
                {error, Reason} ->
                    ok = logger:warning("Waiting up connection ~p; opts: ~p~n", [Reason, Opts]),
                    {error, Reason}
            end;
        {error, Reason} ->
            ok = logger:error("Connection ~p; opts: ~p~n", [Reason, Opts]),
            {error, Reason}
    end.

close_connection(Connection) ->
    ok = gun:close(Connection).

get_stream_ref(Connection, get, Path, Headers, _Body, ReqOpts) ->
    gun:get(Connection, Path, Headers, ReqOpts);
get_stream_ref(Connection, head, Path, Headers, _Body, ReqOpts) ->
    gun:head(Connection, Path, Headers, ReqOpts);
get_stream_ref(Connection, options, Path, Headers, _Body, ReqOpts) ->
    gun:options(Connection, Path, Headers, ReqOpts);
get_stream_ref(Connection, delete, Path, Headers, _Body, ReqOpts) ->
    gun:delete(Connection, Path, Headers, ReqOpts);
get_stream_ref(Connection, put, Path, Headers, Body, ReqOpts) ->
    gun:put(Connection, Path, Headers, Body, ReqOpts);
get_stream_ref(Connection, patch, Path, Headers, Body, ReqOpts) ->
    gun:patch(Connection, Path, Headers, Body, ReqOpts);
get_stream_ref(Connection, post, Path, Headers, Body, ReqOpts) ->
    gun:post(Connection, Path, Headers, Body, ReqOpts).

make_service_name(#{scheme := Schema, host := Host} = UriMap) ->
    Port = case Schema of
        <<"http">> -> 80;
        <<"https">> -> 443
    end,
    FinalPort = maps:get(port, UriMap, Port),
    binary_to_atom(<<Host/binary, ":", (integer_to_binary(FinalPort))/binary>>, utf8).

get_request_path(UriMap) ->
    #{path := Path} = UriMap,
    case maps:is_key(query, UriMap) of
        true ->
            Query = maps:get(query, UriMap),
            <<Path/binary, "?", Query/binary>>;
        false ->
            Path
    end.
