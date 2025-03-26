-module(bloom_stats).
-export([info/0]).
-export([init/0]).
-export([add/1]).
-export([delete/1]).
-export([lookup/1]).
-export([update/3, update_counter/2]).

-record(stat, {
    name,
    req_total = 0,
    req_succeed = 0,
    req_failed = 0,
    req_top = [],
    conn_broken = 0,
    conn_info = #{},
    conn_max_used = 0,
    rps_req_count = 0,
    rps_start_time = os:system_time(seconds)
}).

-define(AMOUNT_TOP_REQ, 10).

init() ->
    KeyPos = #stat.name,
    _ = ets:new(?MODULE, [named_table, public, {keypos, KeyPos}, {write_concurrency, true}]),
    ok.

add(Name) ->
    true = ets:insert(?MODULE, #stat{name = Name}),
    ok.

delete(Name) ->
    true = ets:delete(?MODULE, Name),
    ok.

info() ->
    ListOfServices = ets:tab2list(?MODULE),
    [begin
        ConnInfo = Info#stat.conn_info,
        Name = Info#stat.name,
        Rps = calculate_rps(Info),
        MaxConnected = calculate_max_connected(Info),
        TopRequests = take_top_req(Name, Info#stat.req_top),
        {Name, [
            {pool_config, [
                {pool_size, maps:get(pool_size, ConnInfo, 0)},
                {pool_max_size, maps:get(pool_max_size, ConnInfo, 0)}
            ]},
            {connections, [
                {ready, maps:get(ready, ConnInfo, 0)},
                {busy, maps:get(busy, ConnInfo, 0)},
                {init, maps:get(init, ConnInfo, 0)},
                {connected,  maps:get(connected, ConnInfo, 0)},
                {max_connected, MaxConnected},
                {total_broken, Info#stat.conn_broken}
            ]},
            {requests, [
                {rps, Rps},
                {total, Info#stat.req_total},
                {succeed, Info#stat.req_succeed},
                {failed, Info#stat.req_failed},
                {now_waiting, maps:get(now_waiting, ConnInfo, 0)},
                {total_waiting, maps:get(total_waiting, ConnInfo, 0)},
                {'top longest', TopRequests}
            ]}
        ]}
    end || Info <- ListOfServices].

lookup(Key) ->
    ets:lookup_element(?MODULE, Key, 2).

update(Name, execution_time, MethodTime) ->
    Position = get_position(req_top),
    TopTenReq = ets:lookup_element(?MODULE, Name, Position),
    update(Name, req_top, [MethodTime|TopTenReq]);
update(Name, Param, Value) ->
    Position = get_position(Param),
    true = ets:update_element(?MODULE, Name, {Position, Value}),
    ok.

update_counter(Name, req_total) ->
    ReqTotalPos = get_position(req_total),
    RpsCountPos = get_position(rps_req_count),
    _ = ets:update_counter(?MODULE, Name, [{ReqTotalPos, 1}, {RpsCountPos, 1}]),
    ok;
update_counter(Name, Param) ->
    Position = get_position(Param),
    _ = ets:update_counter(?MODULE, Name, {Position, 1}),
    ok.

take_top_req(Name, AllTopReq) ->
    Fun = fun({_, Time1}, {_, Time2}) ->
        Time1 > Time2
    end,
    SortedList = lists:sort(Fun, AllTopReq),
    TopReq = take_away_top_req(SortedList, []),
    ok = update(Name, req_top, TopReq),
    TopReq.

take_away_top_req([], TopReq) ->
    lists:reverse(TopReq);
take_away_top_req(_, TopReq) when length(TopReq) == ?AMOUNT_TOP_REQ ->
    lists:reverse(TopReq);
take_away_top_req([{Path, ExecTime}|RestReq], TopReq) ->
    NewTopReq = case lists:keyfind(Path, 1, TopReq) of
        false ->
            [{Path, ExecTime}|TopReq];
        {Path, _} ->
            TopReq
    end,
    take_away_top_req(RestReq, NewTopReq).

calculate_rps(#stat{name = Name, rps_req_count = ReqCount, rps_start_time = RpsStartTime} = _) ->
    CurrentTime = os:system_time(seconds),
    ok = update(Name, rps_req_count, 0),
    ok = update(Name, rps_start_time, CurrentTime),
    case CurrentTime - RpsStartTime of
        0 -> ReqCount;
        Time -> round(ReqCount/Time)
    end.

calculate_max_connected(#stat{name = Name, conn_max_used = MaxConnected, conn_info = ConnInfo} = _) ->
    Connected = maps:get(connected, ConnInfo, 0),
    case Connected > MaxConnected of
        true ->
            Position = get_position(conn_max_used),
            true = ets:update_element(?MODULE, Name, {Position, Connected}),
            Connected;
        false ->
            MaxConnected
    end.

get_position(req_total)         -> #stat.req_total;
get_position(req_succeed)       -> #stat.req_succeed;
get_position(req_failed)        -> #stat.req_failed;
get_position(req_top)           -> #stat.req_top;
get_position(conn_broken)       -> #stat.conn_broken;
get_position(conn_info)         -> #stat.conn_info;
get_position(conn_max_used)     -> #stat.conn_max_used;
get_position(rps_req_count)     -> #stat.rps_req_count;
get_position(rps_start_time)    -> #stat.rps_start_time.



