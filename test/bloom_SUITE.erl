-module(bloom_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
	[init_api,
		bloom_start_stop,
		get_request,
		post_request,
		head_request,
		request_map_headers,
		request_put_patch_delete_options,
		request_with_query_string,
		req_arity_4_and_5,
		req_options_put_patch_delete,
		stats_exposed,
		unsupported_method,
		service_not_exists,
		stats_counters,
		req_with_timeout_opt,
		info_returns_list,
		waiter_timeout_does_not_leak,
		cancel_wait_no_late_conn,
		late_bloom_conn_lockin,
		concurrent_cap_all_succeed,
		extra_worker_two_concurrent,
		kill_worker_recovers,
		kill_holder_returns_conn,
		kill_manager_no_duplicate,
		duplicate_init_preserves_stats,
		delete_then_req_service_not_exists,
		cancel_wait_reclaims_checkout,
		lockout_timeout_slow_extra,
		lockout_call_timeout_reclaims,
		worker_init_does_not_block_manager].

init_per_suite(Config) ->
	application:ensure_all_started(bloom),
	Config.

end_per_suite(_Config) ->
	application:stop(bloom),
	ok.

init_api(_Config) ->
	{ok, Pid, Port} = http_echo_server:start(),
	Name = list_to_atom("svc_" ++ integer_to_list(Port)),
	ConnOpts = #{host => "127.0.0.1", port => Port, http_opts => #{}},
	PoolOpts = #{pool_size => 1, pool_max_size => 2},
	ok = bloom:init(Name, PoolOpts, ConnOpts),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Pid).

bloom_start_stop(_Config) ->
	ok = application:stop(bloom),
	{ok, _} = bloom:start(),
	{ok, Pid, Port} = http_echo_server:start(),
	Url = list_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),
	{ok, 200, _H, Body} = bloom:request(Url, get, [], #{}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body),
	ok = http_echo_server:stop(Pid),
	ok = bloom:stop(),
	{ok, _} = application:ensure_all_started(bloom).

get_request(_Config) ->
	{ok, _Pid, Port} = http_echo_server:start(),
	Url = list_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),
	{ok, 200, _H, Body} = bloom:request(Url, get, [], #{}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body).

post_request(_Config) ->
	{ok, _Pid, Port} = http_echo_server:start(),
	Url = list_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),
	Headers = [{<<"content-type">>, <<"text/plain">>}],
	Body = <<"payload">>,
	{ok, 200, _H, RBody} = bloom:request(Url, post, Headers, Body, #{}),
	?assertMatch(<<"OK /echo ", _/binary>>, RBody).

head_request(_Config) ->
	{ok, _Pid, Port} = http_echo_server:start(),
	Url = list_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),
	{ok, 200, _H, <<>>} = bloom:request(Url, head, [], #{}),
	ok.

request_map_headers(_Config) ->
	{ok, _Pid, Port} = http_echo_server:start(),
	Url = list_to_binary(io_lib:format("http://127.0.0.1:~p/echo", [Port])),
	H = #{<<"accept">> => <<"text/plain">>},
	{ok, 200, _H, Body} = bloom:request(Url, get, H, #{}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body).

request_put_patch_delete_options(_Config) ->
	{ok, _Pid, Port} = http_echo_server:start(),
	Base = list_to_binary(io_lib:format("http://127.0.0.1:~p/item", [Port])),
	CT = [{<<"content-type">>, <<"application/octet-stream">>}],
	{ok, 200, _, B1} = bloom:request(Base, put, CT, <<"u">>, #{}),
	?assertMatch(<<"OK /item ", _/binary>>, B1),
	{ok, 200, _, B2} = bloom:request(Base, patch, CT, <<"p">>, #{}),
	?assertMatch(<<"OK /item ", _/binary>>, B2),
	{ok, 200, _, B3} = bloom:request(Base, delete, [], #{}),
	?assertMatch(<<"OK /item ", _/binary>>, B3),
	{ok, 200, _, B4} = bloom:request(Base, options, [], #{}),
	?assertMatch(<<"OK /item ", _/binary>>, B4).

request_with_query_string(_Config) ->
	{ok, _Pid, Port} = http_echo_server:start(),
	Url = list_to_binary(io_lib:format("http://127.0.0.1:~p/search?q=1&x=y", [Port])),
	{ok, 200, _H, Body} = bloom:request(Url, get, [], #{}),
	?assertMatch(<<"OK /search?q=1&x=y ", _/binary>>, Body).

req_arity_4_and_5(_Config) ->
	{ok, Pid, Port} = http_echo_server:start(),
	Name = list_to_atom("svc_req_" ++ integer_to_list(Port)),
	ConnOpts = #{host => "127.0.0.1", port => Port, http_opts => #{}},
	PoolOpts = #{pool_size => 1, pool_max_size => 2},
	ok = bloom:init(Name, PoolOpts, ConnOpts),
	{ok, 200, _, Body4} = bloom:req(Name, get, <<"/echo">>, []),
	?assertMatch(<<"OK /echo ", _/binary>>, Body4),
	{ok, 200, _, Body5} = bloom:req(Name, post, <<"/echo">>, [{<<"content-type">>, <<"text/plain">>}], <<"z">>),
	?assertMatch(<<"OK /echo ", _/binary>>, Body5),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Pid).

req_options_put_patch_delete(_Config) ->
	{ok, Pid, Port} = http_echo_server:start(),
	Name = list_to_atom("svc_m_" ++ integer_to_list(Port)),
	ConnOpts = #{host => "127.0.0.1", port => Port, http_opts => #{}},
	PoolOpts = #{pool_size => 1, pool_max_size => 2},
	ok = bloom:init(Name, PoolOpts, ConnOpts),
	Path = <<"/res">>,
	CT = [{<<"content-type">>, <<"text/plain">>}],
	{ok, 200, _, _} = bloom:req(Name, options, Path, [], <<>>, #{}),
	{ok, 200, _, _} = bloom:req(Name, put, Path, CT, <<"1">>, #{}),
	{ok, 200, _, _} = bloom:req(Name, patch, Path, CT, <<"2">>, #{}),
	{ok, 200, _, _} = bloom:req(Name, delete, Path, [], <<>>, #{}),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Pid).

stats_exposed(_Config) ->
	{ok, _Pid, Port} = http_echo_server:start(),
	Name = list_to_atom("svc_" ++ integer_to_list(Port)),
	ConnOpts = #{host => "127.0.0.1", port => Port, http_opts => #{}},
	PoolOpts = #{pool_size => 1, pool_max_size => 2},
	ok = bloom:init(Name, PoolOpts, ConnOpts),
	_ = bloom:req(Name, get, <<"/health">>, [], <<>>, #{}),
	Info = bloom:info(),
	?assert(is_list(Info)),
	ok = bloom:delete(Name).

unsupported_method(_Config) ->
	{error, method_isnt_supported} =
		bloom:req(nonexistent_service, foo, <<"/">>, [], <<>>, #{}).

service_not_exists(_Config) ->
	{error, service_not_exists} =
		bloom:req(?MODULE, get, <<"/">>, [], <<>>, #{}).

stats_counters(_Config) ->
	{ok, _Pid, Port} = http_echo_server:start(),
	Name = list_to_atom("svc_stats_" ++ integer_to_list(Port)),
	ConnOpts = #{host => "127.0.0.1", port => Port, http_opts => #{}},
	PoolOpts = #{pool_size => 1, pool_max_size => 2},
	ok = bloom:init(Name, PoolOpts, ConnOpts),
	_ = bloom:req(Name, get, <<"/health">>, [], <<>>, #{}),
	_ = bloom:req(Name, get, <<"/health">>, [], <<>>, #{}),
	Info = bloom:info(),
	{Name, Stats} = lists:keyfind(Name, 1, Info),
	{requests, ReqStats} = lists:keyfind(requests, 1, Stats),
	{total, Total} = lists:keyfind(total, 1, ReqStats),
	{succeed, Succeed} = lists:keyfind(succeed, 1, ReqStats),
	?assertEqual(2, Total),
	?assertEqual(2, Succeed),
	ok = bloom:delete(Name).

req_with_timeout_opt(_Config) ->
	{ok, Pid, Port} = http_echo_server:start(),
	Name = list_to_atom("svc_to_" ++ integer_to_list(Port)),
	ConnOpts = #{host => "127.0.0.1", port => Port, http_opts => #{}},
	PoolOpts = #{pool_size => 1, pool_max_size => 2},
	ok = bloom:init(Name, PoolOpts, ConnOpts),
	{ok, 200, _, Body} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 60000}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Pid).

info_returns_list(_Config) ->
	Info = bloom:info(),
	?assert(is_list(Info)).

waiter_timeout_does_not_leak(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_wait_" ++ integer_to_list(Port)),
	ok = bloom:init(Name, #{pool_size => 1, pool_max_size => 1}, conn_opts(Port)),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	Parent = self(),
	Holder = spawn_link(fun() -> occupy_conn(Name, Parent, holder) end),
	ok = wait_held_msg(holder),
	{error, timeout_no_free_connections} =
		bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 300}),
	?assertEqual(0, manager_waiter_count(Name)),
	?assertEqual(1, manager_busy(Name)),
	Holder ! release,
	ok = wait_released_msg(holder),
	ok = wait_until(fun() -> manager_busy(Name) =:= 0 end, 5000),
	{ok, 200, _, Body} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 5000}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

cancel_wait_no_late_conn(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_cw_" ++ integer_to_list(Port)),
	ok = bloom:init(Name, #{pool_size => 1, pool_max_size => 1}, conn_opts(Port)),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	Parent = self(),
	Holder = spawn_link(fun() -> occupy_conn(Name, Parent, holder) end),
	ok = wait_held_msg(holder),
	Waiter = spawn_link(fun() ->
		case bloom_pool_manager:lockout(Name, 15000) of
			{error, no_free_connections, WaitRef} ->
				Parent ! {waiter, waiting, WaitRef},
				receive
					check ->
						receive
							{bloom_conn, WaitRef, Id, _Conn} ->
								Parent ! {waiter, unexpected_conn, Id}
						after 0 ->
							Parent ! {waiter, empty}
						end
				after 15000 ->
					Parent ! {waiter, check_timeout}
				end;
			Other ->
				Parent ! {waiter, unexpected_lockout, Other}
		end
	end),
	WaitRef = receive
		{waiter, waiting, Ref} -> Ref
	after 15000 ->
		error(waiter_waiting_timeout)
	end,
	ok = bloom_pool_manager:cancel_wait(Name, WaitRef),
	?assertEqual(0, manager_waiter_count(Name)),
	Holder ! release,
	ok = wait_released_msg(holder),
	ok = wait_until(fun() -> manager_busy(Name) =:= 0 end, 5000),
	Waiter ! check,
	receive
		{waiter, empty} -> ok;
		{waiter, unexpected_conn, Id} -> error({late_conn_after_cancel, Id});
		WaiterOther -> error({unexpected_waiter, WaiterOther})
	after 5000 ->
		error(waiter_check_timeout)
	end,
	ok = wait_until(fun() -> manager_busy(Name) =:= 0 end, 5000),
	{ok, 200, _, Body} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 5000}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

late_bloom_conn_lockin(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_late_" ++ integer_to_list(Port)),
	ok = bloom:init(Name, #{pool_size => 1, pool_max_size => 1}, conn_opts(Port)),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	Parent = self(),
	Holder = spawn_link(fun() -> occupy_conn(Name, Parent, holder) end),
	ok = wait_held_msg(holder),
	Waiter = spawn_link(fun() ->
		case bloom_pool_manager:lockout(Name, 15000) of
			{error, no_free_connections, WaitRef} ->
				Parent ! {waiter, waiting},
				receive
					{bloom_conn, WaitRef, Id, _Conn} ->
						ok = bloom_pool_manager:lockin(Name, Id),
						Parent ! {waiter, returned}
				after 15000 ->
					Parent ! {waiter, wait_timeout}
				end;
			Other ->
				Parent ! {waiter, unexpected_lockout, Other}
		end
	end),
	receive
		{waiter, waiting} -> ok
	after 15000 ->
		error(late_waiting_timeout)
	end,
	Holder ! release,
	ok = wait_released_msg(holder),
	receive
		{waiter, returned} -> ok;
		WaiterOther -> error({unexpected_late_waiter, WaiterOther})
	after 15000 ->
		error(late_conn_timeout)
	end,
	unlink(Waiter),
	ok = wait_until(fun() -> manager_busy(Name) =:= 0 end, 5000),
	{ok, 200, _, Body} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 5000}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

concurrent_cap_all_succeed(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_cap_" ++ integer_to_list(Port)),
	ok = bloom:init(Name, #{pool_size => 2, pool_max_size => 2}, conn_opts(Port)),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	Parent = self(),
	Pids = [spawn_link(fun() -> occupy_conn(Name, Parent, {occ, N}) end) || N <- [1, 2]],
	ok = wait_held_msg({occ, 1}),
	ok = wait_held_msg({occ, 2}),
	?assertEqual(2, manager_busy(Name)),
	[Pid ! release || Pid <- Pids],
	ok = wait_released_msg({occ, 1}),
	ok = wait_released_msg({occ, 2}),
	ok = wait_until(fun() -> manager_busy(Name) =:= 0 end, 5000),
	Parent2 = self(),
	ReqPids = [spawn_link(fun() ->
		Parent2 ! {self(), bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 5000})}
	end) || _ <- [1, 2]],
	lists:foreach(fun(Pid) ->
		receive
			{Pid, {ok, 200, _, Body}} ->
				?assertMatch(<<"OK /echo ", _/binary>>, Body)
		after 10000 ->
			error({concurrent_timeout, Pid})
		end
	end, ReqPids),
	ok = wait_until(fun() -> manager_busy(Name) =:= 0 end, 5000),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

extra_worker_two_concurrent(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_extra_" ++ integer_to_list(Port)),
	ok = bloom:init(Name, #{pool_size => 1, pool_max_size => 2}, conn_opts(Port)),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	Parent = self(),
	Pids = [spawn_link(fun() -> occupy_conn(Name, Parent, {occ, N}) end) || N <- [1, 2]],
	ok = wait_held_msg({occ, 1}),
	ok = wait_held_msg({occ, 2}),
	?assertEqual(2, length(live_workers(Name))),
	?assertEqual(2, manager_busy(Name)),
	[Pid ! release || Pid <- Pids],
	ok = wait_released_msg({occ, 1}),
	ok = wait_released_msg({occ, 2}),
	ok = wait_until(fun() -> manager_busy(Name) =:= 0 end, 5000),
	Parent2 = self(),
	ReqPids = [spawn_link(fun() ->
		Parent2 ! {self(), bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 5000})}
	end) || _ <- [1, 2]],
	lists:foreach(fun(Pid) ->
		receive
			{Pid, {ok, 200, _, Body}} ->
				?assertMatch(<<"OK /echo ", _/binary>>, Body)
		after 10000 ->
			error({extra_worker_http_timeout, Pid})
		end
	end, ReqPids),
	?assert(length(live_workers(Name)) =< 2),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

kill_worker_recovers(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_kw_" ++ integer_to_list(Port)),
	ok = bloom:init(Name, #{pool_size => 1, pool_max_size => 1}, conn_opts(Port)),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	WorkerPid = live_worker(Name),
	Mon = erlang:monitor(process, WorkerPid),
	exit(WorkerPid, kill),
	receive
		{'DOWN', Mon, process, WorkerPid, _} -> ok
	after 5000 ->
		error(worker_kill_timeout)
	end,
	ok = wait_until(fun() -> checkout_live_gun(Name) end, 15000),
	{ok, 200, _, Body} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 15000}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

kill_holder_returns_conn(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_kh_" ++ integer_to_list(Port)),
	ok = bloom:init(Name, #{pool_size => 1, pool_max_size => 1}, conn_opts(Port)),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	Parent = self(),
	Holder = spawn(fun() -> occupy_conn(Name, Parent, holder) end),
	ok = wait_held_msg(holder),
	Mon = erlang:monitor(process, Holder),
	exit(Holder, kill),
	receive
		{'DOWN', Mon, process, Holder, _} -> ok
	after 5000 ->
		error(holder_kill_timeout)
	end,
	ok = wait_until(fun() -> manager_busy(Name) =:= 0 end, 5000),
	{ok, 200, _, Body} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 5000}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

kill_manager_no_duplicate(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_km_" ++ integer_to_list(Port)),
	Max = 2,
	ok = bloom:init(Name, #{pool_size => 2, pool_max_size => Max}, conn_opts(Port)),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	Mgr = whereis(manager_name(Name)),
	?assert(is_pid(Mgr)),
	OldSup = whereis(worker_sup_name(Name)),
	OldWorkers = live_workers(Name),
	?assert(is_pid(OldSup)),
	?assert(length(OldWorkers) > 0),
	Mon = erlang:monitor(process, Mgr),
	exit(Mgr, kill),
	receive
		{'DOWN', Mon, process, Mgr, _} -> ok
	after 5000 ->
		error(manager_kill_timeout)
	end,
	ok = wait_until(fun() ->
		case whereis(manager_name(Name)) of
			undefined -> false;
			Pid -> Pid =/= Mgr
		end
	end, 5000),
	{ok, 200, _, Body} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 15000}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body),
	?assertEqual(false, is_process_alive(OldSup)),
	lists:foreach(fun(Pid) ->
		?assertEqual(false, is_process_alive(Pid))
	end, OldWorkers),
	NewSup = whereis(worker_sup_name(Name)),
	?assert(is_pid(NewSup)),
	?assert(NewSup =/= OldSup),
	WorkerCount = length(live_workers(Name)),
	?assert(WorkerCount =< Max),
	?assert(manager_connected(Name) =< Max),
	Connected = conn_stat(Name, connected),
	?assert(Connected =< Max),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

duplicate_init_preserves_stats(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_dup_" ++ integer_to_list(Port)),
	PoolOpts = #{pool_size => 1, pool_max_size => 2},
	ConnOpts = conn_opts(Port),
	ok = bloom:init(Name, PoolOpts, ConnOpts),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	?assertEqual(2, req_stat(Name, total)),
	ok = bloom:init(Name, PoolOpts, ConnOpts),
	?assertEqual(2, req_stat(Name, total)),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

delete_then_req_service_not_exists(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_del_" ++ integer_to_list(Port)),
	ok = bloom:init(Name, #{pool_size => 1, pool_max_size => 1}, conn_opts(Port)),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	ok = bloom:delete(Name),
	{error, service_not_exists} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	ok = http_echo_server:stop(Server).

cancel_wait_reclaims_checkout(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_reclaim_" ++ integer_to_list(Port)),
	ok = bloom:init(Name, #{pool_size => 1, pool_max_size => 1}, conn_opts(Port)),
	{ok, 200, _, _} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{}),
	Mgr = whereis(manager_name(Name)),
	WaitRef = make_ref(),
	{ok, _Id, _Conn} = gen_server:call(Mgr, {lockout, self(), WaitRef}),
	?assertEqual(1, manager_busy(Name)),
	ok = bloom_pool_manager:cancel_wait(Name, WaitRef),
	ok = drain_bloom_conn(WaitRef),
	?assertEqual(0, manager_busy(Name)),
	?assertEqual(0, manager_waiter_count(Name)),
	{ok, 200, _, Body} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 5000}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body),
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

lockout_timeout_slow_extra(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_slowx_" ++ integer_to_list(Port)),
	ConnOpts = (conn_opts(Port))#{connect_timeout => 10000, retry => 0},
	ok = bloom:init(Name, #{pool_size => 1, pool_max_size => 2}, ConnOpts),
	ok = wait_until(fun() ->
		maps:get(connected, bloom_pool_manager:pool_counts(Name)) >= 1
	end, 5000),
	Parent = self(),
	Holder = spawn_link(fun() -> occupy_conn(Name, Parent, holder) end),
	ok = wait_held_msg(holder),
	?assertEqual(1, manager_busy(Name)),
	ok = http_echo_server:refuse_new(Server),
	Caller = spawn_link(fun() ->
		Result = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 400}),
		Parent ! {caller, Result},
		receive stay -> ok after 30000 -> ok end
	end),
	receive
		{caller, {error, timeout_no_free_connections}} -> ok;
		{caller, Other} -> error({unexpected_caller, Other})
	after 5000 ->
		error(caller_timeout)
	end,
	?assertEqual(true, is_process_alive(Caller)),
	?assertEqual(0, manager_waiter_count(Name)),
	?assertEqual(1, manager_busy(Name)),
	Holder ! release,
	ok = wait_released_msg(holder),
	ok = wait_until(fun() -> manager_busy(Name) =:= 0 end, 5000),
	{ok, 200, _, Body} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 5000}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body),
	Caller ! stay,
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

%% Forces the HIGH leak path: gen_server:call timeout while a free connection
%% exists. OTP drops the {ok, Id, Conn} reply; cancel_wait must reclaim busy
%% without killing the living caller.
lockout_call_timeout_reclaims(_Config) ->
	{ok, Server, Port} = http_echo_server:start(),
	Name = list_to_atom("p0_gs_to_" ++ integer_to_list(Port)),
	ok = bloom:init(Name, #{pool_size => 1, pool_max_size => 1}, conn_opts(Port)),
	ok = wait_until(fun() ->
		maps:get(connected, bloom_pool_manager:pool_counts(Name)) >= 1
	end, 5000),
	?assertEqual(0, manager_busy(Name)),
	Mgr = whereis(manager_name(Name)),
	?assert(is_pid(Mgr)),
	CallTimeout = 200,
	ok = sys:suspend(Mgr),
	Parent = self(),
	Caller = spawn_link(fun() ->
		Result = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => CallTimeout}),
		Parent ! {caller, Result},
		receive stay -> ok after 30000 -> ok end
	end),
	Deadline = erlang:monotonic_time(millisecond) + CallTimeout + 150,
	ok = wait_until(fun() -> erlang:monotonic_time(millisecond) >= Deadline end, 2000),
	ok = sys:resume(Mgr),
	receive
		{caller, {error, timeout_no_free_connections}} -> ok;
		{caller, Other} -> error({unexpected_gs_timeout_caller, Other})
	after 5000 ->
		error(gs_timeout_caller_timeout)
	end,
	?assertEqual(true, is_process_alive(Caller)),
	?assertEqual(0, manager_busy(Name)),
	?assertEqual(0, manager_waiter_count(Name)),
	{ok, 200, _, Body} = bloom:req(Name, get, <<"/echo">>, [], <<>>, #{timeout => 5000}),
	?assertMatch(<<"OK /echo ", _/binary>>, Body),
	Caller ! stay,
	ok = bloom:delete(Name),
	ok = http_echo_server:stop(Server).

%% start_child / manager init must return without gun:await_up. A TCP listener
%% that never accepts keeps await_up blocked; pool_counts must still answer.
worker_init_does_not_block_manager(_Config) ->
	{ok, LSock} = gen_tcp:listen(0, [binary, {packet, raw}, {active, false}, {reuseaddr, true}]),
	{ok, Port} = inet:port(LSock),
	Name = list_to_atom("p0_async_" ++ integer_to_list(Port)),
	ConnOpts = #{host => "127.0.0.1", port => Port, http_opts => #{},
		retry => 0, connect_timeout => 8000},
	T0 = erlang:monotonic_time(millisecond),
	ok = bloom:init(Name, #{pool_size => 1, pool_max_size => 1}, ConnOpts),
	InitMs = erlang:monotonic_time(millisecond) - T0,
	?assert(InitMs < 2000),
	T1 = erlang:monotonic_time(millisecond),
	Counts = bloom_pool_manager:pool_counts(Name),
	CountsMs = erlang:monotonic_time(millisecond) - T1,
	?assert(CountsMs < 1000),
	?assertEqual(0, maps:get(busy, Counts)),
	?assertEqual(0, maps:get(connected, Counts)),
	?assert(maps:get(init, Counts) >= 1),
	ok = gen_tcp:close(LSock),
	ok = bloom:delete(Name).

conn_opts(Port) ->
	#{host => "127.0.0.1", port => Port, http_opts => #{}}.

%% Counts come from bloom_pool_manager:pool_counts/1 so tests do not depend
%% on #state{} field order.
manager_busy(Name) ->
	maps:get(busy, bloom_pool_manager:pool_counts(Name)).

manager_waiter_count(Name) ->
	maps:get(waiters, bloom_pool_manager:pool_counts(Name)).

manager_connected(Name) ->
	maps:get(connected, bloom_pool_manager:pool_counts(Name)).

checkout_live_gun(Name) ->
	case bloom_pool_manager:lockout(Name, 2000) of
		{ok, Id, Conn} ->
			Alive = is_process_alive(Conn),
			ok = bloom_pool_manager:lockin(Name, Id),
			Alive;
		{error, no_free_connections, WaitRef} ->
			ok = bloom_pool_manager:cancel_wait(Name, WaitRef),
			ok = drain_bloom_conn(WaitRef),
			false;
		_ ->
			false
	end.

drain_bloom_conn(WaitRef) ->
	receive
		{bloom_conn, WaitRef, _Id, _Conn} ->
			ok
	after 0 ->
		ok
	end.

occupy_conn(Name, Parent, Tag) ->
	case bloom_pool_manager:lockout(Name, 15000) of
		{ok, Id, _Conn} ->
			occupy_held(Name, Parent, Tag, Id);
		{error, no_free_connections, WaitRef} ->
			receive
				{bloom_conn, WaitRef, Id, _Conn} ->
					occupy_held(Name, Parent, Tag, Id)
			after 15000 ->
				Parent ! {Tag, {error, wait_timeout}}
			end;
		Other ->
			Parent ! {Tag, Other}
	end.

occupy_held(Name, Parent, Tag, Id) ->
	Parent ! {Tag, held},
	receive
		release ->
			ok = bloom_pool_manager:lockin(Name, Id),
			Parent ! {Tag, released}
	after 30000 ->
		ok = bloom_pool_manager:lockin(Name, Id),
		Parent ! {Tag, {error, occupy_timeout}}
	end.

wait_held_msg(Tag) ->
	receive
		{Tag, held} -> ok;
		{Tag, Other} -> error({unexpected_occupy, Tag, Other})
	after 15000 ->
		error({wait_held_timeout, Tag})
	end.

wait_released_msg(Tag) ->
	receive
		{Tag, released} -> ok;
		{Tag, Other} -> error({unexpected_release, Tag, Other})
	after 5000 ->
		error({wait_released_timeout, Tag})
	end.

manager_name(Name) ->
	binary_to_atom(<<"bloom_", (atom_to_binary(Name, utf8))/binary, "_pool_manager">>, utf8).

worker_sup_name(Name) ->
	binary_to_atom(<<"bloom_", (atom_to_binary(Name, utf8))/binary, "_worker_sup">>, utf8).

live_workers(Name) ->
	[Pid || {_, Pid, worker, _} <- supervisor:which_children(worker_sup_name(Name)), is_pid(Pid)].

live_worker(Name) ->
	[Pid | _] = live_workers(Name),
	Pid.

req_stat(Name, Key) ->
	{Name, Stats} = lists:keyfind(Name, 1, bloom:info()),
	{requests, ReqStats} = lists:keyfind(requests, 1, Stats),
	{Key, Value} = lists:keyfind(Key, 1, ReqStats),
	Value.

conn_stat(Name, Key) ->
	{Name, Stats} = lists:keyfind(Name, 1, bloom:info()),
	{connections, ConnStats} = lists:keyfind(connections, 1, Stats),
	{Key, Value} = lists:keyfind(Key, 1, ConnStats),
	Value.

wait_until(Fun, Timeout) ->
	Deadline = erlang:monotonic_time(millisecond) + Timeout,
	wait_until_loop(Fun, Deadline).

wait_until_loop(Fun, Deadline) ->
	case Fun() of
		true ->
			ok;
		false ->
			case erlang:monotonic_time(millisecond) >= Deadline of
				true ->
					error(wait_until_timeout);
				false ->
					receive
					after 25 ->
						wait_until_loop(Fun, Deadline)
					end
			end
	end.
