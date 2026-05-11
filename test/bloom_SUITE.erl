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
		info_returns_list].

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
