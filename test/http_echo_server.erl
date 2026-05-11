-module(http_echo_server).

-export([start/0, stop/1, port/1]).

start() ->
	{ok, LSock} = gen_tcp:listen(0, [binary, {packet, raw}, {active, false}, {reuseaddr, true}, {backlog, 128}]),
	Port = port(LSock),
	Pid = spawn_link(fun() -> acceptor_loop(LSock) end),
	{ok, Pid, Port}.

stop(Pid) ->
	exit(Pid, shutdown),
	ok.

port(LSock) when is_port(LSock) ->
	{ok, Port} = inet:port(LSock),
	Port.

acceptor_loop(LSock) ->
	process_flag(trap_exit, true),
	case gen_tcp:accept(LSock) of
		{ok, Sock} ->
			spawn_link(fun() -> handle_conn(Sock) end),
			acceptor_loop(LSock);
		Error ->
			Error
	end.

handle_conn(Sock) ->
	inet:setopts(Sock, [{active, false}]),
	case recv_headers(Sock, <<>>) of
		{ok, Method, Path, Headers, BodyLen} ->
			Body = case BodyLen of
				0 -> <<>>;
				N -> recv_exact(Sock, N)
			end,
			ReplyBody = reply_body(Method, Path, Headers, Body),
			Reply = build_response(ReplyBody),
			_ = gen_tcp:send(Sock, Reply),
			gen_tcp:close(Sock);
		_ ->
			gen_tcp:close(Sock)
	end.

recv_headers(Sock, Acc) ->
	case gen_tcp:recv(Sock, 0, 5000) of
		{ok, Data} ->
			Bin = <<Acc/binary, Data/binary>>,
			case binary:match(Bin, <<"\r\n\r\n">>) of
				{_Pos, _Len} -> parse_request(Bin);
				nomatch -> recv_headers(Sock, Bin)
			end;
		_ -> {error, timeout}
	end.

parse_request(Bin) ->
	%% Split headers from body
	{HeadersBin, Rest} = split_headers(Bin),
	[ReqLine|HeaderLines] = binary:split(HeadersBin, <<"\r\n">>, [global]),
	[MethodBin, PathBin | _] = binary:split(ReqLine, <<" ">>, [global]),
	Headers = parse_headers(HeaderLines, #{}),
	BodyLen = maps:get(<<"content-length">>, Headers, 0),
	{ok, MethodBin, PathBin, Headers, BodyLen - byte_size(Rest)}.

split_headers(Bin) ->
	Delim = <<"\r\n\r\n">>,
	{Pos, _} = binary:match(Bin, Delim),
	HeadersLen = Pos,
	<<HeadersBin:HeadersLen/binary, _Delim:4/binary, Rest/binary>> = Bin,
	{HeadersBin, Rest}.

parse_headers([], Acc) ->
	Acc;
parse_headers([<<>>|T], Acc) ->
	parse_headers(T, Acc);
parse_headers([Line|T], Acc) ->
	case binary:split(Line, <<": ">>) of
		[Key, Val] ->
			LowerKey = to_lower(Key),
			ValInt = case LowerKey of
				<<"content-length">> -> list_to_integer(binary_to_list(Val));
				_ -> Val
			end,
			parse_headers(T, maps:put(LowerKey, ValInt, Acc));
		_ -> parse_headers(T, Acc)
	end.

to_lower(B) ->
	list_to_binary(string:lowercase(binary_to_list(B))).

recv_exact(Sock, 0) -> <<>>;
recv_exact(Sock, N) ->
	case gen_tcp:recv(Sock, N, 5000) of
		{ok, Data} when byte_size(Data) =:= N -> Data;
		{ok, Data} -> <<Data/binary, (recv_exact(Sock, N - byte_size(Data)))/binary>>;
		_ -> <<>>
	end.

reply_body(<<"HEAD">>, _Path, _Headers, _Body) -> <<>>;
reply_body(_Method, Path, _Headers, Body) ->
	%% Echo path and body length
	BodyLen = integer_to_binary(byte_size(Body)),
	<<"OK ", Path/binary, " ", BodyLen/binary>>.

build_response(Body) ->
	Status = <<"HTTP/1.1 200 OK\r\n">>,
	CL = integer_to_binary(byte_size(Body)),
	Headers = <<
		"content-type: text/plain\r\n",
		"content-length: ", CL/binary, "\r\n",
		"connection: close\r\n",
		"\r\n"
	>>, 
	<<Status/binary, Headers/binary, Body/binary>>.


