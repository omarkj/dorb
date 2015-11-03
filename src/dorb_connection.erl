-module(dorb_connection).

-export([start_connection/1,
	 start_connection/2,
	 get_socket/1,
	 % get_socket/2,
	 return_socket/1,
	 return_socket/2,
	 return_socket/3,
	 maybe_start_pools/1
	]).

start_connection(Hosts) ->
    start_connection(default, Hosts).

start_connection(ClusterName, [Pair|Rest]) ->
    start_connection(start_pool(ClusterName, Pair), Rest);
start_connection(_, []) ->
    ok.

get_socket([Host|_]) ->
    get_socket(Host);
get_socket(Host) ->
    dorb_socket:start(Host).

return_socket(Socket) ->
    dorb_socket:stop(Socket).

%% get_socket(Pair) ->
%%     get_socket(default, Pair).
%% get_socket(_, []) ->
%%     {error, timeout};
%% get_socket(ClusterName, [Pair|Rest]) ->
%%     case get_socket(ClusterName, Pair) of
%%     	{ok, Pid} ->
%%     	    {Pair, {ok, Pid}};
%%     	{error, timeout} ->
%%     	    get_socket(ClusterName, Rest)
%%     end;
%% get_socket(ClusterName, {Host, Port}) when is_binary(Host) ->
%%     get_socket(ClusterName, {binary_to_list(Host), Port});
%% get_socket(ClusterName, Pair) ->
%%     episcina:get_connection({ClusterName, Pair}).

return_socket(Pair, Socket) ->
    return_socket(default, Pair, Socket).

return_socket(ClusterName, {Host, Port}, Socket) when is_binary(Host) ->
    return_socket(ClusterName, {binary_to_list(Host), Port}, Socket);
return_socket(ClusterName, Pair, Socket) ->
    episcina:return_connection({ClusterName, Pair}, Socket).

maybe_start_pools(Pairs) ->
    maybe_start_pools(default, Pairs).

maybe_start_pools(_, []) -> ok;
maybe_start_pools(ClusterName, [{_NodeId, Pair}|Rest]) ->
    case pool_exists(ClusterName, Pair) of
	yes ->
	    maybe_start_pools(ClusterName, Rest),
	    ok;
	no ->
	    start_pool(ClusterName, Pair),
	    maybe_start_pools(ClusterName, Rest)
    end.
	    
open_connection(Pair) ->
    dorb_socket:start_link(Pair).

close_connection(Socket) ->
    dorb_socket:stop(Socket).

start_pool(ClusterName, {K, V}) when is_binary(K) ->
    start_pool(ClusterName, {binary_to_list(K), V});
start_pool(ClusterName, Pair) ->
    episcina:start_pool({ClusterName, Pair}, 5, 10000,
			fun() -> open_connection(Pair) end,
			fun close_connection/1).

pool_exists(Cluster, {K, V}) when is_binary(K) ->
    pool_exists(Cluster, {binary_to_list(K), V});
pool_exists(Cluster, Pair) ->
    case gproc:lookup_pids({n, l, {epna_pool, {Cluster, Pair}}}) of
	[] ->
	    no;
	_ ->
	    yes
    end.
