-module(dorb_connection).

-export([get_socket/1,
	 return_socket/1,
	 return_bad_socket/1]).

-include("dorb.hrl").

-type socket() :: #socket{}.
-export_type([socket/0]).

-spec get_socket(dorb:host()|[dorb:host()]) ->
			{ok, socket()}.
get_socket([Host|_]) ->
    get_socket(Host);
get_socket(Host) ->
    {ok, Pid} = dorb_socket:start(Host),
    {ok, #socket{pid = Pid}}.

return_socket(#socket{pid = Pid}) ->
    dorb_socket:stop(Pid).

return_bad_socket(#socket{pid = Pid}) ->
    dorb_socket:stop(Pid).
