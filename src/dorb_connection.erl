-module(dorb_connection).

-export([get_socket/1,
	 return_socket/1
	]).

get_socket([Host|_]) ->
    get_socket(Host);
get_socket(Host) ->
    dorb_socket:start(Host).

return_socket(Socket) ->
    dorb_socket:stop(Socket).
