-module('dorb').

%% API exports
-export([start/1,
	 start/2]).

-type host() :: {inet:ip_address()|inet:hostname(), inet:port_number()}.
-type hosts() :: [host()].

-export_type([hosts/0]).
%%====================================================================
%% API functions
%%====================================================================
start(Hosts) ->
    start(default, Hosts).

start(ClusterName, Hosts) ->
    dorb_connection:start_connection(ClusterName, Hosts).

%%====================================================================
%% Internal functions
%%====================================================================
