-module('dorb').

%% API exports
-type host() :: {inet:ip_address()|inet:hostname(), inet:port_number()}.
-type hosts() :: [host()].

-export_type([hosts/0]).
%%====================================================================
%% API functions
%%====================================================================

%%====================================================================
%% Internal functions
%%====================================================================
