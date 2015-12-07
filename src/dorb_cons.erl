-module(dorb_cons).
-behaviour(gen_fsm).

-export([start_link/3]).

-export([init/1,
	 handle_sync_event/4,
	 handle_event/3,
	 handle_info/3,
	 terminate/3,
	 code_change/4]).

-export([down/3]).

-type opt() :: session_timeout|member_id|protocol_type|group_protocols|
	       socket_timeout|retries.

-record(s, {
	  hosts :: [dorb:host()],
	  cg :: dorb_cg:cg(),
	  sock_timeout :: non_neg_integer(),
	  retries :: non_neg_integer()
	 }).

start_link(Hosts, GroupId, Opts) ->
    gen_fsm:start_link(?MODULE, [Hosts, GroupId, Opts], []).

init([Hosts, GroupId, Opts]) ->
    erlang:process_flag(trap_exit, true),
    SessionTimeout = default(session_timeout, Opts),
    SockTimeout = default(socket_timeout, Opts),
    MemberId = default(member_id, Opts),
    ProtocolType = default(protocol_type, Opts),
    GroupProtocols = default(group_protocols, Opts),
    Retries = default(retries, Opts),
    {ok, Cg} = dorb_cg:init(SessionTimeout, GroupId, MemberId, ProtocolType,
			    GroupProtocols),
    {ok, down, #s{hosts=Hosts, cg=Cg, sock_timeout=SockTimeout, 
		  retries=Retries}}.

down(join, _From, #s{retries=Retries}=S) ->
    case retry([fun coordinator/1, fun join/1, fun sync/1], Retries, undefined,
	       S) of
	{ok, S1} ->
	    {ok, member, member, S1};
	{error, Reason, S1} ->
	    {stop, normal, Reason, S1}
    end.

handle_sync_event(_Event, _From, SN, S) ->
    {next_state, SN, S}.

handle_event(_Event, SN, S) ->
    {next_state, SN, S}.

handle_info(_Info, SN, S) ->
    {next_state, SN, S}.

code_change(_OldVsn, SN, S, _Extra) ->
    {ok, SN, S}.

terminate(_Reason, _, #s{cg=Cg, retries=Retries}) ->
    retry([fun dorb_cg:leave/1], Retries, undefined, Cg),
    ok.

%% Internal
-spec coordinator(#s{}) -> {ok, #s{}}|{error, term(), #s{}}.
coordinator(#s{hosts=[Host|Hosts], cg=Cg, sock_timeout=ST}=S) ->
    {ok, Sock} = dorb_socket:get(Host),
    case dorb_cg:coordinator(Sock, Cg, ST) of
	{Sock1, {ok, Cg1}} ->
	    dorb_socket:return(Sock1),
	    {ok, S#s{cg=Cg1}};
	{Sock1, {error, _}=Err} ->
	    dorb_socket:return(Sock1),
	    {error, Err, S#s{hosts=[Host|Hosts]}};
	{Sock1, {conn_error, _}=Err} ->
	    dorb_socket:return_bad(Sock1),
	    {error, Err, S#s{hosts=Hosts++[Host]}}
    end.

-spec join(#s{}) -> {ok, #s{}}|{error, term(), #s{}}.
join(#s{cg=Cg, sock_timeout=ST}=S) ->
    case dorb_cg:join(Cg, ST) of
	{ok, Cg1} ->
	    {ok, S#s{cg=Cg1}};
	{error, Err, Cg1} ->
	    {error, {error, Err}, S#s{cg=Cg1}};
	{conn_error, Err, Cg1} ->
	    {error, {conn_error, Err}, S#s{cg=Cg1}}
    end.

-spec sync(#s{}) -> {ok, #s{}}|{error, term(), #s{}}.
sync(#s{cg=Cg, sock_timeout=ST}=S) ->
    case dorb_cg:sync(Cg, ST) of
	{ok, Cg1} ->
	    {ok, S#s{cg=Cg1}};
	{error, Err, Cg1} ->
	    {error, {error, Err}, S#s{cg=Cg1}};
	{conn_error, Err, Cg1} ->
	    {error, {conn_error, Err}, S#s{cg=Cg1}}
    end.

retry([], _, Retval, S) ->
    {Retval, S};
retry(_, 0, Retval, S) ->
    {Retval, S};
retry([F|Rest]=Funs, Retries, _, S) ->
    case F(S) of
	{error, Reason, S1} ->
	    retry(Funs, Retries-1, {error, Reason}, S1);
	{ok, S1} ->
	    Retries1 = 5,
	    retry(Rest, Retries1, ok, S1)
    end.

-spec default(opt(), [{opt(), term()}]) -> term().
default(session_timeout, Opts) ->
    proplists:get_value(session_timeout, Opts, 6000);
default(member_id, Opts) ->
    proplists:get_value(member_id, Opts, <<>>);
default(protocol_type, Opts) ->
    proplists:get_value(protocol_type, Opts, <<"consumer">>);
default(group_protocols, Opts) ->
    proplists:get_value(group_protocols, Opts, [{<<"range">>, <<>>}]);
default(socket_timeout, Opts) ->
    proplists:get_value(socket_timeout, Opts, 6000);
default(retries, Opts) ->
    proplists:get_value(retries, Opts, 5).

