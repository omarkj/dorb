-module(dorb_cg).

-export([init/5,
	 coordinator/2,
	 coordinator/3,
	 join/1,
	 join/2,
	 sync/3,
	 sync/2,
	 heartbeat/1,
	 heartbeat/2,
	 leave/1,
	 leave/2]).

-record(coor, {host :: inet:ip_address()|inet:hostname(),
	       port :: inet:port_number(),
	       id   :: non_neg_integer()}).

-record(cg, {group_id        :: dorb_msg:group_id(),
	     member_id       :: dorb_msg:member_id(),
	     leader_id       :: dorb_msg:leader_id(),
	     generation_id   :: dorb_msg:generation_id(),
	     group_protocol  :: dorb_msg:group_protocol(),
	     group_protocols :: dorb_msg:group_protocols(),
	     session_timeout :: dorb_msg:session_timeout(),
	     protocol_type   :: dorb_msg:protocol_type(),
	     coor            :: #coor{}}).

-opaque cg() :: #cg{}.
-export_type([cg/0]).

-define(TIMEOUT, 5000).

-spec init(dorb_msg:session_timeout(), dorb_msg:group_id(), 
	   dorb_msg:member_id(), dorb_msg:protocol_type(),
	   dorb_msg:group_protocols()) -> {ok, cg()}.
init(SessionTimeout, GroupId, MemberId, ProtocolType, GroupProtocols) ->
    {ok, #cg{session_timeout=SessionTimeout,
	     group_id=GroupId,
	     member_id=MemberId,
	     protocol_type=ProtocolType,
	     group_protocols=GroupProtocols}}.

% @doc Discover the coordinator for this particular group. If this group does
% not exists it will be created, and a coordinator will be returned. This does
% not retry - that's something the caller should do.
coordinator(Connection, Cg) ->
    coordinator(Connection, Cg, ?TIMEOUT).

-spec coordinator(dorb_socket:socket(), cg(), integer()) ->
			 {dorb_socket:socket(),
			  {ok, cg()}|{error, dorb_error:msg()}|
			  {conn_error, term()}}.
coordinator(Connection, #cg{group_id=GroupId}=Cg, Timeout) ->
    Message = dorb_msg:group_coordinator(GroupId),
    Response = dorb_socket:send_sync(Connection, Message, Timeout),
    coordinator_response(Response, Connection, Cg).

% @doc Join a group. The consumer issues a join group request advertising what
% partition protocols it supports. The response to this request might indicate
% that consumer becomes the leader.
join(Cg) ->
    join(Cg, ?TIMEOUT).

-spec join(cg(), integer()) ->
		  {ok, {joining, member, integer()}, cg()}|
		  {ok, {joining, leader, [#{member_id => binary(),
					    member_metadata => binary()}],
			integer()}, cg()}|
		  {error, dorb_error:msg(), cg()}|
		  {conn_error, term(), cg()}.
join(#cg{group_id=Gid, coor=Coor, session_timeout=SessionTimeout,
	 member_id=MemberId, protocol_type=ProtocolType,
	 group_protocols=GroupProtocols}=Cg, Timeout) ->
    Message = dorb_msg:join_group(Gid, SessionTimeout, MemberId, ProtocolType,
				  GroupProtocols),
    Response = send_sync(Coor, Message, Timeout),
    join_response(Response, Cg#cg{session_timeout = SessionTimeout}).

% @doc Sync the group. If the caller is the leader it should include the group
% assigment of all member nodes. If the caller is not the leader it should not
% include any group assigmnet information.
sync(GroupAssignment, Cg) ->
    sync(GroupAssignment, Cg, ?TIMEOUT).

-spec sync(dorb_msg:group_assignment(), cg(), integer()) ->
		  {ok, {stable, binary()}, cg()}|
		  {error, dorb_error:msg(), cg()}|{conn_error, term(), cg()}.
sync(GroupAssignment, #cg{group_id = GroupId, generation_id = GenerationId,
			  member_id = MemberId, coor = Coor} = Cg, Timeout) ->
    Message = dorb_msg:sync_group(GroupId, GenerationId, MemberId,
				  GroupAssignment),
    Response = send_sync(Coor, Message, Timeout),
    sync_response(Response, Cg).

heartbeat(Cg) ->
    heartbeat(Cg, ?TIMEOUT).

% @doc A caller needs to call heartbeats at minimum once every SessionTimeout
% but should be called a bit more often because of latency, etc.
-spec heartbeat(cg(), integer()) ->
		       {ok, cg()}|{error, dorb_error:msg()}|
		       {conn_error, term(), cg()}.
heartbeat(#cg{group_id = GroupId, generation_id = GenerationId,
	      member_id = MemberId, coor = Coor} = Cg, Timeout) ->
    Message = dorb_msg:heartbeat(GroupId, GenerationId, MemberId),
    Response = send_sync(Coor, Message, Timeout),
    heartbeat_response(Response, Cg).

leave(Cg) ->
    leave(Cg, ?TIMEOUT).

% @doc A member can leave the group two ways; by no longer sending heartbeats or
% by sending the leave request. The leave requests minimizes the time it takes
% to rebalance the group from SessionTimeout + rebalance time to just rebalance
% time.
-spec leave(cg(), integer()) ->
		   {ok, cg()}|{error, dorb_error:msg()}|
		   {conn_error, term(), cg()}.
leave(#cg{coor = Coor, group_id = GroupId, member_id = MemberId} = Cg,
      Timeout) ->
    Message = dorb_msg:leave_group(GroupId, MemberId),
    Response = send_sync(Coor, Message, Timeout),
    leave_response(Response, Cg).

leave_response(#{error_code := 0}, Cg) ->
    {ok, Cg};
leave_response(#{error_code := ErrorCode}, Cg) ->
    {error, dorb_error:error(ErrorCode), Cg};
leave_response({conn_error, Error}, Cg) ->
    {conn_error, Error, Cg}.

heartbeat_response(#{error_code := 0}, Cg) ->
    {ok, Cg};
heartbeat_response(#{error_code := ErrorCode}, Cg) ->
    {error, dorb_error:error(ErrorCode), Cg};
heartbeat_response({conn_error, Error}, Cg) ->
    {conn_error, Error, Cg}.

sync_response(#{error_code        := 0,
		member_assignment := MemberAssignment}, Cg) ->
    {ok, {stable, MemberAssignment}, Cg};
sync_response(#{error_code := ErrorCode}, Cg) ->
    {error, dorb_error:error(ErrorCode), Cg};
sync_response({conn_error, Error}, Cg) ->
    {conn_error, Error, Cg}.

join_response(#{error_code     := 0,
		generation_id  := Geid,
		group_protocol := Gproto,
		leader_id      := Lid,
		member_id      := Mid,
		members        := []},
	      #cg{session_timeout = St} = Cg) ->
    % This is not a leader
    {ok, {joining, member, St}, Cg#cg{member_id      = Mid,
				      leader_id      = Lid,
				      generation_id  = Geid,
				      group_protocol = Gproto}};
join_response(#{error_code     := 0,
		generation_id  := Geid,
		group_protocol := Gproto,
		leader_id      := Lid,
		member_id      := Mid,
		members        := Members},
	      #cg{session_timeout = St} = Cg) ->
    % This is a leader.
    {ok, {joining, leader, Members, St}, Cg#cg{member_id = Mid,
					       leader_id = Lid,
					       generation_id = Geid,
					       group_protocol = Gproto}};
join_response(#{error_code := ErrorCode}, Cg) ->
    {error, dorb_error:error(ErrorCode), Cg};
join_response({conn_error, Error}, Cg) ->
    {conn_error, Error, Cg}.

coordinator_response({ok, #{error_code       := 0,
			    coordinator_id   := Id,
			    coordinator_host := Host,
			    coordinator_port := Port}}, Connection, Cg) ->
    {Connection, {ok, Cg#cg{coor = #coor{host=Host,
					 port=Port,
					 id=Id}}}};
coordinator_response({ok, #{error_code := ErrorCode}}, Connection, _) ->
    {Connection, {error, dorb_error:error(ErrorCode)}};
coordinator_response({error, Error}, Connection, _) ->
    {Connection, {conn_error, Error}}.

send_sync(#coor{host = Host, port = Port}, Message, Timeout) ->
    {ok, Connection} = dorb_socket:get({Host, Port}),
    case dorb_socket:send_sync(Connection, Message, Timeout) of
	{ok, Message1} ->
	    dorb_socket:return(Connection),
	    Message1;
	{error, Error} ->
	    dorb_socket:return_bad(Connection),
	    {conn_error, Error}
    end.

%% Tests
-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

-include("dorb.hrl").

coordinator_test() ->
    {ok, Cg} = init(5000, <<"group">>, <<>>, <<"consumer">>, [<<"range">>,
							      <<>>]),
    meck:new(dorb_socket),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{ok, #{error_code => 0,
			       coordinator_id => 1,
			       coordinator_host => "localhost",
			       coordinator_port => 9092}} end),
    Res = coordinator(#socket{}, Cg),
    ?assertMatch({#socket{},
		  {ok, #cg{coor = #coor{host = "localhost",
					port = 9092,
					id = 1},
			   group_id = <<"group">>}}}, Res),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{ok, #{error_code => 16}} end),
    Res1 = coordinator(#socket{}, Cg),
    ?assertMatch({#socket{}, {error, not_coordinator_for_consumer}}, Res1),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{error, connerr} end),
    Res2 = coordinator(#socket{}, Cg),
    ?assertMatch({#socket{}, {conn_error, timeout}}, Res2),
    meck:unload(dorb_socket).

join_test() ->
    meck:new(dorb_socket),
    {ok, Cg0} = init(5000, <<"group">>, <<>>, <<"consumer">>, [<<"range">>,
							       <<>>]),
    Cg = Cg0#cg{coor = #coor{}},
    meck:expect(dorb_socket, get,
		fun({undefined, undefined}) ->
			{ok, connection}
		end),
    meck:expect(dorb_socket, return, fun(connection) -> ok end),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{ok, #{error_code => 0,
			       generation_id => 1,
			       group_protocol => <<"range">>,
			       leader_id => 1,
			       member_id => 2,
			       members => []}}
		end),
    Res = join(Cg),
    ?assertMatch({ok, {joining, member, 5000},
		  #cg{leader_id = 1,
		      member_id = 2,
		      generation_id = 1,
		      session_timeout = 5000,
		      group_protocol = <<"range">>}}, Res),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{ok, #{error_code => 0,
			       generation_id => 1,
			       group_protocol => <<"range">>,
			       leader_id => 1,
			       member_id => 2,
			       members => [#{member_id => <<"node1">>,
					     member_metadata => <<>>}]}}
		end),
    Res1 = join(Cg),
    ?assertMatch({ok, {joining, leader, [#{member_id := <<"node1">>,
					   member_metadata := <<>>}], 5000},
		  #cg{leader_id = 1,
		      member_id = 2,
		      generation_id = 1,
		      session_timeout = 5000,
		      group_protocol = <<"range">>}}, Res1),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{ok, #{error_code => 16}}
		end),
    Res2 = join(Cg),
    ?assertMatch({error, not_coordinator_for_consumer,
		  #cg{session_timeout = 5000}}, Res2),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{error, connerr} end),
    meck:expect(dorb_socket, return_bad, fun(connection) -> ok end),
    Res3 = join(Cg),
    ?assertMatch({conn_error, timeout, #cg{session_timeout = 5000}}, Res3),
    meck:validate(dorb_socket),
    meck:unload(dorb_socket).

sync_test() ->
    meck:new(dorb_socket),
    Cg = #cg{group_id = <<"test">>, generation_id = 1, member_id = <<"member">>,
	     coor = #coor{}},
    meck:expect(dorb_socket, get,
		fun({undefined, undefined}) ->
			{ok, connection}
		end),
    meck:expect(dorb_socket, return, fun(connection) -> ok end),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{ok, #{error_code => 0,
			       member_assignment => <<"sync">>}}
		end),
    Res = sync([{<<"node1">>, <<"details">>}], Cg),
    ?assertMatch({ok, {stable, <<"sync">>}, Cg}, Res),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{ok, #{error_code => 16}}
		end),
    Res2 = sync([{<<"node1">>, <<"details">>}], Cg),
    ?assertMatch({error, not_coordinator_for_consumer, Cg}, Res2),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{error, connerr} end),
    meck:expect(dorb_socket, return_bad, fun(connection) -> ok end),
    Res3 = sync([{<<"node1">>, <<"details">>}], Cg),
    ?assertMatch({conn_error, timeout, Cg}, Res3),
    meck:validate(dorb_socket),
    meck:unload(dorb_socket).

heartbeat_test() ->
    meck:new(dorb_socket),
    Cg = #cg{group_id = <<"test">>, generation_id = 1, member_id = <<"member">>,
	     coor = #coor{}},
    meck:expect(dorb_socket, get,
		fun({undefined, undefined}) ->
			{ok, connection}
		end),
    meck:expect(dorb_socket, return, fun(connection) -> ok end),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{ok, #{error_code => 0}}
		end),
    Res = heartbeat(Cg),
    ?assertMatch({ok, Cg}, Res),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{ok, #{error_code => 16}}
		end),
    Res1 = heartbeat(Cg),
    ?assertMatch({error, not_coordinator_for_consumer, Cg}, Res1),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{error, connerr} end),
    meck:expect(dorb_socket, return_bad, fun(connection) -> ok end),
    Res2 = heartbeat(Cg),
    ?assertMatch({conn_error, timeout, Cg}, Res2),
    meck:validate(dorb_socket),
    meck:unload(dorb_socket).

leave_test() ->
    Cg = #cg{group_id = <<"test">>, generation_id = 1, member_id = <<"member">>,
	     coor = #coor{}},
    meck:expect(dorb_socket, get,
		fun({undefined, undefined}) ->
			{ok, connection}
		end),
    meck:expect(dorb_socket, return, fun(connection) -> ok end),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{ok, #{error_code => 0}}
		end),
    Res = leave(Cg),
    ?assertMatch({ok, Cg}, Res),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{ok, #{error_code => 16}}
		end),
    Res1 = leave(Cg),
    ?assertMatch({error, not_coordinator_for_consumer, Cg}, Res1),
    meck:expect(dorb_socket, send_sync,
		fun(_Connection, _Message, _Timeout) ->
			{error, connerr} end),
    meck:expect(dorb_socket, return_bad, fun(connection) -> ok end),
    Res2 = leave(Cg),
    ?assertMatch({conn_error, timeout, Cg}, Res2),
    meck:validate(dorb_socket),
    meck:unload(dorb_socket).

-endif.
