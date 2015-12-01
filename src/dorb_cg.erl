-module(dorb_cg).

-export([coordinator/2,
	 coordinator/3,
	 join/5,
	 join/6,
	 sync/3,
	 sync/2,
	 heartbeat/1,
	 heartbeat/2,
	 leave/1,
	 leave/2]).

-record(coor, {host :: inet:ip_address()|inet:hostname(),
	       port :: inet:port_number(),
	       id :: non_neg_integer()}).

-record(cg, {group_id :: binary(),
	     member_id :: binary(),
	     leader_id :: binary(),
	     generation_id :: integer(),
	     group_protocol :: binary(),
	     session_timeout :: non_neg_integer(),
	     coor :: #coor{}}).

-opaque cg() :: #cg{}.
-export_type([cg/0]).

-define(TIMEOUT, 5000).

% @doc Discover the coordinator for this particular group. If this group does
% not exists it will be created, and a coordinator will be returned. This does
% not retry - that's something the caller should do.
coordinator(Connection, GroupId) ->
    coordinator(Connection, GroupId, ?TIMEOUT).

-spec coordinator(dorb_socket:socket(), dorb_msg:group_id(), integer()) ->
			 {dorb_socket:socket(), {ok, cg()}|
			  {error, dorb_error:msg()}}.
coordinator(Connection, GroupId, Timeout) ->
    Message = dorb_msg:group_coordinator(GroupId),
    Response = dorb_socket:send_sync(Connection, Message, Timeout),
    coordinator_response(Response, Connection, #cg{group_id=GroupId}).

% @doc Join a group. The consumer issues a join group request advertising what
% partition protocols it supports. The response to this request might indicate
% that consumer becomes the leader.
join(SessionTimeout, MemberId, ProtocolType, GroupProtocols, Cg) ->
    join(SessionTimeout, MemberId, ProtocolType, GroupProtocols, Cg,
	 ?TIMEOUT).

-spec join(dorb_msg:session_timeout(), dorb_msg:member_id(),
	   dorb_msg:protocol_type(), dorb_msg:group_protocols(), cg(),
	   integer()) ->
		  {ok, {joining, member, integer()}, cg()}|
		  {ok, {joining, leader, [#{member_id => binary(),
					    member_metadata => binary()}],
			integer()}, cg()}|
		  {error, dorb_error:msg(), cg()}|
		  {conn_error, term(), cg()}.
join(SessionTimeout, MemberId, ProtocolType, GroupProtocols,
     #cg{group_id = Gid, coor = Coor} = Cg,
     Timeout) ->
    Message = dorb_msg:join_group(Gid, SessionTimeout, MemberId, ProtocolType,
				  GroupProtocols),
    Response = send_sync(Coor, Message, Timeout),
    join_response(Response, Cg#cg{session_timeout = SessionTimeout}).

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
sync_response(#{error_coder := ErrorCode}, Cg) ->
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
    {ok, {joining, member, St}, Cg#cg{member_id = Mid,
				      leader_id = Lid,
				      generation_id = Geid,
				      group_protocol = Gproto}};
join_response(#{error_code := 0,
		generation_id := Geid,
		group_protocol := Gproto,
		leader_id := Lid,
		member_id := Mid,
		members := Members}, #cg{session_timeout = St} = Cg) ->
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
	{ok, Message} ->
	    dorb_socket:return_connection(Connection),
	    Message;
	{error, Error} ->
	    dorb_socket:return_bad(Connection),
	    {conn_error, Error}
    end.
