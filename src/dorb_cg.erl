-module(dorb_cg).

-export([coordinator/2,
	 coordinator/3,
	 join_group/5,
	 join_group/6]).

-record(coor, {host :: inet:ip_address()|inet:hostname(),
	       port :: inet:port_number(),
	       id :: non_neg_integer() }).

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
join_group(SessionTimeout, MemberId, ProtocolType, GroupProtocols, Cg) ->
    join_group(SessionTimeout, MemberId, ProtocolType, GroupProtocols, Cg,
	       ?TIMEOUT).

-spec join_group(dorb_msg:session_timeout(), dorb_msg:member_id(),
		 dorb_msg:protocol_type(), term(), cg(), integer()) ->
			{ok, {joined, member, integer()}, cg()}|
			{ok, {joined, leader, [#{member_id => binary(),
						 member_metadata => binary()}],
			      integer()}, cg()}|
			{error, dorb_error:msg(), cg()}|
			{conn_error, term(), cg()}.
join_group(SessionTimeout, MemberId, ProtocolType, GroupProtocols,
	   #cg{group_id = Gid, coor = Coor} = Cg,
	   Timeout) ->
    Message = dorb_msg:join_group(Gid, SessionTimeout, MemberId, ProtocolType,
				  GroupProtocols),
    Response = send_sync(Coor, Message, Timeout),
    join_group_response(Response, Cg#cg{session_timeout = SessionTimeout}).

join_group_response(#{error_code     := 0,
		      generation_id  := Geid,
		      group_protocol := Gproto,
		      leader_id      := Lid,
		      member_id      := Mid,
		      members        := []},
		    #cg{session_timeout = St} = Cg) ->
    % This is not a leader
    % @todo indicate to the caller to set a timeout to SessionTimeout since that
    % is the maximum time the client should wait to 
    {ok, {joined, member, St}, Cg#cg{member_id = Mid,
				     leader_id = Lid,
				     generation_id = Geid,
				     group_protocol = Gproto}};
join_group_response(#{error_code := 0,
		      generation_id := Geid,
		      group_protocol := Gproto,
		      leader_id := Lid,
		      member_id := Mid,
		      members := Members}, #cg{session_timeout = St} = Cg) ->
    % This is a leader.
    {ok, {joined, leader, Members, St}, Cg#cg{member_id = Mid,
					      leader_id = Lid,
					      generation_id = Geid,
					      group_protocol = Gproto}};
join_group_response(#{error_code := ErrorCode}, Cg) ->
    {error, dorb_error:error(ErrorCode), Cg};
join_group_response({conn_error, Error}, Cg) ->
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
