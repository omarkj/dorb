-module(dorb_consumer_group).

-export([metadata/3,
	 join/7,
	 fetch_offset/4,
	 commit_offset/4,
	 heartbeat/5]).

-spec metadata(pid(), binary(), non_neg_integer()) ->
		      {ok, term()}|
		      {kafka_error, atom()}|
		      {error, term()}.
metadata(Socket, GroupId, Timeout) ->
    Metadata = dorb_msg:group_metadata(GroupId),
    SocketResp = dorb_socket:send_sync(Socket, Metadata, Timeout),
    metadata_response(SocketResp).

join(Socket, GroupId, SessionTimeout, Topics, ConsumerId, PSA, Timeout) ->
    JoinGroup = dorb_msg:join_group(GroupId, SessionTimeout, Topics,
				    ConsumerId, PSA),
    SocketResp = dorb_socket:send_sync(Socket, JoinGroup, Timeout),
    join_response(SocketResp).

fetch_offset(Socket, GroupId, TopicsPartitions, Timeout) ->
    FetchOffset = dorb_msg:fetch_offset(GroupId, TopicsPartitions),
    SocketResp = dorb_socket:send_sync(Socket, FetchOffset, Timeout),
    fetch_offset_response(SocketResp).

commit_offset(Socket, GroupId, Topics, Timeout) ->
    CommitOffset = dorb_msg:offset_commit(GroupId, Topics),
    SocketResp = dorb_socket:send_sync(Socket, CommitOffset, Timeout),
    commit_offset_response(SocketResp).

heartbeat(Socket, GroupId, GGI, ConsumerId, Timeout) ->
    Heartbeat = dorb_msg:heartbeat(GroupId, GGI, ConsumerId),
    SocketResp = dorb_socket:send_sync(Socket, Heartbeat, Timeout),
    heartbeat_response(SocketResp).

%% Internal
heartbeat_response({ok, #{error_code := 0}}) ->
    {ok, online};
heartbeat_response({ok, #{error_code := ErrorCode}}) ->
    {kafka_error, dorb_error:error(ErrorCode)};
heartbeat_response(Error) ->
    {error, Error}.

commit_offset_response({ok, #{topics := Topics}}) ->
    Topics1 = [#{topic => TopicName,
		 partitions =>
		     [#{id => Id,
			error => dorb_error:error(Error)}
		      || #{partition := Id,
			   error_code := Error} <- Partitions]
		} || #{topic_name := TopicName,
		       partitions := Partitions} <- Topics],
    {ok, Topics1};
commit_offset_response(Error) ->
    {error, Error}.

fetch_offset_response({ok, #{topics := Topics}}) ->
    Topics1 = [#{topic => TopicName,
		 partitions => 
		     [#{id => Id,
			offset => Offset,
			error => dorb_error:error(Error)}
		      || #{partition := Id,
			   offset := Offset,
			   error_code := Error} <- Partitions]
		} || #{topic_name := TopicName,
		       partitions := Partitions} <- Topics],
    {ok, Topics1};
fetch_offset_response(Error) ->
    {error, Error}.

join_response({ok, #{error_code := 0,
		     consumer_id := ConsumerId,
		     group_generation_id := GGI,
		     partitions_to_own := PTO}}) ->

    PTO1 = [{TopicName, [Partition || #{partition := Partition} <- Partitions]}
	    || #{topic_name := TopicName, partitions := Partitions} <- PTO],
    {ok, #{consumer_id => ConsumerId,
	   ggi => GGI,
	   pto => PTO1}};
join_response({ok, #{error_code := ErrorCode}}) ->
    {kafka_error, dorb_error:error(ErrorCode)};
join_response(Error) ->
    {error, Error}.

metadata_response({ok, #{error_code := 0,
			 coordinator_host := Host,
			 coordinator_port := Port}}) ->
    {ok, {binary_to_list(Host), Port}};
metadata_response({ok, #{error_code := ErrorCode}}) ->
    {kafka_error, dorb_error:error(ErrorCode)};
metadata_response(Error) ->
    {error, Error}.

%% Tests
-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

%% These tests require Kafka running on localhost (9092)

%     [#{partitions => [#{partition => 0}],topic_name => <<"test">>}]
%% metadata_test() ->
%%     {ok, Socket} = dorb_socket:start_link({"localhost", 9092}),
%%     ?assertMatch({_, _}, metadata(Socket, <<"MetaDataTestGroup">>, 1000)),
%%     dorb_socket:stop(Socket).

%% join_test() ->
%%     {ok, Socket} = dorb_socket:start_link({"localhost", 9092}),
%%     % Kafka is fickle. It can take much more than 2 second to get a reply back
%%     % from it, even over localhost, so this test is non-determenistic.
%%     JoinResp = join(Socket, <<"JoinGroupTest">>, 10000, [<<"test">>], <<>>,
%% 		    <<"range">>, 2000),
%%     ?assertMatch({ok, #{consumer_id := _,
%% 			ggi := _,
%% 			pto := _}}, JoinResp).

%% % fetch_offset(Socket, GroupId, TopicsPartitions, Timeout) ->
%% fetch_offset_test() ->
%%     {ok, Socket} = dorb_socket:start_link({"localhost", 9092}),
%%     {ok, #{consumer_id := _,
%% 	   pto := _ }} = join(Socket, <<"FetchOffsetTest">>, 10000,
%% 			      [<<"test">>], <<>>, <<"range">>, 2000),
%%     X = fetch_offset(Socket, <<"FetchOffsetTest">>, [{<<"test">>, [0]}], 2000),
%%     ?assertMatch({ok, [#{partitions :=
%% 			     [#{error:= unknown_topic_or_partition,
%% 				id := 0,
%% 				offset := -1}]}]}, X).

%% commit_offset_test() ->
%%     {ok, Socket} = dorb_socket:start({"localhost", 9092}),
%%     ?assertMatch({_, _}, metadata(Socket, <<"CommitOffsetTest">>, 1000)),
%%     {ok, #{consumer_id := _,
%% 	   pto := _ }} = join(Socket, <<"CommitOffsetTest">>, 10000,
%% 			      [<<"test">>], <<>>, <<"range">>, 2000),
%%     {ok, _} = commit_offset(Socket, <<"CommitOffsetTest">>,
%% 			    [{<<"test">>, [{0, 0, erlang:system_time(),
%% 				      <<>>}]}], 2000),
%%     ?assertMatch({ok,
%% 		  [#{partitions :=
%% 			 [#{offset := 0}]}]}, 
%% 		 fetch_offset(Socket, <<"CommitOffsetTest2">>,
%% 			      [{<<"test">>, [0]}], 2000)).

%% % heartbeat(Socket, GroupId, GGI, ConsumerId, Timeout) ->
%% heartbeat_test() ->
%%     {ok, Socket} = dorb_socket:start({"localhost", 9092}),
%%     {ok, #{consumer_id := CID,
%% 	   ggi := GGI,
%%  	   pto := _ }} = join(Socket, <<"HeartbeatTest">>, 10000,
%% 			      [<<"test">>], <<>>, <<"range">>, 2000),
%%     ?assertEqual({ok, online}, heartbeat(Socket, <<"HeartbeatTest">>, GGI, CID,
%% 					 2000)).

-endif.
