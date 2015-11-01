-module(dorb_msg).

% This module is here to make it easier to create requests when working with the
% library. This is helpful since the encoder has a funky interface.

% This module has a dumb name.

-compile(export_all). % @todo REMOVE ME

-type req() :: {dorb_parser:api_command(),
		dorb_parser:encode_spec()}.

-export_type([req/0]).

%% Message
message(Key, Value) ->
    {message, message(0, Key, Value)}.

message(Attributes, Key, Value) ->
    [{int8, 0}, % Magic Byte
    {int8, Attributes}, % Compression (not currently supported)
    {bytes, Key},
    {bytes, Value}].

message_set(Messages) ->
    {message_set, [Messages]}.

%% Produce API
produce(RequiredAcks, Timeout, Topics) ->
    {0, [{int16, RequiredAcks},
	 {int32, Timeout},
	 {array, encode_topics(Topics, [])}]}.

encode_topics([], Acc) -> Acc;
encode_topics([{Topic, PartitionDetails}|Res], Acc) ->
    PartitionDetails1 = encode_partition_details(PartitionDetails, []),
    encode_topics(Res, [[{string, Topic},
			 {array, PartitionDetails1}]|Acc]).

encode_partition_details([], Acc) -> Acc;
encode_partition_details([{Partition, MessageSet}|Res], Acc) ->
    encode_partition_details(Res, [[{int32, Partition},
				    MessageSet]|Acc]).

%% Fetch API
-spec fetch(MaxWaitTime, MinBytes, Topics) ->
		   {ApiKey, EncodedMsg} when
      MaxWaitTime :: non_neg_integer(),
      MinBytes :: non_neg_integer(),
      Topics :: [{TopicName, [{Partition, FetchOffset, MaxBytes}]}],
      TopicName :: binary(),
      Partition :: non_neg_integer(),
      FetchOffset :: non_neg_integer(),
      MaxBytes :: non_neg_integer(),
      ApiKey :: 1,
      EncodedMsg :: dorb_parser:encode_spec().
fetch(MaxWaitTime, MinBytes, Topics) ->
    {1, [{int32, -1},
	 {int32, MaxWaitTime},
	 {int32, MinBytes},
	 {array, fetch_topics(Topics, [])}]}.

fetch_topics([], Res) ->
    Res;
fetch_topics([{TopicName, FetchDetails}|Rest], Res) ->
    % FetchDetails1 = fetch_details(FetchDetails, []),
    fetch_topics(Rest,
		 [[{string, TopicName},
		   {array, [[{int32, Partition},
			     {int64, FetchOffset},
			     {int32, MaxBytes}]
			    || {Partition, FetchOffset, MaxBytes}
				   <- FetchDetails]}]|Res]).

%% fetch_details([], Acc) ->
%%     Acc;
%% fetch_details([{Partition, FetchOffset, MaxBytes}|Rest], Acc) ->
%%     fetch_details(Rest, [[{int32, Partition},
%% 				 {int64, FetchOffset},
%% 				 {int32, MaxBytes}]|Acc]).

%% Offset API
offset(ReplicaId, Topics) ->
    {2, [{int32, ReplicaId},
	 {array, offset_topics(Topics, [])}]}.

offset_topics([], Acc) ->
    Acc;
offset_topics([{TopicName, OffsetDetails}|Rest], Acc) ->
    offset_topics(Rest,
		  [[{string, TopicName},
		    {array, [[{int32, Partition},
			      {int64, Time},
			      {int32, MaxNumberOfOffsets}]
			     || {Partition, Time, MaxNumberOfOffsets}
				    <- OffsetDetails]}]|Acc]).

%% Metadata API
-spec topic_metadata(Topics) ->
			    {ApiKey, EncodedMsg} when
      Topics :: [binary()],
      ApiKey :: 3,
      EncodedMsg :: dorb_parser:encode_spec().
topic_metadata(Topics) ->
    {3, [{array, [[{string, Topic}]
		 || Topic <- Topics]}]}.

%% Consumer Group API
group_metadata(GroupId) ->
    {10, [{string, GroupId}]}.

join_group(GroupId, SessionTimeout, Topics, ConsumerId,
	   PartitionAssignmentStrategy) ->
    {11, [{string, GroupId},
	  {int32, SessionTimeout},
	  {array, [[{string, TopicName}] || TopicName <- Topics]},
	  {string, ConsumerId},
	  {string, PartitionAssignmentStrategy}]}.

heartbeat(GroupId, GroupGenerationId, ConsumerId) ->
    {12, [{string, GroupId},
	  {int32, GroupGenerationId},
	  {string, ConsumerId}]}.

offset_commit(GroupId, GroupGenerationId, ConsumerId, RetentionTime,
	      Topics) ->
    {8, [{string, GroupId},
	 {int32, GroupGenerationId},
	 {string, ConsumerId},
	 {int64, RetentionTime},
	 {array, offset_commit_topics(Topics, [])}]}.

offset_commit_topics([], Acc) ->
    Acc;
offset_commit_topics([{TopicName, CommitDetails}|Rest], Acc) ->
    offset_commit_topics(Rest,
			 [[{string, TopicName},
			   {array, [[{int32, Partition},
				     {int64, Offset},
				     {string, Metadata}]
				    || {Partition, Offset, Metadata}
					   <- CommitDetails]}]|Acc]).

fetch_offset(GroupId, Topics) ->
    {9, [{string, GroupId},
	 {array, [[{string, TopicName},
		   {array, [[{int32, Partition}]
			   || Partition <- Partitions]}]
		  || {TopicName, Partitions} <- Topics]}]}.

% Internals
topic_metadata([], Res) ->
    [{array, Res}];
topic_metadata([Topic|Topics], Res) when is_binary(Topic) ->
    topic_metadata(Topics, [{string, Topics}|Res]).

%% Tests
-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

%% offset_test() ->
%%     {ApiCode, Offset} = offset(-1, [{<<"test">>, [{0, -1, 100}]}]),
%%     X = dorb_parser:encode(ApiCode, <<"dorb">>, 1, Offset),
%%     {ok, S} = gen_tcp:connect({127,0,0,1}, 9092, [binary, {active, false}]),
%%     gen_tcp:send(S, X),
%%     {ok, <<Size:32/signed>>} = gen_tcp:recv(S, 4),
%%     {ok, <<_:32/signed, Res/binary>>} = gen_tcp:recv(S, Size),
%%     ?debugFmt("~p", [Res]),
%%     {ok, Val} = dorb_parser:parse(ApiCode, Res),
%%     ?debugFmt("~p", [Val]).

%% fetch_test() ->
%%     {ApiCode, Fetch} = fetch(0, 0, [{<<"test2">>, [{0, 2, 1000}]}
%% 				   ]),
%%     X = dorb_parser:encode(ApiCode, <<"dorb">>, 1, Fetch),
%%     {ok, S} = gen_tcp:connect({127,0,0,1}, 9092, [binary, {active, false}]),
%%     gen_tcp:send(S, X),
%%     {ok, <<Size:32/signed>>} = gen_tcp:recv(S, 4),
%%     {ok, <<_:32/signed, Res/binary>>} = gen_tcp:recv(S, Size),
%%     {ok, Val} = dorb_parser:parse(ApiCode, Res).
    
%% encode_produce_message_test() ->
%%     _Message = dorb_msg:message(<<"test">>, <<"having fun with my homies">>),
%%     _Message1 = dorb_msg:message(<<"test">>, <<"having fun with my homies2">>),
%%     %_MessageSet = dorb_msg:message_set([Message, Message1]),
%%     MessageSet1 = dorb_msg:message_set([dorb_msg:message(<<"test">>,
%% 							 <<"testtest">>)]),
%%     {ApiKey, Produce} = dorb_msg:produce(1, 100, [{<<"test2">>, [{0, MessageSet1}]}
%% 						 ]),
%%     X = dorb_parser:encode(ApiKey, <<"dorb">>, 1, Produce),
%%     {ok, S} = gen_tcp:connect({127,0,0,1}, 9092, [binary, {active, true}]),
%%     gen_tcp:send(S, X),
%%     receive
%% 	{tcp, _, <<_:32/signed, _:32/signed, Res/binary>>} ->
%% 	    {ok, Val} = dorb_parser:parse(0, Res),
%% 	    ct:pal("VAL ~p", [Val])
		
%%     end.

-endif.
