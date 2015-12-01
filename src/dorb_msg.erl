-module(dorb_msg).

-export([message/2,
	 message_set/1,
	 produce/3,
	 fetch/3,
	 offset/2,
	 topic_metadata/1,
	 offset_commit/5,
	 offset_fetch/2,
	 consumer_metadata/1,
	 join_group/5,
	 heartbeat/3,
	 leave_group/2,
	 sync_group/4,
	 describe_groups/1,
	 list_groups/0]).

-export([parse/2,
	 encode/4]).

-type produce_request() :: 0.
-type fetch_request() :: 1.
-type offset_request() :: 2.
-type metadata_request() :: 3.
-type offset_commit_request() :: 8.
-type offset_fetch_request() :: 9.
-type consumer_metadata_request() :: 10.
-type join_group_request() :: 11.
-type heartbeat_request() :: 12.
-type leave_group_request() :: 13.
-type sync_group_request() :: 14.
-type describe_groups_request() :: 15.
-type list_groups_request() :: 16.
-type api_key() :: produce_request()|fetch_request()|offset_request()|
		   metadata_request()|offset_commit_request()|
		   offset_fetch_request()|consumer_metadata_request()|
		   join_group_request()|heartbeat_request()|
		   leave_group_request()|sync_group_request()|
		   describe_groups_request()|list_groups_request().
-type encode_spec() :: [{string, binary()}|
		       {bytes, binary()}|
		       {int64|int32|int16|int8, integer()}|
		       {array, encode_spec()}|
		       {message_set, [message()]}].
-type parse_spec() :: [{term(), string|bytes|int64|int32|int16|int8|
		       parse_spec()}].
-type reply() :: #{}.
-type req() :: {api_key(), encode_spec()}.

-type key() :: binary().
-type value() :: binary().
-type message() :: {message, encode_spec()}.
-type message_set() :: {message_set, [message()]}.
-type max_wait_time() :: integer().
-type min_bytes() :: integer().
-type topic() :: binary().
-type partition() :: integer().
-type offset() :: integer().
-type max_bytes() :: integer().
-type acks() :: integer().
-type dtimeout() :: integer().
-type replica_id() :: non_neg_integer().
-type group_id() :: binary().
-type generation_id() :: integer().
-type member_id() :: binary().
-type retention_time() :: integer().
-type metadata() :: binary().
-type protocol_type() :: binary().
-type protocol_name() :: binary().
-type member_assignment() :: binary().

-export_type([req/0,
	      api_key/0,
	      encode_spec/0,
	      reply/0]).

%% Message
-spec message(Key, Value) -> Message when
      Key :: key(), Value :: value(), Message :: message().
message(Key, Value) ->
    {message, [{int8, 0},
	       {int8, 0},
	       {bytes, Key},
	       {bytes, Value}]}.

-spec message_set([message()]) -> message_set().
message_set(Messages) ->
    {message_set, Messages}.

%% Produce API
-spec produce(Acks, Timeout, Topics) -> {ApiKey, EncodeSpec} when
      Acks :: acks(), Timeout :: dtimeout(),
      Topics :: [{Topic, [{Partition, MessageSet}]}],
      Topic :: topic(), Partition :: partition(), MessageSet :: message_set(),
      ApiKey :: produce_request(), EncodeSpec :: encode_spec().
produce(Acks, Timeout, Topics) ->
    {0, [{int16, Acks},
	 {int32, Timeout},
	 {array, [[{int32, Topic},
		   {array, [[{int32, Partition},
			     MessageSet]
			    || {Partition, MessageSet} <- Partitions]}]
		  || {Topic, Partitions} <- Topics]}]}.

%% Fetch API
-spec fetch(MaxWaitTime, MinBytes, Topics) -> {ApiKey, EncodeSpec} when
      MaxWaitTime :: max_wait_time(), MinBytes :: min_bytes(),
      Topics :: [{Topic, [{Partition, FetchOffset, MaxBytes}]}],
      Topic :: topic(), Partition :: partition(), FetchOffset :: offset(),
      MaxBytes :: max_bytes(), ApiKey :: fetch_request(),
      EncodeSpec :: encode_spec().
fetch(MaxWaitTime, MinBytes, Topics) ->
    {1, [{int32, -1},
	 {int32, MaxWaitTime},
	 {int32, MinBytes},
	 {array, [[{string, Topic},
		   {array, [[{int32, Partition},
			     {int64, FetchOffset},
			     {int32, MaxBytes}]
			    || {Partition, FetchOffset, MaxBytes}
				   <- Partitions]}
		  ] || {Topic, Partitions} <- Topics]}]}.

%% Offset API
-spec offset(ReplicaId, Topics) -> {ApiKey, EncodeSpec} when
      ReplicaId :: replica_id(),
      Topics :: [{Topic, [{Partition, Timeout, MaxNumberOfOffsets}]}],
      Topic :: topic(), Partition :: partition(), Timeout :: dtimeout(),
      MaxNumberOfOffsets :: offset(), ApiKey :: offset_request(),
      EncodeSpec :: encode_spec().
offset(ReplicaId, Topics) ->
    {2, [{int32, ReplicaId},
	 {array, [[{string, Topic},
		   {array, [[{int32, Partition},
			     {int64, Timestamp},
			     {int32, MaxNumberOfOffsets}]
			    || {Partition, Timestamp, MaxNumberOfOffsets}
				   <- Partitions]}]
		  || {Topic, Partitions} <- Topics]}]}.

%% Metadata API
-spec topic_metadata(Topics) -> {ApiKey, EncodeSpec} when
      Topics :: [topic()], ApiKey :: metadata_request(),
      EncodeSpec :: encode_spec().
topic_metadata(Topics) ->
    {3, [{array, [[{string, Topic}]
		 || Topic <- Topics]}]}.

-spec offset_commit(GroupId, GenerationId, MemberId, RetentionTime, Topics) ->
			   {ApiKey, EncodeSpec} when
      GroupId :: group_id(), GenerationId :: generation_id(),
      MemberId :: member_id(), RetentionTime :: retention_time(),
      Topics :: [{Topic, [{Partition, Offset, Metadata}]}],
      Topic :: topic(), Partition :: partition(), Offset :: offset(),
      Metadata :: metadata(), ApiKey :: offset_commit_request(),
      EncodeSpec :: encode_spec().
offset_commit(GroupId, GenerationId, MemberId, RetentionTime, Topics) ->
    {8, [{string, GroupId},
	 {int32, GenerationId},
	 {string, MemberId},
	 {int32, RetentionTime},
	 {array, [[{string, Topic},
		   {array, [[{int32, Partition},
			     {int64, Offset},
			     {string, Metadata}]
			    || {Partition, Offset, Metadata} <- Partitions]}]
		  || {Topic, Partitions} <- Topics]}]}.

-spec offset_fetch(GroupId, Topics) -> {ApiKey, EncodeSpec} when
      GroupId :: group_id(), Topics :: [{Topic, [Partition]}],
      Topic :: topic(), Partition :: partition(),
      ApiKey :: offset_fetch_request(), EncodeSpec :: encode_spec().
offset_fetch(GroupId, Topics) ->
    {9, [{string, GroupId},
	 {array, [[{string, Topic},
		   {array, [[{int32, Partition}]
			    || Partition <- Partitions]}]
		  || {Topic, Partitions} <- Topics ]}]}.

-spec consumer_metadata(GroupId) -> {ApiKey, EncodeSpec} when
      GroupId :: group_id(), ApiKey :: consumer_metadata_request(),
      EncodeSpec :: encode_spec().
consumer_metadata(GroupId) ->
    {10, [{string, GroupId}]}.

-spec join_group(GroupId, SessionTimeout, MemberId, ProtocolType,
		 GroupProtocols) -> {ApiKey, EncodeSpec} when
      GroupId :: group_id(), SessionTimeout :: dtimeout(),
      MemberId :: member_id(), ProtocolType :: protocol_type(),
      GroupProtocols :: [{ProtocolName, ProtocolMetadata}],
      ProtocolName :: protocol_name(), ProtocolMetadata :: metadata(),
      ApiKey :: join_group_request(), EncodeSpec :: encode_spec().
join_group(GroupId, SessionTimeout, MemberId, ProtocolType, GroupProtocols) ->
    {11, [{string, GroupId},
	  {int32, SessionTimeout},
	  {string, MemberId},
	  {string, ProtocolType},
	  {array, [[{string, ProtocolName},
		    {bytes, ProtocolMetadata}]
		   || {ProtocolName, ProtocolMetadata} <- GroupProtocols]}]}.

-spec heartbeat(GroupId, GenerationId, MemberId) -> {ApiKey, EncodeSpec} when
      GroupId :: group_id(), GenerationId :: generation_id(),
      MemberId :: member_id(), ApiKey :: heartbeat_request(),
      EncodeSpec :: encode_spec().
heartbeat(GroupId, GenerationId, MemberId) ->
    {12, [{string, GroupId},
	  {int32, GenerationId},
	  {string, MemberId}]}.

-spec leave_group(GroupId, MemberId) -> {ApiKey, EncodeSpec} when
      GroupId :: group_id(), MemberId :: member_id(),
      ApiKey :: leave_group_request(), EncodeSpec :: encode_spec().
leave_group(GroupId, MemberId) ->
    {13, [{string, GroupId},
	  {string, MemberId}]}.

-spec sync_group(GroupId, GenerationId, MemberId, GroupAssignment) ->
			{ApiKey, EncodeSpec} when
      GroupId :: group_id(), GenerationId :: generation_id(),
      MemberId :: member_id(),
      GroupAssignment :: [{MemberId, MemberAssignment}],
      MemberAssignment :: member_assignment(), ApiKey :: sync_group_request(),
      EncodeSpec :: encode_spec().
sync_group(GroupId, GenerationId, MemberId, GroupAssignment) ->
    {14, [{string, GroupId},
	  {int32, GenerationId},
	  {string, MemberId},
	  {array, [[{string, MemberId1},
		    {bytes, MemberAssignment}]
		   || {MemberId1, MemberAssignment} <- GroupAssignment]}]}.

-spec describe_groups(Groups) -> {ApiKey, EncodeSpec} when
      Groups :: [group_id()], ApiKey :: describe_groups_request(),
      EncodeSpec :: encode_spec().
describe_groups(Groups) ->
    {15, [{array, [[{string, Group}] || Group <- Groups]}]}.

-spec list_groups() -> {ApiKey, []} when ApiKey :: list_groups_request().
list_groups() ->
    {16, []}.

% A implementation of a Kafka Protocol parser. A good introduction to the
% protocol can be found here:
% https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Preliminaries
% All Kafka APIs are a request-response pair. Requests go out with a CorrId and
% responses have that same ID in them. That ID is helpful when parsing messages
% from Kafka since there is no indication of the type of message incoming.

-spec parse(ApiKey, Message) ->
		   {ok, ParsedMessage} when
      ApiKey :: api_key(),
      Message :: binary(),
      ParsedMessage :: #{}.
parse(0, ProduceResponse) ->
    Spec = [{topics,
	     [{topic_name, string},
	      {partitions,
	       [{partition, int32},
		{error_code, int16},
		{offset, int64}]
	      }]}],
    {Resp, <<>>} = parse(ProduceResponse, Spec, #{}),
    {ok, Resp};
parse(1, FetchResponse) ->
    Spec = [{responses,
	    [{topic_name, string},
	     {partitions,
	      [{partition, int32},
	       {error_code, int16},
	       {high_water_mark_offset, int64},
	       {messages, message_set}
	      ]}
	    ]}],
    {Resp, <<>>} = parse(FetchResponse, Spec, #{}),
    {ok, Resp};
parse(2, OffsetsResponse) ->
    Spec = [{topics,
	     [{topic_name, string},
	      {partitions,
	       [{partition, int32},
		{error_code, int16},
		{offsets, [{offset, int64}]}
	       ]}]}],
    {Resp, <<>>} = parse(OffsetsResponse, Spec, #{}),
    {ok, Resp};
parse(3, MetadataResponse) ->
    BrokerSpec = [{brokers,
		   [{node_id, int32},
		    {host, string},
		    {port, int32}]}],
    TopicMetadataSpec = [{topic_metadata,
    			  [{topic_error_code, int16},
    			   {topic_name, string},
    			   {partition_metadata,
    			    [{partition_error_code, int16},
    			     {partition_id, int32},
    			     {leader, int32},
    			     {replicas,
    			      [{id, int32}]},
    			     {isr, 
    			     [{id, int32}]}]}]
    			 }],
    {#{brokers := Brokers}, Res} = parse(MetadataResponse, BrokerSpec, #{}),
    {#{topic_metadata := TopicsMetadata}, <<>>} =
	parse(Res, TopicMetadataSpec, #{}),
    {ok, #{brokers => Brokers,
	   topics_metadata => TopicsMetadata}};
parse(8, OffsetCommitResponse) ->
    Spec = [{topics,
	     [{topic_name, string},
	      {partitions,
	       [{partition, int32},
		{error_code, int16}
		% {unknown, int64} % No idea what this is, but it's there...
	       ]}
	     ]}],
    {Resp, <<>>} = parse(OffsetCommitResponse, Spec, #{}),
    {ok, Resp};
parse(9, OffsetFetchResponse) ->
    Spec = [{topics,
	     [{topic_name, string},
	      {partitions,
	       [{partition, int32},
		{offset, int64},
		{metadata, string},
		{error_code, int16}]}
	     ]}],
    {Resp, <<>>} = parse(OffsetFetchResponse, Spec, #{}),
    {ok, Resp};
parse(10, GroupCoordinatorResponse) ->
    Spec = [{error_code, int16},
	    {coordinator_id, int32},
	    {coordinator_host, string},
	    {coordinator_port, int32}],
    {Resp, <<>>} = parse(GroupCoordinatorResponse, Spec, #{}),
    {ok, Resp};
parse(11, JoinGroupResponse) ->
    Spec = [{error_code, int16},
	    {generation_id, int32},
	    {group_protocol, string},
	    {leader_id, string},
	    {member_id, string},
	    {members,
	     [{member_id, string},
	      {member_metadata, bytes}]}],
    {Resp, <<>>} = parse(JoinGroupResponse, Spec, #{}),
    {ok, Resp};
parse(12, HeartbeatResponse) ->
    Spec = [{error_code, int16}],
    {Resp, <<>>} = parse(HeartbeatResponse, Spec, #{}),
    {ok, Resp};
parse(13, LeaveGroupResponse) ->
    Spec = [{error_code, int16}],
    {Resp, <<>>} = parse(LeaveGroupResponse, Spec, #{}),
    {ok, Resp};
parse(14, SyncGroupResponse) ->
    Spec = [{error_code, int16},
	    {member_assignment, bytes}],
    {Resp, <<>>} = parse(SyncGroupResponse, Spec, #{}),
    {ok, Resp};
parse(15, DescribeGroupsResponse) ->
    Spec = [{error_code, int16},
	    {group_id, string},
	    {state, string},
	    {protocol_type, string},
	    {protocol, string},
	    {members,
	     [{member_id, string},
	      {client_id, string},
	      {client_host, string},
	      {member_metadata, bytes},
	      {member_assignment, bytes}]}],
    {Resp, <<>>} = parse(DescribeGroupsResponse, Spec, #{}),
    {ok, Resp};
parse(16, ListGroupsResponse) ->
    Spec = [{error_code, int16},
	    {groups,
	     [{group_id, string},
	      {protocol_type, string}]}],
    {Resp, <<>>} = parse(ListGroupsResponse, Spec, #{}),
    {ok, Resp}.

-spec encode(ApiKey, ClientId, CorrId, Req) ->
		    EncodedReq when
      ApiKey :: api_key(),
      ClientId :: binary(),
      CorrId :: non_neg_integer(),
      Req :: encode_spec(),
      EncodedReq :: iolist().
encode(ApiKey, ClientId, CorrId, Req) ->
    Req1 = encode([{int16, ApiKey},
		   {int16, 0}, % API Version
		   {int32, CorrId},
		   {string, ClientId}] ++ Req),
    Size = erlang:iolist_size(Req1),
    [<<Size:32/signed>>, Req1].

%% Internal
-spec encode(EncodeSpec) -> EncodedRequest when
      EncodeSpec :: encode_spec(),
      EncodedRequest :: iolist().
encode(ToEncode) ->
    encode(ToEncode, []).

-spec encode(EncodeSpec, EncodedRequest) -> EncodedRequest when
      EncodeSpec :: encode_spec(),
      EncodedRequest :: iolist().
encode([], Req) ->
    lists:reverse(Req);
encode([{string, String}|Rest], Req) ->
    Size = erlang:size(String),
    encode(Rest, [[<<Size:16/signed>>, String]|Req]);
encode([{bytes, Bytes}|Rest], Req) ->
    Size = erlang:size(Bytes),
    encode(Rest, [[<<Size:32/signed>>, Bytes]|Req]);
encode([{int64, Int64}|Rest], Req) ->
    encode(Rest, [<<Int64:64/signed>>|Req]);
encode([{int32, Int32}|Rest], Req) ->
    encode(Rest, [<<Int32:32/signed>>|Req]);
encode([{int16, Int16}|Rest], Req) ->
    encode(Rest, [<<Int16:16/signed>>|Req]);
encode([{int8, Int8}|Rest], Req) ->
    encode(Rest, [<<Int8:8/signed>>|Req]);
encode([{message, Message}|Rest], Req) ->
    % A special handler for individual message. This is because messages need to
    % have the CRC computed and appended to the byte array before being sent
    % out.
    Message1 = encode(Message, []),
    CRC = erlang:crc32(Message1),
    MessageIolist = [<<CRC:32/signed>>, Message1],
    MessageSize = erlang:iolist_size(MessageIolist),
    EncodedMessage = [<<0:64/signed, MessageSize:32/signed>>, MessageIolist],
    encode(Rest, [EncodedMessage|Req]);
encode([{message_set, MessageSet}|Rest], Req) ->
    Messages = [encode(Message, []) || Message <- MessageSet],
    MessageSetSize = erlang:iolist_size(Messages),
    encode(Rest, [[<<MessageSetSize:32/signed>>,
		   Messages]|Req]);
encode([{array, Array}|Rest], Req) ->
    ArrayLength = erlang:length(Array),
    ArrayElements = [encode(Arr, []) || Arr <- Array],
    encode(Rest, [[<<ArrayLength:32/signed>>,
		   ArrayElements]|Req]).

-spec parse(Binary, ParseSpec, Resp) -> {Resp, Binary} when
      Binary :: binary(),
      ParseSpec :: parse_spec(),
      Resp :: #{}.
parse(<<-1:16/signed-integer,
	Bin/binary>>, [{FieldName, string}|Rest], Resp) ->
    parse(Bin, Rest, Resp#{FieldName => undefined});
parse(<<StringSize:16/signed-integer,
	String:StringSize/binary,
	Bin/binary>>, [{FieldName, string}|Rest], Resp) ->
    parse(Bin, Rest, Resp#{FieldName => String});
parse(<<-1:32/signed-integer,
	Bin/binary>>, [{FieldName, bytes}|Rest], Resp) ->
    parse(Bin, Rest, Resp#{FieldName => undefined});
parse(<<BytesSize:32/signed-integer,
	Bytes:BytesSize/binary,
	Bin/binary>>, [{FieldName, bytes}|Rest], Resp) ->
    parse(Bin, Rest, Resp#{FieldName => Bytes});
parse(<<Int64:64/signed-integer,
	Bin/binary>>, [{FieldName, int64}|Rest], Resp) ->
    parse(Bin, Rest, Resp#{FieldName => Int64});
parse(<<Int32:32/signed-integer,
	Bin/binary>>, [{FieldName, int32}|Rest], Resp) ->
    parse(Bin, Rest, Resp#{FieldName => Int32});
parse(<<Int16:16/signed-integer,
	Bin/binary>>, [{FieldName, int16}|Rest], Resp) ->
    parse(Bin, Rest, Resp#{FieldName => Int16});
parse(<<Int8:8/signed-integer,
	Bin/binary>>, [{FieldName, int8}|Rest], Resp) ->
    parse(Bin, Rest, Resp#{FieldName => Int8});
parse(<<MessageSetSize:32/signed-integer,
	RawMessages:MessageSetSize/binary,
	Bin/binary>>, [{FieldName, message_set}|Rest], Resp) ->
    % This is a special parse function for parsing message sets since they
    % look different on the wirte than other Kafka types
    Messages = parse_message_set(RawMessages, []),
    parse(Bin, Rest, Resp#{FieldName => Messages});
parse(<<ArraySize:32/signed-integer,
	Bin/binary>>, [{FieldName, Arr}|Rest], Resp) when is_list(Arr) ->
    {Arr1, Bin1} = parse_array(ArraySize, Bin, Arr, []),
    parse(Bin1, Rest, Resp#{FieldName => Arr1});
parse(Bin, [], Resp) ->
    {Resp, Bin}.

parse_message_set(<<Offset:64/signed-integer,
		    MessageSize:32/signed-integer,
		    RawMessage:MessageSize/binary,
		    Rest/binary>>, Acc) ->
    {Message, <<>>} = parse(RawMessage, [{crc, int32},
					 {magic_byte, int8},
					 {attributes, int8},
					 {key, bytes},
					 {value, bytes}],
			    #{offset => Offset}),
    parse_message_set(Rest, [Message|Acc]);
parse_message_set(_, Acc) ->
    % As an optimization the server is allowed to return a partial message at
    % the end of the message set. Clients should handle this case.
    Acc.

-spec parse_array(non_neg_integer(), binary(), term(), list()) ->
		 {list(), binary()}.
parse_array(0, Bin, _FieldSpec, Acc) ->
    {lists:reverse(Acc), Bin};
parse_array(ArraySize, Bin, FieldSpec, Acc) ->
    {Resp, Bin1} = parse(Bin, FieldSpec, #{}),
    parse_array(ArraySize - 1, Bin1, FieldSpec, [Resp|Acc]).

%% Tests
-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

parse_produce_test() ->
    ProduceResp = <<0,0,0,2,0,5,116,101,115,116,49,0,0,0,1,0,0,0,0,0,0,0,0,0,0,
		    0,0,0,4,0,4,116,101,115,116,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,
		    0,37>>,
    ?assertEqual({ok, #{topics =>
			   [#{partitions =>
				 [#{error_code => 0,
				    offset => 4,
				    partition => 0}],
			     topic_name => <<"test1">>},
			   #{partitions =>
				[#{error_code => 0,
				   offset => 37,
				   partition => 0}],
			    topic_name => <<"test">>}]}},
		parse(0, ProduceResp)).

parse_fetch_test() ->
    FetchResp = <<0,0,0,1,0,5,116,101,115,116,50,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,
		  0,0,4,0,0,0,76,0,0,0,0,0,0,0,2,0,0,0,26,127,31,212,21,0,0,0,0,
		  0,4,116,101,115,116,0,0,0,8,116,101,115,116,116,101,115,116,0,
		  0,0,0,0,0,0,3,0,0,0,26,127,31,212,21,0,0,0,0,0,4,116,101,115,
		  116,0,0,0,8,116,101,115,116,116,101,115,116>>,
    ?assertEqual({ok, #{responses =>
			   [#{partitions =>
				 [#{error_code => 0,
				    high_water_mark_offset => 4,
				    messages => [#{attributes => 0,
						   crc => 2132792341,
						   key => <<"test">>,
						   magic_byte => 0,
						   offset => 3,
						   value => <<"testtest">>},
						 #{attributes => 0,
						   crc => 2132792341,
						   key => <<"test">>,
						   magic_byte => 0,
						   offset => 2,
						   value => <<"testtest">>}],
				    partition => 0}],
			     topic_name => <<"test2">>}]}},
		 parse(1, FetchResp)).

parse_offset_test() ->
    OffsetResp = <<0,0,0,2,0,5,116,101,115,116,49,0,0,0,1,0,0,0,0,0,0,0,0,0,2,
		   0,0,0,0,0,0,0,10,0,0,0,0,0,0,0,0,0,4,116,101,115,116,0,0,0,1,
		   0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,43,0,0,0,0,0,0,0,0>>,
    ?assertEqual({ok, #{topics => [#{partitions =>
					[#{error_code => 0,
					   offsets => 
					       [#{offset => 10},#{offset => 0}],
					   partition => 0}],
				    topic_name => <<"test1">>},
				  #{partitions =>
				       [#{error_code => 0,offsets =>
					     [#{offset => 43},#{offset => 0}],
					 partition => 0}],
				    topic_name => <<"test">>}]}},
		 parse(2, OffsetResp)).

parse_metadata_test() ->
    Metadata = <<0,0,0,1,0,0,0,0,0,11,49,57,50,46,49,54,56,46,48,46,52,0,0,35,
		 132,0,0,0,1,0,0,0,4,116,101,115,116,0,0,0,1,0,0,0,0,0,0,0,0,0,
		 0,0,0,0,1,0,0,0,0,0,0,0,1,0,0,0,0>>,
    ?assertEqual({ok, #{brokers =>
			   [#{host => <<"192.168.0.4">>,
			      node_id => 0,
			      port => 9092}],
			topics_metadata =>
			    [#{partition_metadata =>
				  [#{isr => [#{id => 0}],
				     leader => 0,
				     partition_error_code => 0,
				     partition_id => 0,
				     replicas => [#{id => 0}]}],
			      topic_error_code => 0,
			      topic_name => <<"test">>}]}}, parse(3, Metadata)).

parse_offset_commit_test() ->
    OffsetCommit = <<0,0,0,1,0,4,116,101,115,116,0,0,0,1,0,0,0,0,0,0>>,
		     %0,0,0,0,0,0,0,49>>,
    ?assertEqual({ok, #{topics => [#{partitions =>
					[#{error_code => 0,
					   partition => 0
					   % unknown => 49
					  }
					],
				    topic_name => <<"test">>}]}},
		parse(8, OffsetCommit)).

parse_fetch_offset_test() ->
    FetchOffset = <<0,0,0,1,0,4,116,101,115,116,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,1
		    ,0,0,0,0>>,
    ?assertEqual({ok, #{topics => [#{partitions => [#{error_code => 0,
						      metadata => <<>>,
						      offset => 1,
						      partition => 0}],
				     topic_name => <<"test">>}]}},
		 parse(9, FetchOffset)).

parse_group_metadata_test() ->
    GroupMetadata = <<0,0,0,0,0,0,0,11,49,57,50,46,49,54,56,46,48,46,52,0,0,35,
		      132>>,
    ?assertEqual({ok, #{coordinator_host => <<"192.168.0.4">>,
			coordinator_id => 0,
			coordinator_port => 9092,
			error_code => 0}}, parse(10, GroupMetadata)).

parse_join_group_test() ->
    JoinGroup = <<0,0,0,0,0,1,0,36,97,102,51,101,50,101,98,56,45,98,54,98,48,45,
		  52,54,53,51,45,97,52,51,56,45,98,56,48,48,100,52,50,97,56,49,
		  56,50,0,0,0,1,0,4,116,101,115,116,0,0,0,1,0,0,0,0>>,
    ?assertEqual({ok,
		  #{consumer_id => <<"af3e2eb8-b6b0-4653-a438-b800d42a8182">>,
		    error_code => 0,
		    group_generation_id => 1,
		    partitions_to_own =>
			[#{partitions => [#{partition => 0}],
			   topic_name => <<"test">>}]}},
		 parse(11, JoinGroup)).

parse_heartbeat_test() ->
    Heartbeat = <<0,0>>,
    ?assertEqual({ok, #{error_code => 0}}, parse(12, Heartbeat)).

-endif.
