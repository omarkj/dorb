-module(dorb_producer).
-behaviour(gen_server).

% API
-export([start_link/2,
	 send/4]).

% gen_server callbacks
-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

-type partitioner_fun() :: fun((binary(), binary(), non_neg_integer()) ->
				      non_neg_integer()).
-type required_acks() :: 0|-1|1..32767.
-type resp_timeout() :: 0..2147483647.

-type option() :: {required_acks, required_acks()}|
	    {partitioner, partitioner_fun()}.
-type options() :: [option()].

-record(state, {
	  hosts :: dorb:hosts(),
	  node_ids :: [{non_neg_integer(), dorb:hosts()}],
	  partitioner :: partitioner_fun(),
	  required_acks :: required_acks(),
	  timeout :: resp_timeout(),
	  topics_metadata = [] :: [{non_neg_integer(),
				    non_neg_integer()}]
	 }).

% API implementation
-spec start_link(Hosts, Options) ->
			{ok, Producer} when
      Hosts :: dorb:hosts(),
      Options :: options(),
      Producer :: pid().
start_link(Hosts, Opts) ->
    gen_server:start_link(?MODULE, [Hosts, Opts], []).

-spec send(pid(), binary(), [{binary(), binary()}], non_neg_integer()) ->
		  ok.
send(Producer, Topic, Messages, Timeout) ->
    gen_server:call(Producer, {send, Topic, Messages}, Timeout).

init([Hosts, Opts]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #state{hosts = Hosts,
		partitioner = proplists:get_value(partitioner, Opts,
						  fun default_partitioner/3),
		required_acks = proplists:get_value(required_acks, Opts, 1),
		timeout = proplists:get_value(timeout, Opts, 100)
	       }}.

handle_call({send, Topic, Messages}, _From, State) ->
    send(Topic, Messages, State);
handle_call(_Msg, _From, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _NewVsn) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%% Internal
send(Topic, Messages, #state{
			 hosts=Hosts,
			 topics_metadata=TopicsMetadata,
			 partitioner=Partitioner,
			 required_acks=RequiredAcks,
			 node_ids = NodeIds,
			 timeout=Timeout
			}=State) ->
    MetadataForTopic = proplists:get_value(Topic, TopicsMetadata),
    case prepare_send(Topic, lists:reverse(Messages), Partitioner,
		      RequiredAcks, Timeout, MetadataForTopic) of
	{error, refresh_metadata} ->
	    {ok, Socket} = dorb_socket:get(Hosts),
	    {NodeIds1, MetadataForTopic1} =
		metadata_for_topics(Socket, [Topic]),
	    dorb_socket:return(Socket),
            % question: maybe remove a tuple with the same name if exists in
	    % the topics metadata list?
	    TopicsMetadata1 = TopicsMetadata ++ MetadataForTopic1,
	    send(Topic, Messages, State#state{
				    topics_metadata = TopicsMetadata1,
				    node_ids = NodeIds1
				   });
	EncodedMessages ->
	    Reply = send_(EncodedMessages, NodeIds, []),
	    {reply, Reply, State}
    end.

send_([], _, Refs) ->
    Refs;
send_([{LeaderId, EncodedMessage}|Rest], Leaders, Acc) ->
    Leader = proplists:get_value(LeaderId, Leaders),
    {ok, Socket} = dorb_socket:get(Leader),
    {ok, Resp} = dorb_socket:send_sync(Socket, EncodedMessage, 1000),
    dorb_socket:return(Socket),
    send_(Rest, Leaders, [Resp|Acc]).

default_partitioner(_Topic, _Key, Partitions) ->
    random:uniform(Partitions) - 1.

prepare_send(_, _, _, _, _, undefined) ->
    {error, refresh_metadata};
prepare_send(Topic, Messages, Partitioner, RequiredAcks, Timeout,
	     TopicsMetadata) ->
    case length(TopicsMetadata) of
	0 ->
	    {error, refresh_metadata};
	NumberOfPartitions ->
	    PartitionedMessages = partition_messages(Topic,
						     Messages,
						     Partitioner,
						     NumberOfPartitions,
						     []),
	    MessagesWithLeader =
		pair_leaders_to_message_sets(PartitionedMessages,
					     TopicsMetadata,
					     []),
	    TopicsMessagesSets = pair_topic_with_message_sets(
				   Topic,
				   MessagesWithLeader,
				   []),
	    encode_produce_per_leader(TopicsMessagesSets,
				      RequiredAcks,
				      Timeout,
				      [])
    end.

metadata_for_topics(Socket, Topics) ->
    Msg = dorb_msg:topic_metadata(Topics),
    {ok,
     #{brokers := Brokers, topics_metadata := TopicsMetadata}} =
	dorb_socket:send_sync(Socket, Msg, 1000),
    Brokers1 = [{NodeId, {Host, Port}} || #{host := Host,
					    port := Port,
					    node_id := NodeId} <- Brokers],
    TopicsMetadata1 = [{TopicName, [{PartitionId, Leader} ||
				       #{partition_id := PartitionId,
					 leader := Leader} <- Partitions]}
		      || #{topic_name := TopicName,
			   partition_metadata := Partitions}
			     <- TopicsMetadata],
    {Brokers1, TopicsMetadata1}.

encode_produce_per_leader([], _, _, Acc) ->
    Acc;
encode_produce_per_leader([{Leader, Topics}|Res], RequiredAcks, Timeout, Acc) ->
    Produce = dorb_msg:produce(RequiredAcks, Timeout, [Topics]),
    encode_produce_per_leader(Res, RequiredAcks, Timeout, [{Leader, Produce}|
							  Acc]).

pair_topic_with_message_sets(_, [], Acc) ->
    Acc;
pair_topic_with_message_sets(Topic, [{Leader, PartitionDetails}|Res], Acc) ->
    pair_topic_with_message_sets(Topic,
				  Res,
				  [{Leader, {Topic, PartitionDetails}}|Acc]).

pair_leaders_to_message_sets([], _, Acc) ->
    [{Key, proplists:get_all_values(Key, Acc)}
    || Key <- proplists:get_keys(Acc)];
pair_leaders_to_message_sets([{Partition, _MessageSet}=M|Res], PartitionLeaders,
				Acc) ->
    Leader = proplists:get_value(Partition, PartitionLeaders),
    pair_leaders_to_message_sets(Res, PartitionLeaders,
				 [{Leader, M}|Acc]).

-spec partition_messages(binary(), [{binary(), binary()}], partitioner_fun(),
			 non_neg_integer(), [term()]) -> [term()].
partition_messages(_Topic, [], _, _, Retval) ->
    [{Key, dorb_msg:message_set(proplists:get_all_values(Key, Retval))}
    || Key <- proplists:get_keys(Retval)];
partition_messages(Topic, [{Key, Value}|Res], Partitioner, NumberOfPartitions,
		   Acc) ->
    Partition = Partitioner(Topic, Key, NumberOfPartitions),
    Message = dorb_msg:message(Key, Value),
    partition_messages(Topic, Res, Partitioner, NumberOfPartitions,
		       [{Partition, [Message]}|Acc]).

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

prepare_send_test() ->
    ?assertMatch([{3,
		   {0,
		    [{int16,1},
		     {int32,1},
		     {array,
		      [[{string,<<"test">>},
			{array,
			 [[{int32,2},
			   {message_set,
			    [[{message,
			       [{int8,0},
				{int8,0},
				{bytes,<<"key2">>},
				{bytes,<<"d">>}]}]]}]]}]]}]}},
		  {2,
		   {0,
		    [{int16,1},
		     {int32,1},
		     {array,
		      [[{string,<<"test">>},
			{array,
			 [[{int32,1},
			   {message_set,
			    [[{message,
			       [{int8,0},
				{int8,0},
				{bytes,<<"key1">>},
				{bytes,<<"b">>}]}]]}]]}]]}]}}],
		 prepare_send(<<"test">>, [{<<"key1">>, <<"b">>},
					   {<<"key2">>, <<"d">>}],
			      fun (_, <<"key1">>, _) -> 1;
				  (_, <<"key2">>, _) -> 2 end,
			      1, 1, [{0,1},
				     {1,2},
				     {2,3},
				     {3,4}])).

encode_produce_per_leader_test() ->
    PartitionedMessages = 
	partition_messages(<<"test">>, [{<<"key1">>, <<"val1">>},
					{<<"key2">>, <<"val2">>},
					{<<"key3">>, <<"val2">>},
					{<<"key3">>, <<"val2">>},
					{<<"key4">>, <<"val2">>}
				       ],
			   fun (_, <<"key1">>, _) -> 1;
			       (_, <<"key2">>, _) -> 2;
			       (_, <<"key3">>, _) -> 3;
			       (_, <<"key4">>, _) -> 4 end, 4, []),
    Leadered = pair_leaders_to_message_sets(PartitionedMessages, [{1, 0},
								  {2, 1},
								  {3, 2},
								  {4, 3}], []),
    PairedMessageSets = pair_topic_with_message_sets(<<"test">>, Leadered, []),
    Encoded = encode_produce_per_leader(PairedMessageSets, 1, 0, []),
    ?assertMatch([{0, {0, _}},
		  {3, {0, _}},
		  {2, {0, _}},
		  {1, {0, _}}], Encoded).

pair_topics_with_message_sets_test() ->
    PartitionedMessages = 
	partition_messages(<<"test">>, [{<<"key1">>, <<"val1">>},
					{<<"key2">>, <<"val2">>},
					{<<"key3">>, <<"val2">>},
					{<<"key3">>, <<"val2">>},
					{<<"key4">>, <<"val2">>}],
			   fun (_, <<"key1">>, _) -> 1;
			       (_, <<"key2">>, _) -> 2;
			       (_, <<"key3">>, _) -> 3;
			       (_, <<"key4">>, _) -> 4 end, 4, []),
    Leadered = pair_leaders_to_message_sets(PartitionedMessages, [{1, 0},
								  {2, 1},
								  {3, 2},
								  {4, 3}], []),
    ?assertMatch([{1, {<<"test">>, [{2, {message_set, [_]}}]}},
		  {2, {<<"test">>, [{3, {message_set, [_,_]}}]}},
		  {3, {<<"test">>, [{4, {message_set, [_]}}]}},
		  {0, {<<"test">>, [{1, {message_set, [_]}}]}}],
		 pair_topic_with_message_sets(<<"test">>, Leadered, [])).

pair_leaders_to_message_sets_test() ->
    PartitionedMessages = 
	partition_messages(<<"test">>, [{<<"key1">>, <<"val1">>},
					{<<"key2">>, <<"val2">>},
					{<<"key3">>, <<"val2">>},
					{<<"key4">>, <<"val2">>}],
			   fun (_, <<"key1">>, _) -> 1;
			       (_, <<"key2">>, _) -> 2;
			       (_, <<"key3">>, _) -> 3;
			       (_, <<"key4">>, _) -> 4 end, 4, []),
    ?assertMatch(
       [{0, [{1, _}]},
	{3, [{4, _}]},
	{2, [{3, _}]},
	{1, [{2, _}]}],
	 pair_leaders_to_message_sets(PartitionedMessages, [{1, 0},
							    {2, 1},
							    {3, 2},
							    {4, 3}], [])).

partition_messages_test() ->
    ?assertMatch([{3, {message_set, _}},
     		  {2, {message_set, _}},
     		  {1, {message_set, _}},
		  {4, {message_set, _}}],
		  partition_messages(<<"test">>, [{<<"key1">>, <<"val1">>},
					{<<"key2">>, <<"val2">>},
					{<<"key3">>, <<"val2">>},
					{<<"key3">>, <<"val2">>},
					{<<"key4">>, <<"val2">>}],
		       fun (_, <<"key1">>, _) -> 1;
			   (_, <<"key2">>, _) -> 2;
			   (_, <<"key3">>, _) -> 3;
			   (_, <<"key4">>, _) -> 4 end, 4, [])).

-endif.
