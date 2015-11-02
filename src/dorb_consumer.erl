-module(dorb_consumer).

% The Dorb Consumer is supposed to be similar to the Kafka High Level consumer.
% It interfaces with Kafka's Consumer Group API, gathering information on
% partitions it is responsible for.
% The consumer group needs to be able to manage connections to make sure no
% messages are consumed when the group is inactive.

-behaviour(gen_fsm).

-record(state, {
	  hosts :: dorb:hosts(),
	  group_id :: binary(),
	  topics :: [binary()],
	  coordinator :: dorb:host(),
	  session_timeout :: non_neg_integer(),
	  consumer_id = <<>> :: binary(),
	  psa :: binary(),
	  ggi :: non_neg_integer(),
	  timer_ref :: reference(),
	  pto :: term()
	 }).

% API
-export([start_link/5,
	 startup/1
	]).

% States
-export([down/2,
	 down/3
	]).

% gen_fsm callbacks
-export([init/1,
	 handle_event/3,
	 handle_sync_event/4,
	 handle_info/3,
	 code_change/4,
	 terminate/3
	]).

% API
start_link(Hosts, GroupId, Topics, Psa, Opts) ->
    gen_fsm:start_link(?MODULE, [Hosts, GroupId, Topics, Psa, Opts], []).

startup(Cg) ->
    gen_fsm:sync_send_event(Cg, startup, 10000).

% gen_fsm implementation
init([Hosts, GroupId, Topics, Psa, Opts]) ->
    erlang:process_flag(trap_exit, true),
    {ok, down, #state{hosts = Hosts,
		      group_id = GroupId,
		      topics = Topics,
		      psa = Psa,
		      consumer_id = proplists:get_value(consumer_id, Opts,
							<<>>),
		      session_timeout = proplists:get_value(session_timeout,
							    Opts, 10000)}}.

down(startup, From, #state{hosts=Hosts,
			    group_id = GroupId,
			    psa = Psa,
			    consumer_id = ConsumerId,
			    session_timeout = SessionTimeout,
			    topics=Topics}=State) ->
    GroupMetadata = dorb_msg:group_metadata(GroupId),
    case group_metadata(GroupMetadata, Hosts, 100, 5) of
	{ok, Pair} ->
	    ok = dorb_connection:maybe_start_pools([{x, Pair}]), % @todo fix API
	    JoinGroup = dorb_msg:join_group(GroupId,
					    SessionTimeout,
					    Topics,
					    ConsumerId,
					    Psa),
	    case join_group(JoinGroup, Pair, 100, 5) of
		{ok, {ConsumerId1, GGI, PTO}} ->
		    Ref = timeout(SessionTimeout),
		    {reply, joined,
		     next_state, member, State#state{
					   consumer_id = ConsumerId1,
					   pto = PTO,
					   ggi = GGI,
					   coordinator = Pair,
					   timer_ref = Ref}};
		_Error ->
		    down(startup, From, State)
	    end;
	_Error ->
	    down(startup, From, State)
    end;
down(_, _From, State) ->
    {reply, unsupported, down, State}.

timeout(SessionTimeout) ->
    SessionTimeout1 = random:uniform(SessionTimeout - 10),
    erlang:send_after(SessionTimeout1, self(), liveliness).

down(_Event, State) ->
    {next_state, down, State}.

handle_info(liveliness, member, #state{group_id = GroupId,
				       ggi = GGI,
				       coordinator = Pair,
				       session_timeout = SessionTimeout,
				       consumer_id = ConsumerId} = State) ->
    Heartbeat = dorb_msg:heartbeat(GroupId, GGI, ConsumerId),
    case dorb_connection:get_socket(Pair) of
	{ok, Socket} ->
	    case dorb_socket:send_sync(Socket, Heartbeat, 1000) of
		{ok, #{error_code := 0}} ->
		    Ref = timeout(SessionTimeout),
		    {next_state, member, State#state{timer_ref = Ref}};
		_ ->
		    % todo handle errors
		    {stop, normal, member, State}
	    end;
	_ ->
	    {stop, normal, member, State}
    end;
handle_info(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

% Internal
join_group(_, _, _, 0) ->
    {error, unable_to_join_group};
join_group(JoinGroup, {Host, Port}=Pair, Timeout, Retries) ->
    % Since it is a consumer group, it gets a persistant connection to the
    % coordinator.
    Pool = {binary_to_list(Host), Port},
    case dorb_connection:get_socket(Pool) of
	{ok, Socket} ->
	    join_group_(Pool, Socket, JoinGroup, Timeout, Retries);
	_Error ->
	    timer:sleep(Timeout),
	    join_group(JoinGroup, Pair, Timeout, Retries - 1)
    end.

join_group_(Pool, Socket, _, _, 0) ->
    dorb_connection:return_socket(Pool, Socket),
    {error, unable_to_join_group};
join_group_(Pool, Socket, JoinGroup, Timeout, Retries) ->
    case dorb_socket:send_sync(Socket, JoinGroup, 500) of
	{ok, #{error_code := 0,
	       consumer_id := ConsumerId,
	       group_generation_id := GGI,
	       partitions_to_own := PTO}} ->
	    dorb_connection:return_socket(Pool, Socket),
	    {ok, {ConsumerId, GGI, PTO}};
	_Error ->
	    timer:sleep(Timeout),
	    join_group_(Pool, Socket, JoinGroup, Timeout, Retries - 1)
    end.

group_metadata(_, _, _, 0) ->
    {error, unable_to_get_metadata};
group_metadata(GroupRequest, Hosts, Timeout, Retries) ->
    case dorb_connection:get_socket(Hosts) of
	{Pool, {ok, Socket}} ->
	    case dorb_socket:send_sync(Socket, GroupRequest, 1000) of
		{ok, #{error_code := 0,
		       coordinator_host := Host,
		       coordinator_port := Port}} ->
		    dorb_connection:return_socket(Pool, Socket),
		    {ok, {Host, Port}};
		_ ->
		    dorb_connection:return_socket(Pool, Socket),
		    timer:sleep(Timeout),
		    group_metadata(GroupRequest, Hosts, Timeout, Retries - 1)
	    end;
	_ ->
	    timer:sleep(Timeout),
	    group_metadata(GroupRequest, Hosts, Timeout, Retries - 1)
    end.

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

start_process_test() ->
    {ok, Pid} = start_link([{"localhost, 9092"}],
			   <<"test">>,
			   [<<"test">>],
			   <<"range">>, []),
    ?assertEqual(true, erlang:is_process_alive(Pid)).

join_group_test() ->
    application:ensure_all_started(dorb),
    dorb:start([{"localhost", 9092}]),
    {ok, Pid} = start_link([{"localhost", 9092}],
			   <<"i">>,
			   [<<"test">>],
			   <<"range">>, []),
    unlink(Pid),
    ?assertEqual(true, erlang:is_process_alive(Pid)),
    startup(Pid).
    
-endif.

	    %% TopicsPartitions =
	    %% 	[{TopicName,
	    %% 	  [Partition || Partition
	    %% 			    <- #{partition := Partition} <- Partitions]}
	    %% 	 || #{topic_name := TopicName, partitions := Partitions}
	    %% 	    <- PTO],
