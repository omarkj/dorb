-module(dorb_consumer).
-behaviour(gen_fsm).

-export([start_link/5,
	 start_link/6]).

% gen_fsm callbacks
-export([init/1,
	 handle_event/3,
	 handle_sync_event/4,
	 handle_info/3,
	 code_change/4,
	 terminate/3
	]).

-export([down/2,
	 down/3,
	 member/2,
	 member/3,
	 rebalancing/3,
	 rebalancing/2
	]).

-export([join/2,
	 commit_offset/3,
	 fetch_offset/3
	]).

-record(state, {
	  hosts :: dorb:hosts() | dorb:host(),
	  group_id :: binary(),
	  topics :: [binary()],
	  psa :: binary(),
	  consumer_id :: binary(),
	  pto :: [#{}],
	  ggi :: non_neg_integer(),
	  session_timeout :: non_neg_integer(),
	  socket_timeout :: non_neg_integer(),
	  coordinator :: dorb:host(),
	  rebalance_retries :: non_neg_integer(),
	  tref :: reference(),
	  commit_offset_retries :: non_neg_integer(),
	  sup :: pid()|undefined,
	  worker_mfa :: mfa()|undefined,
	  rebalance_commit_timeout :: non_neg_integer()
	 }).

start_link(Sup, Hosts, GroupId, Topics, PSA, Opts) ->
    gen_fsm:start_link(?MODULE, [Hosts, GroupId, Topics, PSA,
				 Opts++[{sup, Sup}]], []).

start_link(Hosts, GroupId, Topics, PSA, Opts) ->
    gen_fsm:start_link(?MODULE, [Hosts, GroupId, Topics, PSA, Opts], []).

join(Pid, Timeout) ->
    gen_fsm:sync_send_event(Pid, join, Timeout).

commit_offset(Pid, Topics, Timeout) ->
    gen_fsm:sync_send_event(Pid, {commit_offset, Topics}, Timeout).

fetch_offset(Pid, Topics, Timeout) ->
    gen_fsm:sync_send_event(Pid, {fetch_offset, Topics}, Timeout).

init([Hosts, GroupId, Topics, PSA, Opts]) ->
    ConsumerId = proplists:get_value(consumer_id, Opts, <<>>),
    SessionTimeout = proplists:get_value(session_timeout, Opts, 6000),
    SocketTimeout = proplists:get_value(socket_timeout, Opts, 6000),
    RebalanceRetries = proplists:get_value(rebalance_retries, Opts, 4),
    CommitOffsetRetries = proplists:get_value(commit_offset_retries, Opts, 5),
    Sup = proplists:get_value(sup, Opts),
    WorkerMfa = proplists:get_value(worker_mfa, Opts),
    RebalanceCommitTimeout = proplists:get_value(rebalance_commit_timeout,
						 Opts, 5000),
    erlang:process_flag(trap_exit, true),
    {ok, down, #state{
		  hosts = Hosts,
		  group_id = GroupId,
		  topics = Topics,
		  psa = PSA,
		  consumer_id = ConsumerId,
		  session_timeout = SessionTimeout,
		  socket_timeout = SocketTimeout,
		  rebalance_retries = RebalanceRetries,
		  commit_offset_retries = CommitOffsetRetries,
		  sup = Sup,
		  worker_mfa = WorkerMfa,
		  rebalance_commit_timeout = RebalanceCommitTimeout
		 }}.

down(join, _From, #state{rebalance_retries=RBR} = State) ->
    join(State, RBR, undefined).

down(_Event, State) ->
    {next_state, down, State}.

member({commit_offset, Pairs}, _From,
       #state{
	  commit_offset_retries = CommitOffsetRetries
	 } = State) ->
    commit_offset(Pairs, State, CommitOffsetRetries, undefined);
member({fetch_offset, Pairs}, _From,
       #state{
	  commit_offset_retries = CommitOffsetRetries
	  } = State) ->
    fetch_offset(Pairs, State, CommitOffsetRetries, undefined).

member(_Event, State) ->
    {next_state, member, State}.

rebalancing({commit_offset, Pairs}, _From,
	    #state{
	       commit_offset_retries = CommitOffsetRetries
	      } = State) ->
    case commit_offset(Pairs, State, CommitOffsetRetries, undefined) of
	{reply, {ok, Res}, _, State1} ->
	    {reply, {ok, Res}, rebalancing, State1};
	O ->
	    O
    end.

rebalancing(_, State) ->
    {next_state, rebalancing, State}.

handle_info(heartbeat, member, State) ->
    heartbeat(State);
handle_info(rebalance, rebalancing, State) ->
    rebalance(State);
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
heartbeat_timer(SessionTimeout) ->
    erlang:send_after(SessionTimeout - random:uniform(SessionTimeout),
		      self(), heartbeat).

discover_coordinator(_, 0, LastError) ->
    LastError;
discover_coordinator(#state{
			hosts = Hosts,
			group_id = GroupId,
			socket_timeout = SocketTimeout
		       } = State, Retries, _LastError) ->
    {ok, Socket} = dorb_connection:get_socket(Hosts),
    case dorb_consumer_group:metadata(Socket, GroupId, SocketTimeout) of
	{ok, Coordinator} ->
	    dorb_connection:return_socket(Socket),
	    {ok, Coordinator};
	{kafka_error, _E} = Error ->
	    dorb_connection:return_socket(Socket),
	    timer:sleep(500),
	    discover_coordinator(State, Retries-1, Error);
	Error ->
	    dorb_connection:return_socket(Socket),
	    discover_coordinator(State, Retries-1, Error)
    end.

join(State, 0, LastError) ->
    % Unable to join the consumer group and gave up retrying. There is no way to
    % recover from this and we give up.
    {stop, normal, LastError, State};
join(#state{
	group_id = GroupId,
	topics = Topics,
	psa = PSA,
	consumer_id = ConsumerId,
	socket_timeout = SocketTimeout,
	session_timeout = SessionTimeout,
	sup=Sup,
	worker_mfa=WorkerMfa
       } = State, RBR, _LastError) ->
    case discover_coordinator(State, RBR, undefined) of
	{ok, Coordinator} ->
	   % Get a coordinator socket
	    {ok, Socket1} = dorb_connection:get_socket(Coordinator),
	    case dorb_consumer_group:join(Socket1, GroupId, SessionTimeout,
					  Topics, ConsumerId, PSA,
					  SocketTimeout) of
		{ok, #{consumer_id := ConsumerId1,
		       ggi := GGI,
		       pto := PTO}} ->
		    % Joined Successfully. Return the socket
		    dorb_connection:return_socket(Socket1),
		    % Set a timer to maintain membership via heartbeats
		    Tref = heartbeat_timer(SessionTimeout),
		    maybe_start_workers(Sup, WorkerMfa, PTO),
		    {reply, {ok, PTO}, member, State#state{
						 consumer_id = ConsumerId1,
						 pto = PTO,
						 ggi = GGI,
						 coordinator = Coordinator,
						 tref = Tref}};
		{kafka_error, inconsistent_partition_assignment_strategy=R} ->
		    {stop, normal, R, State};
		{kafka_error, unknown_partition_assignment_strategy=R} ->
		    {stop, normal, R, State};
		{kafka_error, unknown_consumer_id=R} ->
		    {stop, normal, R, State};
		{kafka_error, invalid_timeout=R} ->
		    {stop, normal, R, State};
		{kafka_error, _Error} = KE ->
		    % Hit a Kafka error. Back off, and retry
		    dorb_connection:return_socket(Socket1),
		    timer:sleep(500),
		    join(State, RBR-1, KE);
		{error, _Error} = E ->
		    % Hit a socket error (most probably a timeout)
		    dorb_connection:return_socket(Socket1),
		    join(State, RBR-1, E)
	    end;
	{error, _Error} = E ->
	    % Hit a socket error (most probably a timeout)
	    join(State, 0, E) % The discover function loops
    end.

commit_offset(_, State, 0, LastError) ->
    {reply, LastError, down, State};
commit_offset(Pairs, #state{
			group_id = GroupId,
			coordinator = Coordinator,
			socket_timeout = SocketTimeout
		       } = State, COR, _LastError) ->
    {ok, Socket} = dorb_connection:get_socket(Coordinator),
    case dorb_consumer_group:commit_offset(Socket, GroupId, Pairs,
					   SocketTimeout) of
	{ok, Res} ->
	    % Got a commit offset from the cluster
	    dorb_connection:return_socket(Socket),
	    {reply, {ok, Res}, member, State};
	{error, _Error} = E ->
	    % Hit a socket error (most probably a timeout)
	    dorb_connection:return_socket(Socket),
	    commit_offset(Pairs, Socket, COR-1, E)
    end.

fetch_offset(Pairs, #state{
		       group_id = GroupId,
		       coordinator = Coordinator,
		       socket_timeout = SocketTimeout
		      } = State, FOR, _LastError) ->
    {ok, Socket} = dorb_connection:get_socket(Coordinator),
    case dorb_consumer_group:fetch_offset(Socket, GroupId, Pairs,
					  SocketTimeout) of
	{ok, Res} ->
	    dorb_connection:return_socket(Socket),
	    {reply, {ok, Res}, member, State};
	{error, _Error} = E ->
	% Hit a socket error (most probably a timeout)
	    dorb_connection:return_socket(Socket),
	    fetch_offset(Pairs, State, FOR-1, E)
    end.

heartbeat(#state{
	     group_id = GroupId,
	     ggi = GGI,
	     consumer_id = ConsumerId,
	     socket_timeout = SocketTimeout,
	     coordinator = Coordinator,
	     session_timeout = SessionTimeout,
	     rebalance_retries = RBR
	    } = State) ->
    {ok, Socket} = dorb_connection:get_socket(Coordinator),
    case dorb_consumer_group:heartbeat(Socket, GroupId, GGI, ConsumerId,
				       SocketTimeout) of
	{ok, online} ->
	    % Consumer group is still online. Set a new counter and continue
	    TRef = heartbeat_timer(SessionTimeout),
	    dorb_connection:return_socket(Socket),
	    {next_state, member, State#state{
				   tref = TRef
				  }};
	{kafka_error, unknown_consumer_id=R} ->
	    % This consumer is unknown by the Kafka cluster. Shut down. Arguably
	    % the consumer group could join with the empty name but this
	    % indicates a bad configuration and the safest thing to do is to
	    % shut down
	    error_logger:info_msg("Invalid Consumer Id for Consumer Group"),
	    dorb_connection:return_socket(Socket),
	    {stop, normal, State};
	{kafka_error, illegal_generation}=R ->
	    dorb_connection:return_socket(Socket),
	    rejoin(State, R);
	{error, _Error} ->
	    dorb_connection:return_socket(Socket),
	    case discover_coordinator(State, RBR, undefined) of
		{ok, Coordinator1} ->
		    % Found a new coordinator. Heartbeat it and maybe trigger a
		    % new generation
		    heartbeat(State#state{
				coordinator = Coordinator1});
		{error, _Error} = E ->
		    % Unable to find a new coordinator. Shut down.
		    {stop, E, State}
	    end
    end.

rejoin(#state{sup=undefined}=State, LastErr) ->
    join(State, LastErr);
rejoin(#state{sup=Sup,
	      rebalance_commit_timeout=RCT}=State, _LastErr) ->
    % The kafka cluster has rebalanced. All consumers should stop
    % consumption, commit offsets and the consumer group should rejoin.
    dorb_consumer_sup:notify_partitions(Sup, commit_offsets),
    erlang:send_after(RCT, self(), rebalance),
    {next_state, rebalancing, State}.

rebalance(#state{sup=undefined}=State) ->
    join(State, undefined);
rebalance(#state{sup=Sup}=State) ->
    dorb_consumer_sup:stop_partitions(Sup),
    join(State, undefined).

maybe_start_workers(undefined, _, _) ->
    ok;
maybe_start_workers(_, undefined, _) ->
    ok;
maybe_start_workers(_, _, []) ->
    ok;
maybe_start_workers(Sup, MFA, [{Topic, Partitions}|Rest]) ->
    start_worker(Sup, MFA, Topic, Partitions),
    maybe_start_workers(Sup, MFA, Rest).

start_worker(_, _, _, []) ->
    ok;
start_worker(Sup, MFA, Topic, [Partition|Partitions]) ->
    dorb_consumer_sup:start_partition(Sup, Topic, Partition,
				      self(), MFA),
    start_worker(Sup, MFA, Topic, Partitions).

