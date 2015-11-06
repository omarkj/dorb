-module(dorb_consumer_sup).
-behaviour(supervisor).

% API
-export([start_link/5,
	 start_link/0,
	 start_partition/5,
	 notify_partitions/2,
	 stop_partitions/1
	]).

% callbacks
-export([init/1]).

start_link(Hosts, GroupId, Topics, PSA, Opts) ->
    supervisor:start_link(?MODULE,
			  [cg, Hosts, GroupId, Topics, PSA, Opts]).

start_link() ->
    supervisor:start_link(?MODULE, []).

start_partition(ConsumerSup, Topic, Partition,
		ConsumerGroup, {M,F,A}) ->
    ConsumersSup = proplists:get_value(consumers,
				       supervisor:which_children(ConsumerSup)),
    A1 = A++[#{topic => Topic,
	       partition => Partition,
	       consumer_group => ConsumerGroup}],
    supervisor:start_child(ConsumersSup,
			   #{id => {Topic, Partition},
			     start => {M, F, A1},
			     restart => transient,
			     shutdown => 1000,
			     type => worker,
			     modules => [M]});
start_partition(_, _, _, _, _) -> ok.


notify_partitions(ConsumerSup, Message) ->
    ConsumersSup = proplists:get_value(consumers,
				       supervisor:which_children(ConsumerSup)),
    Partitions = supervisor:which_children(ConsumersSup),
    [Pid ! Message || {_, Pid, _, _} <- Partitions,
		      is_pid(Pid)],
    ok.

stop_partitions(ConsumerSup) ->
    supervisor:restart_child(ConsumerSup, consumers),
    ok.
    
init([cg, Hosts, GroupId, Topics, PSA, Opts]) ->
    % This is the Supervisor for the consumer group process and another
    % supervisor that manages the individual (partition) consumers.
    
    % This supervisor will terminate itself and all children if:
    % * The consumer group process crashes more than once over a 5 second
    %   period
    {ok, {#{strategy => one_for_all,
	    intensity => 1,
	    period => 5},
	  [
	   #{id => consumers,
	     start => {?MODULE, start_link, []},
	     restart => permanent,
	     shutdown => 5000,
	     type => supervisor,
	     modules => [?MODULE]},
	   #{id => consumer_group,
	     start => {dorb_consumer, start_link, [self(), Hosts, GroupId,
						   Topics, PSA, Opts]},
	     restart => permanent,
	     shutdown => 5000,
	     type => worker,
	     modules => [dorb_consumer]}
	  ]}
    };
init([]) ->
    {ok, {#{strategy => one_for_one,
	    intensity => 1,
	    period => 5}, []}}.
    

