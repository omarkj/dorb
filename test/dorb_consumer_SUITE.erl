-module(dorb_consumer_SUITE).

-include_lib("common_test/include/ct.hrl").


-export([all/0]).

-export([init_per_suite/1,
	 end_per_suite/1
	 %init_per_testcase/2,
	 %end_per_testcase/2
	]).

-export([flow/1]).

all() ->
    % I am not using suite/0 here because that means rebar3 returns 1 when
    % skipping, which would look like a failed test in travisci.
    case { os:getenv("KAFKA_TEST") } of
	{false} -> [];
	_ -> [flow]
    end.

init_per_suite(Config) ->
    application:ensure_all_started(dorb),
    [{group_name, base64:encode(crypto:strong_rand_bytes(10))}|Config].

end_per_suite(Config) ->
    Config.

flow(Config) ->
    ct:pal("Creating Group: ~p", [?config(group_name, Config)]),
    {ok, Pid} = dorb_consumer:start_link({"localhost", 9092},
					 ?config(group_name, Config),
					 [<<"test">>],
					 <<"range">>,
					 []),
    unlink(Pid),
    true = erlang:is_process_alive(Pid),
    %% Join a group, the group name is dynamically created so this will also
    %% create a new group
    {ok, PTO} = dorb_consumer:join(Pid, infinity),
    PTO = [{<<"test">>,[0]}],
    %% Additionally a timer should now be set, lets inspect the state
    {member, State} = sys:get_state(Pid),
    TRef1 = element(13, State),
    true = is_reference(TRef1),
    %% Commit a offset to a partition for this consumer group.
    {ok, CommittedOffset} =
	dorb_consumer:commit_offset(Pid, [{<<"test">>, [{0, 0, 0, <<>>}]}],
				    infinity),
    CommittedOffset = [#{partitions => [#{error => no_error,id => 0}],
			 topic => <<"test">>}],
    %% Check if the offset was successfully committed to the cluster
    {ok, FetchedOffset} = dorb_consumer:fetch_offset(Pid, [{<<"test">>, [0]}],
						     infinity),
    FetchedOffset = [#{partitions => [#{error => no_error,id => 0,
					offset => 0}],
		       topic => <<"test">>}],
    % Fake a heartbeat and check if the TRef has changed
    Pid ! heartbeat,
    erlang:yield(),
    {member, State1} = sys:get_state(Pid),
    TRef2 = element(13, State1),
    true = is_reference(TRef2),
    false = TRef1 == TRef2,
    Config.
