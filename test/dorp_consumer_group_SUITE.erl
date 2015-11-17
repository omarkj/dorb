-module(dorp_consumer_group_SUITE).

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
    {ok, Socket} = dorb_socket:get({"localhost", 9092}),
    [{socket, Socket},
     {group_name, base64:encode(crypto:strong_rand_bytes(10))}|Config].

end_per_suite(Config) ->
    dorb_socket:return(?config(socket, Config)),
    Config.

flow(Config) ->
    Socket = ?config(socket, Config),
    GroupName = ?config(group_name, Config),
    {_, _} = dorb_consumer_group:metadata(Socket, GroupName, 1000),
    {Socket1, {ok, #{consumer_id := CID,
		     ggi := GGI}}}
	= join_group(Socket, GroupName, 5, undefined),
    {ok,[
	 #{partitions :=
	       [#{error := unknown_topic_or_partition,
		  id := 0,
		  offset := -1}],
	   topic := <<"test">>}]} =
	dorb_consumer_group:fetch_offset(Socket1, GroupName, [{<<"test">>, [0]}],
					 2000),
    {ok,
     [#{partitions := 
	    [#{error := no_error,
	       id := 0}],
	topic := <<"test">>}]} =
	dorb_consumer_group:commit_offset(Socket1, GroupName,
					  [{<<"test">>, [{0, 0,
							  erlang:system_time(),
							  <<>>}]}], 2000),
    {ok,[
	 #{partitions :=
	       [#{error := no_error,
		  id := 0,
		  offset := 0}],
	   topic := <<"test">>}]} =
	dorb_consumer_group:fetch_offset(Socket1, GroupName, [{<<"test">>, [0]}],
					 2000),
    {ok, online} = dorb_consumer_group:heartbeat(Socket1, GroupName, GGI, CID,
						 2000),
    dorb_socket:return(Socket),
    Config.

%% Internal
join_group(_, _, 0, Error) ->
    Error;
join_group(Socket, GroupName, Retries, _Error) ->
    case dorb_consumer_group:join(Socket,
				  GroupName,
				  10000, [<<"test">>], <<>>,
				  <<"range">>, 2000) of
	{ok, _} = Res ->
	    {Socket, Res};
	{kafka_error, KafkaError} ->
	    join_group(Socket, GroupName, Retries-1, KafkaError);
	{error, _} = Err ->
	    dorb_socket:return(Socket),
	    {ok, Socket1} = dorb_socket:get({"localhost", 9092}),
	    join_group(Socket1, GroupName, Retries-1, Err)
    end.
