-module(dorb_socket).
-behaviour(gen_server).

% The Dorb Socket is a implementation of a socket that implements the Kafka
% protocol. Kafka's protocol allows for multiplexing of commands identified by a
% CurrId in the header.
% Header: [ApiKey (int16), ApiVersion (int16), CorrId (int32),
% ClientIdLength (int16), ClientId (string)]
% Kafka's replies look like so:
% [CorrId (int32), Reply (mixed)]
% Replies are not identified by a magic byte or a constant so the only way to
% parse them efficiently is to be able to already know what type of reply is
% expected for a given CorrId and it is therefore crucial to keep it around.
%
% This process does not try to reconnect. As soon as a connection is lost for
% whatever reason it is lost, and cannot be recovered, and all the state is
% invalid.

-record(state, {
	  host :: inet:ip_address()|inet:hostname(),
	  port :: inet:port_number(),
	  socket :: gen_tcp:socket()|undefined,
	  buffer = <<>> :: binary(),
	  corrid = 1 :: non_neg_integer(),
	  sock_opts :: [],
	  corrids = #{} :: #{}
	 }).

-record(waiter, {
	  corr_id :: integer(),
	  api_key :: integer(),
	  caller :: pid(),
	  caller_ref :: reference(),
	  message :: term()|undefined
	 }).

% API
-export([start_link/1,
	 stop/1,
	 send/2,
	 send_sync/3]).

% gen_server callbacks
-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3]).

% API implementation
-spec start_link({Host, Port}) -> {ok, Pid} when
      Host :: inet:ip_address()|inet:hostname(),
      Port :: inet:port_number(),
      Pid :: pid().
start_link({_Host, _Port}=Args) ->
    gen_server:start_link(?MODULE, [Args], []).

-spec stop(Connection) -> ok when
      Connection :: pid().
stop(Connection) ->
    gen_server:cast(Connection, stop).

-spec send(Connection, Msg) ->
		  {ok, Ref} when
      Connection :: pid(),
      Msg :: dorb_msg:msg(),
      Ref :: reference().
send(Connection, Msg) ->
    gen_server:call(Connection, {send, self(), Msg}).

-spec send_sync(Connection, Msg, Timeout) -> {ok, Message}|
					     {error, timeout} when
      Connection :: pid(),
      Msg :: dorb_msg:msg(),
      Timeout :: integer(),
      Message :: dorb_parser:reply().
send_sync(Connection, Msg, Timeout) ->
    {ok, Ref} = send(Connection, Msg),
    receive
	{dorb_msg, Ref, Message} ->
	    {ok, Message}
    after Timeout ->
	    {error, timeout}
    end.

% gen_server implementation
init([{Host, Port}]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #state{host = Host,
		port = Port}}.

handle_call({send, Sender, {ApiKey, Msg}}, From,
	    #state{corrid=CorrId,
		   socket=Socket,
		   host=Host,
		   port=Port,
		   corrids=CorrIds}=State) ->
    Ref = erlang:make_ref(),
    gen_server:reply(From, {ok, Ref}),
    case maybe_connect(Socket, Host, Port) of
	{ok, Socket1} ->
	    EncodedMsg = dorb_parser:encode(ApiKey, <<"dorb">>, CorrId,
					    Msg),
	    case send_message(Socket1, EncodedMsg) of
		ok ->
		    Waiter = #waiter{api_key = ApiKey,
				     caller = Sender,
				     caller_ref = Ref},
		    {noreply,
		     State#state{socket=Socket1,
				 corrid=next_corr_id(CorrId),
				 corrids=CorrIds#{CorrId=>Waiter}}};
		{error, _Reason} ->
		    {stop, normal, State}
	    end;
	{error, _Reason} ->
	    {stop, normal, State}
    end;
handle_call(_Call, _From, State) ->
    {noreply, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Bin}, #state{socket=Socket,
				       buffer=Buffer,
				       corrids=CorrIds}=State) ->
    {Messages, CorrIds1, Tail} = parse_incoming(<<Buffer/binary, Bin/binary>>,
						CorrIds, []),
    notify(Messages),
    inet:setopts(Socket, [{active, once}]),
    {noreply, State#state{buffer=Tail,
			  corrids=CorrIds1}};
handle_info({tcp_error, Socket, _Reason}, #state{socket=Socket}=State) ->
    % The socket has closed. All the state kept in this process is now outdated
    % and the process can be shut down.
    {stop, normal, State#state{socket=undefined}};
handle_info({tcp_closed, Socket}, #state{socket=Socket}=State) ->
    % The socket has closed. All the state kept in this process is now outdated
    % and the process can be shut down.
    {stop, normal, State#state{socket=undefined}};
handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _NewVsn) ->
    {ok, State}.

terminate(_Reason, #state{socket=undefined}) ->
    ok;
terminate(_Reason, #state{socket=Socket}) ->
    gen_tcp:close(Socket),
    ok.

% Internals
next_corr_id(2147483647) ->
    1;
next_corr_id(CorrId) ->
    CorrId + 1.

-spec maybe_connect(undefined|port(), inet:ip_address()|inet:hostname(),
		    Port :: inet:port_number()) -> {ok, port()}.
maybe_connect(undefined, Host, Port) ->
    gen_tcp:connect(Host, Port, [{active, once}, {packet, raw}, binary], 5000);
maybe_connect(Socket, _, _) ->
    {ok, Socket}.

send_message(Socket, Message) ->
    gen_tcp:send(Socket, Message),
    inet:setopts(Socket, [{active, once}]).

-spec notify([#waiter{}]) -> ok.
notify([#waiter{caller=Pid, caller_ref=Ref,
		message=Message}|Messages]) ->
    Pid ! {dorb_msg, Ref, Message},
    notify(Messages);
notify([]) ->
    ok.

-spec parse_incoming(Buffer, CorrIds, Acc) ->
			    {Acc, CorrIds, Buffer} when
      Buffer :: binary(),
      CorrIds :: #{},
      Acc :: [#waiter{}].
parse_incoming(<<Size:32/signed-integer,
		 Message:Size/binary, Rest/binary>>,
	       CorrIds, Acc) ->
    % Found a whole message. Parse and try and pair the message
    {CorrIds1, Acc1} = parse_message(Message, CorrIds, Acc),
    parse_incoming(Rest, CorrIds1, Acc1);
parse_incoming(Buffer, CorrIds, Acc) ->
    % No more parsing to be done. Return
    {Acc, CorrIds, Buffer}.

-spec parse_message(Message, CorrIds, Acc) ->
			   {CorrIds, Acc} when
      Message :: binary(),
      CorrIds :: #{},
      Acc :: [#waiter{}].
parse_message(<<CorrId:32/signed-integer, Msg1/binary>>, CorrIds, Acc) ->
    case maps:find(CorrId, CorrIds) of
	{ok, #waiter{api_key=ApiKey}=Waiter} ->
	    % Parsed a message and found a corresponding CorrId. Add it to the
	    % Acc and move parse some more.
	    {ok, ParsedMessage} = dorb_parser:parse(ApiKey, Msg1),
	    CorrIds1 = maps:remove(CorrId, CorrIds),
	    {CorrIds1, [Waiter#waiter{message=ParsedMessage}|Acc]};
	error ->
	    % Parsed a message, but there is no corresponding CorrId in the map,
	    % throw the message away and move on.
	    {CorrIds, Acc}
    end.

-include_lib("eunit/include/eunit.hrl").
-ifdef(TEST).

next_corr_id_test() ->
    ?assertEqual(2, next_corr_id(1)),
    ?assertEqual(1, next_corr_id(2147483647)).

notify_test() ->
    Ref1 = erlang:make_ref(),
    Ref2 = erlang:make_ref(),
    ?assertEqual(ok, notify([#waiter{caller=self(),
				     caller_ref=Ref1,
				     message=test1},
			     #waiter{caller=self(),
				     caller_ref=Ref2,
				     message=test2}
			    ])),
    receive
	Res1 ->
	    ?assertEqual({dorb_msg, Ref1, test1}, Res1),
	    receive
		Res2 ->
		    ?assertEqual({dorb_msg, Ref2, test2}, Res2)
	    end
    end.

parse_incoming_skips_partial_messages_test() ->
    Buf = <<22:32/signed, "not-a-full-message">>,
    ?assertEqual({[], #{}, Buf}, parse_incoming(Buf, #{}, [])).

parse_incoming_can_parse_many_test() ->
    Buf = <<6:32/signed, 1:32/signed, 0,0,
	    6:32/signed, 2:32/signed, 0,0>>,
    CorrIds = #{1 => #waiter{api_key=12},
		2 => #waiter{api_key=12}},
    ?assertEqual({[#waiter{api_key=12,
			   message=#{error_code => 0}},
		   #waiter{api_key=12,
			   message=#{error_code => 0}}], #{}, <<>>},
		 parse_incoming(Buf, CorrIds, [])).

parse_incoming_parses_portions_if_needed_test() ->
    Buf = <<6:32/signed, 1:32/signed, 0,0,
	    8:32/signed, 2:32/signed, 0,0>>,
    CorrIds = #{1 => #waiter{api_key=12}},
    ?assertEqual({[#waiter{api_key=12,
			   message=#{error_code => 0}}], #{},
		  <<8:32/signed, 2:32/signed, 0,0>>},
		 parse_incoming(Buf, CorrIds, [])).

-endif.
