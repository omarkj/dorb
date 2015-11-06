-module(dorb_error).
-export([error/1]).

-type code() :: 0|1|2|3|4|5|6|7|8|9|10|11|12|14|15|16.
-type msg() :: no_error|internal_error|offsets_out_of_range|invalid_message|
	 unknown_topic_or_partition|invalid_message_size|leader_not_available|
	 no_leader_for_partition|request_timed_out|broker_not_available|
	 replica_not_available|message_size_too_large|stale_controller_epoch|
	 offset_metadata_too_large|offset_load_in_progress|
	 consumer_coordinator_not_available|not_coordinator_for_consumer.

-export_type([code/0,
	      msg/0]).

-spec error(code()) -> msg().
error(0) ->
    % No error--it worked!
    no_error;
error(-1) ->
    % An unexpected server error
    internal_error;
error(1) ->
    % The requested offset is outside the range of offsets maintained by the
    % server for the given topic/partition.
    offsets_out_of_range;
error(2) ->
    % This indicates that a message contents does not match its CRC
    invalid_message;
error(3) ->
    % This request is for a topic or partition that does not exist on this
    % broker.
    unknown_topic_or_partition;
error(4) ->
    % The message has a negative size
    invalid_message_size;
error(5) ->
    % This error is thrown if we are in the middle of a leadership election and
    % there is currently no leader for this partition and hence it is
    % unavailable for writes.
    leader_not_available;
error(6) ->
    % This error is thrown if the client attempts to send messages to a replica
    % that is not the leader for some partition. It indicates that the clients
    % metadata is out of date.
    no_leader_for_partition;
error(7) ->
    % This error is thrown if the request exceeds the user-specified time limit
    % in the request.
    request_timed_out;
error(8) ->
    % This is not a client facing error and is used mostly by tools when a
    % broker is not alive.
    broker_not_available;
error(9) ->
    % If replica is expected on a broker, but is not (this can be safely
    % ignored).
    replica_not_available;
error(10) ->
    % The server has a configurable maximum message size to avoid unbounded
    % memory allocation. This error is thrown if the client attempt to produce a
    % message larger than this maximum.
    message_size_too_large;
error(11) ->
    % Internal error code for broker-to-broker communication.
    stale_controller_epoch;
error(12) ->
    % If you specify a string larger than configured maximum for offset metadata
    offset_metadata_too_large;
error(14) ->
    % The broker returns this error code for an offset fetch request if it is
    % still loading offsets (after a leader change for that offsets topic
    % partition).
    offset_load_in_progress;
error(15) ->
    % The broker returns this error code for consumer metadata requests or
    % offset commit requests if the offsets topic has not yet been created.
    consumer_coordinator_not_available;
error(16) ->
    % The broker returns this error code if it receives an offset fetch or
    % commit request for a consumer group that it is not a coordinator for.
    not_coordinator_for_consumer;
error(17) ->
    invalid_topic_exception;
error(18) ->
    record_list_too_large;
error(19) ->
    not_enough_replicas;
error(20) ->
    not_enough_replicast_after_append;
error(21) ->
    invalid_required_acks;
error(22) ->
    illegal_generation;
error(23) ->
    inconsistent_partition_assignment_strategy;
error(24) ->
    unknown_partition_assignment_strategy;
error(25) ->
    unknown_consumer_id;
error(26) ->
    invalid_timeout.
