%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%
%   Licensed under the Apache License, Version 2.0 (the "License");
%   you may not use this file except in compliance with the License.
%   You may obtain a copy of the License at
%
%       http://www.apache.org/licenses/LICENSE-2.0
%
%   Unless required by applicable law or agreed to in writing, software
%   distributed under the License is distributed on an "AS IS" BASIS,
%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%   See the License for the specific language governing permissions and
%   limitations under the License.
%%%-------------------------------------------------------------------
%%% File    : tmanager_pac.erl
%%% Author  : Monika Moser <moser@zib.de>
%%% Description : Paxos Atomic Commit for the transaction manager
%%% Created :  11 Feb 2007 by Monika Moser <moser@zib.de>
%%%-------------------------------------------------------------------
%% @author Monika Moser <moser@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id$
%% @doc 
-module(transstore.tmanager_pac).

-author('moser@zib.de').
-vsn('$Id$').

-include("trecords.hrl").

-export([process_vote/2, process_vote_ack/5, process_read_vote/2, collect_rv_ack/5, vote_for_suspected/1]).

-import(cs_symm_replication).
-import(cs_send).
-import(io).
-import(dict).
-import(lists).
-import(config).
-iport(erlang).


%%--------------------------------------------------------------------
%% Function: check_vote/2
%% Purpose:  check whether the vote can be accepted or there has been
%%                 a vote with a higher timestamp accepted yet

%% Returns:  {vote_at_tm, {Decision, ReadTimestamp, WriteTimestamp}}|
%%                  {nack}
%% Description: this corresponds to accepting votes in the write phase
%%                   of Paxos consensus
%%-------------------------------------------------------------------
process_vote(TMState, Vote)->
    CurrentVote = trecords:get_vote(TMState, Vote#vote.key, Vote#vote.rkey),
    {_CurrVal, CurrRTS, CurrWTS} = CurrentVote,
    
    if 
	Vote#vote.timestamp >= CurrRTS andalso Vote#vote.timestamp >= CurrWTS -> 
	    TMState2 = trecords:store_vote(TMState, 
					   Vote#vote.key,
					   Vote#vote.rkey,
					   {Vote#vote.decision, CurrRTS, Vote#vote.timestamp}
					  ),
	    tsend:send(TMState2#tm_state.leader,  {vote_ack, 
						   Vote#vote.key, 
						   Vote#vote.rkey, 
						   Vote#vote.decision, 
						   Vote#vote.timestamp}),
	    TMState2;
	true ->
	    TMState							    
    end.


process_vote_ack(#tm_state{decision = undecided} = TMState, Key, RKey, Dec, TS) ->
    TMState2 = trecords:store_vote_acks(TMState, Key, RKey, {Dec, TS}),
    Decision = check_acks(TMState2),
    %io:format("~p ~p~n", [self(), process_vote_ack]),
    if
	Decision == undecided ->
	    TMState2;
	true -> %% Decision == commit/abort
	    Message = {decision, 
		       #tp_message{
			 item_key = unknown, 
			 message = {cs_send:this(),TMState2#tm_state.transID, Decision}
			}
			      },
	    tsend:send_to_participants(TMState2, Message),
	    tsend:send_to_rtms(TMState2, {decision, Decision}),
	    TMState2#tm_state{decision = Decision}
    end;
process_vote_ack(TMState, Key, RKey, Dec, TS) ->
    trecords:store_vote_acks(TMState, Key, RKey, {Dec, TS}).

%%--------------------------------------------------------------------
%% Function: check_acks/1
%% Purpose:  check the acknowledgements for all item
%% Args:     TMState - State holding the Acks   
%% Returns:  commit/abort/undecided
%%--------------------------------------------------------------------
check_acks(TMState)->
    Acks = trecords:get_vote_acks(TMState),
    AcksList = dict:to_list(Acks),
    check_acks_item(AcksList).

%%--------------------------------------------------------------------
%% Function: check_acks_item/1
%% Purpose:  check the acknowledgements per each item
%% Args:     Acks - List with a dict for each item
%%                  [{Item, dict({Replica, List}}, ..]   
%% Returns:  commit/abort/undecided
%% Description: Uses the fold function for the dictionary
%%                   which holds the acknowledgements for each replica
%%                   of an item.
%%              This fold function sorts acknowledgments into two
%%                   separate lists, holding either acks for prepared
%%                   or acks for abort.
%%              In the it counts the prepared and abort decisions.
%%              If there are enough for abort it returns abort, 
%%                   if there are enough for prepared it continues
%%                   with the next item.
%%              If all items are prepared it returns commit.
%%-------------------------------------------------------------------
check_acks_item([])->
    commit;
check_acks_item([{_, AcksReplica}| Rest]) ->
    %% sort replicas that have decision prepared into PreparedList, those who are abort into AbortList
    {NumPrepared, NumAbort} = dict:fold(fun(_RKey, AcksPerReplicaList, {PreparedAcc, AbortAcc})-> 
						case check_acks_replica(AcksPerReplicaList) of
						    prepared ->
							{PreparedAcc + 1, AbortAcc};
						    abort ->
							{PreparedAcc, AbortAcc + 1};
						    _ ->
							{PreparedAcc, AbortAcc}
						end
					end,
					{0, 0},
					AcksReplica),
    PreparedLimit = config:quorumFactor(),
    AbortLimit = config:replicationFactor() - config:quorumFactor() + 1, 

    if
	NumAbort >= AbortLimit ->
	    abort;
	NumPrepared >= PreparedLimit ->
	    check_acks_item(Rest);
	true ->
	    undecided
    end.

%%--------------------------------------------------------------------
%% Function: check_acks_replica/1
%% Purpose:  check the acknowledgements per each replica
%% Args:     ARList - List containing Acks for replicas
%%                  [{Decision, Timestamp}, ..]   
%% Returns:  prepared/abort/undecided
%% Description: Uses the fold function for a list to accumulate
%%                   the votes for each timestamp occuring in the list 
%%              If there is an ack for a certain timestamp, all acks
%%                   for that timestamp have the same decision 
%%              There has to be a majority of acks with the same time-
%%                   stamp.
%%              If there is a majority we know, that the certain 
%%                   Paxos consensus instance with that timestamp can
%%                   make a decision.
%%-------------------------------------------------------------------
-type(decision_type() :: prepared | abort).
-type(timestamp_type() :: pos_integer()).
-spec(check_acks_replica/1 :: (list({decision_type(),timestamp_type()})) -> decision_type() | undecided).
check_acks_replica(AcksForReplicaList) ->
    count_acks_replica(-1, bottom, 0, erlang:trunc(config:replicationFactor()/2) + 1, lists:keysort(2, AcksForReplicaList)).
    
count_acks_replica(_LastTimeStamp, LastDecision, Count, Limit, _) when Count >= Limit ->
    LastDecision;
count_acks_replica(_LastTimeStamp, _LastDecision, _Count, _Limit, []) ->
    undecided;
count_acks_replica(LastTimeStamp, LastDecision, Count, Limit, [{LastDecision, LastTimeStamp} | Rest]) ->
    count_acks_replica(LastTimeStamp, LastDecision, Count + 1, Limit, Rest);
count_acks_replica(_LastTimeStamp, _LastDecision, _Count, Limit, [{NewDecision, NewTimeStamp} | Rest]) ->
    count_acks_replica(NewTimeStamp, NewDecision, 1, Limit, Rest).
    
    


%%--------------------------------------------------------------------
%% Function: read_vote/2
%% Purpose:  If a TP has failed the TM will vote in the TP's consensus
%%              instance by executing a read phase
%% Args: TMState - the state of the TM
%%       Vote - the vote
%% Description: Check whether the Vote can be acknowledged for the
%%                  read phase
%%--------------------------------------------------------------------
process_read_vote(TMState, Vote)->
    CheckResult = check_read_vote(TMState, Vote),
    if
	CheckResult == nack ->
	    TMState;
	true ->
	    {Val, _TS, WTS} = CheckResult,
	    TMState2 = trecords:store_vote(TMState, Vote#vote.key, Vote#vote.rkey, CheckResult), 
	    cs_send:send(TMState2#tm_state.leader, {read_vote_ack, Vote#vote.key, Vote#vote.rkey, Vote#vote.timestamp, {Val, WTS}}),
	    TMState2
    end.

%%--------------------------------------------------------------------
%% Function: check_read_vote/2
%% Purpose:  check whether the vote can be accepted or there has been
%%                 a vote with a higher timestamp accepted yet
%% Args:     TMState - State holding the Acks
%%           Vote - the vote to pe checked
%% Returns:  {read_vote_at_tm, {Decision, ReadTimestamp, WriteTimestamp}}|
%%                  {nack}
%% Description: this corresponds to accepting votes in the read phase
%%                   of Paxos consensus
%%-------------------------------------------------------------------

check_read_vote(TMState, Vote)->
    CurrentVote = trecords:get_vote(TMState, Vote#vote.key, Vote#vote.rkey),
    {CurrVal, CurrRTS, CurrWTS} = CurrentVote,
    if 
	Vote#vote.timestamp > CurrRTS andalso Vote#vote.timestamp > CurrWTS ->
	    {CurrVal, Vote#vote.timestamp, CurrWTS};
	true ->
	    nack
    end.


%%--------------------------------------------------------------------
%% Function: collect_rv_ack/5 
%% Purpose:  collect all the acknowledgements we get for a read phase
%% Args: TMState - the state of the TM
%%       Key - the key the ack refers to
%%       RKey - the replica the ack refers to
%%       Timestamp - the timestamp of the acknowledged vote
%%       StoredVote - {Value, Timestamp}, the latest vote accepted
%% Description: Collect all the votes and check whether we can execute
%%                  a write phase for that vote
%%--------------------------------------------------------------------

collect_rv_ack(TMState, Key, RKey, Timestamp, StoredVote) ->
    TMState2 = trecords:store_read_vote_acks(TMState, Key, RKey, {StoredVote, Timestamp}),
    RVAcks = trecords:get_read_vote_acks(TMState2, Key, RKey),
    WriteVote = check_rv_acks(RVAcks),
    if
	WriteVote == not_enough ->
	    TMState2;
	true ->
	    {VoteToAdopt, TSForWriteVote} = WriteVote,
	    NewVote = trecords:new_vote(TMState2#tm_state.transID, Key, RKey, VoteToAdopt, TSForWriteVote),
	    lists:map(fun({_RKey, Address}) ->
			      tsend:send(Address, {vote, cs_send:this(), NewVote}) end, 
		      TMState2#tm_state.rtms),
	    TMState2
    end.
	    
check_rv_acks(RVAcksList)->    
    CounterList = [],
    NewCounterList = lists:foldl(fun({{Value, Timestamp}, AckTimestamp}, CAcc)->
					CurrCounter = lists:keysearch(AckTimestamp, 1, CAcc),
					if
					    false == CurrCounter ->
						NewCounter = {AckTimestamp, {Value, Timestamp}, 1},
						[NewCounter|CAcc];
					    true ->
						%% _AckTS == AckTimestamp, the one we searched for in the list
						{value, {_AckTS, {Val, Ts}, Count}} = CurrCounter,
						if
						    Timestamp > Ts ->
							NewCounter = {AckTimestamp, {Value, Timestamp}, (Count + 1)},
							lists:keyreplace(Timestamp, 1, CAcc, NewCounter);
						    true ->
							NewCounter = {AckTimestamp, {Val, Ts}, (Count + 1)},
							lists:keyreplace(Timestamp, 1, CAcc, NewCounter)
						end
					end
				end,
				CounterList,
				RVAcksList),
    Limit = config:quorumFactor(),
    check_counter_fh(NewCounterList, Limit).


check_counter_fh([], _Limit)->
    not_enough;
check_counter_fh([{Ts, {VoteToAdopt, _VTs}, Count} | Rest], Limit) ->
    if
	Count >= Limit ->
	    if
		VoteToAdopt == bottom ->
		    {abort, Ts};
		true ->
		    {VoteToAdopt, Ts}
	    end;
	true ->
	    check_counter_fh(Rest, Limit)
    end.

%% Vote for the TPs we suspect to have failed, since we did not get a vote from them yet
vote_for_suspected(TMState)->
    %%Filter all items that have no decision yet
    Undecided = filter_undecided_items(TMState),
    lists:foreach(fun({Key, _Val})->
			  RKeys = cs_symm_replication:get_keys_for_replicas(Key),
			  lists:foreach(fun(RKey)->
						Vote = trecords:new_vote(TMState#tm_state.transID, Key, RKey, abort, TMState#tm_state.myBallot),
						Message = {read_vote, Vote},
						tsend:send_to_rtms(TMState, Message)
					end,
					RKeys)
		  end,
		  Undecided).
    
    

%% Filter all participants for which we do not have a vote yet
filter_undecided_items(TMState)->
    Acks = trecords:get_vote_acks(TMState),
    AcksList = dict:to_list(Acks),
    lists:filter(fun(Ack)->
			 DecAck = check_acks_item([Ack]),
			 if
			     DecAck == undecided ->
				 true;
			     true ->
					     false
			 end
		 end,
		 AcksList).


