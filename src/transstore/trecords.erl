%  Copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : trecords.erl
%%% Author  : Monika Moser <moser@zib.de>
%%% Description : Functions to create and operate on the records
%%%
%%% Created : 20 Sep 2007 by Monika Moser <moser@zib.de>
%%%-------------------------------------------------------------------
-module(trecords).

-author('moser@zib.de').
-vsn('$Id$ ').

-include("trecords.hrl").
-include("scalaris.hrl").

-export([new_tm_state/4, 
         get_vote/3, 
         store_vote/4, 
         get_vote_acks/1, 
         get_vote_acks/2, 
         get_vote_acks/3, 
         store_vote_acks/4, 
         get_read_vote_acks/3, 
         store_read_vote_acks/4, 
         new_tm_item/4, 
         new_tm_message/2, 
         new_vote/5, 
         create_items/1]).
% get_decision/3, 
% store_decision/4, 
% get_read_vote_acks/1, 
-export([items_get_keys/1]).
-export([items_get_item_by_key/2, items_update_item/3]).

-import(dict).
-import(lists).
-import(?RT).
-import(txlog).

%%--------------------------------------------------------------------
%% Function: new_tm_state/4
%% Purpose:  create a record with the state of the tm
%% Args:     TransID - Transaction ID
%%           Items - dict holding the items involved in the transaction
%%           Leader - PID of the Leader
%%           SELF - {Key, PID} where replicated key of the TID, this 
%%                  TM is responsible for, PID is the TMs PID 
%% Returns:  record tm_state
%%-------------------------------------------------------------------
new_tm_state(TransID, Items, Leader, Self)->
    Votes = dict:new(),
    Acks = dict:new(),
    RVAcks = dict:new(),
    Decisions = dict:new(),

    %% Initialize the Votes (Sorry for this code :), but it was fun ;) )
    %% And at the same time the Acks which will be collected by the leader
    %% And the read_vote_acks (RVAcks) that are acks for the read phase (failure handling)
    %% And the dict with decisions - used to ease the code 
    %%
    %% Votes : dict(Item.key, dict(Replica.key, Vote))
    %% Acks  : dict(Item.key, dict(Replica.key, []))
    %% RVAcks: dict(Item.key, dict(Replica.key, []))
    %% Decisions: dict(Item.key, dict(Replica.key undecided/commit/abort)
    %%
    %% Do the following for Votes and accordingly for Acks at the same time:
    %% 1st fold: get keys from Items and accumulate them in the dict called Votes
    %%               where these keys are the keys from Votes
    %%               and where the value is again a dict created in the second fold
    %% 2nd fold: get replicakeys for each key and accumulate them in a dict
    %%               called ReplicaDict
    %%               where replicakeys are the keys and the value is an inital vote
    %% An inital vote: {bottom : emptyval, 1 : initial read timestamp (Paxos Atomic
    %%                     Commit starts with a write phase),
    %%                  0: initial write timestamp}
    %%                  a leader voting on behalf of a tp must start with ts 2!!!!
    %% Initially acks are an empty list

    {NewVotes,
     NewVoteAcks,
     NewRVAcks,
     _NewDecisions} = lists:foldl(fun(Entry, {MyVotes, MyAcks, MyRVAcks, MyDec}) ->
                                         Key = Entry#tm_item.key,
                                         RKeys = ?RT:get_keys_for_replicas(Key),
                                         {VotesReplica, AcksReplica, _RVAcksReplica} =
                                             lists:foldl(fun(Elem, {Acc1,Acc2,Acc3})->
                                                                 {dict:store(Elem, {bottom, 0, 0}, Acc1),
                                                                  dict:store(Elem, [], Acc2),
                                                                  dict:store(Elem, [], Acc3)}
                                                         end,
                                                         {MyVotes, dict:new(), MyRVAcks},
                                                         RKeys),
                                         {VotesReplica,%dict:store(Key, VotesReplica, MyVotes),
                                          dict:store(Key, AcksReplica, MyAcks),
                                          MyRVAcks,%dict:store(Key, _RVAcksReplica, MyRVAcks),
                                          MyDec %dict:store(Key, DecReplica, MyDec)
                                         }
                                 end,
                                 {Votes, Acks, RVAcks, Decisions},
                                 Items),
    #tm_state{transID = TransID,
              items = Items,
              numitems = length(Items),
              leader = Leader,
              myBallot = 0,
              rtms = [Self], %% the replicated transaction managers
              rtms_found = 1,
              votes = NewVotes,
              vote_acks = NewVoteAcks,
              read_vote_acks = NewRVAcks,
              %% decisions = _NewDecisions,
              tps_found = 0,
              status = collecting,
              decision = undecided
             }.

%%--------------------------------------------------------------------
%% @spec get_vote(TMState::tm_state, Key::string(), RKey::List)->{Dec, ReadTS, WriteTS}
%%       Dec = atom()
%%       ReadTS = integer()
%%       WriteTS = integer()

get_vote(TMState, _Key, RKey)->
    Votes = TMState#tm_state.votes,
    dict:fetch(RKey, Votes).

%% @spec store_vote(TMState::tm_state, Key::string(), RKey::List, Vote::Vote) -> tm_state
%%       List = [Prefix + string()]
%%       Prefix = char()
%%       Vote = {atom, ReadTS, WriteTS}
%%       ReadTS = integer() 
%%       WriteTS = integer()
%% @doc Store a specific vote for a certain replica, identified by a key and a replicakey,
%% in the state of the transaction manager.

store_vote(TMState, _Key, RKey, Vote)->
    Votes = TMState#tm_state.votes,
    NewVotes = dict:store(RKey, Vote, Votes),
    TMState#tm_state{votes = NewVotes}.

get_vote_acks(TMState)->
    TMState#tm_state.vote_acks.

%%--------------------------------------------------------------------
%% @spec get_vote_acks(TMState::tm_state, Key::string())-> dict(Key, dict(RKey, [{Dec, TS}]))
%%       Dec = atom()
%%       TS = integer()

get_vote_acks(TMState, Key)->
    VoteAcks = TMState#tm_state.vote_acks,
    dict:fetch(Key, VoteAcks).

%%--------------------------------------------------------------------
%% @spec get_vote_acks(TMState::tm_state, Key::string(), RKey::List)->[{Dec, TS}]
%%       Dec = atom()
%%       TS = integer()

get_vote_acks(TMState, Key, RKey)->
    VoteAcks = TMState#tm_state.vote_acks,
    dict:fetch(RKey, dict:fetch(Key, VoteAcks)).


%%--------------------------------------------------------------------
%% @spec store_vote_acks(TMState::tm_state, Key::string(), RKey::List, Ack::Ack)->tm_state
%%       Ack = {VoteDecision, TS}
%%       VoteDecision = atom()
%%       TS = integer()

%% Ack := {Decision, Timestamp}
store_vote_acks(TMState, Key, RKey, Ack)->
    VoteAcks = TMState#tm_state.vote_acks,
    RKeyDict = dict:fetch(Key, VoteAcks),
    RKeyAcks = dict:fetch(RKey, RKeyDict),

    NewRKeyAcks = [Ack | RKeyAcks],

    NewRKeyDict = dict:store(RKey, NewRKeyAcks, RKeyDict),
    NewVoteAcks = dict:store(Key, NewRKeyDict, VoteAcks),
    TMState#tm_state{vote_acks = NewVoteAcks}.

%% get_read_vote_acks(TMState)->
%%     TMState#tm_state.read_vote_acks.

get_read_vote_acks(TMState, _Key, RKey)->
    VoteAcks = TMState#tm_state.read_vote_acks,
    %dict:fetch(RKey, dict:fetch(_Key, VoteAcks)).
    dict:fetch(RKey, VoteAcks).

%%--------------------------------------------------------------------
%% @spec store_read_vote_acks(TMState::tm_state, Key::string(), RKey::List, Ack::RVAck)->tm_state
%%       Ack = {{AcceptedVoteDecision, AcceptedVoteTS}, NewTS}
%%       AcceptedVoteDecision = atom()
%%       AcceptedVoteTS = integer()
%%       NewTS = integer()

%% Ack := {{AcceptedVote, AcceptedVoteTimestamp}, Timestamp}
store_read_vote_acks(TMState, _Key, RKey, Ack)->
    RVAcks = TMState#tm_state.read_vote_acks,
    RKeyAcks = dict:fetch(RKey, RVAcks),

    NewRKeyAcks = [Ack | RKeyAcks],

    NewVoteAcks = dict:store(RKey, NewRKeyAcks, RVAcks),
    TMState#tm_state{read_vote_acks = NewVoteAcks}.

%% get_decision(TMState, Key, RKey)->
%%     Decisions = TMState#tm_state.decisions,
%%     dict:fetch(RKey, dict:fetch(Key, Decisions)).

%% store_decision(TMState, Key, RKey, Decision)->
%%     Decisions = TMState#tm_state.decisions,
%%     ItemDecs = dict:fetch(Key, Decisions),

%%     NewItemDecs = dict:store(RKey, Decision, ItemDecs),
%%     NewDecisions = dict:store(Key, NewItemDecs, Decisions),
%%     TMState#tm_state{decisions = NewDecisions}.


%%--------------------------------------------------------------------
%% Function: new_tm_item/4
%% Purpose:  create a record with the state of the tm
%% Args:     Key - the key
%%           Value - the value
%%           Version - the version
%%           Operation - read/write
%% Returns:  record tm_item
%%-------------------------------------------------------------------
new_tm_item(Key, Value, Version, Operation) ->
    #tm_item{
             key = Key,
             value = Value,
             version = Version,
             operation = Operation,
             tps = [], %% list with tuple: {key_of_replica, tpPID}
             tps_found = 0
     }.

%%--------------------------------------------------------------------
%% Function: new_tm_message/2
%% Purpose:  create a message sent by a tm
%% Args:     TID - Transaction ID
%%           Message - tuple with content
%% Returns:  tm_message record
%%-------------------------------------------------------------------
new_tm_message(TID, Message)->
    #tm_message{
                transaction_id = TID,
                tm_key = unknown,
                message = Message
               }.


%%--------------------------------------------------------------------
%% Function: new_vote/4
%% Purpose:  create a vote
%% Args:     TID - Transaction ID
%%           Key - Key the vote refers to
%%           RN  - replica number
%%           Decision - prepared/abort
%% Returns:  vote record
%%-------------------------------------------------------------------
new_vote(TID, Key, RN, Decision, TS)->
    #vote{
	  transactionID = TID,
	  key = Key,
	  rkey = RN,
	  decision = Decision,
	  timestamp = TS
	  }.

%%--------------------------------------------------------------------
%% Function: create_items/1
%% Purpose:  create an Items list from a TransLog
%% Args:     TransLog = [{read,"key3",ok,"value3",0},...]
%% Returns:  items list
%%-------------------------------------------------------------------
create_items(TransLog) ->
    [ txlog:get_entry_as_tm_item(X) || X <- TransLog].

items_get_keys(Items) ->
    [ Elem#tm_item.key || Elem <- Items ].

items_get_item_by_key(Items, Key) ->
    [Item] = [ X || X <- Items,
                    Key == X#tm_item.key ],
    Item.

items_update_item(Items, OldItem, NewItem) ->
    F = fun(X) -> case X of
                      OldItem -> NewItem;
                      _ -> X
                  end
        end,
    [ F(X) || X <- Items ].
