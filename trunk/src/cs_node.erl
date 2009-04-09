%  Copyright 2007-2008 Konrad-Zuse-Zentrum f√ºr Informationstechnik Berlin
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
%%% File    : cs_node.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : chord# node main file
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum f¸r Informationstechnik Berlin
%% @version $Id$
-module(cs_node).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("transstore/trecords.hrl").
-include("chordsharp.hrl").

-behaviour(gen_component).

-export([start_link/1, start_link/2]).
-export([on/2, init/1]).

% state of the cs_node loop
-type(state() :: cs_state:state()).

% accepted messages of cs_node processes
-type(message() :: any()).

%% @doc message handler
-spec(on/2 :: (message(), state()) -> state()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Kill Messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({kill}, _State) ->
    kill;

on({churn}, _State) ->
    cs_keyholder:reinit(),
    kill;

on({halt}, _State) ->
    util:sleep_for_ever();

on({die}, _State) ->
    kill;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Ping Messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({ping, Ping_PID, Cookie}, State) ->
    cs_send:send(Ping_PID, {pong, Cookie}),
    State;

on({ping, Ping_PID}, State) ->
    cs_send:send(Ping_PID, {pong, Ping_PID}),
    State;

on({ping_with_cookie, Ping_PID, Cookie}, State) ->
    cs_send:send(Ping_PID, {pong_with_cookie, Cookie}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Ring Maintenance
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({rm_update_pred_succ, Pred, Succ}, State) ->
    cs_state:update_pred_succ(State, Pred, Succ);
    
on({rm_update_pred, Pred}, State) ->
    cs_state:update_pred(State, Pred);

on({rm_update_succ, Succ}, State) ->
    cs_state:update_succ(State,Succ);
    
on({succ_left, SuccList}, State) ->
    ?RM:succ_left(SuccList),
    State;

on({pred_left, Pred}, State) ->
    ?RM:pred_left(Pred),
    State;

on({update_succ, Succ}, State) -> 
    ?RM:update_succ(Succ),
    State;

on({get_pred_succ, Pid}, State) ->
    cs_send:send(Pid, {get_pred_succ_response, cs_state:pred(State), 
		       cs_state:succ(State)}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Finger Maintenance 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({rt_update, RoutingTable}, State) ->
    cs_state:set_rt(State, RoutingTable);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Transactions (see transstore/*.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({read, SourcePID, Key}, State) ->
    transstore.transaction:quorum_read(Key, SourcePID),
    State;

on({delete, SourcePID, Key}, State) ->
    transstore.transaction:delete(SourcePID, Key),
    State;

on({parallel_reads, SourcePID, Keys, TLog}, State) ->
    transstore.transaction:parallel_quorum_reads(Keys, TLog, SourcePID),
    State;

%%  initiate a read phase
on({do_transaction, TransFun, SuccessFun, FailureFun, Owner}, State) ->
    transstore.transaction:do_transaction(State, TransFun, SuccessFun, FailureFun, Owner),
    State;

%% do a transaction without a read phase
on({do_transaction_wo_rp, Items, SuccessFunArgument, SuccessFun, FailureFun, Owner}, State) ->
    transstore.transaction:do_transaction_wo_readphase(State, Items, SuccessFunArgument, SuccessFun, FailureFun, Owner),
    State;

%% answer - lookup for transaction participant
on({lookup_tp, Message}, State) ->
    {Leader} = Message#tp_message.message,
    {RangeBeg, RangeEnd} = cs_state:get_my_range(State),
    Responsible = util:is_between(RangeBeg, Message#tp_message.item_key, RangeEnd),
    if
	Responsible == true ->
	    cs_send:send(Leader, {tp, Message#tp_message.item_key, Message#tp_message.orig_key, cs_send:this()}),
	    State;
	true ->
	    log:log(info,"[ Node ] LookupTP: Got Request for Key ~p, it is not between ~p and ~p ~n", [Message#tp_message.item_key, RangeBeg, RangeEnd]),
	    State
    end;

	%% answer - lookup for replicated transaction manager
on({init_rtm, Message}, State) ->
    transstore.transaction:initRTM(State, Message);

%% a validation request for a node acting as a transaction participant
on({validate, TransID, Item}, State) ->
    transstore.tparticipant:tp_validate(State, TransID, Item);

%% this message contains the final decision for a certain transaction
on({decision, Message}, State) ->
    {_, TransID, Decision} = Message#tp_message.message,
    if
	Decision == commit ->
	    transstore.tparticipant:tp_commit(State, TransID);
	true ->
	    transstore.tparticipant:tp_abort(State, TransID)
    end;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Ring Maintenance (rm_chord)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({get_pred, Source_Pid}, State) ->
    cs_send:send(Source_Pid, {get_pred_response, cs_state:pred(State)}),
    State;

on({get_succ_list, Source_Pid}, State) ->
    cs_send:send(Source_Pid, {get_succ_list_response, cs_state:me(State), 
			      rm_chord:get_successorlist()}),
    State;

on({notify, Pred}, State) -> 
    rm_chord:notify(Pred),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Finger Maintenance 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({lookup_pointer, Source_Pid, Index}, State) ->
    cs_send:send(Source_Pid, {lookup_pointer_response, Index, ?RT:lookup(cs_state:rt(State), Index)}),
    State;

on({rt_get_node, Source_PID, Cookie}, State) ->
    cs_send:send(Source_PID, {rt_get_node_response, Cookie, cs_state:me(State)}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Lookup (see lookup.erl) 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({lookup_aux, Key, Hops, Msg}, State) -> 
    lookup:lookup_aux(State, Key, Hops, Msg),
    State;

on({lookup_fin, Hops, Msg}, State) -> 
    lookup:lookup_fin(Hops, Msg),
    State;

on({get_node, Source_PID, Key}, State) ->
    cs_send:send(Source_PID, {get_node_response, Key, cs_state:me(State)}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Cyclon (see cyclon/*.erl) 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({get_cyclon_pid, Pid,Me}, State) ->
    CyclonPid = cs_send:get(get_local_cyclon_pid(), cs_send:this()),
    cs_send:send(Pid,{cyclon_pid,Me,CyclonPid}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% database 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({get_key, Source_PID, Key}, State) ->
    {RangeBeg, RangeEnd} = cs_state:get_my_range(State),
    case util:is_between(RangeBeg, Key, RangeEnd) of
	true ->
	    lookup:get_key(State, Source_PID, Key, Key),
	    State;
	false ->
	    log:log(info,"[ Node ] Get_Key: Got Request for Key ~p, it is not between ~p and ~p", [Key, RangeBeg, RangeEnd]),
	    State
    end;

on({set_key, Source_PID, Key, Value, Versionnr}, State) ->
    {RangeBeg, RangeEnd} = cs_state:get_my_range(State),
    case util:is_between(RangeBeg, Key, RangeEnd) of
	true ->
	    lookup:set_key(State, Source_PID, Key, Value, Versionnr);
	false ->
	    log:log(info,"[ Node ] Set_Key: Got Request for Key ~p, it is not between ~p and ~p ", [Key, RangeBeg, RangeEnd]),
	    State
    end;

on({delete_key, Source_PID, Key}, State) ->
    {RangeBeg, RangeEnd} = cs_state:get_my_range(State),
    case util:is_between(RangeBeg, Key, RangeEnd) of
	true ->
	    lookup:delete_key(State, Source_PID, Key);
	false ->
	    log:log(info,"[ Node ] delete_key: Got Request for Key ~p, it is not between ~p and ~p ", [Key, RangeBeg, RangeEnd]),
	    State
    end;    

on({drop_data, Data, Sender}, State) ->
    cs_send:send(Sender, {drop_data_ack}),
    DB = ?DB:add_data(cs_state:get_db(State), Data),
    cs_state:set_db(State, DB);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% bulk owner messages (see bulkowner.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({bulk_owner, I, Msg}, State) ->
    bulkowner:bulk_owner(State, I, Msg),
    State;

on({start_bulk_owner, I, Msg}, State) ->
    bulkowner:start_bulk_owner(I, Msg),
    State;

on({bulkowner_deliver, Range, {bulk_read_with_version, Issuer}}, State) ->
    cs_send:send(Issuer, {bulk_read_with_version_response, cs_state:get_my_range(State), 
			  ?DB:get_range_with_version(cs_state:get_db(State), Range)}),
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% load balancing messages (see cs_lb.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({get_load, Source_PID}, State) ->
    cs_send:send(Source_PID, {get_load_response, cs_send:this(), ?DB:get_load(cs_state:get_db(State))}),
    State;

on({get_load_response, Source_PID, Load}, State) ->
    cs_lb:check_balance(State, Source_PID, Load),
    State;

on({get_middle_key, Source_PID}, State) ->
    {MiddleKey, NewState} = cs_lb:get_middle_key(State),
    cs_send:send(Source_PID, {get_middle_key_response, cs_send:this(), MiddleKey}),
    NewState;

on({get_middle_key_response, Source_PID, MiddleKey}, State) ->
    cs_lb:move_load(State, Source_PID, MiddleKey);

on({reset_loadbalance_flag}, State) ->
    cs_lb:reset_loadbalance_flag(State);

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% join messages (see cs_join.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% userdevguide-begin cs_node:join_message
on({join, Source_PID, Id, UniqueId}, State) ->
    cs_join:join_request(State, Source_PID, Id, UniqueId);
%% userdevguide-end cs_node:join_message

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({stabilize_loadbalance}, State) ->
    cs_lb:balance_load(State),
    State;

%% misc.
on({get_node_details, Pid, Cookie}, State) ->
    cs_send:send(Pid, {get_node_details_response, Cookie, cs_state:details(State)}),
    State;

on({dump}, State) -> 
    cs_state:dump(State),
    State;

on({'$gen_cast', {debug_info, Requestor}}, State) ->
    Requestor ! {debug_info_response, [{"rt_size", ?RT:get_size(cs_state:rt(State))}]},
    State;

on({reregister}, State) ->
    cs_reregister:reregister(),
    State;

%% unit_tests
on({bulkowner_deliver, Range, {unit_test_bulkowner, Owner}}, State) ->
    Res = lists:map(fun ({Key, {Value, _, _, _}}) ->
			    {Key, Value}
		    end, 
		    lists:filter(fun ({Key, _}) ->
					 intervals:in(Key, Range)
				 end, ?DB:get_data(cs_state:get_db(State)))),
    Owner ! {unit_test_bulkowner_response, Res, cs_state:id(State)},
    State;

%% @TODO buggy ...
on({get_node_response, _, _}, State) ->
    State;

%% @TODO: cleanup
on({send_to_group_member,Processname,Mesg}, State) ->
    InstanceId = erlang:get(instance_id),
    Pid = process_dictionary:lookup_process(InstanceId,Processname),
    Pid ! Mesg,
    State;

on(_, _State) ->
    unknown_event.

%% userdevguide-begin cs_node:start
%% @doc joins this node in the ring and calls the main loop
-spec(init/1 :: ([any()]) -> cs_state:state()).
init([_InstanceId, Options]) ->
    case lists:member(first, Options) of
	true ->
	    ok;
	false ->
	    timer:sleep(crypto:rand_uniform(1, 100) * 100)
    end,
    Id = cs_keyholder:get_key(),
    {First, State} = cs_join:join(Id),
    if
	not First ->
	    cs_replica_stabilization:recreate_replicas(cs_state:get_my_range(State));
	true ->
	    ok
    end,
    log:log(info,"[ Node ~w ] joined",[self()]),
    State.
%% userdevguide-end cs_node:start

%% userdevguide-begin cs_node:start_link
%% @doc spawns a scalaris node, called by the scalaris supervisor process
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    start_link(InstanceId, []).

start_link(InstanceId, Options) ->
    gen_component:start_link(?MODULE, [InstanceId, Options], [{register, InstanceId, cs_node}]).
%% userdevguide-end cs_node:start_link

get_local_cyclon_pid() ->
    InstanceId = erlang:get(instance_id),
    if
	InstanceId == undefined ->
	   log:log(error,"[ Node ] ~p", [util:get_stacktrace()]);
	true ->
	    ok
    end,
    process_dictionary:lookup_process(InstanceId, cyclon).
