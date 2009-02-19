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

-export([start_link/1, start_link/2, start/3]).


%logging on
%-define(LOG(S, L), io:format(S, L)).
%logging off
-define(LOG(S, L), ok).

%debuggin on
%-define(DEBUG(State), State).
%debugging off
-define(DEBUG(State), ok).


%% @doc The main loop of a chord# node
%% @spec loop(State, Debug) -> State
loop(State, Debug) ->
    receive
	{kill} ->
        ok;
    {churn} ->
        cs_keyholder:reinit(),
        ok;
    {halt} ->
        util:sleep_for_ever();
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Ping Messages
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	{ping, Ping_PID, Cookie} ->
	    cs_send:send(Ping_PID, {pong, Cookie}),
	    loop(State, ?DEBUG(Debug));
	{ping, Ping_PID} ->
	    cs_send:send(Ping_PID, {pong, Ping_PID}),
	    loop(State, ?DEBUG(Debug));
	{ping_with_cookie, Ping_PID, Cookie} ->
	    cs_send:send(Ping_PID, {pong_with_cookie, Cookie}),
	    loop(State, ?DEBUG(Debug));

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Ring Maintenance
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	{rm_update_pred_succ, Pred, Succ} ->
	    NewState = cs_state:update_pred_succ(State, Pred, Succ),
	    loop(NewState, ?DEBUG(Debug));
    
    {rm_update_pred, Pred} ->
	    NewState = cs_state:update_pred(State, Pred),
	    loop(NewState, ?DEBUG(Debug));
    {rm_update_succ, Succ} ->
	    NewState = cs_state:update_succ(State,Succ),
	    loop(NewState, ?DEBUG(Debug));
    
     
	{succ_left, SuccList} = _Message ->
	    ?RM:succ_left(SuccList),
	    loop(State, ?DEBUG(Debug));
	{pred_left, Pred} = _Message ->
	    ?RM:pred_left(Pred),
	    loop(State, ?DEBUG(Debug));
	{update_succ, Succ} = _Message -> 
	    ?RM:update_succ(Succ),
	    loop(State, ?DEBUG(Debug));
	{get_pred_succ, Pid} ->
	    cs_send:send(Pid, {get_pred_succ_response, cs_state:pred(State), 
			       cs_state:succ(State)}),
	    loop(State, ?DEBUG(Debug));

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Finger Maintenance 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	{rt_update, RoutingTable} ->
	    loop(cs_state:set_rt(State, RoutingTable), ?DEBUG(Debug));

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Transactions (see transstore/*.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	{read, SourcePID, Key}->
	    transstore.transaction:quorum_read(Key, SourcePID),
	    loop(State, ?DEBUG(Debug));
	{parallel_reads, SourcePID, Keys, TLog}->
	    transstore.transaction:parallel_quorum_reads(Keys, TLog, SourcePID),
	    loop(State, ?DEBUG(Debug));
	%%  initiate a read phase
	{do_transaction, TransFun, SuccessFun, FailureFun, Owner} ->
	    transstore.transaction:do_transaction(State, TransFun, SuccessFun, FailureFun, Owner),
	    loop(State, ?DEBUG(Debug));
	%% do a transaction without a read phase
	{do_transaction_wo_rp, Items, SuccessFunArgument, SuccessFun, FailureFun, Owner}->
	    transstore.transaction:do_transaction_wo_readphase(State, Items, SuccessFunArgument, SuccessFun, FailureFun, Owner),
	    loop(State, ?DEBUG(Debug));
	%% answer - lookup for transaction participant
	{lookup_tp, Message}->
	    ?TLOG("received lookup_tp"),
	    {Leader} = Message#tp_message.message,
	    {RangeBeg, RangeEnd} = cs_state:get_my_range(State),
	    Responsible = util:is_between(RangeBeg, Message#tp_message.item_key, RangeEnd),
	    if
		Responsible == true ->
		    cs_send:send(Leader, {tp, Message#tp_message.item_key, Message#tp_message.orig_key, cs_send:this()}),
		    loop(State, ?DEBUG(Debug));
		true ->
		   log:log(info,"[ Node ] LookupTP: Got Request for Key ~p, it is not between ~p and ~p ~n", [Message#tp_message.item_key, RangeBeg, RangeEnd]),	    
		    loop(State, ?DEBUG(Debug))
	    end;
	%% answer - lookup for replicated transaction manager
	{init_rtm, Message} ->
	    ?TLOG("received init_rtm"),
	    NewState = transstore.transaction:initRTM(State, Message),
	    loop(NewState, ?DEBUG(Debug));
	%% a validation request for a node acting as a transaction participant
	{validate, TransID, Item}->
	    ?LOG("received validate~n", []),
	    NewState = transstore.tparticipant:tp_validate(State, TransID, Item),
	    loop(NewState, ?DEBUG(Debug));
	%% this message contains the final decision for a certain transaction
	{decision, Message} ->
	    {_, TransID, Decision} = Message#tp_message.message,
	    ?TLOG2("received decision", Decision),
	    if
		Decision == commit ->
		    NewState = transstore.tparticipant:tp_commit(State, TransID);
		true ->
		    NewState = transstore.tparticipant:tp_abort(State, TransID)
	    end,
	    loop(NewState, ?DEBUG(Debug));
	 

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Ring Maintenance (rm_chord)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	{get_pred, Source_Pid} ->
	    cs_send:send(Source_Pid, {get_pred_response, cs_state:pred(State)}),
	    loop(State, ?DEBUG(Debug));
	{get_succ_list, Source_Pid} ->
	    cs_send:send(Source_Pid, {get_succ_list_response, cs_state:me(State), 
				      rm_chord:get_successorlist()}),
	    loop(State, ?DEBUG(Debug));
	{notify, Pred} = _Message -> 
	    rm_chord:notify(Pred),
	    loop(State, ?DEBUG(Debug));

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Finger Maintenance 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	{lookup_pointer, Source_Pid, Index} ->
	    cs_send:send(Source_Pid, {lookup_pointer_response, Index, ?RT:lookup(cs_state:rt(State), Index)}),
	    loop(State, ?DEBUG(Debug));

	{rt_get_node, Source_PID, Cookie} ->
	    cs_send:send(Source_PID, {rt_get_node_response, Cookie, cs_state:me(State)}),
	    loop(State, ?DEBUG(Debug));

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Lookup (see lookup.erl) 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	{lookup_aux, Key, Hops, Msg} -> 
	    lookup:lookup_aux(State, Key, Hops, Msg),
	    loop(State, ?DEBUG(Debug));
	{lookup_fin, Hops, Msg} -> 
	    lookup:lookup_fin(Hops, Msg),
	    loop(State, ?DEBUG(Debug));
	{get_node, Source_PID, Key} -> 	    
	    cs_send:send(Source_PID, {get_node_response, Key, cs_state:me(State)}),
	    loop(State, ?DEBUG(Debug));

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Cyclon (see cyclon/*.erl) 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
{get_cyclon_pid, Pid,Me} ->
	    CyclonPid = cs_send:get(get_local_cyclon_pid(), cs_send:this()),
	    cs_send:send(Pid,{cyclon_pid,Me,CyclonPid}),
	    loop(State, ?DEBUG(Debug));

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% database 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	{get_key, Source_PID, Key}-> 	    
	    {RangeBeg, RangeEnd} = cs_state:get_my_range(State),
	    Responsible = util:is_between(RangeBeg, Key, RangeEnd),
	    if
		Responsible == true ->
		    lookup:get_key(State, Source_PID, Key, Key),
		    loop(State, ?DEBUG(Debug));
		true ->
		    log:log(info,"[ Node ] Get_Key: Got Request for Key ~p, it is not between ~p and ~p", [Key, RangeBeg, RangeEnd]),
		    %self() ! {lookup_aux, Key, Msg},
		    loop(State, ?DEBUG(Debug))
	    end;
	{set_key, Source_PID, Key, Value, Versionnr} = _Message -> 	    
	    {RangeBeg, RangeEnd} = cs_state:get_my_range(State),
	    Responsible = util:is_between(RangeBeg, Key, RangeEnd),
	    if
		Responsible == true ->
		    State2 = lookup:set_key(State, Source_PID, Key, Value, Versionnr),
		    loop(State2, ?DEBUG(cs_debug:debug(Debug, State2, _Message)));
		true ->
		    log:log(info,"[ Node ] Set_Key: Got Request for Key ~p, it is not between ~p and ~p ", [Key, RangeBeg, RangeEnd]),
		    %cs_send:send(Source_PID, {get_key_response, Key, failed}),
		    loop(State, ?DEBUG(cs_debug:debug(Debug, State, _Message)))
	    end;
	{drop_data, Data, Sender} = _Message ->
	    cs_send:send(Sender, {drop_data_ack}),
	    DB = ?DB:add_data(cs_state:get_db(State), Data),
	    loop(cs_state:set_db(State, DB), ?DEBUG(cs_debug:debug(Debug, State, _Message)));

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% bulk owner messages (see bulkowner.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	{bulk_owner, I, Msg} ->
	    bulkowner:bulk_owner(State, I, Msg),
	    loop(State, ?DEBUG(Debug));
	{start_bulk_owner, I, Msg} ->
	    bulkowner:start_bulk_owner(I, Msg),
	    loop(State, ?DEBUG(Debug));
	{bulkowner_deliver, Range, {bulk_read_with_version, Issuer}} ->
	    cs_send:send(Issuer, {bulk_read_with_version_response, cs_state:get_my_range(State), 
				  ?DB:get_range_with_version(cs_state:get_db(State), Range)}),
	    loop(State, ?DEBUG(Debug));

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% load balancing messages (see cs_lb.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	{get_load, Source_PID} ->
	    cs_send:send(Source_PID, {get_load_response, cs_send:this(), ?DB:get_load(cs_state:get_db(State))}),
	    loop(State, ?DEBUG(Debug));

	{get_load_response, Source_PID, Load} ->
	    cs_lb:check_balance(State, Source_PID, Load),
	    loop(State, ?DEBUG(Debug));

	{get_middle_key, Source_PID} = _Message ->
	    {MiddleKey, NewState} = cs_lb:get_middle_key(State),
	    cs_send:send(Source_PID, {get_middle_key_response, cs_send:this(), MiddleKey}),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));

	{get_middle_key_response, Source_PID, MiddleKey} = _Message ->
	    NewState = cs_lb:move_load(State, Source_PID, MiddleKey),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));

	{reset_loadbalance_flag} = _Message ->
	    NewState = cs_lb:reset_loadbalance_flag(State),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% join messages (see cs_join.erl)
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% userdevguide-begin cs_node:join_message
	{join, Source_PID, Id, UniqueId} = _Message ->
	    ?LOG("[ ~w | I | Node   | ~w ] join~n",
		      [calendar:universal_time(), self()]),
	    NewState = cs_join:join_request(State, Source_PID, Id, UniqueId),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));
%% userdevguide-end cs_node:join_message

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% 
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
	{stabilize_loadbalance} ->
	    ?LOG("[ ~w | I | Node   | ~w ] stabilize_load_balance~n",
		      [calendar:universal_time(), self()]),
	    cs_lb:balance_load(State),
	    loop(State, ?DEBUG(Debug));


%% misc.
	{get_node_details, Pid, Cookie} ->
	    cs_send:send(Pid, {get_node_details_response, Cookie, cs_state:details(State)}),
	    loop(State, Debug);

	{dump} -> 
	    ?LOG("[ ~w | I | Node   | ~w ] dump~n",
		      [calendar:universal_time(), self()]),
	    cs_state:dump(State),
	    loop(State, ?DEBUG(Debug));

	{'$gen_cast', {debug_info, Requestor}} ->
	    Requestor ! {debug_info_response, [{"rt_size", ?RT:get_size(cs_state:rt(State))}]},
	    loop(State, ?DEBUG(Debug));
	{die} ->
	    ?LOG("die ~w~n", [self()]),
	    ok;

	{reregister} ->
	    cs_reregister:reregister(),
	    loop(State, ?DEBUG(Debug));

%% transactions
	{transtest, Source_PID, NumElems}->
	    transstore.transaction_test:run_test_write(State, Source_PID, NumElems),
	    loop(State, ?DEBUG(Debug));
	{test1, Source_PID}->
	    transstore.transaction_test:run_test_increment(State, Source_PID),
	    loop(State, ?DEBUG(Debug));
	{test3, Source_PID} ->
	    transstore.transaction_test:run_test_write_5(State, Source_PID),
	    loop(State, ?DEBUG(Debug));
	{test4, Source_PID} ->
	    transstore.transaction_test:run_test_write_20(State, Source_PID),
	    loop(State, ?DEBUG(Debug));
	{test5, Source_PID} ->
	    transstore.transaction_test:run_test_read_5(State, Source_PID),
	    loop(State, ?DEBUG(Debug));
	{test6, Source_PID} ->
	    transstore.transaction_test:run_test_read_20(State, Source_PID),
	    loop(State, ?DEBUG(Debug));
	

%% unit_tests
	{bulkowner_deliver, Range, {unit_test_bulkowner, Owner}} ->
	    Owner ! {unit_test_bulkowner_response, lists:map(fun ({Key, {Value, _, _, _}}) ->
								     {Key, Value}
							     end, 
							     lists:filter(fun ({Key, _}) ->
										  intervals:in(Key, Range)
									  end, ?DB:get_data(cs_state:get_db(State)))),
							    cs_state:id(State)},
	    loop(State, ?DEBUG(Debug));

%% TODO buggy ...
	{get_node_response, _, _} ->
	    loop(State, ?DEBUG(Debug));
%% 
	{send_to_group_member,Processname,Mesg} ->
        %% @TODO: cleanup
    	InstanceId = erlang:get(instance_id),
       	Pid = process_dictionary:lookup_process(InstanceId,Processname),
        %PidNode = cs_send:get(Pid, cs_send:this()),
        %cs_send:send(PidNode,Mesg),
        Pid ! Mesg,
        loop(State, ?DEBUG(Debug));

	X ->
	    log:log(warn,"[ Node ] unknown message ~w", [X]),
	    %ok
	    loop(State, ?DEBUG(Debug))
    end.

%% userdevguide-begin cs_node:start
%% @doc joins this node in the ring and calls the main loop
-spec(start/3 :: (any(), any(), list()) -> cs_state:state()).
start(InstanceId, Parent, Options) ->
    process_dictionary:register_process(InstanceId, cs_node, self()),
    Parent ! done,
    case lists:member(first, Options) of
	true ->
	    ok;
	false ->
	    timer:sleep(crypto:rand_uniform(1, 100) * 100)
    end,
    Id = cs_keyholder:get_key(),
    boot_server:connect(),
    {First, State} = cs_join:join(Id),
    if
	not First ->
	    cs_replica_stabilization:recreate_replicas(cs_state:get_my_range(State));
	true ->
	    ok
    end,
    log:log(info,"[ Node ~w ] joined",[self()]),
    loop(State, cs_debug:new()).
%% userdevguide-end cs_node:start

%% userdevguide-begin cs_node:start_link
%% @doc spawns a chord# node, called by the chord# supervisor process
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    start_link(InstanceId, []).

start_link(InstanceId, Options) ->
    Link = spawn_link(?MODULE, start, [InstanceId, self(), Options]),
    receive
	done ->
	    ok
    end,
    {ok, Link}.
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
