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
%%% File    : cs_node.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : chord# node main file
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: cs_node.erl 518 2008-07-14 10:45:26Z schintke $
-module(cs_node).

-author('schuett@zib.de').
-vsn('$Id: cs_node.erl 518 2008-07-14 10:45:26Z schintke $ ').

-include("transstore/trecords.hrl").
-include("chordsharp.hrl").

-export([start_link/1, start/1]).


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
%% pred_changed gets high priority
	{pred_changed, OldPred, NewPred} ->
	    ?LOG("[ ~w | I | Node   | ~w ] pred_changed~n",
		      [calendar:universal_time(), self()]),
	    State1 = cs_stabilize:update_range(State, OldPred, NewPred),
	    loop(State1, ?DEBUG(Debug));

%% ping gets high priority
	{kill} ->
	    ok;
	{ping, Ping_PID, Cookie} ->
	    UniqueId = cs_state:uniqueId(State),
	    if
		Cookie == UniqueId ->
		    ?LOG("[ ~w | I | Node   | ~w ] ping~n",
			      [calendar:universal_time(), Cookie]),
		    cs_send:send(Ping_PID, {pong, Cookie}),
		    loop(State, ?DEBUG(Debug));
		true ->
		    loop(State, ?DEBUG(Debug))
	    end;
	{ping, Ping_PID} ->
	    cs_send:send(Ping_PID, {pong, Ping_PID}),
	    loop(State, ?DEBUG(Debug));
	{ping_with_cookie, Ping_PID, Cookie} ->
	    cs_send:send(Ping_PID, {pong_with_cookie, Cookie}),
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
	{read, SourcePID, Key}->
	    ?LOG("[ ~w | I | Node   | ~w ] single_read ~w ~n", [calendar:universal_time(), self(), Key]),
	    transstore.transaction:quorum_read(Key, SourcePID),
	    loop(State, ?DEBUG(Debug));
	{parallel_reads, SourcePID, Keys, TLog}->
	    ?LOG("[ ~w | I | Node   | ~w ] parallel_reads ~w ~n", [calendar:universal_time(), self(), Keys]),
	    transstore.transaction:parallel_quorum_reads(Keys, TLog, SourcePID),
	    loop(State, ?DEBUG(Debug));
	
	%%  initiate a read phase
	{do_transaction, TransFun, SuccessFun, FailureFun, Owner} ->
	    ?TLOG("received do_transaction"),
	    transstore.transaction:do_transaction(State, TransFun, SuccessFun, FailureFun, Owner),
	    loop(State, ?DEBUG(Debug));
	%% do a transaction without a read phase
	{do_transaction_wo_rp, Items, SuccessFunArgument, SuccessFun, FailureFun, Owner}->
	    ?TLOG("received do_transaction_wo_rp"),
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
		    io:format("LookupTP: Got Request for Key ~p, it is not between ~p and ~p ~n", [Message#tp_message.item_key, RangeBeg, RangeEnd]),	    
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
	 

%% stabilize ring
	{get_id, Source_Pid} ->
	    ?LOG("[ ~w | I | Node   | ~w ] get_id~n",
		      [calendar:universal_time(), self()]),
	    cs_send:send(Source_Pid, {get_id_response, cs_send:this(), cs_state:id(State)}),
	    loop(State, ?DEBUG(Debug));
	{get_pred, Source_Pid} ->
	    ?LOG("[ ~w | I | Node   | ~w ] get_pred~n",
		      [calendar:universal_time(), self()]),
	    cs_send:send(Source_Pid, {get_pred_response, cs_state:pred(State), cs_state:me(State)}),
	    loop(State, ?DEBUG(Debug));
	{get_succ_list, Source_Pid} ->
	    ?LOG("[ ~w | I | Node   | ~w ] get_succ_list~n",
		 [calendar:universal_time(), self()]),
	    cs_send:send(Source_Pid, {get_succ_list_response, cs_state:me(State), cs_state:succ_list(State)}),
	    loop(State, ?DEBUG(Debug));
	{get_pred_response, SuccsPred, Succ} = _Message ->
	    ?LOG("[ ~w | I | Node   | ~w ] get_pred_respone ~w ~w~n",
		      [calendar:universal_time(), self(), SuccsPred, Succ]),
	    NewState = cs_stabilize:stabilize2(State, SuccsPred, Succ),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));
	{get_succ_list_response, Succ, SuccList} = _Message ->
	    ?LOG("[ ~w | I | Node   | ~w ] get_succ_list ~w ~w~n",
		      [calendar:universal_time(), self(), Succ, SuccList]),
	    NewState = cs_stabilize:stabilize(State, Succ, SuccList),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));
	{succ_left, SuccList} = _Message ->
	    ?LOG("[ ~w | I | Node   | ~w ] succ_left ~w~n",
		      [calendar:universal_time(), self(), SuccList]),
	    NewState = cs_stabilize:succ_left(State, SuccList),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));
	{pred_left, Pred} = _Message ->
	    ?LOG("[ ~w | I | Node   | ~w ] pred_left ~w~n",
		      [calendar:universal_time(), self(), Pred]),
	    NewState = cs_stabilize:pred_left(State, Pred),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));
	{notify, Pred} = _Message -> 
	    ?LOG("[ ~w | I | Node   | ~w ] notify~n",
		 [calendar:universal_time(), self()]),
	    NewState = cs_stabilize:notify(State, Pred),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));
	{update_succ, Succ} = _Message -> 
	    ?LOG("[ ~w | I | Node   | ~w ] update_succ~n",
		      [calendar:universal_time(), self()]),
	    NewState = cs_stabilize:update_succ(State, Succ),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));
	{stabilize_ring} ->
	    ?LOG("[ ~w | I | Node   | ~w ] stabilize_ring~n",
		      [calendar:universal_time(), self()]),
	    cs_stabilize:stabilize(State),
	    loop(State, ?DEBUG(Debug));

	{stabilize_failuredetector} ->
	    ?LOG("[ ~w | I | Node   | ~w ] stabilize_failuredetector~n",
		      [calendar:universal_time(), self()]),
	    cs_stabilize:update_failuredetector(State),
	    loop(State, ?DEBUG(Debug));

	{stabilize_loadbalance} ->
	    ?LOG("[ ~w | I | Node   | ~w ] stabilize_load_balance~n",
		      [calendar:universal_time(), self()]),
	    cs_lb:balance_load(State),
	    loop(State, ?DEBUG(Debug));

%% stabilize pointers
	{stabilize_pointers} = _Message ->
	    NewState = ?RT:stabilize(State),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));

	% for cs_rt
	{lookup_pointer, Source_Pid, Index} ->
	    cs_send:send(Source_Pid, {lookup_pointer_response, Index, ?RT:lookup(State, Index)}),
	    loop(State, ?DEBUG(Debug));

	{lookup_pointer_response, Index, Node} = _Message ->
	    NewState = ?RT:stabilize(State, Index, Node),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));

	% for chord_rt
	{rt_get_node, Source_PID, Idx} ->
	    cs_send:send(Source_PID, {rt_get_node_response, Idx, cs_state:me(State)}),
	    loop(State, ?DEBUG(Debug));

	{rt_get_node_response, Index, Node} = _Message ->
	    NewState = ?RT:stabilize(State, Index, Node),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));

%% find
	{lookup_aux, Key, Hops, Msg} -> 
	    ?LOG("[ ~w | I | Node   | ~w ] lookup_aux~n",
		      [calendar:universal_time(), self()]),
	    lookup:lookup_aux(State, Key, Hops, Msg),
	    loop(State, ?DEBUG(Debug));
	{lookup_fin, Hops, Msg} -> 
	    ?LOG("[ ~w | I | Node   | ~w ] lookup_fin~n",
		      [calendar:universal_time(), self()]),
	    lookup:lookup_fin(Hops, Msg),
	    loop(State, ?DEBUG(Debug));
	{get_node, Source_PID, Key} -> 	    
	    ?LOG("[ ~w | I | Node   | ~w ] get_node~n",
		      [calendar:universal_time(), self()]),
	    cs_send:send(Source_PID, {get_node_response, Key, cs_state:me(State)}),
	    loop(State, ?DEBUG(Debug));

% db
	{get_key, Source_PID, Key}-> 	    
	    ?LOG("[ ~w | I | Node   | ~w ] get_key~n",
		      [calendar:universal_time(), self()]),
	    {RangeBeg, RangeEnd} = cs_state:get_my_range(State),
	    Responsible = util:is_between(RangeBeg, Key, RangeEnd),
	    if
		Responsible == true ->
		    lookup:get_key(State, Source_PID, Key, Key),
		    loop(State, ?DEBUG(Debug));
		true ->
		    io:format("Get_Key: Got Request for Key ~p, it is not between ~p and ~p ~n", [Key, RangeBeg, RangeEnd]),
		    %self() ! {lookup_aux, Key, Msg},
		    loop(State, ?DEBUG(Debug))
	    end;
	{set_key, Source_PID, Key, Value, Versionnr} = _Message -> 	    
	    ?LOG("[ ~w | I | Node   | ~w ] set_key~n",
		 [calendar:universal_time(), self()]),
	    {RangeBeg, RangeEnd} = cs_state:get_my_range(State),
	    Responsible = util:is_between(RangeBeg, Key, RangeEnd),
	    if
		Responsible == true ->
		    State2 = lookup:set_key(State, Source_PID, Key, Value, Versionnr),
		    loop(State2, ?DEBUG(cs_debug:debug(Debug, State2, _Message)));
		true ->
		    io:format("Set_Key: Got Request for Key ~p, it is not between ~p and ~p ~n", [Key, RangeBeg, RangeEnd]),
		    %cs_send:send(Source_PID, {get_key_response, Key, failed}),
		    loop(State, ?DEBUG(cs_debug:debug(Debug, State, _Message)))
	    end;
	{drop_data, Data, Sender} = _Message ->
	    ?LOG("[ ~w | I | Node   | ~w ] drop_data ~w~n",
		      [calendar:universal_time(), self(), Data]),
	    cs_send:send(Sender, {drop_data_ack}),
	    DB = ?DB:add_data(cs_state:get_db(State), Data),
	    loop(cs_state:set_db(State, DB), ?DEBUG(cs_debug:debug(Debug, State, _Message)));

%% bulk
	{bulk_owner, I, Msg} ->
	    ?LOG("[ ~w | I | Node   | ~w ] bulk_owner~n",
		      [calendar:universal_time(), self()]),
	    bulkowner:bulk_owner(State, I, Msg),
	    loop(State, ?DEBUG(Debug));
	{start_bulk_owner, I, Msg} ->
	    ?LOG("[ ~w | I | Node   | ~w ] start_bulk_owner~n",
		      [calendar:universal_time(), self()]),
	    bulkowner:start_bulk_owner(I, Msg),
	    loop(State, ?DEBUG(Debug));
	{bulkowner_deliver, Range, {bulk_read_with_version, Issuer}} ->
	    cs_send:send(Issuer, {bulk_read_with_version_response, cs_state:get_my_range(State), 
				  ?DB:get_range_with_version(cs_state:get_db(State), Range)}),
	    loop(State, ?DEBUG(Debug));

% load balancing
	{get_load, Source_PID} ->
	    ?LOG("[ ~w | I | Node   | ~w ] get_load~n",
		      [calendar:universal_time(), self()]),
	    cs_send:send(Source_PID, {get_load_response, cs_send:this(), ?DB:get_load(cs_state:get_db(State))}),
	    loop(State, ?DEBUG(Debug));

	{get_load_response, Source_PID, Load} ->
	    ?LOG("[ ~w | I | Node   | ~w ] get_load_response~n",
		      [calendar:universal_time(), self()]),
	    cs_lb:check_balance(State, Source_PID, Load),
	    loop(State, ?DEBUG(Debug));

	{get_middle_key, Source_PID} = _Message ->
	    ?LOG("[ ~w | I | Node   | ~w ] get_middle_key~n",
		      [calendar:universal_time(), self()]),
	    {MiddleKey, NewState} = cs_lb:get_middle_key(State),
	    cs_send:send(Source_PID, {get_middle_key_response, cs_send:this(), MiddleKey}),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));

	{get_middle_key_response, Source_PID, MiddleKey} = _Message ->
	    ?LOG("[ ~w | I | Node   | ~w ] get_middle_key_response~n",
		      [calendar:universal_time(), self()]),
	    NewState = cs_lb:move_load(State, Source_PID, MiddleKey),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));

	{reset_loadbalance_flag} = _Message ->
	    NewState = cs_lb:reset_loadbalance_flag(State),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));
%% join
	{join, Source_PID, Id, UniqueId} = _Message -> 
	    ?LOG("[ ~w | I | Node   | ~w ] join~n",
		      [calendar:universal_time(), self()]),
	    NewState = cs_join:join_request(State, Source_PID, Id, UniqueId),
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));

%% misc.
	{get_node_details, Pid, Cookie} ->
	    cs_send:send(Pid, {get_node_details_response, Cookie, cs_state:details(State)}),
	    loop(State, Debug);
	{crash, Id, _, _} = _Message -> 
	    ?LOG("[ ~w | I | Node   | ~w ] crash ~w~n",
		      [calendar:universal_time(), self(), Id]),
	    NewState = cs_state:filterDeadNodes(cs_state:addDeadNode(Id, State)), 
	    loop(NewState, ?DEBUG(cs_debug:debug(Debug, NewState, _Message)));

	{dump} -> 
	    ?LOG("[ ~w | I | Node   | ~w ] dump~n",
		      [calendar:universal_time(), self()]),
	    cs_state:dump(State),
	    loop(State, ?DEBUG(Debug));

	{'$gen_cast', {debug_info, Requestor}} ->
	    Requestor ! {debug_info_response, [{"rt_debug", ?RT:dump(State)}, {"rt_size", ?RT:get_size(cs_state:rt(State))}]},
	    loop(State, ?DEBUG(Debug));
	{die} ->
	    ?LOG("die ~w~n", [self()]),
	    ok;

	{reregister} ->
	    cs_reregister:reregister(cs_state:uniqueId(State)),
	    %cs_send:send(config:bootPid(), {register, cs_send:this(), cs_state:uniqueId(State)}),
	    %timer:send_after(config:reregisterInterval(), self(), {reregister}),
	    loop(State, ?DEBUG(Debug));

%% unit_tests
	{bulkowner_deliver, _Range, {unit_test_bulkowner, Owner}} ->
	    Owner ! {unit_test_bulkowner_response, lists:map(fun ({Key, {Value, _, _, _}}) ->
									    {Key, Value}
								    end, ?DB:get_data(cs_state:get_db(State)))},
	    loop(State, ?DEBUG(Debug));

%%testing purpose
%% 	{print_locked_items} ->
%% 	    ?DB:print_locked_items(),
%% 	    timer:send_after(15000, self(), {print_locked_items}),
%% 	    loop(State, Debug);
%% TODO buggy ...
	{get_node_response, _, _} ->
	    loop(State, ?DEBUG(Debug));
	X ->
	    io:format("cs_node: unknown message ~w~n", [X]),
	    %ok
	    loop(State, ?DEBUG(Debug))
    end.

%% @doc joins this node in the ring and calls the main loop
%% @spec start(term()) -> cs_state:state()
start(InstanceId) ->
    %register(cs_node, self()),
    process_dictionary:register_process(InstanceId, cs_node, self()),
    randoms:init(),
    timer:sleep(random:uniform(100) * 100),
    Id = cs_keyholder:get_key(),
    failuredetector:set_owner(self()),
    boot_server:connect(),
    {First, State} = cs_join:join(Id),
    if
	not First ->
	    cs_replica_stabilization:recreate_replicas(cs_state:get_my_range(State));
	true ->
	    ok
    end,
    timer:send_after(config:stabilizationInterval(), self(), {stabilize_ring}),
    timer:send_after(config:pointerStabilizationInterval(), self(), {stabilize_pointers}),
    timer:send_after(config:failureDetectorUpdateInterval(), self(), {stabilize_failuredetector}),
    %timer:send_after(config:loadBalanceStartupInterval(), self(), {stabilize_loadbalance}),
    %% begin testing purpose
%    timer:send_after(15000, self(), {print_locked_items}),
    %% end testing purpose
    io:format("[ I | Node   | ~w ] joined~n",[self()]),
    loop(State, cs_debug:new()).
    
%% @doc spawns a chord# node, called by the chord# supervisor process
%% @spec start_link(term()) -> {ok, pid()}
start_link(InstanceId) ->
    {ok, spawn_link(?MODULE, start, [InstanceId])}.

