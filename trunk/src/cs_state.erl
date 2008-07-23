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
%%% File    : cs_state.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : 
%%%
%%% Created :  7 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: cs_state.erl 463 2008-05-05 11:14:22Z schuett $
-module(cs_state).

-author('schuett@zib.de').
-vsn('$Id: cs_state.erl 463 2008-05-05 11:14:22Z schuett $ ').

-include("transstore/trecords.hrl").
-include("chordsharp.hrl").

-export([new/6, 
	 id/1, me/1, uniqueId/1,
	 succ/1, succ_pid/1, succ_id/1, update_succ/2,
	 succ_list/1, succ_list_pids/1, set_succlist/2,
	 pred_pid/1, pred_id/1, pred/1, update_pred/2, 
	 filterDeadNodes/1,
	 dump/1,
	 set_rt/2, rt/1,
	 get_lb/1, set_lb/2,
	 get_nodes/1,
	 addDeadNode/2,
	 details/1,
	 filterDeadNodes/2,
	 assert/1,
	 get_my_range/1, set_my_range/2,
	 %%transactions
	 get_trans_log/1,
	 set_trans_log/2]).

%% @type state() = {state, gb_trees:gb_tree(), list(), pid()}. the state of a chord# node
-record(state, {routingtable, successorlist, predecessor, me, my_range, lb, deadnodes, join_time, trans_log}).

new(RT, SuccessorList, Predecessor, Me, MyRange, LB) ->
    #state{
     routingtable = RT, 
     successorlist = SuccessorList,
     predecessor = Predecessor,
     me = Me,
     my_range = MyRange,
     lb=LB,
     join_time=now(),
     deadnodes = gb_sets:new(),
     trans_log = #translog{
       tid_tm_mapping = dict:new(),
       decided = gb_trees:empty(),
       undecided = gb_trees:empty()
      }
    }.

get_my_range(#state{my_range=MyRange}) ->
    MyRange.

set_my_range(State, MyRange) ->
    State#state{my_range=MyRange}.

get_lb(#state{lb=LB}) ->
    LB.

set_lb(State, LB) ->
    State#state{lb=LB}.

me(#state{me=Me}) ->
    Me.

id(#state{me=Me}) -> 
    node:id(Me).

uniqueId(#state{me=Me}) -> 
    node:uniqueId(Me).

join_time(#state{join_time=JoinTime}) ->
    JoinTime.
    
%%% Successor
succ(#state{successorlist=[Succ | _]}) -> Succ.

succ_pid(#state{successorlist=[Succ | _]}) -> node:pidX(Succ).

succ_id(#state{successorlist=[Succ | _]}) -> node:id(Succ).
%succ_id(State) -> io:format("unknown state ~w~n", [State]), "a".

update_succ(#state{successorlist=[_ | Rest]}=State, NewSucc) -> 
    State#state{successorlist=[NewSucc | Rest]}.

%%% Successor List
succ_list(#state{successorlist=SuccList}) -> SuccList.

succ_list_pids(State) -> 
    SuccList = succ_list(State),
    lists:map(fun (Node) ->
		      node:pidX(Node) end,
	      SuccList).

set_succlist(State, SuccList) ->
    State#state{successorlist=SuccList}.

%%% Predecessor

pred_pid(#state{predecessor=Pred}) -> node:pidX(Pred).

pred_id(#state{predecessor=Pred}) -> node:id(Pred).

pred(#state{predecessor=Pred}) -> Pred.

update_pred(#state{predecessor=OldPred} = State, Pred) ->
    if
	OldPred == Pred ->
	    ok;
	true ->
	    self() ! {pred_changed, OldPred, Pred}
    end,
    State#state{predecessor=Pred}.


%%% Routing Table

rt(#state{routingtable=RT}) ->
    RT.

set_rt(State, RT) ->
    State#state{routingtable=RT}.

%%% dead nodes 

filterList(L, Id) ->
    lists:filter(fun (X) -> node:uniqueId(X) /= Id end, L).

filterListSet(L, IdSet) ->
    gb_sets:fold(fun (Id, List) -> filterList(List, Id) end, L, IdSet).

filterDeadNodes(List, #state{deadnodes=DeadNodes}) ->
    filterListSet(List, DeadNodes).
    
addDeadNode(DeadNode, State) ->
    State#state{deadnodes=gb_sets:add_element(DeadNode, gb_sets:new())}.
%    State#state{deadnodes=gb_sets:add_element(DeadNode, DeadNodes)}.

filterPred(Pred, DeadNodes) ->
    IsNull = node:is_null(Pred),
    if
	IsNull -> 
	    Pred;
	true ->
	    PredIsDead = gb_sets:is_element(node:uniqueId(Pred), DeadNodes),
	    if
		PredIsDead ->
		    self() ! {pred_changed, Pred, node:null()},
		    node:null();
		true ->
		    Pred
	    end
    end.
    
filterDeadNodes(#state{routingtable=RT,successorlist=SuccList, predecessor=Pred, deadnodes=DeadNodes}=State) ->
    NewRT = ?RT:filterDeadNodes(RT, DeadNodes),
    NewSuccList = filterListSet(SuccList, DeadNodes),
    NewSuccListLength = util:lengthX(NewSuccList),
    Limit = config:succListLength() / 2,
    checkSuccList(NewSuccList, State, DeadNodes),
    if
	NewSuccListLength < Limit ->
	    cs_send:send(succ_pid(State), {get_succ_list, cs_send:this()});
	true ->
	    void
    end,
    NewPred = filterPred(Pred, DeadNodes),
    State#state{routingtable=NewRT,
		successorlist=NewSuccList, 
		predecessor=NewPred
	       }.


checkSuccList([], State, DeadNodes) ->
    boot_logger:log(io_lib:format("online: ~p; oldlist ~p; me ~p; pred ~p; deadnodes ~p", [timer:now_diff(now(), join_time(State)), succ_list(State), me(State), pred(State), DeadNodes]));
checkSuccList(_, _, _) ->
    void.

%%% util

dump(State) ->
    io:format("dump <~s,~w> <~s,~w> <~s,~w>~n", [id(State), self()
						 , pred_id(State), pred_pid(State), succ_id(State), succ_pid(State)]),
    ok.

get_nodes(#state{routingtable=RT,successorlist=SuccList, predecessor=Pred}) ->
    if
	Pred == null ->
	    lists:map(fun (Node) -> {node:uniqueId(Node), node:id(Node), node:pidX(Node)} end, 
		      lists:append([?RT:to_node_list(RT), SuccList]));
	true ->
	    lists:map(fun (Node) -> {node:uniqueId(Node), node:id(Node), node:pidX(Node)} end, 
		      lists:append([?RT:to_node_list(RT), SuccList, [Pred]]))
    end.

details(State) ->
    Pred = pred(State),
    Node = me(State),
    SuccList = succ_list(State),
    Load = cs_db_otp:get_load(),
    FD_Size = failuredetector:node_count(),
    Hostname = net_adm:localhost(),
    RTSize = ?RT:get_size(rt(State)),
    node_details:new(Pred, Node, SuccList, Load, FD_Size, Hostname, RTSize, cs_message:get_details(), erlang:memory(total)).

%%% Assert / Debugging

assert(State) ->
    assert_succlist(State).

assert_succlist(State) ->
    SuccListLength = length(succ_list(State)),
    if
	SuccListLength == 0 ->
	    {fail, "succ list empty"};
	true ->
	    ok
    end.

%%% Transactions
%%% Information on transactions that all possible TMs and TPs share

%% get the transaction log
get_trans_log(#state{trans_log=Log}) ->
    Log.
%% set the transaction log
set_trans_log(State, NewLog) ->
    State#state{trans_log=NewLog}.

