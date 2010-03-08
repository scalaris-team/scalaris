%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(cs_state).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("transstore/trecords.hrl").
-include("../include/scalaris.hrl").

-export([new/6, new/7,
	 id/1, me/1,
	 succ/1, succ_pid/1, succ_id/1,
	 pred_pid/1, pred_id/1, pred/1,
	 load/1,
	 update_pred_succ/3,
     update_succ/2,
     update_pred/2,
	 dump/1,
	 set_rt/2, rt/1,
	 get_db/1, set_db/2,
	 get_lb/1, set_lb/2,
	 details/1, details/2,
	 get_my_range/1, 
	 next_interval/1,
	 %%transactions
	 get_trans_log/1,
	 set_trans_log/2]).

%% @type state() = {state, gb_trees:gb_tree(), list(), pid()}. the state of a chord# node
-record(state, {routingtable :: ?RT:rt(), 
		successor, 
		predecessor, 
		me, 
		my_range, 
		lb, 
		deadnodes, 
		join_time, 
		trans_log, 
		db}).
-type(state() :: #state{}).

new(RT, Successor, Predecessor, Me, MyRange, LB) ->
    new(RT, Successor, Predecessor, Me, MyRange, LB, ?DB:new(node:id(Me))).

%% userdevguide-begin cs_state:state
new(RT, Successor, Predecessor, Me, MyRange, LB, DB) ->
    #state{
     routingtable = RT, 
     successor = Successor,
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
      },
     db = DB
    }.
%% userdevguide-end cs_state:state

% @spec next_interval(state()) -> intervals:interval()
next_interval(State) ->
    intervals:new(id(State), succ_id(State)).

get_my_range(#state{my_range=MyRange}) ->
    MyRange.

get_db(#state{db=DB}) ->
    DB.

set_db(State, DB) ->
    State#state{db=DB}.

get_lb(#state{lb=LB}) ->
    LB.

set_lb(State, LB) ->
    State#state{lb=LB}.

me(#state{me=Me}) ->
    Me.

id(#state{me=Me}) ->
    node:id(Me).

%%% Successor
succ(#state{successor=Succ}) -> Succ.

succ_pid(State) -> node:pidX(succ(State)).

succ_id(State) -> node:id(succ(State)).

%%% Predecessor

pred_pid(#state{predecessor=Pred}) -> node:pidX(Pred).

pred_id(#state{predecessor=Pred}) -> node:id(Pred).

pred(#state{predecessor=Pred}) -> Pred.

%%% Load

load(State) -> ?DB:get_load(get_db(State)).

%%% Routing Table

rt(#state{routingtable=RT}) ->
    RT.

set_rt(State, RT) ->
    State#state{routingtable=RT}.

rt_size(State) -> ?RT:get_size(rt(State)).

%%% util

dump(State) ->
    io:format("dump <~s,~w> <~s,~w> <~s,~w>~n", [id(State), self()
						 , pred_id(State), pred_pid(State), succ_id(State), succ_pid(State)]),
    ok.

%% @doc Gets the requested details about the current node.
-spec details(state(), [predlist | pred | me | my_range | succ | succlist | load | hostname | rt_size | message_log | memory]) -> node_details:node_details().
details(State, Which) ->
	ExtractValues =
		fun(Elem, NodeDetails) ->
				case Elem of
					predlist    -> ring_maintenance:get_predlist(),
    				               PredList = 
    				                   receive
    				                       {get_predlist_response, X} -> X
    				                   end,
    				               node_details:set(NodeDetails, predlist, PredList);
					pred        -> node_details:set(NodeDetails, pred, pred(State));
					me          -> node_details:set(NodeDetails, node, me(State));
					my_range    -> node_details:set(NodeDetails, my_range, get_my_range(State));
					succ        -> node_details:set(NodeDetails, succ, succ(State));
					succlist    -> ring_maintenance:get_successorlist(),
    				               SuccList = 
    				                   receive
    				                       {get_successorlist_response, Y} -> Y
    				                   end,
    				               node_details:set(NodeDetails, succlist, SuccList);
					load        -> node_details:set(NodeDetails, load, load(State));
					hostname    -> node_details:set(NodeDetails, hostname, net_adm:localhost());
					rt_size     -> node_details:set(NodeDetails, rt_size, rt_size(State));
					message_log -> node_details:set(NodeDetails, message_log, ok);
					memory      -> node_details:set(NodeDetails, memory, erlang:memory(total))
				end
		end,
	lists:foldl(ExtractValues, node_details:new(), Which).

%% @doc Gets the following details about the current node:
%%      predecessor and successor lists, the node itself, its load, hostname and
%%      routing table size
-spec details(state()) -> node_details:node_details_record().
details(State) ->
    ring_maintenance:get_predlist(),
    PredList =  receive
                    {get_predlist_response, X} -> X
                end,
    ring_maintenance:get_successorlist(),
    SuccList = receive
                   {get_successorlist_response, Y} -> Y
               end,
    %Predlist = [pred(State)],
    Node = me(State),
  
    %SuccList = [succ(State)],
    Load = load(State),
    Hostname = net_adm:localhost(),
    RTSize = rt_size(State),
    node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, erlang:memory(total)).

%%% Transactions
%%% Information on transactions that all possible TMs and TPs share

%% get the transaction log
get_trans_log(#state{trans_log=Log}) ->
    Log.
%% set the transaction log
set_trans_log(State, NewLog) ->
    State#state{trans_log=NewLog}.

update_pred_succ(State, Pred, Succ) ->
    case node:is_null(Pred) of
	true ->
	    State#state{predecessor=Pred, successor=Succ};
	false ->
	    State#state{predecessor=Pred, successor=Succ, my_range={node:id(Pred), id(State)}}
    end.

update_pred(State, Pred) ->
    case node:is_null(Pred) of
	true ->
	    State#state{predecessor=Pred};
	false ->
	    State#state{predecessor=Pred, my_range={node:id(Pred), id(State)}}
    end.
update_succ(State, Succ) ->
	    State#state{successor=Succ}.
