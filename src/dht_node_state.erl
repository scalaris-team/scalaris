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
%%% File    : dht_node_state.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : 
%%%
%%% Created :  7 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(dht_node_state).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("transstore/trecords.hrl").
-include("scalaris.hrl").

-export([new/6, new/7,
	 id/1, me/1,
	 succ/1, succ_pid/1, succ_id/1,
	 pred_pid/1, pred_id/1, pred/1,
	 load/1,
	 update_preds_succs/3,
         update_succs/2,
         update_preds/2,
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

-type my_range() :: {?RT:key(), ?RT:key()}.
-type join_time() :: {non_neg_integer(), non_neg_integer(), non_neg_integer()}. % {MegaSecs, Secs, MicroSecs}

%% @type state() = {state, gb_trees:gb_tree(), list(), pid()}. the state of a chord# node
-record(state, {routingtable :: ?RT:rt(),
                successors   :: [node:node_type(),...], 
		        predecessors :: [node:node_type(),...], 
		        me           :: node:node_type(),
		        my_range     :: my_range(), 
		        lb           :: dht_node_lb:lb(),
		        deadnodes    :: gb_set(),
		        join_time    :: join_time(),
		        trans_log    :: #translog{},
		        db           :: ?DB:db()}).
-type state() :: #state{}.

-spec new(?RT:rt(), node:node_type(), node:node_type(), node:node_type(), my_range(), dht_node_lb:lb()) -> state().
new(RT, Successor, Predecessor, Me, MyRange, LB) ->
    new(RT, Successor, Predecessor, Me, MyRange, LB, ?DB:new(node:id(Me))).

%% userdevguide-begin dht_node_state:state
-spec new(?RT:rt(), node:node_type(), node:node_type(), node:node_type(), my_range(), dht_node_lb:lb(), ?DB:db()) -> state().
new(RT, Successor, Predecessor, Me, MyRange, LB, DB) ->
    #state{
     routingtable = RT, 
     successors = [Successor],
     predecessors = [Predecessor],
     me = Me,
     my_range = MyRange,
     lb = LB,
     join_time = now(),
     deadnodes = gb_sets:new(),
     trans_log = #translog{
       tid_tm_mapping = dict:new(),
       decided        = gb_trees:empty(),
       undecided      = gb_trees:empty()
      },
     db = DB
    }.
%% userdevguide-end dht_node_state:state

-spec next_interval(state()) -> intervals:simple_interval().
next_interval(State) -> intervals:new(id(State), succ_id(State)).

-spec get_my_range(state()) -> my_range().
get_my_range(#state{my_range=MyRange}) -> MyRange.

-spec get_db(state()) -> ?DB:db().
get_db(#state{db=DB}) -> DB.

-spec set_db(state(), ?DB:db()) -> state().
set_db(State, DB) -> State#state{db=DB}.

-spec get_lb(state()) -> dht_node_lb:lb().
get_lb(#state{lb=LB}) -> LB.

-spec set_lb(state(), dht_node_lb:lb()) -> state().
set_lb(State, LB) -> State#state{lb=LB}.

-spec me(state()) -> node_details:node_type().
me(#state{me=Me}) -> Me.

-spec id(state()) -> ?RT:key().
id(#state{me=Me}) -> node:id(Me).

%%% Successor
-spec succs(state()) -> [node:node_type(),...].
succs(#state{successors=Succs}) -> Succs.

-spec succ(state()) -> node:node_type().
succ(State) -> hd(succs(State)).

-spec succ_pid(state()) -> cs_send:mypid().
succ_pid(State) -> node:pidX(succ(State)).

-spec succ_id(state()) -> ?RT:key().
succ_id(State) -> node:id(succ(State)).

%%% Predecessor
-spec preds(state()) -> [node:node_type(),...].
preds(#state{predecessors=Preds}) -> Preds.

-spec pred(state()) -> node:node_type().
pred(State) -> hd(preds(State)).

-spec pred_pid(state()) -> cs_send:mypid().
pred_pid(State) -> node:pidX(pred(State)).

-spec pred_id(state()) -> ?RT:key().
pred_id(State) -> node:id(pred(State)).

%%% Load
-spec load(state()) -> integer().
load(State) -> ?DB:get_load(get_db(State)).

%%% Routing Table
-spec rt(state()) -> ?RT:rt().
rt(#state{routingtable=RT}) -> RT.

-spec set_rt(state(), ?RT:rt()) -> state().
set_rt(State, RT) -> State#state{routingtable=RT}.

-spec rt_size(state()) -> integer().
rt_size(State) -> ?RT:get_size(rt(State)).

%%% util
-spec dump(state()) -> ok.
dump(State) ->
    io:format("dump <~s,~w> <~s,~w> <~s,~w>~n", [id(State), self()
						 , pred_id(State), pred_pid(State), succ_id(State), succ_pid(State)]),
    ok.

%% @doc Gets the requested details about the current node.
-spec details(state(), [node_details:node_details_name()]) -> node_details:node_details().
details(State, Which) ->
    ExtractValues =
        fun(Elem, NodeDetails) ->
                case Elem of
                    predlist    -> node_details:set(NodeDetails, predlist, preds(State));
                    pred        -> node_details:set(NodeDetails, pred, pred(State));
                    node        -> node_details:set(NodeDetails, node, me(State));
                    my_range    -> node_details:set(NodeDetails, my_range, get_my_range(State));
                    succ        -> node_details:set(NodeDetails, succ, succ(State));
                    succlist    -> node_details:set(NodeDetails, succlist, succs(State));
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
    PredList = preds(State),
    SuccList = succs(State),
    Node = me(State),
    %SuccList = [succ(State)],
    Load = load(State),
    Hostname = net_adm:localhost(),
    RTSize = rt_size(State),
    node_details:new(PredList, Node, SuccList, Load, Hostname, RTSize, erlang:memory(total)).

%%% Transactions
%%% Information on transactions that all possible TMs and TPs share

-spec get_trans_log(state()) -> #translog{}.
%% @doc Gets the transaction log.
get_trans_log(#state{trans_log=Log}) ->
    Log.

%% @doc Sets the transaction log.
-spec set_trans_log(state(), #translog{}) -> state().
set_trans_log(State, NewLog) ->
    State#state{trans_log=NewLog}.

%% @doc Sets the predecessor and successor lists.
-spec update_preds_succs(state(), [node:node_type(),...], [node:node_type(),...]) -> state().
update_preds_succs(State, Preds, Succs) ->
    State#state{predecessors=Preds, successors=Succs, my_range={node:id(hd(Preds)), id(State)}}.

%% @doc Sets the predecessor list.
-spec update_preds(state(), [node:node_type(),...]) -> state().
update_preds(State, Preds) ->
    State#state{predecessors=Preds, my_range={node:id(hd(Preds)), id(State)}}.

%% @doc Sets the successor list.
-spec update_succs(state(), [node:node_type(),...]) -> state().
update_succs(State, Succs) ->
    State#state{successors=Succs}.
