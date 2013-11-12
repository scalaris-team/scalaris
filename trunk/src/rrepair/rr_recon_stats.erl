% @copyright 2011-2012 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    Replica Repair Reconciliation Statistics
%% @end
%% @version $Id$
-module(rr_recon_stats).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Exported functions and types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export([new/0, new/1, inc/2, set/2, get/2, merge/2, print/1]).

-ifdef(with_export_type_support).
-export_type([stats/0, status/0]).
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type status() :: wait | abort | finish.
-record(rr_recon_stats,
        {
         session_id         = null   :: rrepair:session_id() | null,
         tree_size          = {0, 0} :: merkle_tree:mt_size(),
         tree_nodesCompared = 0      :: non_neg_integer(),
         tree_compareSkipped= 0      :: non_neg_integer(),
         tree_leavesSynced  = 0      :: non_neg_integer(),
         tree_compareLeft   = 0      :: non_neg_integer(),
         error_count        = 0      :: non_neg_integer(),
         build_time         = 0      :: non_neg_integer(),      %in us
         recon_time         = 0      :: non_neg_integer(),      %in us
         resolve_started    = 0      :: non_neg_integer(),      %number of resolve requests send
         status             = wait   :: status()
         }).
-type stats() :: #rr_recon_stats{}.

-type field_list1()  ::
          [{tree_size, merkle_tree:mt_size()} |
               {tree_nodesCompared, non_neg_integer()} |
               {tree_compareSkipped, non_neg_integer()} |
               {tree_leavesSynced, non_neg_integer()} |
               {tree_compareLeft, non_neg_integer()} |
               {error_count, non_neg_integer()} |
               {build_time, non_neg_integer()} |
               {recon_time, non_neg_integer()} |
               {resolve_started, non_neg_integer()}].

-type field_list2()  ::
          [{session_id, rrepair:session_id() | null} |
               {status, status()}] | field_list1().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec new() -> stats().
new() ->
    #rr_recon_stats{}.

-spec new(field_list2()) -> stats().
new(KVList) ->
    set(KVList, #rr_recon_stats{}).

% @doc increases the record field with name key by value
-spec inc(field_list1(), Old::stats()) -> New::stats().
inc([], Stats) ->
    Stats;
inc([{K, V} | L], Stats) ->
    NS = case K of
             tree_size ->
                 {OldI, OldF} = Stats#rr_recon_stats.tree_size,
                 {IncI, IncF} = V,
                 X = {OldI + IncI, OldF + IncF},
                 Stats#rr_recon_stats{tree_size = X};
             tree_compareLeft ->
                 X = V + Stats#rr_recon_stats.tree_compareLeft,
                 Stats#rr_recon_stats{tree_compareLeft = X};
             tree_nodesCompared ->
                 X = V + Stats#rr_recon_stats.tree_nodesCompared,
                 Stats#rr_recon_stats {tree_nodesCompared = X};
             tree_leavesSynced ->
                 X = V + Stats#rr_recon_stats.tree_leavesSynced,
                 Stats#rr_recon_stats{tree_leavesSynced = X};
             tree_compareSkipped ->
                 X = V + Stats#rr_recon_stats.tree_compareSkipped,
                 Stats#rr_recon_stats{tree_compareSkipped = X};
             error_count ->
                 X = V + Stats#rr_recon_stats.error_count,
                 Stats#rr_recon_stats{error_count = X};
             build_time ->
                 X = V + Stats#rr_recon_stats.build_time,
                 Stats#rr_recon_stats{build_time = X};
             recon_time ->
                 X = V + Stats#rr_recon_stats.recon_time,
                 Stats#rr_recon_stats{recon_time = X};
             resolve_started ->
                 X = V + Stats#rr_recon_stats.resolve_started,
                 Stats#rr_recon_stats{resolve_started = X}
         end,
    inc(L, NS).

% @doc sets the value of record field with name of key to the given value
-spec set(field_list2(), Old::stats()) -> New::stats().
set([], Stats) ->
    Stats;
set([{K, V} | L], Stats) ->
    NS = case K of
             session_id          -> Stats#rr_recon_stats{session_id = V};
             tree_size           -> Stats#rr_recon_stats{tree_size = V};
             tree_compareLeft    -> Stats#rr_recon_stats{tree_compareLeft = V};
             tree_nodesCompared  -> Stats#rr_recon_stats{tree_nodesCompared = V};
             tree_leavesSynced   -> Stats#rr_recon_stats{tree_leavesSynced = V};
             tree_compareSkipped -> Stats#rr_recon_stats{tree_compareSkipped = V};
             error_count         -> Stats#rr_recon_stats{error_count = V};
             build_time          -> Stats#rr_recon_stats{build_time = V};
             recon_time          -> Stats#rr_recon_stats{recon_time = V};
             resolve_started     -> Stats#rr_recon_stats{resolve_started = V};
             status              -> Stats#rr_recon_stats{status = V}
         end,
    set(L, NS).

-spec get(session_id, stats())         -> rrepair:session_id() | null;
         (tree_size, stats())          -> merkle_tree:mt_size();
         (tree_nodesCompared, stats()) -> non_neg_integer();
         (tree_compareSkipped, stats())-> non_neg_integer();
         (tree_leavesSynced, stats())  -> non_neg_integer();
         (tree_compareLeft, stats())   -> non_neg_integer();
         (error_count, stats())        -> non_neg_integer();
         (build_time, stats())         -> non_neg_integer();
         (recon_time, stats())         -> non_neg_integer();
         (resolve_started, stats())    -> non_neg_integer();
         (status, stats())             -> status().
get(session_id         , #rr_recon_stats{session_id          = X}) -> X;
get(tree_size          , #rr_recon_stats{tree_size           = X}) -> X;
get(tree_compareLeft   , #rr_recon_stats{tree_compareLeft    = X}) -> X;
get(tree_nodesCompared , #rr_recon_stats{tree_nodesCompared  = X}) -> X;
get(tree_leavesSynced  , #rr_recon_stats{tree_leavesSynced   = X}) -> X;
get(tree_compareSkipped, #rr_recon_stats{tree_compareSkipped = X}) -> X;
get(error_count        , #rr_recon_stats{error_count         = X}) -> X;
get(build_time         , #rr_recon_stats{build_time          = X}) -> X;
get(recon_time         , #rr_recon_stats{recon_time          = X}) -> X;
get(resolve_started    , #rr_recon_stats{resolve_started     = X}) -> X;
get(status             , #rr_recon_stats{status              = X}) -> X.

-spec merge(stats(), stats()) -> stats().
merge(A, #rr_recon_stats{ tree_size = TS,
                          tree_compareLeft = TCL,
                          tree_nodesCompared = TNC,
                          tree_leavesSynced = TLS,
                          tree_compareSkipped = TCS,
                          error_count = EC,
                          build_time = BT,
                          recon_time = RC,
                          resolve_started = RS }) ->
    inc([{tree_size, TS},
         {tree_compareLeft, TCL},
         {tree_nodesCompared, TNC},
         {tree_leavesSynced, TLS},
         {tree_compareSkipped, TCS},
         {error_count, EC},
         {build_time, BT},
         {recon_time, RC},
         {resolve_started, RS}], A).

-spec print(stats()) -> [any()].
print(Stats) ->
    StatsL = tl(erlang:tuple_to_list(Stats)),
    FieldNames = record_info(fields, rr_recon_stats),
    [rr_recon_stats, lists:zip(FieldNames, StatsL)].
