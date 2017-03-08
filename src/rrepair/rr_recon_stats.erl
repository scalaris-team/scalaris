% @copyright 2011-2016 Zuse Institute Berlin

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
%% @author Nico Kruber <kruber@zib.de>
%% @doc    Replica Repair Reconciliation Statistics
%% @end
%% @version $Id$
-module(rr_recon_stats).
-author('malange@informatik.hu-berlin.de').
-vsn('$Id$').

-include("scalaris.hrl").
-include("record_helpers.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Exported functions and types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export([new/1, new/2, inc/2, set/2, get/2, print/1]).

-export_type([stats/0, status/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type status() :: wait | abort | finish.
-record(rr_recon_stats,
        {
         session_id         = ?required(rr_recon_stats, session_id) :: rrepair:session_id(),
         tree_size          = {0,0,0,0} :: merkle_tree:mt_size(),
         tree_nodesCompared = 0       :: non_neg_integer(),
         tree_compareSkipped= 0       :: non_neg_integer(),
         tree_leavesSynced  = 0       :: non_neg_integer(),
         fail_rate_p1       = 0.0     :: float(),
         fail_rate_p2       = 0.0     :: float(),
         build_time         = 0       :: non_neg_integer(),      %in us
         recon_time         = 0       :: non_neg_integer(),      %in us
         rs_expected        = 0       :: non_neg_integer(),      %number of resolve expected requests
         status             = wait    :: status()
         }).
-type stats() :: #rr_recon_stats{}.

-type field_list1()  ::
          [{tree_size, merkle_tree:mt_size()} |
               {tree_nodesCompared, non_neg_integer()} |
               {tree_compareSkipped, non_neg_integer()} |
               {tree_leavesSynced, non_neg_integer()} |
               {build_time, non_neg_integer()} |
               {recon_time, non_neg_integer()} |
               {rs_expected, non_neg_integer()}].

-type field_list2()  ::
          [{status, status()} | {fail_rate_p1, float()} | {fail_rate_p2, float()}]
            | field_list1().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec new(rrepair:session_id()) -> stats().
new(SID) ->
    ?DBG_ASSERT(SID =/= null),
    #rr_recon_stats{session_id = SID}.

-spec new(rrepair:session_id(), field_list2()) -> stats().
new(SID, KVList) ->
    set(KVList, #rr_recon_stats{session_id = SID}).

% @doc increases the record field with name key by value
-spec inc(field_list1(), Old::stats()) -> New::stats().
inc([], Stats) ->
    Stats;
inc([{K, V} | L], Stats) ->
    NS = case K of
             tree_size ->
                 {OldIn, OldLn, OldELn, OldIt} = Stats#rr_recon_stats.tree_size,
                 {IncIn, IncLn, IncELn, IncIt} = V,
                 X = {OldIn + IncIn, OldLn + IncLn, OldELn + IncELn, OldIt + IncIt},
                 Stats#rr_recon_stats{tree_size = X};
             tree_nodesCompared ->
                 X = V + Stats#rr_recon_stats.tree_nodesCompared,
                 Stats#rr_recon_stats {tree_nodesCompared = X};
             tree_leavesSynced ->
                 X = V + Stats#rr_recon_stats.tree_leavesSynced,
                 Stats#rr_recon_stats{tree_leavesSynced = X};
             tree_compareSkipped ->
                 X = V + Stats#rr_recon_stats.tree_compareSkipped,
                 Stats#rr_recon_stats{tree_compareSkipped = X};
             build_time ->
                 X = V + Stats#rr_recon_stats.build_time,
                 Stats#rr_recon_stats{build_time = X};
             recon_time ->
                 X = V + Stats#rr_recon_stats.recon_time,
                 Stats#rr_recon_stats{recon_time = X};
             rs_expected ->
                 X = V + Stats#rr_recon_stats.rs_expected,
                 Stats#rr_recon_stats{rs_expected = X}
         end,
    inc(L, NS).

% @doc sets the value of record field with name of key to the given value
-spec set(field_list2(), Old::stats()) -> New::stats().
set([], Stats) ->
    Stats;
set([{K, V} | L], Stats) ->
    NS = case K of
             tree_size           -> Stats#rr_recon_stats{tree_size = V};
             tree_nodesCompared  -> Stats#rr_recon_stats{tree_nodesCompared = V};
             tree_leavesSynced   -> Stats#rr_recon_stats{tree_leavesSynced = V};
             tree_compareSkipped -> Stats#rr_recon_stats{tree_compareSkipped = V};
             fail_rate_p1        -> Stats#rr_recon_stats{fail_rate_p1 = V};
             fail_rate_p2        -> Stats#rr_recon_stats{fail_rate_p2 = V};
             build_time          -> Stats#rr_recon_stats{build_time = V};
             recon_time          -> Stats#rr_recon_stats{recon_time = V};
             rs_expected         -> Stats#rr_recon_stats{rs_expected = V};
             status              -> Stats#rr_recon_stats{status = V}
         end,
    set(L, NS).

-spec get(session_id, stats())         -> rrepair:session_id();
         (tree_size, stats())          -> merkle_tree:mt_size();
         (tree_nodesCompared, stats()) -> non_neg_integer();
         (tree_compareSkipped, stats())-> non_neg_integer();
         (tree_leavesSynced, stats())  -> non_neg_integer();
         (fail_rate_p1 | fail_rate_p2 | fail_rate, stats()) -> float();
         (build_time, stats())         -> non_neg_integer();
         (recon_time, stats())         -> non_neg_integer();
         (rs_expected, stats())        -> non_neg_integer();
         (status, stats())             -> status().
get(session_id         , #rr_recon_stats{session_id          = X}) -> X;
get(tree_size          , #rr_recon_stats{tree_size           = X}) -> X;
get(tree_nodesCompared , #rr_recon_stats{tree_nodesCompared  = X}) -> X;
get(tree_leavesSynced  , #rr_recon_stats{tree_leavesSynced   = X}) -> X;
get(tree_compareSkipped, #rr_recon_stats{tree_compareSkipped = X}) -> X;
get(fail_rate_p1       , #rr_recon_stats{fail_rate_p1        = X}) -> X;
get(fail_rate_p2       , #rr_recon_stats{fail_rate_p2        = X}) -> X;
get(build_time         , #rr_recon_stats{build_time          = X}) -> X;
get(recon_time         , #rr_recon_stats{recon_time          = X}) -> X;
get(rs_expected        , #rr_recon_stats{rs_expected         = X}) -> X;
get(status             , #rr_recon_stats{status              = X}) -> X;
get(fail_rate          , #rr_recon_stats{fail_rate_p1 = Fr_p1, fail_rate_p2 = Fr_p2}) ->
    Fr_p1 + Fr_p2.

-spec print(stats()) -> [any()].
print(Stats) ->
    StatsL = tl(erlang:tuple_to_list(Stats)),
    FieldNames = record_info(fields, rr_recon_stats),
    [rr_recon_stats, lists:zip(FieldNames, StatsL)].
