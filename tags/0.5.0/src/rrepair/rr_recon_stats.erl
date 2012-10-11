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
         tree_leafsSynced   = 0      :: non_neg_integer(),
         tree_compareLeft   = 0      :: non_neg_integer(),
         error_count        = 0      :: non_neg_integer(),
         build_time         = 0      :: non_neg_integer(),      %in us
         recon_time         = 0      :: non_neg_integer(),      %in us
         resolve_started    = 0      :: non_neg_integer(),      %number of resolve requests send
         status             = wait   :: status()
         }). 
-type stats() :: #rr_recon_stats{}.

-type stats_types() :: non_neg_integer() | 
                       merkle_tree:mt_size() | 
                       rrepair:session_id() | null |
                       status().
-type field_list()  :: [{atom(), stats_types()}]. %atom = any field name of rr_recon_stats record

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec new() -> stats().
new() ->
    #rr_recon_stats{}.

-spec new(field_list()) -> stats().
new(KVList) ->
    set(KVList, #rr_recon_stats{}).

% @doc increases the record field with name key by value
-spec inc(field_list(), Old::Stats) -> New::Stats when
    is_subtype(Stats, stats()).
inc([], Stats) -> 
    Stats;
inc([{K, V} | L], Stats) ->
    NS = case K of
             tree_size -> Stats#rr_recon_stats{ tree_size = V };
             tree_compareLeft -> 
                 Stats#rr_recon_stats{ tree_compareLeft = 
                                           V + Stats#rr_recon_stats.tree_compareLeft };
             tree_nodesCompared ->
                 Stats#rr_recon_stats { tree_nodesCompared =
                                            V + Stats#rr_recon_stats.tree_nodesCompared };
             tree_leafsSynced ->
                 Stats#rr_recon_stats{ tree_leafsSynced = 
                                           V + Stats#rr_recon_stats.tree_leafsSynced };
             tree_compareSkipped ->
                 Stats#rr_recon_stats{ tree_compareSkipped = 
                                           V + Stats#rr_recon_stats.tree_compareSkipped };
             error_count ->
                 Stats#rr_recon_stats{ error_count = V + Stats#rr_recon_stats.error_count };
             build_time ->
                 Stats#rr_recon_stats{ build_time = V + Stats#rr_recon_stats.build_time };
             recon_time ->
                 Stats#rr_recon_stats{ recon_time = V + Stats#rr_recon_stats.recon_time };
             resolve_started ->
                 Stats#rr_recon_stats{ resolve_started = V + Stats#rr_recon_stats.resolve_started }
         end,
    inc(L, NS).

% @doc sets the value of record field with name of key to the given value
-spec set(field_list(), Old::Stats) -> New::Stats when
    is_subtype(Stats, stats()).
set([], Stats) ->
    Stats;
set([{K, V} | L], Stats) ->
    NS = case K of
             session_id -> Stats#rr_recon_stats{ session_id = V };
             tree_size -> Stats#rr_recon_stats{ tree_size = V };
             tree_compareLeft -> Stats#rr_recon_stats{ tree_compareLeft = V };
             tree_nodesCompared -> Stats#rr_recon_stats { tree_nodesCompared = V };
             tree_leafsSynced -> Stats#rr_recon_stats{ tree_leafsSynced = V };
             tree_compareSkipped -> Stats#rr_recon_stats{ tree_compareSkipped = V };
             error_count -> Stats#rr_recon_stats{ error_count = V };
             build_time -> Stats#rr_recon_stats{ build_time = V };
             recon_time -> Stats#rr_recon_stats{ recon_time = V };             
             resolve_started -> Stats#rr_recon_stats{ resolve_started = V };
             status -> Stats#rr_recon_stats{ status = V }
         end,
    set(L, NS).

-spec get(atom(), stats()) -> stats_types().
get(K, Stats) ->
    case K of
        session_id -> Stats#rr_recon_stats.session_id;
        tree_size -> Stats#rr_recon_stats.tree_size;
        tree_compareLeft -> Stats#rr_recon_stats.tree_compareLeft;
        tree_nodesCompared -> Stats#rr_recon_stats.tree_nodesCompared;
        tree_leafsSynced -> Stats#rr_recon_stats.tree_leafsSynced;
        tree_compareSkipped -> Stats#rr_recon_stats.tree_compareSkipped;
        error_count -> Stats#rr_recon_stats.error_count;
        build_time -> Stats#rr_recon_stats.build_time;
        recon_time -> Stats#rr_recon_stats.recon_time;
        resolve_started -> Stats#rr_recon_stats.resolve_started;
        status -> Stats#rr_recon_stats.status;
        _ -> 0
    end.

-spec merge(stats(), stats()) -> stats().
merge(A, #rr_recon_stats{ tree_size = TS,
                          tree_compareLeft = TCL,
                          tree_nodesCompared = TNC,
                          tree_leafsSynced = TLS,
                          tree_compareSkipped = TCS,
                          error_count = EC,
                          build_time = BT,
                          recon_time = RC,
                          resolve_started = RS }) ->
    inc([{tree_size, TS},
         {tree_compareLeft, TCL},
         {tree_nodesCompared, TNC},
         {tree_leafsSynced, TLS},
         {tree_compareSkipped, TCS},
         {error_count, EC},
         {build_time, BT},
         {recon_time, RC},
         {resolve_started, RS}], A).

-spec print(stats()) -> [any()].
print(Stats) ->
    FieldNames = record_info(fields, rr_recon_stats),
    Res = util:for_to_ex(1, length(FieldNames), 
                         fun(I) ->
                                 {lists:nth(I, FieldNames), erlang:element(I + 1, Stats)}
                         end),
    [erlang:element(1, Stats), lists:flatten(lists:reverse(Res))].
