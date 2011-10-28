% @copyright 2011 Zuse Institute Berlin

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
%% @doc    Replica Update Reconciliation Statistics
%% @end
%% @version $Id$
-module(ru_recon_stats).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-record(ru_recon_stats,
        {
         tree_size          = {0, 0} :: merkle_tree:mt_size(),
         tree_compareLeft   = 0      :: non_neg_integer(),
         tree_nodesCompared = 0      :: non_neg_integer(),
         tree_leafsSynced   = 0      :: non_neg_integer(),
         tree_compareSkipped= 0      :: non_neg_integer(),
         error_count        = 0      :: non_neg_integer(),
         build_time         = 0      :: non_neg_integer(),   %in us
         recon_time         = 0      :: non_neg_integer()    %in us
         }). 
-type stats() :: #ru_recon_stats{}.

-type stats_types() :: non_neg_integer() | merkle_tree:mt_size().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Exported Functions and Types
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export([new/0, inc/2, set/2, get/2,
         print/1]).

-ifdef(with_export_type_support).
-export_type([stats/0]).
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec new() -> stats().
new() ->
    #ru_recon_stats{}.

% @doc increases the record field with name key by value
-spec inc([{Key, Value}], Stats) -> NewStats when
    is_subtype(Key,      atom()), %any field name of ru_recon_stats record
    is_subtype(Value,    stats_types()),
    is_subtype(Stats,    stats()),
    is_subtype(NewStats, stats()).
inc([], Stats) ->
    Stats;
inc([{K, V} | L], Stats) ->
    NS = case K of
             tree_size -> Stats#ru_recon_stats{ tree_size = V };
             tree_compareLeft -> 
                 Stats#ru_recon_stats{ tree_compareLeft = 
                                           V + Stats#ru_recon_stats.tree_compareLeft };
             tree_nodesCompared ->
                 Stats#ru_recon_stats { tree_nodesCompared =
                                            V + Stats#ru_recon_stats.tree_nodesCompared };
             tree_leafsSynced ->
                 Stats#ru_recon_stats{ tree_leafsSynced = 
                                           V + Stats#ru_recon_stats.tree_leafsSynced };
             tree_compareSkipped ->
                 Stats#ru_recon_stats{ tree_compareSkipped = 
                                           V + Stats#ru_recon_stats.tree_compareSkipped };
             error_count ->
                 Stats#ru_recon_stats{ error_count = V + Stats#ru_recon_stats.error_count };
             build_time ->
                 Stats#ru_recon_stats{ build_time = V + Stats#ru_recon_stats.build_time };
             recon_time ->
                 Stats#ru_recon_stats{ recon_time = V + Stats#ru_recon_stats.recon_time }    
         end,
    inc(L, NS).

% @doc sets the value of record field with name of key to the given value
-spec set([{Key, Value}], Stats) -> NewStats when
    is_subtype(Key,      atom()), %any field name of ru_recon_stats record
    is_subtype(Value,    stats_types()),
    is_subtype(Stats,    stats()),
    is_subtype(NewStats, stats()).
set([], Stats) ->
    Stats;
set([{K, V} | L], Stats) ->
    NS = case K of
             tree_size -> Stats#ru_recon_stats{ tree_size = V };
             tree_compareLeft -> Stats#ru_recon_stats{ tree_compareLeft = V };
             tree_nodesCompared -> Stats#ru_recon_stats { tree_nodesCompared = V };
             tree_leafsSynced -> Stats#ru_recon_stats{ tree_leafsSynced = V };
             tree_compareSkipped -> Stats#ru_recon_stats{ tree_compareSkipped = V };
             error_count -> Stats#ru_recon_stats{ error_count = V };
             build_time -> Stats#ru_recon_stats{ build_time = V };
             recon_time -> Stats#ru_recon_stats{ recon_time = V }   
         end,
    set(L, NS).

-spec get(atom(), stats()) -> stats_types().
get(K, Stats) ->
    case K of
        tree_size -> Stats#ru_recon_stats.tree_size;
        tree_compareLeft -> Stats#ru_recon_stats.tree_compareLeft;
        tree_nodesCompared -> Stats#ru_recon_stats.tree_nodesCompared;
        tree_leafsSynced -> Stats#ru_recon_stats.tree_leafsSynced;
        tree_compareSkipped -> Stats#ru_recon_stats.tree_compareSkipped;
        error_count -> Stats#ru_recon_stats.error_count;
        build_time -> Stats#ru_recon_stats.build_time;
        recon_time -> Stats#ru_recon_stats.recon_time;
        _ -> 0
    end.

-spec print(stats()) -> [any()].
print(Stats) ->
    FieldNames = record_info(fields, ru_recon_stats),
    Res = util:for_to_ex(1, length(FieldNames), 
                         fun(I) ->
                                 {lists:nth(I, FieldNames), erlang:element(I + 1, Stats)}
                         end),
    [erlang:element(1, Stats), lists:flatten(lists:reverse(Res))].