%  @copyright 2010-2015 Zuse Institute Berlin

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
%% @version $Id$
-module(lb_active_gossip_request_metric).
-author('michels@zib.de').
-vsn('$Id$').

-behaviour(gossip_load_beh).

-export([get_load/1, init_histo/2]).

-spec get_load(node_details:node_details()) -> gossip_load_beh:load().
get_load(_NodeDetails) ->
    lb_stats:get_request_metric().

-spec init_histo(node_details:node_details(), NumberOfBuckets::pos_integer())
                    -> gossip_load:histogram().
init_histo(NodeDetails, NumberOfBuckets) ->
    MyRange = node_details:get(NodeDetails, my_range),
    Buckets = intervals:split(intervals:all(), NumberOfBuckets),
    [ {BucketInterval, get_load_for_interval(BucketInterval, MyRange)}
        || BucketInterval <- Buckets ].

-spec get_load_for_interval(BucketInterval::intervals:interval(),
    MyRange::intervals:interval()) -> gossip_load:avg() | unknown.
get_load_for_interval(BucketInterval, MyRange) ->
    Intersection = intervals:intersection(BucketInterval, MyRange),
    case intervals:is_empty(Intersection) of
        true -> unknown;
        false ->
            Load = lb_stats:default_value(lb_stats:get_request_metric()),
            {float(Load), 1.0}
    end.
