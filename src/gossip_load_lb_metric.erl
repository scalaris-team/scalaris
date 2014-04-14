%  @copyright 2010-2014 Zuse Institute Berlin

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
-module(gossip_load_lb_metric).
-vsn('$Id$').

-behavior(gossip_load_beh).

-export([get_load/1, init_histo/2]).

-spec get_load(dht_node_state:state()) -> gossip_load_beh:load().
get_load(_DHTNodeState) ->
    lb_active:get_load_metric().

-spec init_histo(DHTNodeState::dht_node_state:state(), NumberOfBuckets::pos_integer()) 
                    -> gossip_load:histogram().
init_histo(DHTNodeState, NumberOfBuckets) ->
    MyRange = dht_node_state:get(DHTNodeState, my_range),
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
            Load = lb_active:get_load_metric(),
            %% TODO Load = histogram:approximate_load_in_interval(BucketInterval),
            {float(Load), 1.0}
    end.
