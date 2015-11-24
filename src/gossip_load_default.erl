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
-module(gossip_load_default).
-vsn('$Id$').

-behaviour(gossip_load_beh).

-export([get_load/1, init_histo/2]).

-spec get_load(node_details:node_details()) -> gossip_load_beh:load().
get_load(NodeDetails) ->
    node_details:get(NodeDetails, load).

-spec init_histo(node_details:node_details(), NumberOfBuckets::pos_integer())
                    -> gossip_load:histogram().
init_histo(NodeDetails, NumberOfBuckets) ->
    DB = node_details:get(NodeDetails, db),
    MyRange = node_details:get(NodeDetails, my_range),
    Buckets = intervals:split(intervals:all(), NumberOfBuckets),
    [ {BucketInterval, get_load_for_interval(BucketInterval, MyRange, DB)}
        || BucketInterval <- Buckets ].

-spec get_load_for_interval(BucketInterval::intervals:interval(),
    MyRange::intervals:interval(), DB::db_dht:db()) -> gossip_load:avg() | unknown.
get_load_for_interval(BucketInterval, MyRange, DB) ->
    Intersection = intervals:intersection(BucketInterval, MyRange),
    case intervals:is_empty(Intersection) of
        true -> unknown;
        false ->
            try
                Load = db_dht:get_load(DB, BucketInterval),
                {float(Load), 1.0}
            catch
                Level:Reason ->
                    log:pal("[ gossip ~p ] error while accessing DB: thrown ~s, reason:~.2p",
                            [self(), Level, Reason]),
                    unknown
            end
    end.
