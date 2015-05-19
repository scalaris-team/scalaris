% @copyright 2012-2014 Zuse Institute Berlin

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

%% @author Magnus Mueller <mamuelle@informatik.hu-berlin.de>
%% @doc    Unit tests for the dc_clustering module.
%% @end
%% @version $Id$
-module(dc_clustering_SUITE).
-author('mamuelle@informatik.hu-berlin.de').
-vsn('$Id$').

-compile(export_all).
-include("unittest.hrl").
-include("scalaris.hrl").
-include("client_types.hrl").

all() -> [
        single_node
        , two_nodes
].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(Testcase, Config) ->
    % dc_clustering must be activated and a radius must exist
    EnableClustering = {dc_clustering_enable, true},
    ClusterRadius = {dc_clustering_radius, 1000.0},
    case Testcase of
        single_node ->
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(1, [{config, [
                            {log_path, PrivDir}
                            , EnableClustering
                            , ClusterRadius
                        ]}]),
            timer:sleep(500);
        two_nodes ->
            {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
            unittest_helper:make_ring(2, [{config, [
                            {log_path, PrivDir}
                            , EnableClustering
                            , ClusterRadius
                        ]}]),
            timer:sleep(500)
    end,

    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.


%% Helper function to retrieve the vivaldi coordinate and centroid information of a node
get_vivaldi_and_centroids(Gossip, Clustering) ->
    comm:send_local(Gossip, {cb_msg, {gossip_vivaldi, default}, {get_coordinate, comm:this()}}),
    comm:send_local(Clustering, {query_clustering, comm:this()}),

    % get vivaldi coordinate first
    Coordinate = receive
        {vivaldi_get_coordinate_response, Coord, _Confidence} ->
            Coord
    end,

    Centroids = receive
        {query_clustering_response, C} ->
            C
    end,
    {Coordinate, Centroids}.

single_node(_) ->
    % in a ring with only one node, only a single cluster shall exist with the centroid
    % being the node of the ring

    %% get the node which forms the ring
    Clustering = pid_groups:find_a(dc_clustering),
    Group = pid_groups:group_of(Clustering),
    Gossip = pid_groups:pid_of(Group, gossip),

    {Coordinate, [{centroid, Center, Size}]} = get_vivaldi_and_centroids(Gossip, Clustering),
    ?equals(Coordinate, Center),
    ?equals(1.0, Size),
    ok
    .

two_nodes(_) ->
    Clustering = pid_groups:find_a(dc_clustering),
    Group = pid_groups:group_of(Clustering),
    Gossip = pid_groups:pid_of(Group, gossip),

    {_Coordinate, Centroids} = get_vivaldi_and_centroids(Gossip, Clustering),
    ?assert(length(Centroids) > 0),
    ?assert(length(Centroids) < 3),
    ?equals(1.0, lists:foldl(fun(C, Acc) -> dc_centroids:get_relative_size(C) + Acc end,
            0, Centroids)),
    ok
    .
