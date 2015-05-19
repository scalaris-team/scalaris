%  @copyright 2012 Zuse Institute Berlin

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
%% @doc    Test suite for the dc_centroids module.
%% @end
%% @version $Id$

-module(dc_centroids_SUITE).
-author('mamuelle@informatik.hu-berlin.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() -> [
        distance
    ].

suite() ->
    [
        {timetrap, {seconds, 30}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

% Test dc_centroids:distance/2
%
% This is the euclidian distance between two centroids
distance(_Config) ->
    U = dc_centroids:new([0.0,0.0], 1.0),
    V = dc_centroids:new([1.0,1.0], 1.0),
    ?equals(dc_centroids:distance(U,V), math:sqrt(2)),
    ok.
