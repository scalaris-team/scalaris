% @copyright 2011-2015 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc : Unit tests for unittest_helper
%% @end
%% @version $Id$
-module(unittest_helper_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [tester_make_ring_with_ids].

suite() ->
    [
     {timetrap, {seconds, 30}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    [{stop_ring, true} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

-spec prop_make_ring_with_ids(IDs::[?RT:key(),...]) -> true.
prop_make_ring_with_ids(IDs) ->
    UniqueIDs = lists:usort(IDs),
    unittest_helper:make_ring_with_ids(
      UniqueIDs,
      [{config, [pdb:get(log_path, ?MODULE)]}]),

    DHTNodes = pid_groups:find_all(dht_node),
    ?equals(length(DHTNodes), length(UniqueIDs)),

    ActualIds = [begin
           comm:send_local(DhtNode,
                           {get_node_details, comm:this(), [node]}),
           % note: no timeout possible - can not leave messages in queue!
           receive
               {get_node_details_response, NodeDetails} ->
                   node:id(node_details:get(NodeDetails, node))
%%            after 500 -> timeout
           end
       end || DhtNode <- DHTNodes],

    ?equals(lists:sort(ActualIds), lists:sort(UniqueIDs)),

    unittest_helper:stop_ring(),
    % wait a bit for all processes to stop
    timer:sleep(100),
    true.

tester_make_ring_with_ids(Config) ->
    {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
    pdb:set({log_path, PrivDir}, ?MODULE),
    tester:test(?MODULE, prop_make_ring_with_ids, 1, 10).
