% @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Unit tests for src/grouped_node/*.erl.
%%% @end
%% @version $Id$
-module(group_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [add_9, add_9_remove_4].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    ct:pal("Starting unittest ~p", [ct:get_status()]),
    ct:pal("~p", [util:is_unittest()]),
    Config.

end_per_suite(_Config) ->
    scalaris2:stop(),
    ok.

init_per_testcase(_TestCase, Config) ->
    unittest_helper:fix_cwd(),
    scalaris2:start(),
    config:write(dht_node_sup, sup_group_node),
    config:write(dht_node, group_node),
    config:write(group_node_trigger, trigger_periodic),
    config:write(group_node_base_interval, 30000),
    Config.

end_per_testcase(_TestCase, Config) ->
    scalaris2:stop(),
    Config.

add_9(_Config) ->
    admin:add_nodes(9),
    wait_for(check_version({1, 11}, 10)),
    ok.

add_9_remove_4(_Config) ->
    admin:add_nodes(9),
    wait_for(check_version({1, 11}, 10)),
    admin:del_nodes(4),
    timer:sleep(3000),
    wait_for(check_version({1, 15}, 6)),
    ok.

wait_for(F) ->
    case F() of
        true ->
            ok;
        false ->
            timer:sleep(500),
            wait_for(F)
    end.

check_version(Version, Length) ->
    fun () ->
            Versions = [V || {_, V} <- group_debug:dbg_version()],
            ct:pal("~p ~p", [lists:usort(Versions), group_debug:dbg_version()]),
            [Version] ==
                lists:usort(Versions) andalso length(Versions) == Length
    end.
