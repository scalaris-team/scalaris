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
    [add_9].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

add_9(_Config) ->
    ct:pal("~p", [node()]),
    unittest_helper:fix_cwd(),
    scalaris2:start(),
    admin:add_nodes(9),
    wait_for(check_version({1, 11})),
    ok.

wait_for(F) ->
    case F() of
        true ->
            ok;
        false ->
            timer:sleep(500),
            wait_for(F)
    end.

check_version(Version) ->
    fun () ->
            Versions = [V || {_, V} <- group_debug:dbg_version()],
            ct:pal("~p ~p", [lists:usort(Versions), group_debug:dbg_version()]),
            [Version] ==
                lists:usort(Versions)
    end.
