% @copyright 2010-2011 Zuse Institute Berlin

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
%% @doc    Unit tests for the scalaris_cth module.
%% @end
%% @version $Id$
-module(scalaris_cth_SUITE).
-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [test_timeout].

suite() ->
    [
     {timetrap, {seconds, 1}},
     {ct_hooks, [scalaris_cth]}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

    
test_timeout(_Config) ->
    unittest_helper:make_ring(4),
    timer:sleep(2000).
