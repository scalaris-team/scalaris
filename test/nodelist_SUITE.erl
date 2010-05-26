%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    nodelist_SUITE.erl
%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Unit tests for src/nodelist.erl
%%% @end
%%% Created : 18 May 2010 by Nico Kruber <kruber@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(nodelist_SUITE).

-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").

all() ->
    [merge].

suite() ->
    [
     {timetrap, {seconds, 20}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

merge(_Config) ->
    N8 = node:new(pid,   8, 0),
    N14 = node:new(pid, 14, 0),
    N33 = node:new(pid, 33, 0),
    %TODO: implement test
%%     ?equals(rm_chord:merge([N8, N14, N8], [N8, N14, N8], 26), [N8, N14]),
%%     ?equals(rm_chord:merge([N8, N14, N33], [N8, N14, N33], 26), [N33, N8, N14]),
    ok.
