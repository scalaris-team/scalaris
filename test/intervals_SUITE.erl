%  Copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : intervals_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/intervals.erl
%%%
%%% Created :  21 Feb 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(intervals_SUITE).

-author('schuett@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-import(intervals).

-include_lib("unittest.hrl").

all() ->
    [new, is_empty, cut].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

new(_Config) ->
    intervals:new("a", "b"),
    ?assert(true).

is_empty(_Config) ->
    NotEmpty = intervals:new("a", "b"),
    Empty = intervals:empty(),
    ?assert(not intervals:is_empty(NotEmpty)),
    ?assert(intervals:is_empty(Empty)).
    
cut(_Config) ->
    NotEmpty = intervals:new("a", "b"),
    Empty = intervals:empty(),
    ?assert(intervals:is_empty(intervals:cut(NotEmpty, Empty))),
    ?assert(intervals:is_empty(intervals:cut(Empty, NotEmpty))),
    ?assert(intervals:is_empty(intervals:cut(NotEmpty, Empty))),
    ?assert(not intervals:is_empty(intervals:cut(NotEmpty, NotEmpty))),
    ?assert(intervals:cut(NotEmpty, NotEmpty) == NotEmpty),
    ok.
    
