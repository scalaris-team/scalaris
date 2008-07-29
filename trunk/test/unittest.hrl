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
%%% File    : unittest.hrl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : contains unittest helpers
%%%
%%% Created :  14 Mar 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schütt <schuett@zib.de>
%% @copyright 2007 Thorsten Schütt
%% @version $Id$

-author('schuett@zib.de').
-vsn('$Id$ ').

-define(assert(Boolean), myassert(Boolean, ??Boolean)).
-define(equals(X, Y), myequals(??X, ??Y, X, Y)).

myassert(true, _Reason) ->
    ok;
myassert(false, Reason) ->
    ct:fail(Reason).

myequals(XS, YS, X, Y) ->
    myequals(X == Y, XS, YS, X, Y).
    
myequals(true, _, _, _, _) ->
    ok;
myequals(false, XS, YS, X ,Y) ->
    ct:fail(lists:flatten(io_lib:format("~p(~p) != ~p(~p)", [XS, X, YS, Y]))).

