%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : intervals.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : interval data structure + functions for bulkowner
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: intervals.erl 463 2008-05-05 11:14:22Z schuett $
-module(intervals).

-author('schuett@zib.de').
-vsn('$Id: intervals.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([new/2, make/1,
	 is_empty/1, empty/0,
	cut/2]).

-record(interval, {first, last}).

new(Begin, End) ->
    #interval{
     first = Begin, 
     last  = End}.

make({Begin, End}) ->
    #interval{
     first = Begin, 
     last  = End}.

empty() ->
    {empty}.

is_empty({empty}) ->
    true;
is_empty(_) ->
    false.

cut(#interval{first=A0, last=A1}, #interval{first=B0, last=B1}) ->
    B0_in_A = util:is_between(A0, B0, A1),
    B1_in_A = util:is_between(A0, B1, A1),
    A0_in_B = util:is_between(B0, A0, B1),
    A1_in_B = util:is_between(B0, A1, B1),
    if 
	B0_in_A or B1_in_A or A0_in_B or A1_in_B ->
	    new(util:max(A0, B0), util:min(A1, B1));
	true ->
	    empty()
    end;

cut({empty}, #interval{}) ->
    empty();
cut(#interval{}, {empty}) ->
    empty();
cut({empty}, {empty}) ->
    empty().

