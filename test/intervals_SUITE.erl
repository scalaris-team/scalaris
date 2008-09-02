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

-include_lib("quickcheck.hrl").

-import(intervals).

-include_lib("unittest.hrl").

all() ->
    [new, qc_is_empty, qc_is_covered, qc_cut, qc_sanitize].

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:is_empty/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_is_empty() ->
    ?FORALL(X, int(),
	    ?FORALL(I, interval(),
		    ?IMPLIES(intervals:is_empty(I), not intervals:in(X, I)))).

qc_is_empty(_Config) ->
    qc:quickcheck(prop_is_empty()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:cut/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_cut() ->    
    ?FORALL(A, interval(),
	    ?FORALL(B, interval(),
		    ?FORALL(X, qc:int(),
			    ?IMPLIES(intervals:in(X, A) 
				     and intervals:in(X, B),
				     intervals:in(X, intervals:cut(A, B)))))).

qc_cut(_Config) ->
    qc:quickcheck(prop_cut()).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:is_covered/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_is_covered() ->
    ?FORALL(I, interval(),
	    ?FORALL(Is, qc:list(interval()),
		    ?FORALL(X, qc:int(),
			    ?IMPLIES(intervals:is_covered(I, Is) and intervals:in(X, I),
				    intervals:in(X, Is))))).

qc_is_covered(_Config) ->
    qc:quickcheck(prop_is_covered()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:sanitize/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_sanitize() ->
    ?FORALL(Is, list(interval()),
	    ?FORALL(X, qc:int(),
		    ?IMPLIES(intervals:in(X, Is),
			     intervals:in(X, intervals:sanitize(Is))))).

qc_sanitize(_Config) ->
    qc:quickcheck(prop_sanitize()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% quickcheck generators
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
interval() ->
    qc:frequency([
		  {6, ?LET(Begin, qc:int(), ?LET(End, qc:int(), return(intervals:new(Begin, End))))},
		  {1, intervals:empty()}
]).

