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
    [new, 
     qc_make, 
     qc_unpack,
     qc_unpack2,
     qc_is_empty, 
     qc_is_covered, 
     qc_cut, 
     qc_not_cut, 
     qc_not_cut2, 
     my_cut,
     qc_cut_iter, 
     qc_sanitize,
     qc_sanitize2,
     qc_in].

suite() ->
    [
     {timetrap, {seconds, 100}}
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
% intervals:make/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_make() ->
    ?FORALL(X, interval_startpoint(),
	    ?FORALL(Y, interval_endpoint(),
		    intervals:make({X,Y}) == intervals:new(X, Y))).

qc_make(_Config) ->
    qc:quickcheck(prop_make()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:unpack/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_unpack() ->
    ?FORALL(X, interval_startpoint(), ?FORALL(Y, interval_endpoint(),
       ?IMPLIES(not (X == Y), {X,Y} == intervals:unpack(intervals:new(X, Y))))).
    
qc_unpack(_Config) ->
    qc:quickcheck(prop_unpack()).

prop_unpack2() ->
    ?FORALL(X, external_datatype(),
	    intervals:is_covered(
	      all, [intervals:make(intervals:unpack(intervals:new(X,X)))])).

qc_unpack2(_Config) ->
    qc:quickcheck(prop_unpack2()).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:is_empty/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_is_empty() ->
    ?FORALL(X, interval_point(),
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
		    ?FORALL(X, interval_point(),
			    ?IMPLIES(intervals:in(X, A) 
				     and intervals:in(X, B),
				     intervals:in(X, intervals:cut(A, B)))))).

prop_not_cut() ->    
    ?FORALL(A, interval(),
	    ?FORALL(B, interval(),
		    ?FORALL(X, interval_point(),
			    ?IMPLIES(intervals:in(X, A) 
				     xor intervals:in(X, B),
				     not intervals:in(X, intervals:cut(A, B)))))).

prop_not_cut2() ->    
    ?FORALL(A, interval(),
	    ?FORALL(B, interval(),
		    ?FORALL(X, interval_point(),
			    ?IMPLIES(not intervals:in(X, A) 
				     and not intervals:in(X, B),
				     not intervals:in(X, intervals:cut(A, B)))))).

qc_cut(_Config) ->
    qc:quickcheck(prop_cut()).

qc_not_cut(_Config) ->
    qc:quickcheck(prop_not_cut()).

qc_not_cut2(_Config) ->
    qc:quickcheck(prop_not_cut2()).

my_cut(_Config) ->
    ?assert(intervals:in(0, intervals:cut(intervals:new(minus_infinity, 0), 
			       intervals:new(0, plus_infinity)))),
    ?assert(intervals:in(0, intervals:cut(intervals:new(minus_infinity, 1),
			       intervals:new(-1, plus_infinity)))),
    ?assert(not intervals:in(-1,
		  intervals:cut(intervals:new(minus_infinity, -1),
				intervals:new(1, plus_infinity)))),
    ?assert(not intervals:in(0, 
		  intervals:cut(intervals:new(minus_infinity, -1),
				intervals:new(1, plus_infinity)))),
    ?assert(not intervals:in(1, intervals:cut(intervals:new(minus_infinity, -1), 
				   intervals:new(1, plus_infinity)))).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:cut_iter/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_cut_iter() ->    
    ?FORALL(
       A, qc:list(interval()),
       ?FORALL(
	  B, qc:list(interval()),
	  ?FORALL(
	     X, interval_point(),
	     ?IMPLIES(
	     intervals:in(X, A) and intervals:in(X, B),
	     not intervals:is_empty(intervals:cut_iter(A, B)))))).
%	     not intervals:is_covered(A, B)))).

qc_cut_iter(_Config) ->
    qc:quickcheck(prop_cut_iter()).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:is_covered/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_is_covered() ->
    ?FORALL(I, interval(),
	    ?FORALL(Is, qc:list(interval()),
		    ?FORALL(X, external_datatype(),
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
	    ?FORALL(X, interval_point(),
		    intervals:in(X, Is) ==
		    intervals:in(X, intervals:sanitize(Is)))).

prop_sanitize2() ->
    ?FORALL(I, interval(),
	    intervals:sanitize(I) == I).

qc_sanitize(_Config) ->
    qc:quickcheck(prop_sanitize()).

qc_sanitize2(_Config) ->
    qc:quickcheck(prop_sanitize2()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:in/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
prop_in() ->
    ?FORALL(X, interval_point(),
	    ?FORALL(Y, interval_point(),
		    intervals:in(X, intervals:new(X, Y)) and
		    intervals:in(Y, intervals:new(X, Y)))).

qc_in(_Config) ->
    qc:quickcheck(prop_in()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% quickcheck generators
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
external_datatype() ->
%    qc:list(qc:int()).
    qc:int().

interval() ->
    qc:frequency([
		  {6, ?LET(Begin, interval_startpoint(), ?LET(End, interval_endpoint(), return(intervals:new(Begin, End))))},
		  {1, intervals:new(external_datatype())},
		  {1, intervals:empty()},
		  {1, all}
]).

interval_point() ->
    qc:frequency([
		  {6, external_datatype()},
		  {1, plus_infinity},
		  {1, minus_infinity}
		  ]).

interval_endpoint() ->
    qc:frequency([
		  {6, external_datatype()},
		  {1, plus_infinity}
		  ]).

interval_startpoint() ->
    qc:frequency([
		  {6, external_datatype()},
		  {1, minus_infinity}
		  ]).
