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
	 cut/2,
	 is_covered/2,
	 unpack/1,
	 in/2,
	 sanitize/1,
	 % for unit testing only
	 wraps_around/1,
	 is_between/3,
	 find_start/3
	 ]).


% @type interval2() = {interval2, term(), term()}. An interval record with a begin and an end
-record(interval2, {first, last}).

% @type interval() = [] | {element, term()} | interval2() | all | [interval2(), interval2()]

% @spec new(term(), term()) -> interval()
new(X, X) ->
    all;
new(Begin, End) ->
    #interval2{
     first = Begin, 
     last  = End}.

% @spec make({term(), term()}) -> interval()
make({Begin, End}) ->
    #interval2{
     first = Begin,
     last  = End}.

% @spec unpack(interval()) -> {term(), term()}
unpack(#interval2{first=First, last=Last}) ->
    {First, Last}.

% @spec is_empty(interval()) -> bool()
is_empty([]) ->
    true;
is_empty(_) ->
    false.

% @spec empty() -> interval()
empty() ->
    [].

% @spec cut(A::interval(), B::interval()) -> interval()
cut([], _) ->
    empty();
cut(_, []) ->
    empty();
cut(A, B) ->
    A_ = normalize(A),
    B_ = normalize(B),
    cut_iter(A_, B_).

cut_iter([First | Rest], B) ->
    [cut_iter(First, B) | cut_iter(Rest, B)];
cut_iter(A, [First | Rest]) ->
    [cut_iter(A, First) | cut_iter(A, Rest)];
cut_iter(all, B) ->
    B;
cut_iter([], B) ->
    [];
cut_iter(A, []) ->
    [];
cut_iter(A, all) ->
    A;
cut_iter(#interval2{first=A0, last=A1} = A, #interval2{first=B0, last=B1} = B) ->
    B0_in_A = is_between(A0, B0, A1),
    B1_in_A = is_between(A0, B1, A1),
    A0_in_B = is_between(B0, A0, B1),
    A1_in_B = is_between(B0, A1, B1),
    if
	A1 == B0 and not A0_in_B ->
	    {element, A1};
	B1 == A0 and not B0_in_A ->
	    {element, B1};
	B0_in_A or B1_in_A or A0_in_B or A1_in_B ->
	    new(util:max(A0, B0), util:min(A1, B1));
	true ->
	    empty()
    end.
	    

% @doc returns true if the intervals cover the complete interval
% @spec is_covered(interval(), [interval()]) -> bool()
is_covered([], _) ->
    true;
is_covered(all, Intervals) ->
    %io:format("is_covered: ~p ~p~n", [all, Intervals]),
    is_covered(new(minus_infinity, plus_infinity), Intervals);
is_covered(Interval, Intervals) ->
    %io:format("is_covered: ~p ~p~n", [Interval, Intervals]),
    NormalIntervals = normalize(Intervals),
    case wraps_around(Interval) of
	true ->
	    is_covered_helper(new(minus_infinity, Interval#interval2.last), NormalIntervals) and 
		is_covered_helper(new(Interval#interval2.first, plus_infinity), NormalIntervals);
	false ->
	    is_covered_helper(Interval, NormalIntervals)
    end.

% @private
is_covered_helper(#interval2{first=First, last=Last}, _) when First >= Last ->
    true;
is_covered_helper(Interval, Intervals) ->
    %io:format("helper: ~p ~p~n", [Interval, Intervals]),
    %io:format("find_start: ~p~n", [find_start(Interval#interval2.first, Intervals, [])]),
    case find_start(Interval#interval2.first, Intervals, []) of
	none ->
	    false;
	{CoversStart, RemainingIntervals} ->
	    if
		CoversStart#interval2.last >= Interval#interval2.last ->
		    true;
		true ->
		    is_covered_helper(intervals:new(CoversStart#interval2.last, Interval#interval2.last), RemainingIntervals)
	    end
    end.

in(X, #interval2{first=First, last=Last}) ->    
    is_between(First, X, Last);
in(X, [I | Rest]) ->
    in(X, I) or in(X, Rest);
in(_, []) ->
    false;
in(_, all) ->
    true;
in(X, {element, X}) ->
    true;
in(_, {element, _}) ->
    false.

sanitize(X) when is_list(X) ->
    sanitize_helper(lists:flatten(X));
sanitize(X) ->
    sanitize_helper(X).

sanitize_helper([]) ->
    [];
sanitize_helper([[] | Rest]) ->
    sanitize_helper(Rest);
sanitize_helper([X | Rest]) ->
    [X | sanitize_helper(Rest)];
sanitize_helper(X) ->
    X.
%%====================================================================
%% private functions
%%====================================================================  

% @private
wraps_around(#interval2{first=First, last=Last}) when First >= Last ->
    true;
wraps_around(_) ->
    false.

normalize([]) ->
    [];
normalize([all | Rest]) ->
    [new(minus_infinity, plus_infinity) | normalize(Rest)];
normalize([[] | Rest]) ->
    normalize(Rest);
normalize([Interval|Rest]) ->
    case wraps_around(Interval) of
	true ->
	    [new(Interval#interval2.first, plus_infinity), new(minus_infinity, Interval#interval2.last) | normalize(Rest)];
	false ->
	    [Interval | normalize(Rest)]
    end;
normalize(#interval2{first=First, last=Last}) when First >= Last ->
    [new(minus_infinity, Last), new(First, plus_infinity)];
normalize(A) ->
    A.

% @private
% @spec find_start(term(), [interval()], [interval()]) -> {interval(), [interval()]} | none
find_start(_Start, [], _) ->
    none;
find_start(Start, [Interval | Rest], Remainder) ->
    case is_between(Interval#interval2.first, Start, Interval#interval2.last) of
	true ->
	    {Interval, Remainder ++ Rest};
	false ->
	    find_start(Start, Rest, [Interval | Remainder])
    end.

% @private
% @spec is_between(term(), term(), term()) -> bool()
is_between(X, _, X) ->
    true;
is_between(_, plus_infinity, plus_infinity) ->
    true;
is_between(X, Y, plus_infinity) ->
    X =< Y;
is_between(minus_infinity, minus_infinity, _) ->
    true;
is_between(minus_infinity, X, Y) ->
    X =< Y;
is_between(_, plus_infinity, _) ->
    false;
is_between(_, minus_infinity, _) ->
    false;
is_between(Begin, Id, End) ->
    if
	Begin < End -> 
	    (Begin =< Id) and (Id =< End);
	Begin == End ->
	    true;
	true -> 
	    (Begin =< Id) or (Id =< End)
    end.
