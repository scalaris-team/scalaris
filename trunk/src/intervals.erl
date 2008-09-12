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
%%%           Florian Schintke <schintke@onscale.de>
%%% Description : interval data structure + functions for bulkowner
%%%
%%% Created :  3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%%            2008 onScale solutions GmbH
%% @version $Id: intervals.erl 463 2008-05-05 11:14:22Z schuett $
-module(intervals).

-author('schuett@zib.de').
-vsn('$Id: intervals.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([first/0,last/0,
	 new/1, new/2, make/1,
	 is_empty/1, empty/0,
	 cut/2,
	 is_covered/2,
	 unpack/1,
	 in/2,
	 sanitize/1,
	 % for unit testing only
	 cut_iter/2,
	 normalize/1,
	 wraps_around/1,
	 is_between/3,
	 find_start/3
	 ]).

% @type interval() = [] | term() | {interval,term(),term()} | all | list(interval())]
% [] -> empty interval
% {element, term()} -> one element interval
% {interval,term(),term()} -> closed interval
% all -> minus_infinity to plus_infinity
% list(interval()) -> [i1, i2, i3,...]
%
% access start and endpoint of a tuple-interval using 
% element(first(),Interval) and element(last(),Interval).
first() ->
    2.
last() ->
    3.


% @spec new(term(), term()) -> interval()
new(X, X) ->
    all;
new(Begin, End) ->
    {interval, Begin, End}.
new(X) ->
    {element,X}.

make({Begin, End}) ->
    new(Begin, End).

% @spec unpack(interval()) -> {term(), term()}
unpack(all) -> {minus_infinity,plus_infinity};
unpack([]) -> empty();
unpack({interval,First,Last}) ->
    {First, Last};
unpack(X) ->
    X.


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
cut({element,X}=A, B) ->
    case in(X,B) of
	true -> A;
	false -> empty()
    end;
cut(A, {element,_}=B) -> 
    cut(B, A);
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
cut_iter([], _) ->
    [];
cut_iter(_, []) ->
    [];
cut_iter(A, all) ->
    A;
cut_iter({element,_}=A, B) ->
    case in(A, B) of
	true -> A;
	false -> empty()
    end;
cut_iter(A, {element,_}=B) ->
    cut_iter(B, A);
cut_iter({interval, A0, A1}, {interval, B0, B1}) ->
    B0_in_A = is_between(A0, B0, A1),
    B1_in_A = is_between(A0, B1, A1),
    A0_in_B = is_between(B0, A0, B1),
    A1_in_B = is_between(B0, A1, B1),
    if
	(A1 == B0) and not A0_in_B ->
	    {element,A1};
	(B1 == A0) and not B0_in_A ->
	    {element,B1};
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
is_covered({element,_}=X, Intervals) ->
    in(X, Intervals);
is_covered({interval, _, _}, {element,_}) ->
    false;
is_covered(Interval, Intervals) ->
    %io:format("is_covered: ~p ~p~n", [Interval, Intervals]),
    NormalIntervals = normalize(Intervals),
    case wraps_around(Interval) of
	true ->
	    is_covered_helper(new(minus_infinity, element(last(), Interval)), NormalIntervals) 
			      and 
			      is_covered_helper(new(element(first(),Interval), plus_infinity), NormalIntervals);
	false ->
	    is_covered_helper(Interval, NormalIntervals)
    end.

% @private
is_covered_helper(Interval, Intervals) ->
%    io:format("helper: ~p ~p~n", [Interval, Intervals]),
%    io:format("find_start: ~p~n", [find_start(element(first(),Interval), Intervals, [])]),
    case find_start(element(first(),Interval), Intervals, []) of
	none ->
	    false;
	{CoversStart, RemainingIntervals} ->	    
	    case greater_equals_than(element(last(),CoversStart), element(last(),Interval)) of
		true ->
		    true;
		false ->
		    is_covered_helper(intervals:new(element(last(),CoversStart), element(last(),Interval)), RemainingIntervals)
	    end
    end.

in(X, {interval, First, Last}) ->    
    is_between(First, X, Last);
in(X, [I | Rest]) ->
    in(X, I) or in(X, Rest);
in(_, []) ->
    false;
in(_, all) ->
    true;
in(X, {element,X}) ->
    true;
in(_, {element,_}) ->
    false.

sanitize(X) when is_list(X) ->
    sanitize_helper(X, []);
sanitize(X) ->
    X.

sanitize_helper([[] | Rest], List) ->
    sanitize_helper(Rest, List);
sanitize_helper([all | _], _) ->
    all;
sanitize_helper([X | Rest], List) when is_list(X) ->
    sanitize_helper(Rest, sanitize_helper(X, List));
sanitize_helper([X | Rest], List) ->
    sanitize_helper(Rest, [X | List]);
sanitize_helper(all, List) ->
    List;
sanitize_helper({interval, _First, _Last} = X, List) ->
    [X | List];
sanitize_helper([], List) ->
    List.
%%====================================================================
%% private functions
%%====================================================================  

% @private
wraps_around({interval, minus_infinity, _}) ->
  false;
wraps_around({interval, _, plus_infinity}) ->
  false;
wraps_around({interval, First, Last}) when First >= Last ->
    true;
wraps_around(_) ->
    false.

normalize([]) ->
    [];
normalize([all | Rest]) ->
    [new(minus_infinity, plus_infinity) | normalize(Rest)];
normalize([[] | Rest]) ->
    normalize(Rest);
normalize([Interval]) -> 
    Interval;
normalize([Interval|Rest]) ->
    case wraps_around(Interval) of
	true ->
	    [new(element(first(),Interval), plus_infinity), new(minus_infinity, element(last(),Interval)) | normalize(Rest)];
	false ->
	    [Interval | normalize(Rest)]
    end;
normalize({interval,First,Last}=I) ->
  case greater_equals_than(First,Last) of
	true ->  [new(minus_infinity, Last), new(First, plus_infinity)];
      false -> I
  end;
normalize(A) ->
    A.

% @private
% @spec find_start(term(), [interval()], [interval()]) -> {interval(), [interval()]} | none
find_start(_Start, [], _) ->
    none;
find_start(Start, [{element,_} | Rest], Remainder) ->
    find_start(Start, Rest, Remainder);
find_start(Start, [Interval | Rest], Remainder) ->
    case is_between(element(first(),Interval), Start, element(last(),Interval)) of
	true ->
	    {Interval, Remainder ++ Rest};
	false ->
	    find_start(Start, Rest, [Interval | Remainder])
    end;
find_start(Start, Interval, Remainder) ->
    find_start(Start, [Interval], Remainder).

% @private
% @spec is_between(term(), term(), term()) -> bool()
is_between(X, _, X) ->
    true;
is_between(_, plus_infinity, plus_infinity) ->
    true;
is_between(minus_infinity, _, plus_infinity) ->
    true;
is_between(_, minus_infinity, plus_infinity) ->
    false;
is_between(X, Y, plus_infinity) ->
    X =< Y;
is_between(minus_infinity, minus_infinity, _) ->
    true;
is_between(minus_infinity, plus_infinity, _) ->
    false;
is_between(minus_infinity, X, Y) ->
    X =< Y;
is_between(X, plus_infinity, Y) when X > Y->
    true;
is_between(_, plus_infinity, _) ->
    false;
is_between(X, minus_infinity, Y) when X > Y->
    true;
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

greater_equals_than(minus_infinity, minus_infinity) ->
    true;
greater_equals_than(minus_infinity, _) ->
    false;
greater_equals_than(plus_infinity, plus_infinity) ->
    true;
greater_equals_than(_, plus_infinity) ->
    false;
greater_equals_than(X, Y) ->
    X >= Y.
