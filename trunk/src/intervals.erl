%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%%            2008 onScale solutions GmbH
%% @version $Id$
-module(intervals).

-author('schuett@zib.de').
-vsn('$Id$ ').

-include("scalaris.hrl").

-export([first/0,last/0,
         new/1, new/2, make/1,
         is_empty/1, empty/0,
         cut/2,
         is_covered/2,
         unpack/1,
         in/2,
         normalize/1,
	 is_well_formed/1
         % for unit testing only
%%       cut_iter/2,
%%         , wraps_around/1
%%         , is_between/3
%%       find_start/3
%%         , greater_equals_than/2
%%         , normalize/2
	 , test/0
        ]).

-type(key() :: ?RT:key() | minus_infinity | plus_infinity).
-type(simple_interval() :: {element, key()} | {interval, key(), key()} | all).
-type(interval() :: simple_interval() | list(simple_interval())).
% @type interval() = [] | term() | {interval,term(),term()} | all | list(interval()).
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


-spec is_well_formed/1 :: (interval()) ->  boolean().
is_well_formed([Interval|Rest]) ->
    is_well_formed(Interval) andalso is_well_formed(Rest);
is_well_formed([]) ->
    true;
is_well_formed({element, _X}) ->
    true;
is_well_formed({interval, X, Y}) ->
    not greater_equals_than(X, Y);
is_well_formed(all) ->
    true;
is_well_formed(_) ->
    false.


% @spec new(term(), term()) -> interval()
-spec new/2 :: (key(), key()) ->  all | {interval, key(), key()} | list(simple_interval()).
new(minus_infinity, plus_infinity) ->
    all;
new(minus_infinity, minus_infinity) ->
    {element, minus_infinity};
new(plus_infinity, plus_infinity) ->
    {element, plus_infinity};
new(plus_infinity, minus_infinity) ->
    [{element, plus_infinity}, {element, minus_infinity}];
new(X, X) ->
    all;
new(plus_infinity, X) ->
    [{interval, minus_infinity, X}, {element, plus_infinity}];
new(X, minus_infinity) ->
    [{interval, X, plus_infinity}, {element, minus_infinity}];
new(Begin, End) ->
    case wraps_around({interval, Begin, End}) of
        true ->  [{interval, minus_infinity, End},
                  {interval, Begin, plus_infinity}];
        false -> {interval, Begin, End}
    end.

-spec new/1 :: (key()) -> {element, key()}.
new(X) ->
    {element,X}.

-spec make/1 :: ({key(), key()}) -> all | {interval, key(), key()}.
make({Begin, End}) ->
    new(Begin, End).

-spec get_end_of_interval/1 :: (simple_interval()) -> key().
get_end_of_interval(all) ->
    plus_infinity;
get_end_of_interval({element, X}) ->
    X;
get_end_of_interval({interval, _, End}) ->
    End.

% @spec unpack(interval()) -> {term(), term()}
unpack(all) -> {minus_infinity,plus_infinity};
unpack([]) -> empty();
unpack({interval,First,Last}) ->
    {First, Last};
unpack(X) ->
    X.


%% @doc checks whether the given set is empty
-spec is_empty/1 :: (interval()) -> boolean().
is_empty([]) ->
    true;
is_empty(_) ->
    false.

%% @doc the empty set
-spec empty/0 :: () -> [].
empty() ->
    [].

%% @doc the cut of two sets
-spec cut/2 :: (interval(), interval()) -> interval().
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
    normalize(cut_iter(A_, B_)).

-spec cut_iter/2 :: (interval(), interval()) -> interval().
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
cut_iter({element,A_Value}=A, B) ->
    case in(A_Value, B) of
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
% @spec is_covered(interval(), [interval()]) -> boolean()
-spec(is_covered/2 :: (interval(), [interval()] | interval()) -> boolean()).
is_covered([], _) ->
    true;
is_covered(_, all) ->
    true;
is_covered({element,X}, Intervals) ->
    in(X, Intervals);
is_covered({interval, _, _}, {element,_}) ->
    false;
is_covered([Interval | Rest], Intervals) ->
    is_covered(Interval, Intervals) andalso is_covered(Rest, Intervals);
is_covered(Interval, {interval, _, _}=I) ->
    is_covered(Interval, [I]);
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
is_covered_helper(all, Intervals) ->
    case find_start(minus_infinity, Intervals, []) of
        none ->
            false;
        {CoversStart, RemainingIntervals} ->
            case greater_equals_than(get_end_of_interval(CoversStart), plus_infinity) of
                true ->
                    true;
                false ->
                    is_covered_helper(intervals:new(get_end_of_interval(CoversStart), plus_infinity), RemainingIntervals)
            end
    end;
is_covered_helper(Interval, Intervals) ->
    %io:format("helper: ~p ~p~n", [Interval, Intervals]),
    %io:format("find_start: ~p~n", [find_start(element(first(),Interval), Intervals, [])]),
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

%% @doc X \in I
-spec in/2 :: (X::key(), I::interval()) -> boolean().
in(X, {interval, First, Last} = I) ->
    case wraps_around(I) of
        false ->
            is_between(First, X, Last);
        true ->
            in(X, normalize(I))
    end;
in(X, [I | Rest]) ->
    in(X, I) orelse in(X, Rest);
in(_, []) ->
    false;
in(_, all) ->
    true;
in(X, {element,X}) ->
    true;
in(_, {element,_}) ->
    false.

%% @doc bring list of intervals to normal form
-spec normalize(interval()) -> interval().
normalize(List) when is_list(List) ->
    case normalize(lists:usort(lists:flatten(List)), []) of
        [Element] ->
            Element;
        X ->
            X
    end;
normalize(Element) ->
    case normalize(Element, []) of
        [Element2] ->
            Element2;
        X ->
            X
    end.

%% @doc helper for normalize/1
-spec normalize/2 :: (interval(), list(interval)) ->
    list(interval()).
normalize(_, [all] = List) ->
    List;
normalize([], List) ->
    List;
normalize([all | _Rest], _List) ->
    [all];
normalize([Interval | Rest], List) ->
    normalize(Rest, normalize(Interval, List));
normalize({element, _} = Element, List) ->
    [Element | List];
normalize({interval,First,First}, List) ->
    [{element, First} | List];
normalize({interval,First,Last}=I, List) ->
    case wraps_around(I) of
        true ->  [new(minus_infinity, Last),
                  new(First, plus_infinity) | List];
        false -> [new(First, Last) | List]
    end;
normalize(all, _List) ->
    [all];
normalize(A, List) ->
    [A | List].

%%====================================================================
%% private functions
%%====================================================================

% @private
wraps_around({interval, X, X}) ->
    false;
wraps_around({interval, minus_infinity, _}) ->
    false;
wraps_around({interval, _, plus_infinity}) ->
    false;
wraps_around({interval, _, minus_infinity}) ->
    true;
wraps_around({interval, plus_infinity, _}) ->
    true;
wraps_around({interval, First, Last}) when First > Last ->
    true;
wraps_around(_) ->
    false.


% @private
% @spec find_start(term(), [interval()], [interval()]) -> {interval(), [interval()]} | none
find_start(_Start, [], _) ->
    none;
find_start(Start, [{element,_} | Rest], Remainder) ->
    find_start(Start, Rest, Remainder);
find_start(_Start, [all | _Rest], _Remainder) ->
    {all, []};
find_start(Start, [Interval | Rest], Remainder) ->
    %% @todo is the precondition of is_between fulfilled?
    case is_between(element(first(),Interval), Start, element(last(),Interval)) of
        true ->
            {Interval, Remainder ++ Rest};
        false ->
            find_start(Start, Rest, [Interval | Remainder])
    end;
find_start(Start, Interval, Remainder) ->
    find_start(Start, [Interval], Remainder).

% @doc Begin &lt;= X &lt;= End
% precondition Begin &lt;= End
% @spec is_between(term(), term(), term()) -> boolean()
-spec is_between/3 :: (Begin::key(), X::key(), End::key()) -> boolean().
is_between(_, X, X) ->
    true;
is_between(X, _, X) ->
    false;
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

%% @doc A >= B
-spec greater_equals_than/2 :: (A::key(), B::key()) -> boolean().
greater_equals_than(minus_infinity, minus_infinity) ->
    true;
greater_equals_than(minus_infinity, _) ->
    false;
greater_equals_than(plus_infinity, plus_infinity) ->
    true;
greater_equals_than(_, plus_infinity) ->
    false;
greater_equals_than(_, minus_infinity) ->
    true;
greater_equals_than(X, Y) ->
    X >= Y.
