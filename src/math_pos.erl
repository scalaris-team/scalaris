%  @copyright 2011 Zuse Institute Berlin

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
%% @doc    Implements mathematical operations on numbers in positional
%%         notations represented by lists, i.e.
%%         [1,2,3] with Base 10 equals 1*10^0 + 2*10^-1 + 3*10^-2.
%%         Note: valid list elements are: 0..(Base-1). 
%% @end
%% @version $Id$
-module(math_pos).
-author('kruber@zib.de').
-vsn('$Id$').

-type position_var() :: [non_neg_integer()].

-export([plus/3, minus/3, divide/3,
         multiply/3, multiply/4,
         make_same_length/3, make_same_length/4, remove_zeros/3,
         from_decimal/2, to_decimal/2]).

%% @doc A + B
-spec plus(A::position_var(), B::position_var(), Base::pos_integer()) -> position_var().
plus(A, B, Base) ->
    plus_rev(lists:reverse(A), lists:reverse(B), 0, [], Base).

-spec plus_rev(A_rev::position_var(), B_rev::position_var(),
        Carry::non_neg_integer(), Sum::position_var(), Base::pos_integer())
    -> Sum::position_var().
plus_rev([A1 | A_rev_Rest], [D1 | D_rev_Rest], Carry, Sum, Base) ->
    S1_new1 = A1 + D1 + Carry,
    NewCarry = S1_new1 div Base,
    S1_new = S1_new1 - NewCarry * Base,
    plus_rev(A_rev_Rest, D_rev_Rest, NewCarry, [S1_new | Sum], Base);
% note: forget first carry (don't change length of lists)
plus_rev([], [], _Carry, Sum, _Base) -> Sum.

%% @doc A - B
-spec minus(A::position_var(), B::position_var(), Base::pos_integer()) -> position_var().
minus(A, B, Base) ->
    minus_rev(lists:reverse(A), lists:reverse(B), 0, [], Base).
  
-spec minus_rev(A_rev::position_var(), B_rev::position_var(), Carry::non_neg_integer(),
        Diff::position_var(), Base::pos_integer()) -> Diff::position_var().
minus_rev([A1 | A_rev_Rest], [B1 | B_rev_Rest], Carry, Diff, Base) ->
    {CurChar, NewCarry} = case (A1 - Carry - B1) of
                              X when X >= 0 -> {X, 0};
                              X when X < (-Base) -> {X + 2 * Base, 2};
                              X -> {X + Base, 1}
                          end,
    minus_rev(A_rev_Rest, B_rev_Rest, NewCarry, [CurChar | Diff], Base);
% note: forget first carry (only important inside the subtraction)
minus_rev([], [], _Carry, Diff, _Base) -> Diff.


%% @doc A * Factor, if Factor is a non-negative integer cutting off any carry
%%      forwards.
-spec multiply(A::position_var(), Factor::non_neg_integer(), Base::pos_integer())
        -> position_var().
multiply(A, Factor, Base) ->
    element(1, multiply(A, Factor, Base, cutoff)).

%% @doc A * Factor, if Factor is a non-negative integer.
-spec multiply(A::position_var(), Factor::non_neg_integer(), Base::pos_integer(),
               Cut::cutoff) -> {Prod::position_var(), Added::0};
              (A::position_var(), Factor::non_neg_integer(), Base::pos_integer(),
               Cut::enlarge) -> {Prod::position_var(), Added::non_neg_integer()}.
multiply(A = [_|_], 0, _Base, _Cut) ->
    {lists:duplicate(erlang:length(A), 0), 0};
multiply(A = [_|_], 1, _Base, _Cut) ->
    {A, 0};
multiply(A = [_|_], Factor, Base, Cut) when is_integer(Factor) andalso Factor > 0 ->
    multiply_rev1(lists:reverse(A), Factor, 0, [], Base, Cut, 0);
multiply([], _Factor, _Base, _Cut) ->
    {[], 0}.

-spec multiply_rev1(A_rev::position_var(), Factor::non_neg_integer(),
                    Carry::non_neg_integer(), Prod::position_var(),
                    Base::pos_integer(), Cut::cutoff,
                    Added::0) -> {Prod::position_var(), Added::0};
                   (A_rev::position_var(), Factor::non_neg_integer(),
                    Carry::non_neg_integer(), Prod::position_var(),
                    Base::pos_integer(), Cut::enlarge,
                    Added::non_neg_integer()) -> {Prod::position_var(), Added::non_neg_integer()}.
multiply_rev1([A1 | A_rev_Rest], Factor, Carry, Prod, Base, Cut, Added) ->
    P1_new1 = A1 * Factor + Carry,
    NewCarry = P1_new1 div Base,
    P1_new = P1_new1 - NewCarry * Base,
    multiply_rev1(A_rev_Rest, Factor, NewCarry, [P1_new | Prod], Base, Cut, Added);
multiply_rev1([], _Factor, 0, Prod, _Base, enlarge, Added) ->
    {Prod, Added};
multiply_rev1([], Factor, Carry, Prod, Base, enlarge = Cut, Added) ->
    % enlarge list length to fit the result
    NewCarry = Carry div Base,
    P1_new = Carry - NewCarry * Base,
    multiply_rev1([], Factor, NewCarry, [P1_new | Prod], Base, Cut, Added + 1);
multiply_rev1([], _Factor, _Carry, Prod, _Base, cutoff, 0) ->
    % forget first carry (don't change length of lists)
    {Prod, 0}.

%% @doc A / Divisor (with rounding to nearest integer not larger than the
%%      result in the last component). Divisor must be a positive integer.
-spec divide(A::position_var(), Divisor::pos_integer(), Base::pos_integer()) -> position_var().
divide(A = [_|_], Divisor, Base) when is_integer(Divisor) andalso Divisor > 1 ->
    divide_helper(A, Divisor, 0, Base);
divide(A = [_|_], 1, _Base) -> A;
divide([], _Divisor, _Base) -> [].

-spec divide_helper(Diff::position_var(), Divisor::pos_integer(), Carry::non_neg_integer(),
                   _Base) -> position_var().
divide_helper([D1 | DR], Divisor, Carry, Base) ->
    Diff0 = Carry * Base + D1,
    Diff2 = Diff0 div Divisor,
    NewCarry = Diff0 rem Divisor,
    [Diff2 | divide_helper(DR, Divisor, NewCarry, Base)];
divide_helper([], _Divisor, _Carry, _Base) -> [].

%% @doc Bring two lists to the same length by appending or prepending zeros.
-spec make_same_length(A::position_var(), B::position_var(), AddTo::front | back)
        -> {A::position_var(), B::position_var(),
            ALen::non_neg_integer(), BLen::non_neg_integer(),
            AddedToA::non_neg_integer(), AddedToB::non_neg_integer()}.
make_same_length(A, B, AddTo) ->
    make_same_length(A, B, AddTo, 0).

%% @doc Bring two lists to the same length by appending or prepending at least MinAdd zeros.
-spec make_same_length(A::position_var(), B::position_var(), AddTo::front | back, MinAdd::non_neg_integer())
        -> {A::position_var(), B::position_var(),
            ALen::non_neg_integer(), BLen::non_neg_integer(),
            AddedToA::non_neg_integer(), AddedToB::non_neg_integer()}.
make_same_length(A, B, AddTo, MinAdd) ->
    A_l = erlang:length(A), B_l = erlang:length(B),
    MaxLength = erlang:max(A_l, B_l) + MinAdd,
    AddToALength = MaxLength - A_l, AddToBLength = MaxLength - B_l,
    AddToA = lists:duplicate(AddToALength, 0),
    AddToB = lists:duplicate(AddToBLength, 0),
    case AddTo of
        back  -> {lists:append(A, AddToA), lists:append(B, AddToB),
                  A_l, B_l, AddToALength, AddToBLength};
        front -> {lists:append(AddToA, A), lists:append(AddToB, B),
                  A_l, B_l, AddToALength, AddToBLength}
    end.

%% @doc Remove leading or trailing 0's.
-spec remove_zeros(A::position_var(), RemoveFrom::front | back, MaxToRemove::non_neg_integer() | all)
        -> A::position_var().
remove_zeros(A, back, C) -> lists:reverse(remove_zeros_front(lists:reverse(A), C));
remove_zeros(A, front, C) -> remove_zeros_front(A, C).

-spec remove_zeros_front(A::position_var(), MaxToRemove::non_neg_integer() | all) -> position_var().
remove_zeros_front(A, 0) -> A;
remove_zeros_front([0 | R], all) -> remove_zeros_front(R, all);
remove_zeros_front([0 | R], C) -> remove_zeros_front(R, C - 1);
remove_zeros_front([_|_] = A, _) -> A;
remove_zeros_front([], _) -> [].

%% @doc Converts a (decimal, non-negative) integer to a position var with the
%%      given Base.
-spec from_decimal(X::non_neg_integer(), Base::pos_integer()) -> position_var().
from_decimal(X, Base) ->
    from_decimal_(X, Base).

from_decimal_(X, Base) when X < Base ->
    [X];
from_decimal_(X, Base) ->
    [X rem Base | from_decimal_(X div Base, Base)].

%% @doc Converts a position var with the given Base to a (decimal, non-negative)
%%      integer.
-spec to_decimal(X::position_var(), Base::pos_integer()) -> non_neg_integer().
to_decimal([], _Base) ->
    0;
to_decimal(X, Base) ->
    element(2, lists:foldr(fun(A, {Fac, Result}) ->
                                   {Fac * Base, Result + A * Fac}
                           end, {1, 0}, X)).
