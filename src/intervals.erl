%  @copyright 2007-2014 Zuse Institute Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @author Florian Schintke <schintke@zib.de>
%% @author Nico Kruber <kruber@zib.de>
%% @doc Interval data structure and handling functions.
%%
%% All intervals created by methods of this module are normalized, i.e. simple
%% intervals having unambiguous representations and complex intervals being
%% lists of simple intervals sorted by the order given by interval_sort/2.
%% Such a list contains no adjacent intervals except for those wrapping around,
%% i.e. the first and the last element of the list. This representation is
%% thus unambiguous as well.
%% @end
%% @version $Id$
-module(intervals).
-author('schuett@zib.de').
-vsn('$Id$').

-compile({inline, [in_simple/2, in/2, is_between/5, interval_sort/2, merge_adjacent/2]}).
-compile({inline, [normalize_internal/1, normalize_simple/1, wraps_around/4]}).
-compile({inline, [intersection_simple/2, intersection_simple_element/2]}).
-compile({inline, [union_simple/2, union/2]}).
-compile({inline, [minus_simple/2, minus_simple2/2]}).
-compile({inline, [split2/7]}).

-include("scalaris.hrl").

-export([empty/0, new/1, new/4, all/0, from_elements/1,
         % testing / comparing intervals
         is_empty/1, is_non_empty/1, is_all/1, is_subset/2, is_continuous/1,
         is_adjacent/2, in/2,
         is_left_of/2, is_right_of/2,
         split/2,
         % operations for intervals
         intersection/2, union/1, union/2, minus/2,
         % getters for certain intervals
         get_bounds/1, get_elements/1, get_simple_intervals/1,
         simple_interval_to_interval/1,
         % various
         wraps_around/4,
         %
         % for unit testing only
         is_well_formed/1, tester_create_interval/1,
         is_well_formed_simple/1,
         tester_create_simple_interval/1, tester_create_continuous_interval/4,
         tester_create_non_empty_interval/2,
         split_feeder/2
        ]).

-export_type([interval/0, key/0, left_bracket/0, right_bracket/0,
              continuous_interval/0, non_empty_interval/0]).
% for tester:
-export_type([invalid_interval/0, invalid_simple_interval/0, simple_interval/0,
              simple_interval2/0]).

-type left_bracket() :: '(' | '['.
-type right_bracket() :: ')' | ']'.
-type key() :: ?RT:key() | ?MINUS_INFINITY_TYPE. % ?MINUS_INFINITY_TYPE unnecessary (should be included in ?RT:key()) but needed for fewer dialyzer warnings
-type simple_interval2() :: {left_bracket(), key(), key(), right_bracket()} |
                            {left_bracket(), key(), ?PLUS_INFINITY_TYPE, ')'}.
-type simple_interval() :: {key()} | all | simple_interval2().
-type invalid_simple_interval() :: {key()} | all | simple_interval2().
-opaque interval() :: [simple_interval()].
-opaque invalid_interval() :: [simple_interval()].
-type continuous_interval() :: interval().
-type non_empty_interval() :: interval().

% @type interval() = [simple_interval()].
% [] -> empty interval
% [simple_interval(),...] -> union of the simple intervals
% @type simple_interval() = {key()} | {left_bracket(), key(), key(), right_bracket()} | {left_bracket(), key(), PLUS_INFINITY, ')'} | all.
% {term()} -> one element interval
% {'[', A::term(), B::term(), ']'} -> closed interval [A, B]
% {'(', A::term(), B::term(), ']'} -> half-open interval (A, B], aka ]A, B]
% {'[', A::term(), B::term(), ')'} -> half-open interval [A, B), aka [A, B[
% {'(', A::term(), B::term(), ')'} -> open interval (A, B), aka ]A, B[
% all -> half open interval [?MINUS_INFINITY, ?PLUS_INFINITY)

% Note: the intervals module uses two special symbols (?MINUS_INFINITY
% and ?PLUS_INFINITY) to define the first, i.e. smallest, valid key and the
% first key that is not valid anymore.
% In Scalaris these values are dependent on the routing table
% implementation. Therefore it is not possible e.g. to use intervals
% over integer ranges and strings at the same time!

-dialyzer({no_contracts, split2_feeder/7}).

%% @doc Creates an empty interval.
-spec empty() -> interval().
empty() -> [].

%% @doc Creates an interval covering the whole key space.
-spec all() -> interval().
all() -> normalize_simple(all).

%% @doc Creates an interval covering a single element.
-spec new(key()) -> interval().
new(X) -> normalize_simple({X}).

%% @doc Creates a new interval depending on the given brackets, i.e.:
%%      - closed interval [A, B],
%%      - half-open interval (A, B], aka ]A, B]
%%      - half-open interval [A, B), aka [A, B[
%%      - open interval (A, B), aka ]A, B[
%%      The new interval may wrap around, e.g. if A > B.
%%      If '[A,A]' is given, an interval with the element A is created.
%%      The special cases '(A,A)', '[A,A)', '(A,A]' and
%%      '(?PLUS_INFINITY,?MINUS_INFINITY,)' translate to an empty interval.
%%      '[?MINUS_INFINITY,?PLUS_INFINITY)' translates to 'all'.
-spec new(left_bracket(), key(),
          key() | ?PLUS_INFINITY_TYPE, %% then right_bracket is ')'
          right_bracket()) -> interval().
new(LeftBr, Begin, End, RightBr) when End =/= ?PLUS_INFINITY orelse RightBr =:= ')' ->
    normalize_simple({LeftBr, Begin, End, RightBr}).

%% @doc Creates an interval from a list of elements.
-spec from_elements(Elements::[key()]) -> interval().
from_elements(Elements) ->
    normalize_internal([{E} || E <- Elements]).

%% @doc Checks whether the given interval is empty.
-spec is_empty(interval()) -> boolean().
is_empty([]) -> true;
is_empty(_) ->  false.

%% @doc Checks whether the given interval is non-empty (mainly for tester type checker).
-spec is_non_empty(interval()) -> boolean().
is_non_empty(I) -> not is_empty(I).

%% @doc Checks whether the given interval is covering everything.
-spec is_all(interval()) -> boolean().
is_all([all]) -> true;
is_all(_) ->  false.

%% @doc Creates the intersection of two intervals.
%%      Precondition: is_well_formed(A) andalso is_well_formed(B).
-spec intersection(A::interval(), B::interval()) -> interval().
intersection([all], B)  -> B;
intersection(A, [all])  -> A;
intersection([] = A, _) -> A;
intersection(_, [] = B) -> B;
intersection([{_} = A], B) -> intersection_element(A, B);
intersection(A, [{_} = B]) -> intersection_element(B, A);
intersection(A, A) -> A;
intersection(A, B) ->
    normalize_internal([IS || IA <- A, IB <- B,
                              [] =/= (IS = intersection_simple(IA, IB))]).

%% @doc Intersection between an element and an interval.
-spec intersection_element(A::{key()}, B::interval()) -> interval().
intersection_element({X} = A, B) ->
    case in(X, B) of
        false -> empty();
        true  -> [A]
    end.

%% @doc Creates the intersection of two simple intervals or empty lists.
-spec intersection_simple(A::simple_interval(), B::simple_interval()) -> simple_interval() | [].
intersection_simple(A, A) -> A;
intersection_simple({A0Br, A0, A1, A1Br},
                    {B0Br, B0, B1, B1Br}) ->
    B0_in_A = is_between(A0Br, A0, B0, A1, A1Br),
    B1_in_A = is_between(A0Br, A0, B1, A1, A1Br),
    A0_in_B = is_between(B0Br, B0, A0, B1, B1Br),
    A1_in_B = is_between(B0Br, B0, A1, B1, B1Br),
    if % are the intervals overlapping?
        B0_in_A orelse B1_in_A orelse A0_in_B orelse A1_in_B ->
            {NewLeft, NewLeftBr} =
                case A0 =:= B0 of
                    true when A0Br =:= '(' -> {A0, '('};
                    true -> {A0, B0Br};
                    false ->
                        case util:max(A0, B0) =:= A0 of
                            true  -> {A0, A0Br};
                            false -> {B0, B0Br}
                        end
                end,
            {NewRight, NewRightBr} =
                case A1 =:= B1 of
                    true when A1Br =:= ')' -> {A1, ')'};
                    true -> {A1, B1Br};
                    false ->
                        case util:min(A1, B1) =:= A1 of
                            true  -> {A1, A1Br};
                            false -> {B1, B1Br}
                        end
                end,
            % note: if left and right are the same in a closed interval, this
            % means 'single element' here, in (half) open intervals, the result
            % is an empty interval
            case NewLeft =:= NewRight of
                true when (NewLeftBr =:= '[') andalso (NewRightBr =:= ']') ->
                    {NewLeft};
                true -> [];
                false -> {NewLeftBr, NewLeft, NewRight, NewRightBr}
            end;
        true -> []
    end;
intersection_simple(all, B)    -> B;
intersection_simple(A, all)    -> A;
intersection_simple({_} = A, B) -> intersection_simple_element(A, B);
intersection_simple(A, {_} = B) -> intersection_simple_element(B, A).

%% @doc Intersection between an element and a simple interval.
-spec intersection_simple_element(A::{key()}, B::simple_interval()) -> {key()} | [].
intersection_simple_element({X} = A, B) ->
    case in_simple(X, B) of
        false -> [];
        true  -> A
    end.

%% @doc Returns true if A is a subset of B, i.e. the intersection of both is A.
%%      Precondition: is_well_formed(A) andalso is_well_formed(B).
-spec(is_subset(A::interval(), B::interval()) -> boolean()).
is_subset(A, B) -> A =:= intersection(A, B).

%% @doc X \in I. Precondition: is_well_formed_simple(I).
-spec in_simple(X::key(), I::simple_interval()) -> boolean().
% inline is_between/5 for performance (+15-20%):
in_simple(X, {LBr, Begin, End, RBr}) ->
    (X > Begin andalso End > X) orelse
        (LBr =:= '[' andalso X =:= Begin) orelse
        (RBr =:= ']' andalso X =:= End);
in_simple(_, all) -> true;
in_simple(X, {E}) -> X =:= E.

%% @doc X \in I. Precondition: is_well_formed(I).
-spec in(X::key(), I::interval()) -> boolean().
in(X, [I | Rest]) -> in_simple(X, I) orelse in(X, Rest);
in(_, [])         -> false.

%% @doc Brings a list of intervals into normal form, i.e. sort, eliminate empty
%%      intervals from the list, convert intervals that wrap around into a set
%%      of intervals not wrapping around, merge adjacent intervals.
%%      Note: Outside this module, use only for testing - all intervals
%%      generated by this module are normalized!
%%      Note: This function also corrects values which are to small, i.e. less
%%      than ?MINUS_INFINITY, or too large, i.e. greater than or equal to
%%      ?PLUS_INFINITY which is only needed if types are created based on
%%      (potentially) inprecise type defs, e.g. in the unit tests. For internal
%%      use prefer normalize_internal/1 which does not fix values and is thus
%%      faster.
-spec tester_create_interval(invalid_interval() | interval()) -> interval().
tester_create_interval(List) ->
    List1 = [normalize_simple_bounds(I)
            || I <- List,
               % filter out {X >= ?PLUS_INFINITY} which is invalid:
               not (is_tuple(I) andalso element(1, I) =:= element andalso element(2, I) >= ?PLUS_INFINITY)],
    normalize_internal(List1).

-spec tester_create_non_empty_interval([simple_interval(),...], FallbackElem::key())
        -> non_empty_interval().
tester_create_non_empty_interval(List, FallbackElem) ->
    I = tester_create_interval(List),
    case is_empty(I) of
        true  -> new(FallbackElem);
        false -> I
    end.

-spec tester_create_continuous_interval(left_bracket(), key(), key() | ?PLUS_INFINITY_TYPE, right_bracket()) -> continuous_interval().
tester_create_continuous_interval(_LBr, Key, Key, _RBr) ->
    new('[', Key, Key, ']');
tester_create_continuous_interval(LBr, LKey, ?PLUS_INFINITY, _RBr) ->
    new(LBr, LKey, ?PLUS_INFINITY, ')');
tester_create_continuous_interval(LBr, LKey, RKey, RBr) ->
    new(LBr, LKey, RKey, RBr).

%% @doc Brings a simple interval into normal form, i.e. if it is a real
%%      interval, its keys must be in order.
%%      Note: Outside this module, use only for testing - all intervals
%%      generated by this module are normalized!
%%      Note: This function also corrects values which are to small, i.e. less
%%      than ?MINUS_INFINITY, or too large, i.e. greater than or equal to
%%      ?PLUS_INFINITY which is only needed if types are created based on
%%      (potentially) inprecise type defs, e.g. in the unit tests.
-spec tester_create_simple_interval(invalid_simple_interval()) -> simple_interval().
tester_create_simple_interval(I0) ->
    I1 = normalize_simple_bounds(I0),
    case normalize_simple(I1) of
        [] -> all;
        [H|_] -> H
    end.

-spec normalize_internal([simple_interval()]) -> interval(). % for dialyzer
normalize_internal(List) ->
    NormalizedList1 = lists:flatmap(fun normalize_simple/1, List),
    merge_adjacent(lists:usort(fun interval_sort/2, NormalizedList1), []).

%% @doc Normalizes simple intervals (see normalize_internal/1).
-spec normalize_simple(invalid_simple_interval()) -> [simple_interval()].
normalize_simple({'(', X, X, _RightBr}) -> [];
normalize_simple({_LeftBr, X, X, ')'}) -> [];
normalize_simple({'[', X, X, ']'}) -> normalize_simple({X});
normalize_simple({'[', ?MINUS_INFINITY, ?PLUS_INFINITY, ')'}) ->
    [all];
normalize_simple({LeftBr, X, ?MINUS_INFINITY, ')'}) ->
    [{LeftBr, X, ?PLUS_INFINITY, ')'}];
normalize_simple({LeftBr, X, ?MINUS_INFINITY, ']'}) ->
    [{?MINUS_INFINITY}, {LeftBr, X, ?PLUS_INFINITY, ')'}];
normalize_simple({LeftBr, Begin, End, RightBr} = I) ->
    case wraps_around(LeftBr, Begin, End, RightBr) of
        true ->  [{'[', ?MINUS_INFINITY, End, RightBr},
                  {LeftBr, Begin, ?PLUS_INFINITY, ')'}];
        false -> [I]
    end;
normalize_simple({_A} = I) -> [I];
normalize_simple(all) -> [all].

-spec normalize_simple_bounds({left_bracket(), key(), key() | ?PLUS_INFINITY_TYPE, right_bracket()} | {key()} | all)
        -> invalid_simple_interval().
normalize_simple_bounds({_LeftBr, Begin, End, RightBr})
  when Begin < ?MINUS_INFINITY orelse Begin >= ?PLUS_INFINITY ->
    normalize_simple_bounds({'[', ?MINUS_INFINITY, End, RightBr});
normalize_simple_bounds({_LeftBr, _Begin, ?PLUS_INFINITY, ')'} = I) ->
    I;
normalize_simple_bounds({LeftBr, Begin, End, _RightBr})
  when End < ?MINUS_INFINITY orelse End >= ?PLUS_INFINITY ->
    {LeftBr, Begin, ?PLUS_INFINITY, ')'};
normalize_simple_bounds({X}) when X < ?MINUS_INFINITY ->
    {?MINUS_INFINITY};
normalize_simple_bounds(I) ->
    I.
  
%% @doc Checks whether the given interval is normalized, i.e. not wrapping
%%      around and no 'interval' with equal borders (see normalize_internal/1).
%%      Use only for testing - all intervals generated by this module are
%%      well-formed, i.e. normalized!
-spec is_well_formed(invalid_interval() | interval()) ->  boolean().
is_well_formed([]) -> true;
is_well_formed([_|_] = List) ->
    lists:all(fun is_well_formed_simple/1, List) andalso
        % sorted and unique:
        List =:= lists:usort(fun interval_sort/2, List) andalso
        % pairwise non-overlapping:
        (lists:flatten([intersection_simple(A, B) || A <- List, B <- List,
                                                     A =/= B]) =:= []).

%% @doc Checks whether a given simple interval is normalized. Complex intervals
%%      or any other value are considered 'not normalized'.
-spec is_well_formed_simple(simple_interval()) ->  boolean().
is_well_formed_simple({_LeftBr, _X, ?MINUS_INFINITY, ')'}) ->
    false;
is_well_formed_simple({_LeftBr, _X, ?PLUS_INFINITY, ')'}) ->
    true;
is_well_formed_simple({_LeftBr, X, Y, _RightBr}) ->
    X < Y;
is_well_formed_simple({_X}) -> true;
is_well_formed_simple(all) -> true;
is_well_formed_simple(_) -> false.

%% @doc Specifies an order over simple intervals (returns true if I1 &lt;= I2).
%%      The order is based on the intervals' first components
%%      and in case of elements based on their value. 'all' is the first and
%%      elements are sorted before intervals with the same values, if two
%%      intervals' first components compare equal the one with '[' is smaller
%%      (to ease merge_adjacent/2), otherwise normal &lt;= from erlang is used.
-spec interval_sort(I1::simple_interval(), I2::simple_interval()) -> boolean().
interval_sort({A0Br, A0, _A1, _A1Br} = A, {B0Br, B0, _B1, _B1Br} = B) ->
    % beware of not accidentally making two intervals equal, which is defined
    % as A==B <=> interval_sort(A, B) andalso interval_sort(B, A)
    B0 > A0 orelse
        (A0 =:= B0 andalso A0Br =:= '[' andalso B0Br =:= '(') orelse
        (A0 =:= B0 andalso (not (A0Br =:= '(' andalso B0Br =:= '[')) andalso A =< B);
interval_sort({A}, {_B0Br, B0, _B1, _B1Br}) ->
    B0 >= A;
interval_sort({_A0Br, A0, _A1, _A1Br}, {B}) ->
    B > A0;
interval_sort({A}, {B}) ->
    B >= A;
interval_sort(all, _Interval2) ->
    true;
interval_sort(_Interval1, _Interval2) ->
    false.

%% @doc Merges adjacent intervals in a sorted list of simple intervals using
%%      union_simple/2.
-spec merge_adjacent([simple_interval()], [simple_interval()]) -> [simple_interval()].
merge_adjacent([all | _T], _Results) ->
    [all];
merge_adjacent([H | T], []) ->
    merge_adjacent(T, [H]);
merge_adjacent([HI | TI], [HR | TR]) ->
    merge_adjacent(TI, union_simple(HI, HR) ++ TR);
merge_adjacent([], Results) ->
    lists:reverse(Results).

%% @doc Creates the union of two intervals.
-spec union(A::interval(), B::interval()) -> interval().
union([all] = A, _B) -> A;
union(_A, [all] = B) -> B;
union([], B)         -> B;
union(A, [])         -> A;
union(A, A)          -> A;
union(A, B)          -> normalize_internal(lists:append(A, B)).

%% @doc Creates the union of a list of intervals.
-spec union([interval()]) -> interval().
union(List) -> normalize_internal(lists:append(List)).

%% @doc Creates the union of two simple intervals or empty lists.
-spec union_simple(A::simple_interval(), B::simple_interval()) -> [simple_interval()].
union_simple(all, _B) -> [all];
union_simple(_A, all) -> [all];
union_simple({A0Br, A0, A1, ']'}, {_B0Br, A1, B1, B1Br}) ->
    new(A0Br, A0, B1, B1Br);
union_simple({A0Br, A0, A1, _A1Br}, {'[', A1, B1, B1Br}) ->
    new(A0Br, A0, B1, B1Br);
union_simple({'[', A0, A1, A1Br}, {B0Br, B0, A0, _B1Br}) ->
    new(B0Br, B0, A1, A1Br);
union_simple({_A0Br, A0, A1, A1Br}, {B0Br, B0, A0, ']'}) ->
    new(B0Br, B0, A1, A1Br);
union_simple({A0Br, A0, A1, A1Br} = A, {B0Br, B0, B1, B1Br} = B) ->
    B0_in_A = is_between(A0Br, A0, B0, A1, A1Br),
    B1_in_A = is_between(A0Br, A0, B1, A1, A1Br),
    A0_in_B = is_between(B0Br, B0, A0, B1, B1Br),
    A1_in_B = is_between(B0Br, B0, A1, B1, B1Br),
    if
        B0_in_A orelse B1_in_A orelse A0_in_B orelse A1_in_B ->
            {NewLeft, NewLeftBr} =
                case A0 =:= B0 of
                    true when A0Br =:= '[' -> {A0, '['};
                    true -> {A0, B0Br};
                    false ->
                        case util:min(A0, B0) =:= A0 of
                            true  -> {A0, A0Br};
                            false -> {B0, B0Br}
                        end
                end,
            {NewRight, NewRightBr} =
                case A1 =:= B1 of
                    true when A1Br =:= ']' -> {A1, ']'};
                    true -> {A1, B1Br};
                    false ->
                        case util:max(A1, B1) =:= A1 of
                            true  -> {A1, A1Br};
                            false -> {B1, B1Br}
                        end
                end,
            % note: if left and right are the same in a closed interval, this
            % means 'single element' here, in (half) open intervals, the result
            % is en empty interval
            % (using new/4 would create 'all' instead)
            % however, the union of two 'real' intervals should never be an element!
            % @todo: add exception here?
            case NewLeft =:= NewRight of
                true when (NewLeftBr =:= '[') andalso (NewRightBr =:= ']') ->
                    new(NewLeft);
                true -> empty();
                false -> new(NewLeftBr, NewLeft, NewRight, NewRightBr)
            end;
        true -> [A, B]
    end;
union_simple({B0}, {_B0Br, B0, B1, B1Br}) ->
    new('[', B0, B1, B1Br);
union_simple({B1}, {B0Br, B0, B1, _B1Br}) ->
    new(B0Br, B0, B1, ']');
union_simple({A0Br, A0, A1, _A1Br}, {A1}) ->
    new(A0Br, A0, A1, ']');
union_simple({_A0Br, A0, A1, A1Br}, {A0}) ->
    new('[', A0, A1, A1Br);
union_simple({A_Value} = A, B) ->
    % note: always place the first element (A) before the second
    % element (B) in a union for merge_adjacent/2
    case in_simple(A_Value, B) of
        true  -> [B];
        false -> [A, B]
    end;
union_simple(A, {B_Value} = B) ->
    % note: always place the first element (A) before the second
    % element (B) in a union for merge_adjacent/2
    case in_simple(B_Value, A) of
        true  -> [A];
        false -> [A, B]
    end.

%% @doc Checks whether the given interval is a continuous interval, i.e. simple
%%      intervals are always continuous, complex intervals are continuous if
%%      they contain 2 simple intervals which are adjacent and wrap around.
%%      Note: empty intervals are not continuous!
-spec is_continuous(interval()) -> boolean().
is_continuous([{_LBr, _L, _R, _RBr}]) -> true;
% complex intervals have adjacent intervals merged except for those wrapping around
% -> if it contains only two simple intervals which are adjacent, it is continuous!
is_continuous([{'[', ?MINUS_INFINITY, _B1, _B1Br},
               {_A0Br, _A0, ?PLUS_INFINITY, ')'}]) -> true;
is_continuous([{?MINUS_INFINITY},
               {_A0Br, _A0, ?PLUS_INFINITY, ')'}]) -> true;
is_continuous([all]) -> true;
is_continuous([{_Key}]) -> true;
is_continuous(_) -> false.

%% @doc Gets the outer bounds of a given non-empty interval including
%%      brackets. Note that here
%%      'all' transfers to {'[', ?MINUS_INFINITY, ?PLUS_INFINITY, ')'},
%%      {Key} to {'[', Key, Key, ']'} and
%%      [{'[',?MINUS_INFINITY,Key,')'},{'(',Key,?PLUS_INFINITY,')'}] to {'(', Key, Key, ')'}.
%%      Other continuous normalized intervals that wrap around (as well as the
%%      first two) are returned the same way they can be constructed with new/4.
%%      Note: the bounds of non-continuous intervals are not optimal!
%%      Note: this method will only work on non-empty intervals
%%      and will throw an exception otherwise!
-spec get_bounds(interval()) -> simple_interval2().
get_bounds([{LBr, L, R, RBr}]) -> {LBr, L, R, RBr};
get_bounds([{'[', ?MINUS_INFINITY, B1, B1Br},
            {A0Br, A0, ?PLUS_INFINITY, ')'}]) -> {A0Br, A0, B1, B1Br};
get_bounds([{?MINUS_INFINITY},
            {A0Br, A0, ?PLUS_INFINITY, ')'}]) -> {A0Br, A0, ?MINUS_INFINITY, ']'};
get_bounds([all]) -> {'[', ?MINUS_INFINITY, ?PLUS_INFINITY, ')'};
get_bounds([{Key}]) -> {'[', Key, Key, ']'};
get_bounds([]) -> erlang:throw('no bounds in empty interval');
% fast creation of bounds by using the first and last value of the interval:
get_bounds([H | T]) ->
    case H of
        {X1} -> LBr = '[', L = X1, ok;
        {LBr, L, _, _} -> ok
    end,
    case lists:last(T) of
        {X2} -> RBr = ']', R = X2, ok;
        {_, _, R, RBr} -> ok
    end,
    {LBr, L, R, RBr}.

%% @doc Gets all elements inside the interval and returnes a "rest"-interval,
%%      i.e. the interval without the elements.
-spec get_elements(interval()) -> {Elements::[key()], RestInt::interval()}.
get_elements(I) -> get_elements_list(I, [], []).

%% @doc Helper for get_elements/1.
-spec get_elements_list(Interval::interval(), Elements::[key()], RestInt::[simple_interval()]) -> {Elements::[key()], RestInt::[simple_interval()]}.
get_elements_list([{Key} | T], Elements, RestInt) ->
    get_elements_list(T, [Key | Elements], RestInt);
get_elements_list([I | T], Elements, RestInt) ->
    get_elements_list(T, Elements, [I | RestInt]);
get_elements_list([], Elements, RestInt) ->
    {lists:reverse(Elements), lists:reverse(RestInt)}.

%% @doc Checks whether two intervals are adjacent, i.e. the intervals are both
%%      continuous, their union is continuous and their intersection is empty,
%%      e.g. ('(A,B]', '(B,C)') with A=/=B and B=/=C.
%%      Note: intervals like (A,B), (B,C) are not considered adjacent because
%%      the element b would be between these two.
-spec is_adjacent(interval(), interval()) -> boolean().
is_adjacent(A, B) ->
    is_continuous(A) andalso is_continuous(B) andalso
        is_empty(intersection(A, B)) andalso is_continuous(union(A, B)).

%% @doc Subtracts the second from the first simple interval.
-spec minus_simple(simple_interval(), simple_interval()) -> interval().
minus_simple(A, A)   -> empty();
minus_simple(A = {_A0Br, _A0, _A1, _A1Br},
             B = {_B0Br, _B0, _B1, _B1Br}) ->
    B_ = intersection_simple(A, B),
    case B_ of
        A                      -> empty();
        {_, _, _, _} -> minus_simple2(A, B_);
        []                     -> [A];
        _                      -> minus_simple(A, B_)
    end;
minus_simple(A = {A0}, B = {_B0Br, _B0, _B1, _B1Br}) ->
    case in_simple(A0, B) of
        true -> empty();
        _    -> [A]
    end;
minus_simple({'[', A0, A1, A1Br}, {A0}) ->
    new('(', A0, A1, A1Br);
minus_simple({A0Br, A0, A1, ']'}, {A1}) ->
    new(A0Br, A0, A1, ')');
minus_simple(A = {A0Br, A0, A1, A1Br}, {B0}) ->
    case in_simple(B0, A) of
        false -> [A];
        true  -> union(new(A0Br, A0, B0, ')'), new('(', B0, A1, A1Br))
    end;
minus_simple(A = {_}, {_}) -> [A];
minus_simple(_, all) -> empty();
minus_simple(all, B = {_B0Br, _B0, _B1, _B1Br}) ->
    % hack: use [?MINUS_INFINITY, ?PLUS_INFINITY) as 'all' and [B0, B0] as element - minus_simple2 can handle this though
    minus_simple2({'[', ?MINUS_INFINITY, ?PLUS_INFINITY, ')'}, B);
minus_simple(all, {B0}) ->
    % hack: use [?MINUS_INFINITY, ?PLUS_INFINITY) as 'all' and [B0, B0] as element - minus_simple2 can handle this though
    minus_simple2({'[', ?MINUS_INFINITY, ?PLUS_INFINITY, ')'},
                  {'[', B0, B0, ']'}).

%% @doc Subtracts the second from the first simple interval (no elements, no
%%      'all', no empty interval). The second interval must be a subset of the
%%      first interval!
-spec minus_simple2(simple_interval2(), simple_interval2()) -> interval().
minus_simple2({A0Br, A0, A1, A1Br}, {B0Br, B0, B1, B1Br}) ->
    First = case B0Br of
                '(' when B0 =:= A0 andalso A0Br =:= '[' ->
                    new(A0);
                '(' when B0 =:= A0 -> empty();
                '('                -> new(A0Br, A0, B0, ']');
                '[' when B0 =:= A0 -> empty();
                '['                -> new(A0Br, A0, B0, ')')
               end,
    Second = case B1Br of
                 ']' when B1 =:= A1 -> empty();
                 ']'                -> new('(', B1, A1, A1Br);
                 ')' when B1 =:= A1 andalso A1Br =:= ']' ->
                     new(A1);
                 ')' when B1 =:= A1 -> empty();
                 ')'                -> new('[', B1, A1, A1Br)
               end,
    union(First, Second).

%% @doc Subtracts the second from the first interval.
-spec minus(interval(), interval()) -> interval().
minus(_A, [all]) -> empty();
minus(A, [])     -> A;
minus(A, A)      -> empty();
minus(A, [HB | TB]) ->
    % from every simple interval in A, remove all simple intervals in B
    % note: we cannot use minus_simple in foldl since the result may be a list again
    normalize_internal(
      lists:flatmap(fun(IA) -> minus(minus_simple(IA, HB), TB) end, A)).

%% @doc Determines whether an interval with the given borders wraps around,
%%      i.e. the interval would cover the (non-existing) gap between
%%      ?PLUS_INFINITY and ?MINUS_INFINITY.
-spec wraps_around(left_bracket(), key(), key() | ?PLUS_INFINITY_TYPE, right_bracket()) -> boolean().
wraps_around(_LeftBr, X, X, _RightBr) ->
    false;
wraps_around(_LeftBr, ?MINUS_INFINITY, _, _RightBr) ->
    false;
wraps_around(_LeftBr, _, ?PLUS_INFINITY, _RightBr) ->
    false;
wraps_around(_LeftBr, _, ?MINUS_INFINITY, ')') ->
    % same as [A, ?PLUS_INFINITY) or (A, ?PLUS_INFINITY)
    false;
wraps_around(_LeftBr, _, ?MINUS_INFINITY, _RightBr) ->
    true;
wraps_around('(', ?PLUS_INFINITY, _, _RightBr) ->
    % same as [?MINUS_INFINITY, A] or [?MINUS_INFINITY, A)
    false;
wraps_around(_LeftBr, ?PLUS_INFINITY, _, _RightBr) ->
    true;
wraps_around(_LeftBr, First, Last, _RightBr) when First > Last ->
    true;
wraps_around(_LeftBr, _First, _Last, _RightBr) ->
    false.

% @doc Begin &lt;= X &lt;= End
% precondition Begin &lt;= End
-spec is_between(BeginBr::left_bracket(), Begin::key(), X::key(), End::key(), EndBr::right_bracket()) -> boolean().
is_between(LBr, Begin, X, End, RBr) ->
    (X > Begin andalso End > X) orelse
        (LBr =:= '[' andalso X =:= Begin) orelse
        (RBr =:= ']' andalso X =:= End).

%% @doc X and Y are adjacent and Y follows X
-spec is_left_of(interval(), interval()) -> boolean().
is_left_of(X, Y) ->
    case is_adjacent(X, Y) of
        true ->
            {_, _A,  B, _} = get_bounds(X),
            {_,  C, _D, _} = get_bounds(Y),
            % in(B, X) =/= in(B, Y) implied by is_adjacent
            (B =:= C andalso (in(B, X) orelse in(B, Y)))
                orelse
            (B =:= ?PLUS_INFINITY andalso C =:= ?MINUS_INFINITY andalso
             in(?MINUS_INFINITY, Y));
        false ->
            false
    end.

%% @doc X and Y are adjacent and X follows Y
-spec is_right_of(interval(), interval()) -> boolean().
is_right_of(X, Y) ->
    is_left_of(Y, X).

-spec split_feeder(continuous_interval(), 1..255)
        -> {continuous_interval(), pos_integer()}.
split_feeder(I, Parts) ->
    {I, Parts}.

%% @doc Splits a continuous interval in X roughly equally-sized subintervals,
%%      the result of non-continuous intervals is undefined.
%%      Returns: List of adjacent intervals
-spec split(continuous_interval(), Parts::pos_integer()) -> [continuous_interval()].
split(I, 1) -> [I];
split(I, Parts) ->
    {LBr, LKey, RKey, RBr} = get_bounds(I),
    % keep brackets inside the split interval if they are different
    % (i.e. one closed, the other open), otherwise exclude split keys
    % from each first interval at each split
    {InnerLBr, InnerRBr} =
        if (LBr =:= '[' andalso RBr =:= ']') orelse
               (LBr =:= '(' andalso RBr =:= ')') -> {'[', ')'};
           true -> {LBr, RBr}
        end,
    case LKey of
        RKey -> [I];
        _ ->
            SplitKeys = ?RT:get_split_keys(LKey, RKey, Parts),
            split2(LBr, LKey, RKey, RBr, SplitKeys, InnerLBr, InnerRBr)
    end.

-compile({nowarn_unused_function, {split2_feeder, 7}}).
-spec split2_feeder
        (left_bracket(), key(), key(), right_bracket(), SplitKeys::[?RT:key()],
         InnerLBr::left_bracket(), InnerRBr::right_bracket())
        -> {left_bracket(), key(), key(), right_bracket(), SplitKeys::[?RT:key()],
            InnerLBr::left_bracket(), InnerRBr::right_bracket()};
        (left_bracket(), key(), ?PLUS_INFINITY_TYPE, ')', SplitKeys::[?RT:key()],
         InnerLBr::left_bracket(), InnerRBr::right_bracket())
        -> {left_bracket(), key(), ?PLUS_INFINITY_TYPE, ')', SplitKeys::[?RT:key()],
            InnerLBr::left_bracket(), InnerRBr::right_bracket()}.
split2_feeder(LBr, LKey, RKey, RBr, SplitKeys, InnerLBr, InnerRBr) ->
    {LBr, LKey, RKey, RBr,
     util:shuffle(lists:usort([X || X <- SplitKeys, X =/= LKey])),
     InnerLBr, InnerRBr}.

-spec split2(left_bracket(), key(), key() | ?PLUS_INFINITY_TYPE,  %% then right_bracket is ')'
             right_bracket(), SplitKeys::[?RT:key()], InnerLBr::left_bracket(),
             InnerRBr::right_bracket()) -> [interval()].
split2(LBr, LKey, RKey, RBr, [], _InnerLBr, _InnerRBr) ->
    [new(LBr, LKey, RKey, RBr)];
split2(LBr, LKey, RKey, RBr, [SplitKey | SplitKeys], InnerLBr, InnerRBr) ->
    ?DBG_ASSERT(LKey =/= SplitKey),
    [new(LBr, LKey, SplitKey, InnerRBr) |
         split2(InnerLBr, SplitKey, RKey, RBr, SplitKeys, InnerLBr, InnerRBr)].

%% @doc returns a list of simple intervals that make up Interval
-spec get_simple_intervals(Interval::interval()) -> [simple_interval()].
get_simple_intervals(Interval) ->
    Interval.

%% @doc Converts a simple interval (e.g. from get_simple_intervals/1) to a
%%      valid interval().
-spec simple_interval_to_interval(SInterval::simple_interval()) -> interval().
simple_interval_to_interval(SInterval) ->
    normalize_simple(SInterval).
