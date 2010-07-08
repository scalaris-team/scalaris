%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%             2008 onScale solutions GmbH
%  @end
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
%%% File    intervals.erl
%%% @author Thorsten Schuett, Florian Schintke <schuett@zib.de, schintke@onscale.de>
%%% @doc    Interval data structure and handling functions.
%%% 
%%% All intervals created by methods of this module are normalized, i.e. simple
%%% intervals having unambiguous representations and complex intervals being
%%% lists of simple intervals sorted by the order given by interval_sort/2.
%%% Such a list contains no adjacent intervals except for those wrapping around,
%%% i.e. the first and the last element of the list. This representation is
%%% thus unambiguous as well.
%%% @end
%%% Created : 3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(intervals).

-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([empty/0, new/1, new/2, new/4, all/0,
         mk_from_node_ids/2, mk_from_nodes/2,
         is_empty/1,
         is_subset/2,
         is_continuous/1,
         is_adjacent/2,
         in/2,
         intersection/2,
         union/2,
         minus/2,
         % for unit testing only
         is_well_formed/1,
         normalize/1
        ]).

-ifdef(with_export_type_support).
-export_type([interval/0]).
-endif.

-type(left_bracket() :: '(' | '[').
-type(right_bracket() :: ')' | ']').
-type(key() :: ?RT:key() | minus_infinity | plus_infinity).
-type(simple_interval() :: {element, key()} | {interval, left_bracket(), key(), key(), right_bracket()} | all).
-type(interval() :: simple_interval() | [simple_interval()]).

% @type interval() = [] | term() | {interval,term(),term()} | all | list(interval()).
% [] -> empty interval
% {element, term()} -> one element interval
% {interval, '[', A::term(), B::term(), ']'} -> closed interval [A, B]
% {interval, '(', A::term(), B::term(), ']'} -> half-open interval (A, B], aka ]A, B]
% {interval, '[', A::term(), B::term(), ')'} -> half-open interval [A, B), aka [A, B[
% {interval, '(', A::term(), B::term(), ')'} -> open interval (A, B), aka ]A, B[
% all -> minus_infinity to plus_infinity
% list(interval()) -> [i1, i2, i3,...]

%% @doc Creates an empty interval.
-spec empty() -> [].
empty() ->
    [].

%% @doc Creates an interval covering the whole key space.
-spec all() -> all.
all() ->
    all.

%% @doc Creates an interval covering a single element.
-spec new(key()) -> {element, key()}.
new(X) ->
    {element, X}.

%% @doc Creates a new closed interval, i.e. "[A, B]". The new interval may wrap
%%      around, i.e. A > B. If A=:=B, an interval covering the whole space is
%%      created.
-spec new(A::key(), B::key()) -> interval().
new(A, B) ->
    new('[', A, B, ']').

%% @doc Creates a new interval depending on the given brackets, i.e.:
%%      - closed interval [A, B],
%%      - half-open interval (A, B], aka ]A, B]
%%      - half-open interval [A, B), aka [A, B[
%%      - open interval (A, B), aka ]A, B[
%%      The new interval may wrap around, i.e. A > B. If A=:=B and not '(A,A)'
%%      is given, an interval covering the whole space is created.
%%      Beware: some combinations create empty intervals, e.g. '(A,A)'.
-spec new(LeftBr::left_bracket(), A::key(), B::key(), RightBr::right_bracket()) -> interval().
new(LeftBr, Begin, End, RightBr) ->
    normalize_simple({interval, LeftBr, Begin, End, RightBr}).

%% @doc Creates an interval that covers all keys a node with MyKey is
%%      responsible for if his predecessor has PredKey, i.e. (PredKey, MyKey]
%%      (provided for convenience).
-spec mk_from_node_ids(PredKey::?RT:key(), MyKey::?RT:key()) -> interval().
mk_from_node_ids(PredKey, MyKey) ->
    new('(', PredKey, MyKey, ']').

%% @doc Creates an interval that covers all keys a node is responsible for given
%%      his predecessor, i.e. (node:id(PredKey), node:id(MyKey)]
%%      (provided for convenience).
-spec mk_from_nodes(Pred::node:node_type(), Node::node:node_type()) -> interval().
mk_from_nodes(Pred, Node) ->
    mk_from_node_ids(node:id(Pred), node:id(Node)).

%% @doc Checks whether the given interval is empty.
-spec is_empty(interval()) -> boolean().
is_empty([]) ->
    true;
is_empty(_) ->
    false.

%% @doc Creates the intersection of two intervals.
%%      Precondition: is_well_formed(A) andalso is_well_formed(B).
-spec intersection(A::interval(), B::interval()) -> interval().
intersection(A, A) ->
    A;
intersection(all, B) ->
    B;
intersection([], _) ->
    empty();
intersection(_, []) ->
    empty();
intersection(A, all) ->
    A;
intersection({element, X} = A, B) ->
    case in(X, B) of
        true  -> A;
        false -> empty()
    end;
intersection(A, {element, _} = B) ->
    intersection(B, A);
intersection(A, B) when is_list(A) orelse is_list(B) ->
    A_ = to_list(A),
    B_ = to_list(B),
    normalize([intersection_simple(IA, IB) || IA <- A_, IB <- B_, intersection_simple(IA, IB) =/= []]);
intersection(A, B) ->
    normalize(intersection_simple(A, B)).

%% @doc Creates the intersection of two simple intervals or empty lists.
-spec intersection_simple(A::simple_interval() | [], B::simple_interval() | []) -> simple_interval() | [].
intersection_simple(A, A) ->
    A;
intersection_simple(all, B) ->
    B;
intersection_simple([], _) ->
    empty();
intersection_simple(_, []) ->
    empty();
intersection_simple(A, all) ->
    A;
intersection_simple({element, A_Value} = A, B) ->
    case in(A_Value, B) of
        true  -> A;
        false -> empty()
    end;
intersection_simple(A, {element, _} = B) ->
    intersection_simple(B, A);
intersection_simple({interval, A0Br, A0, A1, A1Br}, {interval, B0Br, B0, B1, B1Br}) ->
    B0_in_A = is_between(A0Br, A0, B0, A1, A1Br),
    B1_in_A = is_between(A0Br, A0, B1, A1, A1Br),
    A0_in_B = is_between(B0Br, B0, A0, B1, B1Br),
    A1_in_B = is_between(B0Br, B0, A1, B1, B1Br),
    if
        B0_in_A orelse B1_in_A orelse A0_in_B orelse A1_in_B ->
            {NewLeft, NewLeftBr} =
                case A0 =:= B0 of
                    true when A0Br =:= '(' ->
                        {A0, '('};
                    true ->
                        {A0, B0Br};
                    false ->
                        case util:max(A0, B0) =:= A0 of
                            true  -> {A0, A0Br};
                            false -> {B0, B0Br}
                        end
                end,
            {NewRight, NewRightBr} =
                case A1 =:= B1 of
                    true when A1Br =:= ')' ->
                        {A1, ')'};
                    true ->
                        {A1, B1Br};
                    false ->
                        case util:min(A1, B1) =:= A1 of
                            true  -> {A1, A1Br};
                            false -> {B1, B1Br}
                        end
                end,
            % note: if left and right are the same in a closed interval, this
            % means 'single element' here, in (half) open intervals, the result
            % is en empty interval
            % (using new/4 would create 'all' instead)
            case NewLeft =:= NewRight of
                true when (NewLeftBr =:= '[') andalso (NewRightBr =:= ']') ->
                    new(NewLeft);
                true ->
                    empty();
                false ->
                    new(NewLeftBr, NewLeft, NewRight, NewRightBr)
            end;
        true ->
            empty()
    end.

%% @doc Returns true if A is a subset of B, i.e. the intersection of both
%%      equals A.
%%      Precondition: is_well_formed(A) andalso is_well_formed(B).
-spec(is_subset(A::interval(), B::interval()) -> boolean()).
is_subset(A, B) ->
    A =:= intersection(A, B).

%% @doc X \in I. Precondition: is_well_formed(I).
-spec in/2 :: (X::key(), I::interval()) -> boolean().
in(X, {interval, FirstBr, First, Last, LastBr}) ->
    is_between(FirstBr, First, X, Last, LastBr);
in(X, [I | Rest]) ->
    in(X, I) orelse in(X, Rest);
in(_, []) ->
    false;
in(_, all) ->
    true;
in(X, {element, X}) ->
    true;
in(_, {element, _}) ->
    false.

%% @doc Bring list of intervals to normal form, i.e. sort, eliminate empty
%%      intervals, convert intervals with a single element to elements, convert
%%      intervals that wrap around into a set of intervals not wrapping around,
%%      merge adjacent intervals.
%%      Note: Outside this module, use only for testing - all intervals
%%      generated by this module are normalized!
-spec normalize(interval()) -> interval().
normalize(List) when is_list(List) ->
    NormalizedList1 =
        lists:flatmap(fun(I) -> to_list(normalize_simple(I)) end, List),
    case merge_adjacent(lists:usort(fun interval_sort/2, NormalizedList1), []) of
        [Element] ->
            Element;
        X ->
            X
    end;
normalize(Element) ->
    case normalize_simple(Element) of
        [Element2] ->
            Element2;
        X ->
            X
    end.

%% @doc Normalizes simple intervals (see normalize/1).
-spec normalize_simple(simple_interval() | []) -> interval().
normalize_simple(all) ->
    all;
normalize_simple({element, _A} = I) ->
    I;
normalize_simple({interval, '(', X, X, ')'}) ->
    empty();
normalize_simple({interval, '[', minus_infinity, plus_infinity, ']'}) ->
    all;
normalize_simple({interval, LeftBr, minus_infinity, plus_infinity, RightBr}) ->
    {interval, LeftBr, minus_infinity, plus_infinity, RightBr};
normalize_simple({interval, '(', plus_infinity, minus_infinity, ')'}) ->
    empty();
normalize_simple({interval, '(', plus_infinity, minus_infinity, ']'}) ->
    {element, minus_infinity};
normalize_simple({interval, '[', plus_infinity, minus_infinity, ')'}) ->
    {element, plus_infinity};
normalize_simple({interval, '[', plus_infinity, minus_infinity, ']'}) ->
    [{element, minus_infinity}, {element, plus_infinity}];
normalize_simple({interval, _LeftBr, X, X, _RightBr}) ->
    all; % case '(X,X)' has been handled above
normalize_simple({interval, '(', plus_infinity, X, RightBr}) ->
    {interval, '[', minus_infinity, X, RightBr};
normalize_simple({interval, '[', plus_infinity, X, RightBr}) ->
    [{interval, '[', minus_infinity, X, RightBr}, {element, plus_infinity}];
normalize_simple({interval, LeftBr, X, minus_infinity, ')'}) ->
    {interval, LeftBr, X, plus_infinity, ']'};
normalize_simple({interval, LeftBr, X, minus_infinity, ']'}) ->
    [{element, minus_infinity}, {interval, LeftBr, X, plus_infinity, ']'}];
normalize_simple({interval, LeftBr, Begin, End, RightBr}) ->
    case wraps_around({interval, LeftBr, Begin, End, RightBr}) of
        true ->  [{interval, '[', minus_infinity, End, RightBr},
                  {interval, LeftBr, Begin, plus_infinity, ']'}];
        false -> {interval, LeftBr, Begin, End, RightBr}
    end.

%% @doc Checks whether the given interval is normalized, i.e. not wrapping
%%      around and no 'interval' with equal borders (see normalize/1).
%%      Use only for testing - all intervals generated by this module are
%%      well-formed, i.e. normalized!
-spec is_well_formed(interval()) ->  boolean().
is_well_formed([_Interval]) ->
    false;
is_well_formed([_|_] = List) ->
    lists:all(fun is_well_formed_simple/1, List) andalso
        List =:= lists:usort(fun interval_sort/2, List);
is_well_formed([]) ->
    true;
is_well_formed(X) ->
    is_well_formed_simple(X).

%% @doc Checks whether a given simple interval is normalized. Complex intervals
%%      or any other value are considered 'not normalized'.
-spec is_well_formed_simple(interval()) ->  boolean().
is_well_formed_simple({element, _X}) ->
    true;
is_well_formed_simple({interval, '(', plus_infinity, _Y, _RightBr}) ->
    false;
is_well_formed_simple({interval, _LeftBr, _X, minus_infinity, ')'}) ->
    false;
is_well_formed_simple({interval, _LeftBr, X, Y, _RightBr}) ->
    % same as: X=/=Y andalso not wraps_around(Interval)
    not greater_equals_than(X, Y);
is_well_formed_simple(all) ->
    true;
is_well_formed_simple(_) ->
    false.

%% @doc Converts the given interval to a list of simple intervals. If it already
%%      is a list, this list is returned.
-spec to_list(Interval::interval()) -> [simple_interval()].
to_list(Interval) ->
    case Interval of
        X when is_list(X) -> X;
        X -> [X]
    end.

%% @doc Specifies an order over simple intervals based on their first component
%%      and in case of elements based on their value. 'all' is the first and
%%      elements are sorted before intervals with the same values, if two
%%      intervals' values compare equal, normal &lt;= from erlang is used.
-spec interval_sort(Interval1::simple_interval(), Interval2::simple_interval()) -> boolean().
interval_sort(all, _Interval2) ->
    true;
interval_sort({element, A}, {element, B}) ->
    greater_equals_than(B, A);
interval_sort({element, A}, {interval, _B0Br, B0, _B1, _B1Br}) ->
    greater_equals_than(B0, A);
interval_sort({interval, _A0Br, A0, _A1, _A1Br}, {element, B}) ->
    greater_than(B, A0);
interval_sort({interval, _A0Br, A0, _A1, _A1Br} = A, {interval, _B0Br, B0, _B1, _B1Br} = B) ->
    % beware of not accidentally makeing two intervals equal, which is defined
    % as A==B <=> interval_sort(A, B) andalso interval_sort(B, A)
    greater_than(B0, A0) orelse (A0 =:= B0 andalso A =< B);
interval_sort(_Interval1, _Interval2) ->
    false.

%% @doc Merges adjacent intervals in a sorted list of simple intervals using
%%      union_simple/2.
-spec merge_adjacent(interval(), [simple_interval()]) -> [simple_interval()].
merge_adjacent([], Results) ->
    lists:reverse(Results);
merge_adjacent([all | _T], _Results) ->
    [all];
merge_adjacent([H | T], []) ->
    merge_adjacent(T, [H]);
merge_adjacent([HI | TI], [HR | TR]) ->
    merge_adjacent(TI, to_list(union_simple(HI, HR)) ++ TR).

%% @doc Creates the union of two intervals.
-spec union(A::interval(), B::interval()) -> interval().
union(A, A) ->
    A;
union(all, _B) ->
    all;
union([], B) ->
    B;
union(A, []) ->
    A;
union(_A, all) ->
    all;
union(A, B) when is_list(A) orelse is_list(B) ->
    normalize(lists:append(to_list(A), to_list(B)));
union(A, B) ->
    normalize(union_simple(A, B)).

%% @doc Creates the union of two simple intervals or empty lists.
-spec union_simple(A::simple_interval() | [], B::simple_interval() | []) -> interval().
union_simple(all, _B) ->
    all;
union_simple([], B) ->
    B;
union_simple(A, []) ->
    A;
union_simple(_A, all) ->
    all;
union_simple({element, B0}, {interval, _B0Br, B0, B1, B1Br}) ->
    new('[', B0, B1, B1Br);
union_simple({element, B1}, {interval, B0Br, B0, B1, _B1Br}) ->
    new(B0Br, B0, B1, ']');
union_simple({interval, A0Br, A0, A1, _A1Br}, {element, A1}) ->
    new(A0Br, A0, A1, ']');
union_simple({interval, _A0Br, A0, A1, A1Br}, {element, A0}) ->
    new('[', A0, A1, A1Br);
union_simple({element, A_Value} = A, B) ->
    case in(A_Value, B) of
        true  -> B;
        false -> [A, B]
    end;
union_simple(A, {element, B_Value} = B) ->
    % note: always place the first element (A) before the second element (B) in a union 
    case in(B_Value, A) of
        true  -> A;
        false -> [A, B]
    end;
union_simple({interval, A0Br, A0, A1, ']'}, {interval, _B0Br, A1, B1, B1Br}) ->
    new(A0Br, A0, B1, B1Br);
union_simple({interval, A0Br, A0, A1, _A1Br}, {interval, '[', A1, B1, B1Br}) ->
    new(A0Br, A0, B1, B1Br);
union_simple({interval, '[', A0, A1, A1Br}, {interval, B0Br, B0, A0, _B1Br}) ->
    new(B0Br, B0, A1, A1Br);
union_simple({interval, _A0Br, A0, A1, A1Br}, {interval, B0Br, B0, A0, ']'}) ->
    new(B0Br, B0, A1, A1Br);
union_simple({interval, A0Br, A0, A1, A1Br} = A, {interval, B0Br, B0, B1, B1Br} = B) ->
    B0_in_A = is_between(A0Br, A0, B0, A1, A1Br),
    B1_in_A = is_between(A0Br, A0, B1, A1, A1Br),
    A0_in_B = is_between(B0Br, B0, A0, B1, B1Br),
    A1_in_B = is_between(B0Br, B0, A1, B1, B1Br),
    if
        B0_in_A orelse B1_in_A orelse A0_in_B orelse A1_in_B ->
            {NewLeft, NewLeftBr} =
                case A0 =:= B0 of
                    true when A0Br =:= '[' ->
                        {A0, '['};
                    true ->
                        {A0, B0Br};
                    false ->
                        case util:min(A0, B0) =:= A0 of
                            true  -> {A0, A0Br};
                            false -> {B0, B0Br}
                        end
                end,
            {NewRight, NewRightBr} =
                case A1 =:= B1 of
                    true when A1Br =:= ']' ->
                        {A1, ']'};
                    true ->
                        {A1, B1Br};
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
                true ->
                    empty();
                false ->
                    new(NewLeftBr, NewLeft, NewRight, NewRightBr)
            end;
        true ->
            [A, B]
    end.

%% @doc Checks whether the given interval is a continuous interval, i.e. simple
%%      intervals are always continuous, complex intervals are continuous if
%%      they contain 2 simple intervals which are adjacent and wrap around.
%%      Note: empty intervals are not continuous!
-spec is_continuous(interval()) -> boolean().
is_continuous(all) ->
    true;
is_continuous({element, _Key}) ->
    true;
is_continuous({interval, _LBr, _L, _R, _RBr}) ->
    true;
% complex intervals have adjacent intervals merged except for those wrapping around
% -> if it contains only two simple intervals which are adjacent, then it is continuous!
is_continuous([{interval, '[', minus_infinity, _B1, _B1Br}, {interval, _A0Br, _A0, plus_infinity, ']'}]) ->
    true;
is_continuous([{element, minus_infinity}, {interval, _A0Br, _A0, plus_infinity, ']'}]) ->
    true;
is_continuous([{interval, '[', minus_infinity, _B1, _B1Br}, {element, plus_infinity}]) ->
    true;
is_continuous([{element, minus_infinity}, {element, plus_infinity}]) ->
    true;
is_continuous(_) ->
    false.

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
minus_simple(A, A) ->
    empty();
minus_simple(_, all) ->
    empty();
minus_simple(all, {element, B0}) ->
    % hack: use [minus_infinity, plus_infinity] as 'all' and [B0, B0] as element - minus_simple2 can handle this though
    minus_simple2({interval, '[', minus_infinity, plus_infinity, ']'}, {interval, '[', B0, B0, ']'});
minus_simple(all, B = {interval, _B0Br, _B0, _B1, _B1Br}) ->
    % hack: use [minus_infinity, plus_infinity] as 'all' and [B0, B0] as element - minus_simple2 can handle this though
    minus_simple2({interval, '[', minus_infinity, plus_infinity, ']'}, B);
minus_simple({element, A0}, {element, A0}) ->
    empty();
minus_simple(A = {element, _}, {element, _}) ->
    A;
minus_simple(A = {element, A0}, B = {interval, _B0Br, _B0, _B1, _B1Br}) ->
    case in(A0, B) of
        true -> empty();
        _    -> A
    end;
minus_simple({interval, '[', A0, A1, A1Br}, {element, A0}) ->
    new('(', A0, A1, A1Br);
minus_simple({interval, A0Br, A0, A1, ']'}, {element, A1}) ->
    new(A0Br, A0, A1, ')');
minus_simple(A = {interval, A0Br, A0, A1, A1Br}, {element, B0}) ->
    case in(B0, A) of
        false -> A;
        true  -> union(new(A0Br, A0, B0, ')'), new('(', B0, A1, A1Br))
    end;
minus_simple(A = {interval, _A0Br, _A0, _A1, _A1Br}, B = {interval, _B0Br, _B0, _B1, _B1Br}) ->
    B_ = intersection(A, B),
    case B_ of
        []                     -> A;
        A                      -> empty();
        {interval, _, _, _, _} -> minus_simple2(A, B_);
        X when is_list(X)      -> minus(A, B_);
        _                      -> minus_simple(A, B_)
    end.

%% @doc Subtracts the second from the first simple interval (no elements, no
%%      'all', no empty interval). The second interval must be a subset of the
%%      first interval!
-spec minus_simple2({interval, left_bracket(), key(), key(), right_bracket()}, {interval, left_bracket(), key(), key(), right_bracket()}) -> interval().
minus_simple2({interval, A0Br, A0, A1, A1Br}, {interval, B0Br, B0, B1, B1Br}) ->
    First = case B0Br of
                '(' when B0 =:= A0 andalso A0Br =:= '[' ->
                    new(A0);
                '(' when B0 =:= A0 ->
                    empty();
                '(' ->
                    new(A0Br, A0, B0, ']');
                '[' when B0 =:= A0 ->
                    empty();
                '[' ->
                    new(A0Br, A0, B0, ')')
               end,
    Second = case B1Br of
                 ')' when B1 =:= A1 andalso A1Br =:= ']' ->
                     new(A1);
                 ')' when B1 =:= A1 ->
                     empty();
                 ')' ->
                     new('[', B1, A1, A1Br);
                 ']' when B1 =:= A1 ->
                     empty();
                 ']' ->
                     new('(', B1, A1, A1Br)
               end,
    union(First, Second).

%% @doc Subtracts the second from the first interval.
-spec minus(interval(), interval()) -> interval().
minus(A, A) ->
    empty();
minus(_A, all) ->
    empty();
minus(A, []) ->
    A;
minus(A, B) when is_list(A) ->
    B_ = to_list(B),
    % from every simple interval in A, remove all simple intervals in B
    % note: we cannot use minus_simple in foldl since the result may be a list again
    normalize(lists:flatten([lists:foldl(fun(IB, AccIn) ->
                                                 minus(AccIn, IB)
                                         end,
                                         IA, B_) || IA <- A]));
minus(A, B) when is_list(B) ->
    % remove all simple intervals in B
    % note: we cannot use minus_simple in foldl since the result may be a list again
    normalize(lists:foldl(fun(IB, AccIn) ->
                                  minus(AccIn, IB)
                          end,
                          A, B));
minus(A, B) ->
    minus_simple(A, B).

%%====================================================================
%% private functions
%%====================================================================

%% @private
%% @doc Determines whether the given interval wraps around, i.e. the interval
%%      would cover the (non-existing) gap between plus_infinity and
%%      minus_infinity.
-spec wraps_around({interval, left_bracket(), key(), key(), right_bracket()}) -> boolean().
wraps_around({interval, _LeftBr, X, X, _RightBr}) ->
    false;
wraps_around({interval, _LeftBr, minus_infinity, _, _RightBr}) ->
    false;
wraps_around({interval, _LeftBr, _, plus_infinity, _RightBr}) ->
    false;
wraps_around({interval, _LeftBr, _, minus_infinity, ')'}) ->
    % same as [A, plus_infinity] or (A, plus_infinity]
    false;
wraps_around({interval, _LeftBr, _, minus_infinity, _RightBr}) ->
    true;
wraps_around({interval, '(', plus_infinity, _, _RightBr}) ->
    % same as [minus_infinity, A] or [minus_infinity, A)
    false;
wraps_around({interval, _LeftBr, plus_infinity, _, _RightBr}) ->
    true;
wraps_around({interval, _LeftBr, First, Last, _RightBr}) when First > Last ->
    true;
wraps_around(_) ->
    false.

% @doc Begin &lt;= X &lt;= End
% precondition Begin &lt;= End
-spec is_between(BeginBr::left_bracket(), Begin::key(), X::key(), End::key(), EndBr::right_bracket()) -> boolean().
is_between('[', Begin, X, End, ']') ->
    greater_equals_than(X, Begin) andalso greater_equals_than(End, X);
is_between('[', Begin, X, End, ')') ->
    greater_equals_than(X, Begin) andalso greater_than(End, X);
is_between('(', Begin, X, End, ']') ->
    greater_than(X, Begin) andalso greater_equals_than(End, X);
is_between('(', Begin, X, End, ')') ->
    greater_than(X, Begin) andalso greater_than(End, X).

%% @doc A &gt; B
-spec greater_than/2 :: (A::key(), B::key()) -> boolean().
greater_than(X, X) ->
    false;
greater_than(minus_infinity, _) ->
    false;
greater_than(plus_infinity, _) ->
    true;
greater_than(_, plus_infinity) ->
    false;
greater_than(_, minus_infinity) ->
    true;
greater_than(X, Y) ->
    X > Y.

%% @doc A &gt;= B
-spec greater_equals_than/2 :: (A::key(), B::key()) -> boolean().
greater_equals_than(A, B) ->
    (A =:= B) orelse greater_than(A, B).
