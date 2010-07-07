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
         intersection/2,
         is_subset/2,
         in/2,
         union/2,
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

%% @doc Creates a new closed interval, i.e. "[A, B]". The new interval may wrap
%%      around, i.e. A > B. If A=:=B, an interval covering the whole space is
%%      created.
-spec new(A::key(), B::key()) -> interval().
new(A, B) ->
    new('[', A, B, ']').

%% @doc Creates an interval covering a single element.
-spec new(key()) -> {element, key()}.
new(X) ->
    {element, X}.

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
%%      Outside this module, use only for testing - all intervals generated by
%%      this module are normalized!
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
    [{element, minus_infinity}];
normalize_simple({interval, '[', plus_infinity, minus_infinity, ')'}) ->
    [{element, plus_infinity}];
normalize_simple({interval, '[', plus_infinity, minus_infinity, ']'}) ->
    [{element, minus_infinity}, {element, plus_infinity}];
normalize_simple({interval, _LeftBr, X, X, _RightBr}) ->
    all; % case '(X,X)' has been handled above
normalize_simple({interval, '(', plus_infinity, X, RightBr}) ->
    [{interval, '[', minus_infinity, X, RightBr}];
normalize_simple({interval, '[', plus_infinity, X, RightBr}) ->
    [{interval, '[', minus_infinity, X, RightBr}, {element, plus_infinity}];
normalize_simple({interval, LeftBr, X, minus_infinity, ')'}) ->
    [{interval, LeftBr, X, plus_infinity, ']'}];
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
is_well_formed([Interval | Rest] = List) ->
    is_well_formed(Interval) andalso is_well_formed(Rest) andalso
        List =:= lists:usort(fun interval_sort/2, List);
is_well_formed([]) ->
    true;
is_well_formed({element, _X}) ->
    true;
is_well_formed({interval, '(', plus_infinity, _Y, _RightBr}) ->
    false;
is_well_formed({interval, _LeftBr, _X, minus_infinity, ')'}) ->
    false;
is_well_formed({interval, _LeftBr, X, Y, _RightBr}) ->
    % same as: X=/=Y andalso not wraps_around(Interval)
    not greater_equals_than(X, Y);
is_well_formed(all) ->
    true;
is_well_formed(_) ->
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
