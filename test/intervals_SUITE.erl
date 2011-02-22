%  @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @author Nico Kruber <kruber@zib.de>
%% @doc Unit tests for src/intervals.erl.
%% @end
%% @version $Id$
-module(intervals_SUITE).
-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [new, is_empty, intersection, tc1, normalize, is_left_right_of,
     tester_empty_well_formed, tester_empty, tester_empty_continuous,
     tester_all_well_formed, tester_all_continuous, tester_all,
     tester_new1_well_formed, tester_new1, tester_new1_continuous,
     tester_new1_bounds,
     tester_new2_well_formed, tester_new2, tester_new2_continuous,
     tester_new2_bounds,
     tester_new4_well_formed, tester_new4, tester_new4_continuous,
     tester_new4_bounds,
     tester_from_elements1_well_formed,
     tester_from_elements1,
     tester_normalize_well_formed, tester_normalize,
     tester_intersection_well_formed, tester_intersection,
     tester_not_intersection, tester_not_intersection2,
     tester_union_well_formed, tester_union, tester_not_union, tester_union_continuous,
     tester_is_adjacent, tester_is_adjacent_union,
     tester_is_left_right_of,
     tester_is_subset, tester_is_subset2,
     tester_minus_well_formed, tester_minus, tester_minus2, tester_minus3,
     tester_not_minus, tester_not_minus2,
     tester_get_bounds, tester_get_elements,
     tester_mk_from_node_ids_well_formed, tester_mk_from_node_ids, tester_mk_from_node_ids_continuous,
     tester_mk_from_nodes
     ].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

-spec spawn_config_processes(Config::[tuple()]) -> pid().
spawn_config_processes(Config) ->
    ok = unittest_helper:fix_cwd(),
    unittest_helper:start_process(
      fun() ->
              {priv_dir, PrivDir} = lists:keyfind(priv_dir, 1, Config),
              ConfigOptions = unittest_helper:prepare_config([{config, [{log_path, PrivDir}]}]),
              {ok, _ConfigPid} = config:start_link2(ConfigOptions),
              {ok, _LogPid} = log:start_link()
      end).

init_per_suite(Config) ->
    Config2 = unittest_helper:init_per_suite(Config),
    Pid = spawn_config_processes(Config2),
    [{wrapper_pid, Pid} | Config2].

end_per_suite(Config) ->
    {wrapper_pid, Pid} = lists:keyfind(wrapper_pid, 1, Config),
    error_logger:tty(false),
    log:set_log_level(none),
    exit(Pid, kill),
    _ = unittest_helper:end_per_suite(Config),
    ok.

new(_Config) ->
    ?assert(intervals:is_well_formed(intervals:new('[', ?RT:hash_key("a"), ?RT:hash_key("b"), ']'))),
    ?equals(intervals:new(?MINUS_INFINITY), intervals:new('(',?PLUS_INFINITY,?MINUS_INFINITY,']')),
    ?equals(intervals:new(?PLUS_INFINITY), intervals:new('[',?PLUS_INFINITY,?MINUS_INFINITY,')')).

is_empty(_Config) ->
    NotEmpty = intervals:new('[', ?RT:hash_key("a"), ?RT:hash_key("b"), ']'),
    Empty = intervals:empty(),
    ?assert(not intervals:is_empty(NotEmpty)),
    ?assert(intervals:is_empty(Empty)).

intersection(_Config) ->
    NotEmpty = intervals:new('[', ?RT:hash_key("a"), ?RT:hash_key("b"), ']'),
    Empty = intervals:empty(),
    ?assert(intervals:is_empty(intervals:intersection(NotEmpty, Empty))),
    ?assert(intervals:is_empty(intervals:intersection(Empty, NotEmpty))),
    ?assert(intervals:is_empty(intervals:intersection(NotEmpty, Empty))),
    ?assert(not intervals:is_empty(intervals:intersection(NotEmpty, NotEmpty))),
    ?assert(intervals:intersection(NotEmpty, NotEmpty) =:= NotEmpty),
    ?assert(intervals:intersection(all, all) =:= all),
    ok.

tc1(_Config) ->
    % some tests that have once failed:
    ?assert(intervals:is_subset(
              intervals:union(
                intervals:new('[', ?MINUS_INFINITY,
                              42312921949186639748260586507533448975, ']'),
                intervals:new('[', 316058952221211684850834434588481137334,
                              ?PLUS_INFINITY, ']')),
              intervals:new('[', 316058952221211684850834434588481137334,
                            127383513679421255614104238365475501839, ']'))),

    ?assert(intervals:is_subset(
              intervals:union(
                intervals:new(187356034246551717222654062087646951235),
                intervals:new(36721483204272088954146455621100499974)),
              intervals:new('[', 36721483204272088954146455621100499974,
                            187356034246551717222654062087646951235, ']'))),

    ?equals(intervals:union([{interval,'[', ?MINUS_INFINITY, ?PLUS_INFINITY,')'}],
                            [{interval,'[',?MINUS_INFINITY,4,']'},
                             {element,?PLUS_INFINITY}]),
            intervals:all()),

    ok.

normalize(_Config) ->
    % some tests that have once failed:
    % attention: manually defined intervals may have to be adapted to changes in the intervals module!
    ?equals(intervals:normalize([{interval,'[', ?MINUS_INFINITY, ?PLUS_INFINITY,')'}, {element,?MINUS_INFINITY}, {interval,'[', ?PLUS_INFINITY,4,']'}, {interval,'(', ?MINUS_INFINITY, ?PLUS_INFINITY,']'}]),
            intervals:all()),
    
    ?equals(intervals:normalize([{interval,'[', ?MINUS_INFINITY, ?PLUS_INFINITY,')'}, {element,?MINUS_INFINITY}, {interval,'[', ?PLUS_INFINITY,4,']'}]),
            intervals:all()),
    
    ?equals(intervals:normalize([{interval,'[', ?MINUS_INFINITY, ?PLUS_INFINITY,')'}, {interval,'[', ?PLUS_INFINITY, 4, ']'}]),
            intervals:all()),

    ?equals(intervals:normalize([{interval,'[',3,2,']'}, {interval,'[',?PLUS_INFINITY,3,')'}, {interval,'(',3,?PLUS_INFINITY,')'}]),
            intervals:all()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:empty/0, intervals:is_empty/1, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_empty_well_formed() -> boolean().
prop_empty_well_formed() ->
    intervals:is_well_formed(intervals:empty()).

-spec prop_empty_continuous() -> boolean().
prop_empty_continuous() ->
    not intervals:is_continuous(intervals:empty()).

-spec prop_empty(intervals:key()) -> true.
prop_empty(X) ->
    ?assert(intervals:is_empty(intervals:empty())),
    ?assert(not intervals:in(X, intervals:empty())),
    true.

tester_empty_well_formed(_Config) ->
    tester:test(?MODULE, prop_empty_well_formed, 0, 1).

tester_empty_continuous(_Config) ->
    tester:test(?MODULE, prop_empty_continuous, 0, 1).

tester_empty(_Config) ->
    tester:test(?MODULE, prop_empty, 1, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:all/0, intervals:is_all/1, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_all_well_formed() -> boolean().
prop_all_well_formed() ->
    intervals:is_well_formed(intervals:all()).

-spec prop_all_continuous() -> boolean().
prop_all_continuous() ->
    intervals:is_continuous(intervals:all()).

-spec prop_all(intervals:key()) -> true.
prop_all(X) ->
    ?assert(intervals:is_all(intervals:all())),
    ?assert(intervals:in(X, intervals:all())),
    true.

tester_all_well_formed(_Config) ->
    tester:test(?MODULE, prop_all_well_formed, 0, 1).

tester_all_continuous(_Config) ->
    tester:test(?MODULE, prop_all_continuous, 0, 1).

tester_all(_Config) ->
    tester:test(?MODULE, prop_all, 1, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:new/1, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_new1_well_formed(intervals:key()) -> boolean().
prop_new1_well_formed(X) ->
    intervals:is_well_formed(intervals:new(X)).

-spec prop_new1_continuous(intervals:key()) -> boolean().
prop_new1_continuous(X) ->
    intervals:is_continuous(intervals:new(X)).

-spec prop_new1_bounds(intervals:key()) -> true.
prop_new1_bounds(X) ->
    ?equals(intervals:get_bounds(intervals:new(X)), {'[', X, X, ']'}),
    true.

-spec prop_new1(intervals:key()) -> true.
prop_new1(X) ->
    ?equals(intervals:new(X), intervals:new('[', X, X, ']')),
    ?assert(intervals:in(X, intervals:new(X))),
    ?assert(not intervals:is_empty(intervals:new(X))),
    true.

tester_new1_well_formed(_Config) ->
    tester:test(?MODULE, prop_new1_well_formed, 1, 5000).

tester_new1_continuous(_Config) ->
    tester:test(?MODULE, prop_new1_continuous, 1, 5000).

tester_new1_bounds(_Config) ->
    tester:test(?MODULE, prop_new1_bounds, 1, 5000).

tester_new1(_Config) ->
    tester:test(?MODULE, prop_new1, 1, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:new/4, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_new2_well_formed(intervals:key(), intervals:key()) -> boolean().
prop_new2_well_formed(X, Y) ->
    intervals:is_well_formed(intervals:new('[', X, Y, ']')).

-spec prop_new2_continuous(intervals:key(), intervals:key()) -> boolean().
prop_new2_continuous(X, Y) ->
    intervals:is_continuous(intervals:new('[', X, Y, ']')).

-spec prop_new2_bounds(intervals:key(), intervals:key()) -> true.
prop_new2_bounds(X, Y) ->
    I = intervals:new('[', X, Y, ']'),
    case intervals:is_all(I) of
        false -> ?equals(intervals:get_bounds(I), {'[', X, Y, ']'});
        true  -> ?equals(intervals:get_bounds(I), {'[', ?MINUS_INFINITY, ?PLUS_INFINITY, ']'})
    end,
    true.

-spec prop_new2(intervals:key(), intervals:key()) -> true.
prop_new2(X, Y) ->
    % check deterministic behavior
    A = intervals:new('[', X, Y, ']'),
    B = intervals:new('[', X, Y, ']'),
    ?equals(A, B),
    ?assert(intervals:in(X, A)),
    ?assert(intervals:in(Y, A)),
    ?assert(not intervals:is_empty(A)),
    true.

tester_new2_well_formed(_Config) ->
    tester:test(?MODULE, prop_new2_well_formed, 2, 5000).

tester_new2_continuous(_Config) ->
    tester:test(?MODULE, prop_new2_continuous, 2, 5000).

tester_new2_bounds(_Config) ->
    tester:test(?MODULE, prop_new2_bounds, 2, 5000).

tester_new2(_Config) ->
    tester:test(?MODULE, prop_new2, 2, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:new/4, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_new4_well_formed(intervals:left_bracket(), intervals:key(), intervals:key(), intervals:right_bracket()) -> boolean().
prop_new4_well_formed(XBr, X, Y, YBr) ->
    intervals:is_well_formed(intervals:new(XBr, X, Y, YBr)).

-spec prop_new4_continuous(intervals:left_bracket(), intervals:key(), intervals:key(), intervals:right_bracket()) -> boolean().
prop_new4_continuous(XBr, X, Y, YBr) ->
    Interval = intervals:new(XBr, X, Y, YBr),
    ?implies(not intervals:is_empty(Interval), intervals:is_continuous(Interval)).

-spec prop_new4_bounds(intervals:left_bracket(), intervals:key(), intervals:key(), intervals:right_bracket()) -> true.
prop_new4_bounds(XBr, X, Y, YBr) ->
    I = intervals:new(XBr, X, Y, YBr),
    % convert brackets if ?MINUS_INFINITY or ?PLUS_INFINITY are involved
    % (they will be converted due to normalization)
    {Ye, YBre} = case {Y, YBr} of
                     {?MINUS_INFINITY, ')'} -> {?PLUS_INFINITY, ']'};
                     _                     -> {Y, YBr}
                 end,
    {Xe, XBre} = case {X, XBr} of
                     {?PLUS_INFINITY, '('} -> {?MINUS_INFINITY, '['};
                     _                    -> {X, XBr}
                 end,
    case intervals:is_empty(I) of
        true -> true;
        false ->
            case intervals:is_all(I) of
                false -> ?equals(intervals:get_bounds(I), {XBre, Xe, Ye, YBre});
                true  -> ?equals(intervals:get_bounds(I), {'[', ?MINUS_INFINITY, ?PLUS_INFINITY, ']'})
            end,
            true
    end.

-spec prop_new4(intervals:left_bracket(), intervals:key(), intervals:key(), intervals:right_bracket()) -> true.
prop_new4(XBr, X, Y, YBr) ->
    I = intervals:new(XBr, X, Y, YBr),
    case (X =:= Y andalso (XBr =:= '(' orelse YBr =:= ')')) orelse
             ({XBr, X, Y, YBr} =:= {'(', ?PLUS_INFINITY, ?MINUS_INFINITY, ')'}) of
        true  -> ?assert(intervals:is_empty(I));
        false -> 
            ?assert(not intervals:is_empty(I)),
            ?equals(?implies(XBr =:= '[', intervals:in(X, I)), true),
            ?equals(?implies(XBr =:= '(', not intervals:in(X, I)), true),
            ?equals(?implies(YBr =:= ']', intervals:in(Y, I)), true),
            ?equals(?implies(YBr =:= ')', not intervals:in(Y, I)), true)
    end,
    true.

tester_new4_well_formed(_Config) ->
    tester:test(?MODULE, prop_new4_well_formed, 4, 5000).

tester_new4_continuous(_Config) ->
    tester:test(?MODULE, prop_new4_continuous, 4, 5000).

tester_new4_bounds(_Config) ->
    tester:test(?MODULE, prop_new4_bounds, 4, 5000).

tester_new4(_Config) ->
    tester:test(?MODULE, prop_new4, 4, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:from_elements/1 and intervals:in/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_from_elements1_well_formed([intervals:key()]) -> boolean().
prop_from_elements1_well_formed(X) ->
    intervals:is_well_formed(intervals:from_elements(X)).

-spec prop_from_elements1([intervals:key()]) -> true.
prop_from_elements1(X) ->
    I = intervals:from_elements(X),
    ?equals(?implies(X =/= [], not intervals:is_empty(I)), true),
    _ = [?equals(intervals:in(K, I), true) || K <- X],
    true.

tester_from_elements1_well_formed(_Config) ->
    tester:test(?MODULE, prop_from_elements1_well_formed, 1, 5000).

tester_from_elements1(_Config) ->
    tester:test(?MODULE, prop_from_elements1, 1, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:normalize/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_normalize_well_formed(intervals:interval()) -> boolean().
prop_normalize_well_formed(Is) ->
    intervals:is_well_formed(intervals:normalize(Is)).

-spec prop_normalize(intervals:interval(), intervals:key()) -> boolean().
prop_normalize(I, X) ->
    ?implies(intervals:is_well_formed(I),
             intervals:in(X, I) =:= intervals:in(X, intervals:normalize(I))).

tester_normalize_well_formed(_Config) ->
    tester:test(?MODULE, prop_normalize_well_formed, 1, 5000).

tester_normalize(_Config) ->
    tester:test(?MODULE, prop_normalize, 2, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:intersection/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_intersection_well_formed(intervals:interval(), intervals:interval()) -> boolean().
prop_intersection_well_formed(A_, B_) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    intervals:is_well_formed(intervals:intersection(A, B)).

-spec prop_intersection(intervals:interval(), intervals:interval(), intervals:key()) -> boolean().
prop_intersection(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(intervals:in(X, A) andalso intervals:in(X, B),
             intervals:in(X, intervals:intersection(A, B))).

-spec prop_not_intersection(intervals:interval(), intervals:interval(), intervals:key()) -> boolean().
prop_not_intersection(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(intervals:in(X, A) xor intervals:in(X, B),
             not intervals:in(X, intervals:intersection(A, B))).

-spec prop_not_intersection2(intervals:interval(), intervals:interval(), intervals:key()) -> boolean().
prop_not_intersection2(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(not intervals:in(X, A) andalso not intervals:in(X, B),
             not intervals:in(X, intervals:intersection(A, B))).

tester_intersection_well_formed(_Config) ->
    tester:test(?MODULE, prop_intersection_well_formed, 2, 5000).

tester_intersection(_Config) ->
    tester:test(?MODULE, prop_intersection, 3, 5000).

tester_not_intersection(_Config) ->
    tester:test(?MODULE, prop_not_intersection, 3, 5000).

tester_not_intersection2(_Config) ->
    tester:test(?MODULE, prop_not_intersection2, 3, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:union/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_union_well_formed(intervals:interval(), intervals:interval()) -> boolean().
prop_union_well_formed(A_, B_) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    intervals:is_well_formed(intervals:union(A, B)).

-spec prop_union(intervals:interval(), intervals:interval(), intervals:key()) -> boolean().
prop_union(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(intervals:in(X, A) orelse intervals:in(X, B),
             intervals:in(X, intervals:union(A, B))).

-spec prop_not_union(intervals:interval(), intervals:interval(), intervals:key()) -> boolean().
prop_not_union(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(not intervals:in(X, A) andalso not intervals:in(X, B),
             not intervals:in(X, intervals:union(A, B))).

-spec prop_union_continuous(A0Br::intervals:left_bracket(), A0::intervals:key(), A1::intervals:key(), A1Br::intervals:right_bracket(),
                            B0Br::intervals:left_bracket(), B0::intervals:key(), B1::intervals:key(), B1Br::intervals:right_bracket()) -> true.
prop_union_continuous(A0Br, A0, A1, A1Br, B0Br, B0, B1, B1Br) ->
    A = intervals:new(A0Br, A0, A1, A1Br),
    B = intervals:new(B0Br, B0, B1, B1Br),
    ContinuousUnion = intervals:is_continuous(intervals:union(A, B)),
    % list conditions when union(A,B) is continuous
    C1 = A =:= B andalso intervals:is_continuous(A),
    C2 = intervals:is_empty(A) andalso intervals:is_continuous(B),
    C3 = intervals:is_empty(B) andalso intervals:is_continuous(A),
    % if the (continuous) intervals overlap, then their union is continuous:
    % (note: if both are empty, none of the following conditions is true)
    C4 = intervals:in(B0, A) orelse intervals:in(B1, A) orelse
             intervals:in(A0, B) orelse intervals:in(A1, B),
    % unions of continuous intervals wrapping around are continuous
    C5 = intervals:is_continuous(A) andalso intervals:is_continuous(B) andalso
             {A1, A1Br, B0, B0Br} =:= {?PLUS_INFINITY, ']', ?MINUS_INFINITY, '['},
    C6 = intervals:is_continuous(A) andalso intervals:is_continuous(B) andalso
             {B1, B1Br, A0, A0Br} =:= {?PLUS_INFINITY, ']', ?MINUS_INFINITY, '['},
    % if any C* is true, the union must be continuous, if all are false it is not
    % note: split this into several checks for better error messages
    ?equals(?implies(C1, ContinuousUnion), true),
    ?equals(?implies(C2, ContinuousUnion), true),
    ?equals(?implies(C3, ContinuousUnion), true),
    ?equals(?implies(C4, ContinuousUnion), true),
    ?equals(?implies(C5, ContinuousUnion), true),
    ?equals(?implies(C6, ContinuousUnion), true),
    case intervals:is_continuous(intervals:union(A, B)) of
        false -> ?equals(C1, false),
                 ?equals(C2, false),
                 ?equals(C3, false),
                 ?equals(C4, false),
                 ?equals(C5, false),
                 ?equals(C6, false),
                 true;
        true  -> true
    end.

tester_union_well_formed(_Config) ->
    tester:test(?MODULE, prop_union_well_formed, 2, 5000).

tester_union(_Config) ->
    tester:test(?MODULE, prop_union, 3, 5000).

tester_not_union(_Config) ->
    tester:test(?MODULE, prop_not_union, 3, 5000).

tester_union_continuous(_Config) ->
    tester:test(?MODULE, prop_union_continuous, 8, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:is_adjacent/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec(prop_is_adjacent/8 :: (A0Br::intervals:left_bracket(), A0::intervals:key(), A1::intervals:key(), A1Br::intervals:right_bracket(),
                             B0Br::intervals:left_bracket(), B0::intervals:key(), B1::intervals:key(), B1Br::intervals:right_bracket()) -> boolean()).
prop_is_adjacent(A0Br, A0, A1, A1Br, B0Br, B0, B1, B1Br) ->
    A = intervals:new(A0Br, A0, A1, A1Br),
    B = intervals:new(B0Br, B0, B1, B1Br),
    IsAdjacent =
        not intervals:is_empty(A) andalso
        not intervals:is_empty(B) andalso
        intervals:is_empty(intervals:intersection(A, B)) andalso
            (% real intervals, a border is in one interval but not the other:
             {A0Br, A0, B1Br} =:= {'(', B1, ']'} orelse
             {A0Br, A0, B1Br} =:= {'[', B1, ')'} orelse
             {B0Br, B0, A1Br} =:= {'(', A1, ']'} orelse
             {B0Br, B0, A1Br} =:= {'[', A1, ')'} orelse
             % one interval is an element:
             {A, B0Br, B0} =:= {intervals:new(A0), '(', A0} orelse
             {A, B1Br, B1} =:= {intervals:new(A0), ')', A0} orelse
             {A, B0Br, B0} =:= {intervals:new(A1), '(', A1} orelse
             {A, B1Br, B1} =:= {intervals:new(A1), ')', A1} orelse
             % special cases for intervals with ?PLUS_INFINITY or ?MINUS_INFINITY
             % (cases with A0=:=B1 or A1=:=B0 have already been handled above)
             {A0Br, A0, B1, B1Br} =:= {'[', ?MINUS_INFINITY, ?PLUS_INFINITY, ']'} orelse
             {B0Br, B0, A1, A1Br} =:= {'[', ?MINUS_INFINITY, ?PLUS_INFINITY, ']'} orelse
             {A0Br, A0, B1, B1Br} =:= {'(', ?PLUS_INFINITY, ?MINUS_INFINITY, ')'} orelse
             {B0Br, B0, A1, A1Br} =:= {'(', ?PLUS_INFINITY, ?MINUS_INFINITY, ')'} orelse
             {A, B} =:= {intervals:new(?MINUS_INFINITY), intervals:new(?PLUS_INFINITY)} orelse
             {B, A} =:= {intervals:new(?MINUS_INFINITY), intervals:new(?PLUS_INFINITY)}),
    IsAdjacent =:= intervals:is_adjacent(A, B).

tester_is_adjacent_union(_Config) ->
    tester:test(?MODULE, prop_is_adjacent_union, 8, 5000).

-spec prop_is_adjacent_union(A0Br::intervals:left_bracket(), A0::intervals:key(), A1::intervals:key(), A1Br::intervals:right_bracket(),
                             B0Br::intervals:left_bracket(), B0::intervals:key(), B1::intervals:key(), B1Br::intervals:right_bracket()) -> boolean().
prop_is_adjacent_union(A0Br, A0, A1, A1Br, B0Br, B0, B1, B1Br) ->
    A = intervals:new(A0Br, A0, A1, A1Br),
    B = intervals:new(B0Br, B0, B1, B1Br),
    ?implies(intervals:is_adjacent(A, B), intervals:is_continuous(intervals:union(A, B))).

tester_is_adjacent(_Config) ->
    tester:test(?MODULE, prop_is_adjacent, 8, 5000).

-spec(prop_is_left_right_of/8 :: (A0Br::intervals:left_bracket(), A0::intervals:key(),
                                  A1::intervals:key(), A1Br::intervals:right_bracket(),
                                  B0Br::intervals:left_bracket(), B0::intervals:key(),
                                  B1::intervals:key(), B1Br::intervals:right_bracket()) -> true).
prop_is_left_right_of(A0Br, A0, A1, A1Br, B0Br, B0, B1, B1Br) ->
    A = intervals:new(A0Br, A0, A1, A1Br),
    B = intervals:new(B0Br, B0, B1, B1Br),
    ?equals(?implies(intervals:is_left_of(A, B), intervals:is_empty(intervals:minus(intervals:new(A0Br, A0, B1, B1Br), intervals:union(A, B)))), true),
    ?equals(?implies(intervals:is_right_of(A, B), intervals:is_empty(intervals:minus(intervals:new(B0Br, B0, A1, A1Br), intervals:union(A, B)))), true),
    ?equals(intervals:is_adjacent(A, B), intervals:is_left_of(A, B) orelse intervals:is_right_of(A, B)),
    true.

tester_is_left_right_of(_Config) ->
    tester:test(intervals_SUITE, prop_is_left_right_of, 8, 5000).

is_left_right_of(_Config) ->
    X = [{interval,'[',17, ?PLUS_INFINITY, ']'}],
    Y = [{interval,'[',?MINUS_INFINITY,17,')'}],
    ?equals(intervals:is_adjacent(X, Y), true), % @17
    ?equals(intervals:is_left_of(X, Y), true),
    ?equals(intervals:is_left_of(Y, X), true),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:is_subset/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_is_subset(intervals:interval(), intervals:interval()) -> boolean().
prop_is_subset(X_, Y_) ->
    X = intervals:normalize(X_),
    Y = intervals:normalize(Y_),
    % Y covers X iff X \cup Y covers X
    intervals:is_subset(X, Y) =:= intervals:is_subset(X, intervals:intersection(X, Y)).

tester_is_subset(_Config) ->
    tester:test(?MODULE, prop_is_subset, 2, 5000).

-spec prop_is_subset2(intervals:interval(), intervals:interval()) -> boolean().
prop_is_subset2(_X, _Y) ->
    X = intervals:normalize(_X),
    Y = intervals:normalize(_Y),
    Z = intervals:intersection(X, Y),
    % X as well as Y cover X \cup Y
    intervals:is_subset(Z, X) andalso intervals:is_subset(Z, Y).

tester_is_subset2(_Config) ->
    tester:test(?MODULE, prop_is_subset2, 2, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:minus/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_minus_well_formed(intervals:interval(), intervals:interval()) -> boolean().
prop_minus_well_formed(A_, B_) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    intervals:is_well_formed(intervals:minus(A, B)).

-spec prop_minus(intervals:interval(), intervals:interval(), intervals:key()) -> boolean().
prop_minus(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(intervals:in(X, A) andalso not intervals:in(X, B),
             intervals:in(X, intervals:minus(A, B))).

-spec prop_minus2(intervals:left_bracket(), intervals:key(), intervals:key(), intervals:right_bracket()) -> true.
prop_minus2(XBr, X, Y, YBr) ->
    I = intervals:new(XBr, X, Y, YBr),
    I_inv = case intervals:is_empty(I) of
                true  -> intervals:all();
                false ->
                    case I =:= intervals:new(X) of
                        true  -> intervals:union(
                                   intervals:new('[', ?MINUS_INFINITY, X, ')'),
                                   intervals:new('(', X, ?PLUS_INFINITY, ']'));
                        false -> intervals:new(switch_br2(YBr), Y, X, switch_br2(XBr))
                    end
            end,
    ?equals(intervals:minus(intervals:all(), I), I_inv),
    true.

-spec prop_minus3(intervals:interval()) -> true.
prop_minus3(A_) ->
    A = intervals:normalize(A_),
    A_inv = intervals:minus(intervals:all(), A),
    ?equals(intervals:union(A, A_inv), intervals:all()),
    true.

-spec prop_not_minus(intervals:interval(), intervals:interval(), intervals:key()) -> boolean().
prop_not_minus(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(intervals:in(X, B),
             not intervals:in(X, intervals:minus(A, B))).

-spec prop_not_minus2(intervals:interval(), intervals:interval(), intervals:key()) -> boolean().
prop_not_minus2(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(not intervals:in(X, A),
             not intervals:in(X, intervals:minus(A, B))).

tester_minus_well_formed(_Config) ->
    tester:test(?MODULE, prop_minus_well_formed, 2, 5000).

tester_minus(_Config) ->
    tester:test(?MODULE, prop_minus, 3, 5000).

tester_minus2(_Config) ->
    tester:test(?MODULE, prop_minus2, 4, 5000).

tester_minus3(_Config) ->
    tester:test(?MODULE, prop_minus3, 1, 5000).

tester_not_minus(_Config) ->
    tester:test(?MODULE, prop_not_minus, 3, 5000).

tester_not_minus2(_Config) ->
    tester:test(?MODULE, prop_not_minus2, 3, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:get_bounds/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get_bounds(intervals:interval()) -> boolean().
prop_get_bounds(I_) ->
    I = intervals:normalize(I_),
    ?implies(intervals:is_continuous(I),
             case intervals:get_bounds(I) of
                 {'(', Key, Key, ')'} -> I =:= intervals:union(intervals:new('(', Key, ?PLUS_INFINITY, ']'),
                                                               intervals:new('[', ?MINUS_INFINITY, Key, ')'));
                 {LBr, L, R, RBr}     -> I =:= intervals:new(LBr, L, R, RBr)
             end).

tester_get_bounds(_Config) ->
    tester:test(?MODULE, prop_get_bounds, 1, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:get_elements/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_get_elements(intervals:interval()) -> true.
prop_get_elements(I_) ->
    I = intervals:normalize(I_),
    {Elements, RestInt} = intervals:get_elements(I),
    ?equals(lists:foldl(fun(E, Acc) -> intervals:union(intervals:new(E), Acc) end, RestInt, Elements), I),
    true.

tester_get_elements(_Config) ->
    tester:test(?MODULE, prop_get_elements, 1, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% node:mk_interval_between_ids/2, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec prop_mk_from_node_ids_well_formed(?RT:key(), ?RT:key()) -> boolean().
prop_mk_from_node_ids_well_formed(X, Y) ->
    intervals:is_well_formed(node:mk_interval_between_ids(X, Y)).

-spec prop_mk_from_node_ids_continuous(?RT:key(), ?RT:key()) -> boolean().
prop_mk_from_node_ids_continuous(X, Y) ->
    Interval = node:mk_interval_between_ids(X, Y),
    ?implies(not intervals:is_empty(Interval), intervals:is_continuous(Interval)).

-spec prop_mk_from_node_ids(?RT:key(), ?RT:key()) -> true.
prop_mk_from_node_ids(X, Y) ->
    I = node:mk_interval_between_ids(X, Y),
    ?assert(not intervals:is_empty(I)),
    ?assert(intervals:in(Y, I)),
    ?assert(intervals:is_continuous(I)),
    case X =:= Y of
        true  ->
            ?assert(intervals:is_all(I) andalso
                        intervals:in(X, I));
        false ->
            ?assert(intervals:new('(', X, Y, ']') =:= I andalso
                        not intervals:in(X, I))
    end,
    true.

tester_mk_from_node_ids_well_formed(_Config) ->
    tester:test(?MODULE, prop_mk_from_node_ids_well_formed, 2, 5000).

tester_mk_from_node_ids_continuous(_Config) ->
    tester:test(?MODULE, prop_mk_from_node_ids_continuous, 2, 5000).

tester_mk_from_node_ids(_Config) ->
    tester:test(?MODULE, prop_mk_from_node_ids, 2, 5000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% node:mk_interval_between_nodes/2, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec prop_mk_from_nodes(node:node_type(), node:node_type()) -> boolean().
prop_mk_from_nodes(X, Y) ->
    node:mk_interval_between_nodes(X, Y) =:=
        node:mk_interval_between_ids(node:id(X), node:id(Y)).

tester_mk_from_nodes(_Config) ->
    tester:test(?MODULE, prop_mk_from_nodes, 2, 5000).

% helpers:

-spec switch_br('(') -> '['; ('[') -> '(';
               (')') -> ']'; (']') -> ')'.
switch_br('(') -> '[';
switch_br('[') -> '(';
switch_br(')') -> ']';
switch_br(']') -> ')'.

-spec switch_br2('(') -> ']'; ('[') -> ')';
                (')') -> '['; (']') -> '('.
switch_br2('(') -> ']';
switch_br2('[') -> ')';
switch_br2(')') -> '[';
switch_br2(']') -> '('.
