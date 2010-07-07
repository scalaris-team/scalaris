%%%-------------------------------------------------------------------
%%% File    : intervals_SUITE.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Unit tests for src/intervals.erl
%%%
%%% Created :  21 Feb 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(intervals_SUITE).

-author('schuett@zib.de').
-vsn('$Id$').

-compile(export_all).

-include_lib("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [new, is_empty, intersection, tc1, normalize,
     tester_empty_well_formed, tester_empty, tester_empty_continuous,
     tester_new1_well_formed, tester_new1, tester_new1_continuous,
     tester_new2_well_formed, tester_new2, tester_new2_continuous,
     tester_new4_well_formed, tester_new4, tester_new4_continuous,
     tester_mk_from_node_ids_well_formed, tester_mk_from_node_ids, tester_mk_from_node_ids_continuous,
     tester_mk_from_nodes_well_formed, tester_mk_from_nodes, tester_mk_from_nodes_continuous,
     tester_normalize_well_formed, tester_normalize,
     tester_intersection_well_formed, tester_intersection,
     tester_not_intersection, tester_not_intersection2,
     tester_union_well_formed, tester_union, tester_not_union, tester_union_continuous,
     tester_is_adjacent,
     tester_is_subset, tester_is_subset2
     ].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    crypto:start(),
    Config.

end_per_suite(_Config) ->
    ok.

new(_Config) ->
    ?assert(intervals:is_well_formed(intervals:new("a", "b"))),
    ?equals(intervals:new(minus_infinity), intervals:new('(',plus_infinity,minus_infinity,']')).

is_empty(_Config) ->
    NotEmpty = intervals:new("a", "b"),
    Empty = intervals:empty(),
    ?assert(not intervals:is_empty(NotEmpty)),
    ?assert(intervals:is_empty(Empty)).

intersection(_Config) ->
    NotEmpty = intervals:new("a", "b"),
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
    ?assert(intervals:is_subset(intervals:union(intervals:new(minus_infinity, 42312921949186639748260586507533448975),
                                                intervals:new(316058952221211684850834434588481137334, plus_infinity)),
                                intervals:new(316058952221211684850834434588481137334, 127383513679421255614104238365475501839))),

    ?assert(intervals:is_subset(intervals:union(intervals:new(187356034246551717222654062087646951235),
                                                intervals:new(36721483204272088954146455621100499974)),
                                intervals:new(36721483204272088954146455621100499974, 187356034246551717222654062087646951235))),
    
    ?equals(intervals:union({interval,'[', minus_infinity, plus_infinity,')'}, [{interval,'[',minus_infinity,4,']'}, {element,plus_infinity}]),
            intervals:all()),
    
    ok.

normalize(_Config) ->
    % some tests that have once failed:
    % attention: manually defined intervals may have to be adapted to changes in the intervals module!
    ?equals(intervals:normalize([{interval,'[', minus_infinity, plus_infinity,')'}, {element,minus_infinity}, {interval,'[', plus_infinity,4,']'}, {interval,'(', minus_infinity, plus_infinity,']'}]),
            intervals:all()),
    
    ?equals(intervals:normalize([{interval,'[', minus_infinity, plus_infinity,')'}, {element,minus_infinity}, {interval,'[', plus_infinity,4,']'}]),
            intervals:all()),
    
    ?equals(intervals:normalize([{interval,'[', minus_infinity, plus_infinity,')'}, {interval,'[', plus_infinity, 4, ']'}]),
            intervals:all()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:empty/0, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_empty_well_formed/0 :: () -> boolean()).
prop_empty_well_formed() ->
    intervals:is_well_formed(intervals:empty()).

-spec(prop_empty_continuous/0 :: () -> boolean()).
prop_empty_continuous() ->
    not intervals:is_continuous(intervals:empty()).

-spec(prop_empty/1 :: (intervals:key()) -> boolean()).
prop_empty(X) ->
    intervals:is_empty(intervals:empty()) andalso
        not intervals:in(X, intervals:empty()).

tester_empty_well_formed(_Config) ->
    tester:test(intervals_SUITE, prop_empty_well_formed, 0, 1).

tester_empty_continuous(_Config) ->
    tester:test(intervals_SUITE, prop_empty_continuous, 0, 1).

tester_empty(_Config) ->
    tester:test(intervals_SUITE, prop_empty, 1, 1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:new/1, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_new1_well_formed/1 :: (intervals:key()) -> boolean()).
prop_new1_well_formed(X) ->
    intervals:is_well_formed(intervals:new(X)).

-spec(prop_new1_continuous/1 :: (intervals:key()) -> boolean()).
prop_new1_continuous(X) ->
    intervals:is_continuous(intervals:new(X)).

-spec(prop_new1/1 :: (intervals:key()) -> boolean()).
prop_new1(X) ->
    intervals:in(X, intervals:new(X)) andalso
        intervals:new(X) =/= intervals:new('[', X, X, ']') andalso
        not intervals:is_empty(intervals:new(X)).

tester_new1_well_formed(_Config) ->
    tester:test(intervals_SUITE, prop_new1_well_formed, 1, 1000).

tester_new1_continuous(_Config) ->
    tester:test(intervals_SUITE, prop_new1_continuous, 1, 1000).

tester_new1(_Config) ->
    tester:test(intervals_SUITE, prop_new1, 1, 1000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:new/2, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_new2_well_formed/2 :: (intervals:key(), intervals:key()) -> boolean()).
prop_new2_well_formed(X, Y) ->
    intervals:is_well_formed(intervals:new(X, Y)).

-spec(prop_new2_continuous/2 :: (intervals:key(), intervals:key()) -> boolean()).
prop_new2_continuous(X, Y) ->
    intervals:is_continuous(intervals:new(X, Y)).

-spec(prop_new2/2 :: (intervals:key(), intervals:key()) -> boolean()).
prop_new2(X, Y) ->
    intervals:new(X, Y) =:= intervals:new('[', X, Y, ']') andalso
        intervals:in(X, intervals:new(X, Y)) andalso
        intervals:in(Y, intervals:new(X, Y)) andalso
        not intervals:is_empty(intervals:new(X, Y)).

tester_new2_well_formed(_Config) ->
    tester:test(intervals_SUITE, prop_new2_well_formed, 2, 1000).

tester_new2_continuous(_Config) ->
    tester:test(intervals_SUITE, prop_new2_continuous, 2, 1000).

tester_new2(_Config) ->
    tester:test(intervals_SUITE, prop_new2, 2, 1000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:new/4, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_new4_well_formed/4 :: (intervals:left_bracket(), intervals:key(), intervals:key(), intervals:right_bracket()) -> boolean()).
prop_new4_well_formed(XBr, X, Y, YBr) ->
    intervals:is_well_formed(intervals:new(XBr, X, Y, YBr)).

-spec(prop_new4_continuous/4 :: (intervals:left_bracket(), intervals:key(), intervals:key(), intervals:right_bracket()) -> boolean()).
prop_new4_continuous(XBr, X, Y, YBr) ->
    Interval = intervals:new(XBr, X, Y, YBr),
    ?implies(not intervals:is_empty(Interval), intervals:is_continuous(Interval)).

-spec(prop_new4/4 :: (intervals:left_bracket(), intervals:key(), intervals:key(), intervals:right_bracket()) -> boolean()).
prop_new4(XBr, X, Y, YBr) ->
    ?implies(XBr =:= '(' andalso YBr =:= ')' andalso X =:= Y,
             intervals:is_empty(intervals:new(XBr, X, Y, YBr))) andalso
        ?implies(XBr =:= '[' orelse YBr =:= ']',
             not intervals:is_empty(intervals:new(XBr, X, Y, YBr))) andalso
        ?implies(XBr =:= '[',
             intervals:in(X, intervals:new(XBr, X, Y, YBr))) andalso
        ?implies(XBr =:= '(',
             not intervals:in(X, intervals:new(XBr, X, Y, YBr))) andalso
        ?implies(YBr =:= ']',
             intervals:in(Y, intervals:new(XBr, X, Y, YBr))) andalso
        ?implies(XBr =:= ')',
             not intervals:in(Y, intervals:new(XBr, X, Y, YBr))).

tester_new4_well_formed(_Config) ->
    tester:test(intervals_SUITE, prop_new4_well_formed, 4, 1000).

tester_new4_continuous(_Config) ->
    tester:test(intervals_SUITE, prop_new4_continuous, 4, 1000).

tester_new4(_Config) ->
    tester:test(intervals_SUITE, prop_new4, 4, 1000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:mk_from_node_ids/2, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_mk_from_node_ids_well_formed/2 :: (?RT:key(), ?RT:key()) -> boolean()).
prop_mk_from_node_ids_well_formed(X, Y) ->
    intervals:is_well_formed(intervals:mk_from_node_ids(X, Y)).

-spec(prop_mk_from_node_ids_continuous/2 :: (?RT:key(), ?RT:key()) -> boolean()).
prop_mk_from_node_ids_continuous(X, Y) ->
    Interval = intervals:mk_from_node_ids(X, Y),
    ?implies(not intervals:is_empty(Interval), intervals:is_continuous(Interval)).

-spec(prop_mk_from_node_ids/2 :: (?RT:key(), ?RT:key()) -> boolean()).
prop_mk_from_node_ids(X, Y) ->
    intervals:new('(', X, Y, ']') =:= intervals:mk_from_node_ids(X, Y) andalso
        ?implies(X =/= Y, not intervals:in(X, intervals:mk_from_node_ids(X, Y))) andalso
        intervals:in(Y, intervals:mk_from_node_ids(X, Y)) andalso
        not intervals:is_empty(intervals:mk_from_node_ids(X, Y)).

tester_mk_from_node_ids_well_formed(_Config) ->
    tester:test(intervals_SUITE, prop_mk_from_node_ids_well_formed, 2, 1000).

tester_mk_from_node_ids_continuous(_Config) ->
    tester:test(intervals_SUITE, prop_mk_from_node_ids_continuous, 2, 1000).

tester_mk_from_node_ids(_Config) ->
    tester:test(intervals_SUITE, prop_mk_from_node_ids, 2, 1000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:mk_from_nodes/2, intervals:in/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_mk_from_nodes_well_formed/2 :: (node:node_type(), node:node_type()) -> boolean()).
prop_mk_from_nodes_well_formed(X, Y) ->
    %TODO: why does the tester create 'undefined' entries for node ids?
    ?implies(node:id(X) =/= undefined andalso node:id(Y) =/= undefined,
             intervals:is_well_formed(intervals:mk_from_nodes(X, Y))).

-spec(prop_mk_from_nodes_continuous/2 :: (node:node_type(), node:node_type()) -> boolean()).
prop_mk_from_nodes_continuous(X, Y) ->
    Interval = intervals:mk_from_nodes(X, Y),
    %TODO: why does the tester create 'undefined' entries for node ids?
    ?implies(node:id(X) =/= undefined andalso node:id(Y) =/= undefined andalso
                 not intervals:is_empty(Interval),
             intervals:is_continuous(Interval)).

-spec(prop_mk_from_nodes/2 :: (node:node_type(), node:node_type()) -> boolean()).
prop_mk_from_nodes(X, Y) ->
    %TODO: why does the tester create 'undefined' entries for node ids?
    ?implies(node:id(X) =/= undefined andalso node:id(Y) =/= undefined,
             intervals:new('(', node:id(X), node:id(Y), ']') =:= intervals:mk_from_nodes(X, Y) andalso
                 ?implies(node:id(X) =/= node:id(Y), not intervals:in(node:id(X), intervals:mk_from_nodes(X, Y))) andalso
                 intervals:in(node:id(Y), intervals:mk_from_nodes(X, Y)) andalso
                 not intervals:is_empty(intervals:mk_from_nodes(X, Y))).

tester_mk_from_nodes_well_formed(_Config) ->
    tester:test(intervals_SUITE, prop_mk_from_nodes_well_formed, 2, 1000).

tester_mk_from_nodes_continuous(_Config) ->
    tester:test(intervals_SUITE, prop_mk_from_nodes_continuous, 2, 1000).

tester_mk_from_nodes(_Config) ->
    tester:test(intervals_SUITE, prop_mk_from_nodes, 2, 1000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:normalize/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_normalize_well_formed/1 :: (intervals:interval()) -> boolean()).
prop_normalize_well_formed(Is) ->
    intervals:is_well_formed(intervals:normalize(Is)).

-spec(prop_normalize/2 :: (intervals:interval(), intervals:key()) -> boolean()).
prop_normalize(I, X) ->
    ?implies(intervals:is_well_formed(I),
             intervals:in(X, I) =:= intervals:in(X, intervals:normalize(I))).

tester_normalize_well_formed(_Config) ->
    tester:test(intervals_SUITE, prop_normalize_well_formed, 1, 1000).

tester_normalize(_Config) ->
    tester:test(intervals_SUITE, prop_normalize, 2, 1000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:intersection/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_intersection_well_formed/2 :: (intervals:interval(), intervals:interval()) -> boolean()).
prop_intersection_well_formed(A_, B_) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    intervals:is_well_formed(intervals:intersection(A, B)).

-spec(prop_intersection/3 :: (intervals:interval(), intervals:interval(), intervals:key()) -> boolean()).
prop_intersection(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(intervals:in(X, A) andalso intervals:in(X, B),
             intervals:in(X, intervals:intersection(A, B))).

-spec(prop_not_intersection/3 :: (intervals:interval(), intervals:interval(), intervals:key()) -> boolean()).
prop_not_intersection(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(intervals:in(X, A) xor intervals:in(X, B),
             not intervals:in(X, intervals:intersection(A, B))).

-spec(prop_not_intersection2/3 :: (intervals:interval(), intervals:interval(), intervals:key()) -> boolean()).
prop_not_intersection2(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(not intervals:in(X, A) andalso not intervals:in(X, B),
             not intervals:in(X, intervals:intersection(A, B))).

tester_intersection_well_formed(_Config) ->
    tester:test(?MODULE, prop_intersection_well_formed, 2, 1000).

tester_intersection(_Config) ->
    tester:test(?MODULE, prop_intersection, 3, 1000).

tester_not_intersection(_Config) ->
    tester:test(?MODULE, prop_not_intersection, 3, 1000).

tester_not_intersection2(_Config) ->
    tester:test(?MODULE, prop_not_intersection2, 3, 1000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:union/2 and intervals:is_continuous/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_union_well_formed/2 :: (intervals:interval(), intervals:interval()) -> boolean()).
prop_union_well_formed(A_, B_) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    intervals:is_well_formed(intervals:union(A, B)).

-spec(prop_union/3 :: (intervals:interval(), intervals:interval(), intervals:key()) -> boolean()).
prop_union(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(intervals:in(X, A) orelse intervals:in(X, B),
             intervals:in(X, intervals:union(A, B))).

-spec(prop_not_union/3 :: (intervals:interval(), intervals:interval(), intervals:key()) -> boolean()).
prop_not_union(A_, B_, X) ->
    A = intervals:normalize(A_),
    B = intervals:normalize(B_),
    ?implies(not intervals:in(X, A) andalso not intervals:in(X, B),
             not intervals:in(X, intervals:union(A, B))).

-spec(prop_union_continuous/8 :: (A0Br::intervals:left_bracket(), A0::intervals:key(), A1::intervals:key(), A1Br::intervals:right_bracket(),
                                  B0Br::intervals:left_bracket(), B0::intervals:key(), B1::intervals:key(), B1Br::intervals:right_bracket()) -> boolean()).
prop_union_continuous(A0Br, A0, A1, A1Br, B0Br, B0, B1, B1Br) ->
    A = intervals:new(A0Br, A0, A1, A1Br),
    B = intervals:new(B0Br, B0, B1, B1Br),
    UnionIsContinuous =
        (A =:= B andalso intervals:is_continuous(A)) orelse
            (intervals:is_empty(A) andalso intervals:is_continuous(B)) orelse
            (intervals:is_empty(B) andalso intervals:is_continuous(A)) orelse
            % if both intervals are adjacent, they are continuous:
            intervals:in(B0, A) orelse intervals:in(B1, A) orelse
            intervals:in(A0, B) orelse intervals:in(A1, B) orelse
            % unions of continuous intervals wrapping around are continuous
            {A1, A1Br, B0, B0Br} =:= {plus_infinity, ']', minus_infinity, '['} orelse
            {B1, B1Br, A0, A0Br} =:= {plus_infinity, ']', minus_infinity, '['},
    UnionIsContinuous =:= intervals:is_continuous(intervals:union(A, B)).

tester_union_well_formed(_Config) ->
    tester:test(?MODULE, prop_union_well_formed, 2, 1000).

tester_union(_Config) ->
    tester:test(?MODULE, prop_union, 3, 1000).

tester_not_union(_Config) ->
    tester:test(?MODULE, prop_not_union, 3, 1000).

tester_union_continuous(_Config) ->
    tester:test(intervals_SUITE, prop_union_continuous, 8, 1000).

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
             ({A0Br, A0, B1Br} =:= {'(', B1, ']'} orelse
                  {A0Br, A0, B1Br} =:= {'[', B1, ')'} orelse
                  {B0Br, B0, A1Br} =:= {'(', A1, ']'} orelse
                  {B0Br, B0, A1Br} =:= {'[', A1, ')'} orelse
                  % one interval is an element:
                  {A, B0Br, B0} =:= {intervals:new(A0), '(', A0} orelse
                  {A, B1Br, B1} =:= {intervals:new(A0), ')', A0} orelse
                  {A, B0Br, B0} =:= {intervals:new(A1), '(', A1} orelse
                  {A, B1Br, B1} =:= {intervals:new(A1), ')', A1} orelse
                  % special cases for intervals with plus_infinity or minus_infinity
                  {A0Br, A0, B1, B1Br} =:= {'[', minus_infinity, plus_infinity, ']'} orelse
                  {B0Br, B0, A1, A1Br} =:= {'[', minus_infinity, plus_infinity, ']'} orelse
                  {A0Br, A0, B1, B1Br} =:= {'(', plus_infinity, plus_infinity, ']'} orelse
                  {B0Br, B0, A1, A1Br} =:= {'(', plus_infinity, plus_infinity, ']'} orelse
                  {A0Br, A0, B1, B1Br} =:= {'[', minus_infinity, minus_infinity, ')'} orelse
                  {B0Br, B0, A1, A1Br} =:= {'[', minus_infinity, minus_infinity, ')'} orelse
                  {A0Br, A0, B1, B1Br} =:= {'(', plus_infinity, minus_infinity, ')'} orelse
                  {B0Br, B0, A1, A1Br} =:= {'(', plus_infinity, minus_infinity, ')'} orelse
                  {A, B} =:= {intervals:new(minus_infinity), intervals:new(plus_infinity)} orelse
                  {B, A} =:= {intervals:new(minus_infinity), intervals:new(plus_infinity)}),
    IsAdjacent =:= intervals:is_adjacent(A, B).

tester_is_adjacent(_Config) ->
    tester:test(intervals_SUITE, prop_is_adjacent, 8, 1000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:is_subset/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_is_subset/2 :: (intervals:interval(), intervals:interval()) -> boolean()).
prop_is_subset(X_, Y_) ->
    X = intervals:normalize(X_),
    Y = intervals:normalize(Y_),
    % Y covers X iff X \cup Y covers X
    intervals:is_subset(X, Y) =:= intervals:is_subset(X, intervals:intersection(X, Y)).

tester_is_subset(_Config) ->
    tester:test(?MODULE, prop_is_subset, 2, 1000).

-spec(prop_is_subset2/2 :: (intervals:interval(), intervals:interval()) -> boolean()).
prop_is_subset2(_X, _Y) ->
    X = intervals:normalize(_X),
    Y = intervals:normalize(_Y),
    Z = intervals:intersection(X, Y),
    % X as well as Y cover X \cup Y
    intervals:is_subset(Z, X) andalso intervals:is_subset(Z, Y).

tester_is_subset2(_Config) ->
    tester:test(?MODULE, prop_is_subset2, 2, 1000).

