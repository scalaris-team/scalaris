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

-include_lib("unittest.hrl").

all() ->
    [new, is_empty, cut, tc1,
     tester_make, tester_make_well_formed,
     tester_in,
     tester_normalize, tester_normalize_well_formed,
     tester_cut, tester_cut_well_formed,
     tester_not_cut, tester_not_cut2,
     tester_is_covered, tester_is_covered2
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
    intervals:new("a", "b"),
    ?assert(true).

is_empty(_Config) ->
    NotEmpty = intervals:new("a", "b"),
    Empty = intervals:empty(),
    ?assert(not intervals:is_empty(NotEmpty)),
    ?assert(intervals:is_empty(Empty)).

cut(_Config) ->
    NotEmpty = intervals:new("a", "b"),
    Empty = intervals:empty(),
    ?assert(intervals:is_empty(intervals:cut(NotEmpty, Empty))),
    ?assert(intervals:is_empty(intervals:cut(Empty, NotEmpty))),
    ?assert(intervals:is_empty(intervals:cut(NotEmpty, Empty))),
    ?assert(not intervals:is_empty(intervals:cut(NotEmpty, NotEmpty))),
    ?assert(intervals:cut(NotEmpty, NotEmpty) =:= NotEmpty),
    ?assert(intervals:cut(all, all) =:= all),
    ok.

tc1(_Config) ->
    ?assert(intervals:is_covered([{interval,minus_infinity,42312921949186639748260586507533448975},
                                  {interval,316058952221211684850834434588481137334,plus_infinity}],
                                 {interval,316058952221211684850834434588481137334,
                                  127383513679421255614104238365475501839})),
    ?assert(intervals:is_covered([{element,187356034246551717222654062087646951235},
                                  {element,36721483204272088954146455621100499974}],
                                 {interval,36721483204272088954146455621100499974,
                                  187356034246551717222654062087646951235})),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:make/1
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_make/2 :: (intervals:key(), intervals:key()) -> boolean()).
prop_make(X, Y) ->
    intervals:make({X,Y}) =:= intervals:new(X, Y).

tester_make(_Config) ->
    tester:test(intervals_SUITE, prop_make, 2, 100).

-spec(prop_make_well_formed/2 :: (intervals:key(), intervals:key()) -> boolean()).
prop_make_well_formed(X, Y) ->
    intervals:is_well_formed(intervals:make({X,Y})) 
    andalso intervals:is_well_formed(intervals:new(X, Y)).

tester_make_well_formed(_Config) ->
    tester:test(intervals_SUITE, prop_make_well_formed, 2, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:in/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_in/2 :: (intervals:key(), intervals:key()) -> boolean()).
prop_in(X, Y) ->
    intervals:in(X, intervals:new(X, Y)) and
        intervals:in(Y, intervals:new(X, Y)).

tester_in(_Config) ->
    tester:test(intervals_SUITE, prop_in, 2, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:normalize/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_normalize/2 :: (list(intervals:interval()), intervals:key()) -> boolean()).
prop_normalize(Is, X) ->
    intervals:in(X, Is) ==
        intervals:in(X, intervals:normalize(Is)).

tester_normalize(_Config) ->
    tester:test(intervals_SUITE, prop_normalize, 2, 100).

-spec(prop_normalize_well_formed/1 :: (list(intervals:interval())) -> boolean()).
prop_normalize_well_formed(Is) ->
    intervals:is_well_formed(intervals:normalize(Is)).

tester_normalize_well_formed(_Config) ->
    tester:test(intervals_SUITE, prop_normalize_well_formed, 1, 100).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:cut/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_cut/3 :: (intervals:interval(), intervals:interval(), intervals:key()) -> boolean()).
prop_cut(A, B, X) ->
    ?implies(intervals:in(X, A) and intervals:in(X, B),
             intervals:in(X, intervals:cut(A, B))).

-spec(prop_cut_well_formed/2 :: (intervals:interval(), intervals:interval()) -> boolean()).
prop_cut_well_formed(A, B) ->
    intervals:is_well_formed(intervals:cut(A, B)).

-spec(prop_not_cut/3 :: (intervals:interval(), intervals:interval(), intervals:key()) -> boolean()).
prop_not_cut(A, B, X) ->
    ?implies(intervals:in(X, A)
             xor intervals:in(X, B),
             not intervals:in(X, intervals:cut(A, B))).

-spec(prop_not_cut2/3 :: (intervals:interval(), intervals:interval(), intervals:key()) -> boolean()).
prop_not_cut2(A, B, X) ->
    ?implies(not intervals:in(X, A)
             and not intervals:in(X, B),
             not intervals:in(X, intervals:cut(A, B))).

tester_cut(_Config) ->
    tester:test(?MODULE, prop_cut, 3, 1000).

tester_cut_well_formed(_Config) ->
    tester:test(?MODULE, prop_cut_well_formed, 2, 1000).

tester_not_cut(_Config) ->
    tester:test(?MODULE, prop_not_cut, 3, 1000).

tester_not_cut2(_Config) ->
    tester:test(?MODULE, prop_not_cut2, 3, 1000).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% intervals:is_covered/2
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec(prop_is_covered/2 :: (intervals:interval(), intervals:interval()) -> boolean()).
prop_is_covered(_X, _Y) ->
    X = intervals:normalize(_X),
    Y = intervals:normalize(_Y),
    % Y covers X iff X \cup Y covers X
    intervals:is_covered(X, Y) =:= intervals:is_covered(X, intervals:cut(X, Y)).

tester_is_covered(_Config) ->
    tester:test(?MODULE, prop_is_covered, 2, 1000).

-spec(prop_is_covered2/2 :: (intervals:interval(), intervals:interval()) -> boolean()).
prop_is_covered2(_X, _Y) ->
    X = intervals:normalize(_X),
    Y = intervals:normalize(_Y),
    Z = intervals:cut(X, Y),
    % X as well as Y cover X \cup Y
    intervals:is_covered(Z, X) andalso intervals:is_covered(Z, Y).

tester_is_covered2(_Config) ->
    tester:test(?MODULE, prop_is_covered2, 2, 1000).

