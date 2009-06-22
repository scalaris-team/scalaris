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

-import(intervals).

-include_lib("unittest.hrl").

all() ->
    [new, is_empty, cut, tc1].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
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
    ?assert(intervals:cut(NotEmpty, NotEmpty) == NotEmpty),
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
