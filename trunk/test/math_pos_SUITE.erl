%  @copyright 2011 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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
%% @doc    Test suite for the math_pos module.
%% @end
%% @version $Id$
-module(math_pos_SUITE).
-author('kruber@zib.de').
-vsn('$Id$ ').

-compile(export_all).

-include("unittest.hrl").
-include("scalaris.hrl").

all() ->
    [plus, minus, multiply, divide,
     tester_make_same_length, tester_plus, tester_minus].

suite() ->
    [
     {timetrap, {seconds, 10}}
    ].

init_per_suite(Config) ->
    unittest_helper:init_per_suite(Config).

end_per_suite(Config) ->
    unittest_helper:end_per_suite(Config),
    ok.

plus(_Config) ->
    ?equals(math_pos:plus(  [1],   [1], 10),   [2]),
    ?equals(math_pos:plus(  [1],   [2], 10),   [3]),
    ?equals(math_pos:plus(  [1],   [9], 10),   [0]),
    ?equals(math_pos:plus([0,1], [0,9], 10), [1,0]),
    ?equals(math_pos:plus(  [2],   [1], 10),   [3]),
    ?equals(math_pos:plus(  [9],   [1], 10),   [0]),
    ?equals(math_pos:plus([0,9], [0,1], 10), [1,0]).

minus(_Config) ->
    ?equals(math_pos:minus(  [1],   [1], 10),   [0]),
    ?equals(math_pos:minus(  [1],   [2], 10),   [9]),
    ?equals(math_pos:minus(  [1],   [9], 10),   [2]),
    ?equals(math_pos:minus([0,1], [0,9], 10), [9,2]),
    ?equals(math_pos:minus(  [2],   [1], 10),   [1]),
    ?equals(math_pos:minus(  [9],   [1], 10),   [8]),
    ?equals(math_pos:minus([0,9], [0,1], 10), [0,8]).

multiply(_Config) ->
    ?equals(math_pos:multiply(  [1], 2, 10),   [2]),
    ?equals(math_pos:multiply(  [2], 2, 10),   [4]),
    ?equals(math_pos:multiply(  [3], 2, 10),   [6]),
    ?equals(math_pos:multiply(  [4], 2, 10),   [8]),
    ?equals(math_pos:multiply(  [5], 2, 10),   [0]),
    ?equals(math_pos:multiply(  [6], 2, 10),   [2]),
    ?equals(math_pos:multiply(  [7], 2, 10),   [4]),
    ?equals(math_pos:multiply([0,1], 2, 10), [0,2]),
    ?equals(math_pos:multiply([0,2], 2, 10), [0,4]),
    ?equals(math_pos:multiply([0,3], 2, 10), [0,6]),
    ?equals(math_pos:multiply([0,4], 2, 10), [0,8]),
    ?equals(math_pos:multiply([0,5], 2, 10), [1,0]),
    ?equals(math_pos:multiply([0,6], 2, 10), [1,2]),
    ?equals(math_pos:multiply([0,7], 2, 10), [1,4]),
    
    ?equals(math_pos:multiply(  [1], 3, 10),   [3]),
    ?equals(math_pos:multiply(  [2], 3, 10),   [6]),
    ?equals(math_pos:multiply(  [3], 3, 10),   [9]),
    ?equals(math_pos:multiply(  [4], 3, 10),   [2]),
    ?equals(math_pos:multiply(  [5], 3, 10),   [5]),
    ?equals(math_pos:multiply(  [6], 3, 10),   [8]),
    ?equals(math_pos:multiply(  [7], 3, 10),   [1]),
    ?equals(math_pos:multiply([0,1], 3, 10), [0,3]),
    ?equals(math_pos:multiply([0,2], 3, 10), [0,6]),
    ?equals(math_pos:multiply([0,3], 3, 10), [0,9]),
    ?equals(math_pos:multiply([0,4], 3, 10), [1,2]),
    ?equals(math_pos:multiply([0,5], 3, 10), [1,5]),
    ?equals(math_pos:multiply([0,6], 3, 10), [1,8]),
    ?equals(math_pos:multiply([0,7], 3, 10), [2,1]).

divide(_Config) ->
    ?equals(math_pos:divide(  [1], 2, 10),   [0]),
    ?equals(math_pos:divide(  [2], 2, 10),   [1]),
    ?equals(math_pos:divide(  [3], 2, 10),   [1]),
    ?equals(math_pos:divide(  [4], 2, 10),   [2]),
    ?equals(math_pos:divide([1,0], 2, 10), [0,5]),
    ?equals(math_pos:divide([1,1], 2, 10), [0,5]),
    ?equals(math_pos:divide([1,2], 2, 10), [0,6]),
    ?equals(math_pos:divide([1,3], 2, 10), [0,6]),
    ?equals(math_pos:divide([1,4], 2, 10), [0,7]),
    ?equals(math_pos:divide([2,0], 2, 10), [1,0]),
    ?equals(math_pos:divide([2,1], 2, 10), [1,0]),
    ?equals(math_pos:divide([2,2], 2, 10), [1,1]),
    ?equals(math_pos:divide([2,3], 2, 10), [1,1]),
    ?equals(math_pos:divide([2,4], 2, 10), [1,2]),
    
    ?equals(math_pos:divide(  [1], 3, 10),   [0]),
    ?equals(math_pos:divide(  [2], 3, 10),   [0]),
    ?equals(math_pos:divide(  [3], 3, 10),   [1]),
    ?equals(math_pos:divide(  [4], 3, 10),   [1]),
    ?equals(math_pos:divide([1,0], 3, 10), [0,3]),
    ?equals(math_pos:divide([1,1], 3, 10), [0,3]),
    ?equals(math_pos:divide([1,2], 3, 10), [0,4]),
    ?equals(math_pos:divide([1,3], 3, 10), [0,4]),
    ?equals(math_pos:divide([1,4], 3, 10), [0,4]),
    ?equals(math_pos:divide([2,0], 3, 10), [0,6]),
    ?equals(math_pos:divide([2,1], 3, 10), [0,7]),
    ?equals(math_pos:divide([3,0], 3, 10), [1,0]),
    ?equals(math_pos:divide([3,1], 3, 10), [1,0]),
    ?equals(math_pos:divide([3,2], 3, 10), [1,0]),
    ?equals(math_pos:divide([3,3], 3, 10), [1,1]),
    ?equals(math_pos:divide([3,4], 3, 10), [1,1]),
    ?equals(math_pos:divide([3,5], 3, 10), [1,1]),
    ?equals(math_pos:divide([3,6], 3, 10), [1,2]).

-spec prop_make_same_length(A::string(), B::string(), front | back) -> true.
prop_make_same_length(A, B, Pos) ->
    {A1, B1} = math_pos:make_same_length(A, B, Pos),
    ?equals(erlang:length(A1), erlang:length(B1)),
    ?equals(math_pos:remove_zeros(A1, Pos), math_pos:remove_zeros(A, Pos)),
    ?equals(math_pos:remove_zeros(B1, Pos), math_pos:remove_zeros(B, Pos)),
    true.

tester_make_same_length(_Config) ->
    tester:test(?MODULE, prop_make_same_length, 3, 10000).

-spec prop_plus_symm1(A::[0..9], B::[0..9]) -> true.
prop_plus_symm1(A_, B_) ->
    {A, B} = math_pos:make_same_length(A_, B_, front),
    ?equals(math_pos:plus(A, B, 10), math_pos:plus(B, A, 10)),
    true.

-spec prop_plus_symm2(A::string(), B::string()) -> true.
prop_plus_symm2(A_, B_) ->
    {A, B} = math_pos:make_same_length(A_, B_, back),
    ?equals(math_pos:plus(A, B, 16#10ffff + 1), math_pos:plus(B, A, 16#10ffff + 1)),
    true.

-spec prop_plus_symm3(A::nonempty_string(), B::nonempty_string()) -> true.
prop_plus_symm3(A_, B_) ->
    Base = erlang:max(lists:max(A_), lists:max(B_)) + 1,
    {A, B} = math_pos:make_same_length(A_, B_, back),
    ?equals(math_pos:plus(A, B, Base), math_pos:plus(B, A, Base)),
    true.

tester_plus(_Config) ->
    tester:test(?MODULE, prop_plus_symm1, 2, 10000),
    tester:test(?MODULE, prop_plus_symm2, 2, 10000),
    tester:test(?MODULE, prop_plus_symm3, 2, 10000).

-spec prop_minus1(A::[0..9], B::[0..9]) -> true.
prop_minus1(A_, B_) ->
    {A, B} = math_pos:make_same_length(A_, B_, back),
    A_B = math_pos:minus(A, B, 10),
    ?equals(math_pos:plus(A_B, B, 10), A),
    true.

-spec prop_minus2(A::string(), B::string()) -> true.
prop_minus2(A_, B_) ->
    {A, B} = math_pos:make_same_length(A_, B_, back),
    A_B = math_pos:minus(A, B, 16#10ffff + 1),
    ?equals(math_pos:plus(A_B, B, 16#10ffff + 1), A),
    true.

-spec prop_minus3(A::nonempty_string(), B::nonempty_string()) -> true.
prop_minus3(A_, B_) ->
    Base = erlang:max(lists:max(A_), lists:max(B_)) + 1,
    {A, B} = math_pos:make_same_length(A_, B_, back),
    A_B = math_pos:minus(A, B, Base),
    ?equals(math_pos:plus(A_B, B, Base), A),
    true.

tester_minus(_Config) ->
    tester:test(?MODULE, prop_minus1, 2, 10000),
    tester:test(?MODULE, prop_minus2, 2, 10000),
    tester:test(?MODULE, prop_minus3, 2, 10000).
