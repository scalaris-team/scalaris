% @copyright 2018 Zuse Institute Berlin

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
%% @doc common test runner
-module(runner).
-author('schuett@zib.de').

-export([run_spec/1, run_suite/1, run_suite_group/2, run_suite_case/2]).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec run_spec(string()) -> term().
run_spec(Spec) ->
    %% ct:pal("~s~n", [Spec]),
    ct:run_test([
                 {spec, [Spec]},
                 {ct_hooks, [scalaris_cth]}
                ]
               ).

-spec run_suite(string()) -> term().
run_suite(Suite) ->
    %% ct:pal("~s~n", [Suite]),
    ct:run_test([
                 {suite, [Suite]},
                 {ct_hooks, [scalaris_cth]}
                ]
               ).

-spec run_suite_group(string(), string()) -> term().
run_suite_group(Suite, Group) ->
    %% ct:pal("~s:~s~n", [Suite, Group]),
    ct:run_test([
                 {suite, [Suite]},
                 {group, [Group]},
                 {ct_hooks, [scalaris_cth]}
                ]
               ).

-spec run_suite_case(string(), string()) -> term().
run_suite_case(Suite, Case) ->
    %% ct:pal("~s:~s~n", [Suite, Case]),
    ct:run_test([
                 {suite, [Suite]},
                 {testcase, [Case]},
                 {ct_hooks, [scalaris_cth]}
                ]
               ).

