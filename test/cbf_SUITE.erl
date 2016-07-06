%  @copyright 2016 Zuse Institute Berlin

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
%% @author Maik Lange <MLange@informatik.hu-berlin.de>
%% @doc    Tests for bloom filter module.
%% @end
%% @version $Id$
-module(cbf_SUITE).
-author('kruber@zib.de').
-author('mlange@informatik.hu-berlin.de').

-define(BLOOM, cbf).
-define(Fpr_Test_NumTests, 25).

-include("bloom_SUITE.hrl").

all() -> [
          tester_p_add_list,
          tester_add,
          tester_add_list,
          tester_join,
          tester_equals
          %tester_fpr
          %eprof
          %fprof
         ].

suite() ->
    [
     {timetrap, {seconds, 45}}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.
