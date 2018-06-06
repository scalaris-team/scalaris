%% @copyright 2018 Zuse Institute Berlin

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
%% @doc JSON API for talking to Scalaris.
-module(api_jsonclient).
-author('schuett@zib.de').

% for api_json:
-export([run_benchmark_read/0, run_benchmark_incr/0]).

-include("scalaris.hrl").
-include("client_types.hrl").

-spec run_benchmark_incr() -> {struct, [{Key::atom(), Value::term()}]}.
run_benchmark_incr() ->
    {ok, IncrResult} = bench:increment_o(10, 500, []),
    bench_json_helper:result_to_json(IncrResult).

-spec run_benchmark_read() -> {struct, [{Key::atom(), Value::term()}]}.
run_benchmark_read() ->
    {ok, ReadResult} = bench:quorum_read_o(10, 500, []),
    bench_json_helper:result_to_json(ReadResult).
