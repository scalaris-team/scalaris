%  @copyright 2011 Zuse Institute Berlin

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
%% @doc    Periodically executes a small benchmark to monitor the overall
%%         performance of Scalaris.
%% @end
%% @version $Id$
-module(monitor_perf).
-author('kruber@zib.de').
-vsn('$Id$ ').

-behaviour(gen_component).

-include("scalaris.hrl").

% monitor process functions
-export([start_link/1, init/1, on/2, check_config/0]).

-type state() :: null.
-type message() :: {bench}.

-spec run_bench() -> ok.
run_bench() ->
    Key1 = randoms:getRandomId(),
    Key2 = randoms:getRandomId(),
    ReqList = [{read, Key1}, {read, Key2}, {commit}],
    {TimeInUs, _Result} = util:tc(fun api_tx:req_list/1, [ReqList]),
    monitor:proc_set_value(
      ?MODULE, "read_read",
      fun(Old) ->
              Old2 = case Old of
                         % 1m monitoring interval, only keep newest
                         undefined -> rrd:create(60 * 1000000, 1, timing);
                         _ -> Old
                     end,
              rrd:add_now(TimeInUs, Old2)
      end).

%% @doc Message handler when the rm_loop module is fully initialized.
-spec on(message(), state()) -> state().
on({bench}, State) ->
    msg_delay:send_local(get_bench_interval(), self(), {bench}),
    run_bench(),
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Startup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts the monitor process, registers it with the process dictionary
%%      and returns its pid for use by a supervisor.
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, null,
                             [{pid_groups_join_as, DHTNodeGroup, monitor_perf}]).

%% @doc Initialises the module with an empty state.
-spec init(null) -> state().
init(null) ->
    FirstDelay = randoms:rand_uniform(1, get_bench_interval() + 1),
    msg_delay:send_local(FirstDelay, self(), {bench}),
    null.

%% @doc Checks whether config parameters of the rm_tman process exist and are
%%      valid.
-spec check_config() -> boolean().
check_config() ->
    config:is_integer(monitor_perf_interval) and
    config:is_greater_than(monitor_perf_interval, 0).

-spec get_bench_interval() -> pos_integer().
get_bench_interval() ->
    config:read(monitor_perf_interval).
