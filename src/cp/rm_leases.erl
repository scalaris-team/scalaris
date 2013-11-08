% @copyright 2007-2013 Zuse Institute Berlin

%  Licensed under the Apache License, Version 2.0 (the "License");
%  you may not use this file except in compliance with the License.
%  You may obtain a copy of the License at
%
%      http://www.apache.org/licenses/LICENSE-2.0
%
%  Unless required by applicable law or agreed to in writing, software
%  distributed under the License is distributed on an "AS IS" BASIS,
%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%  See the License for the specific language governing permissions and
%  limitations under the License.

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Ring maintenance with leases.
%% @end
%% @version $$
-module(rm_leases).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).
-include("scalaris.hrl").
-include("record_helpers.hrl").

%% gen_component callbacks
-export([start_link/1, init/1, on/2]).

-record(op, {
          missing_range = ?required(op, missing_range) :: intervals:interval(),
          found_leases  = ?required(op, found_leases) :: list(l_on_cseq:lease_t())
         }).

-type op_t() :: #op{}.

-type state() :: op_t() | ok.

%% gen_component functions
%% @doc Starts the failure detector server
-spec start_link(pid_groups:groupname()) -> {ok, pid()} | ignore.
start_link(ServiceGroup) ->
    case config:read(leases) of
        true ->
            gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                                     [wait_for_init, {erlang_register, ?MODULE},
                                      {pid_groups_join_as, ServiceGroup, ?MODULE}]);
        _ ->
            ignore
    end.

%% @doc Initialises the module with an empty state.
-spec init([]) -> state().
init([]) ->
    FilterFun = fun (Old, New, Reason) ->
                        OldRange = nodelist:node_range(Old),
                        NewRange = nodelist:node_range(New),
                        %case OldRange =/= NewRange of
                        %    true ->
                        %        log:log("the range has changed: ~w -> ~w (~w)",
                        %                [OldRange, NewRange, Reason]);
                        %    false ->
                        %        ok
                        %end,
                        case Reason of
                            {slide_finished, _} ->
                                false;
                            {add_subscriber} ->
                                false;
                            {node_crashed, _} ->
                                OldRange =/= NewRange;
                            {node_discovery} ->
                                OldRange =/= NewRange;
                            {unknown} -> % @todo ?
                                false
                        end
                end,
    ExecFun = fun (Pid, _Tag, Old, New) ->
                      comm:send_local(Pid, {rm_change, nodelist:node_range(Old), nodelist:node_range(New)})
              end,
    rm_loop:subscribe(self(), ?MODULE, FilterFun, ExecFun, inf),
    ok.

%% @private
-spec on(comm:message(), state()) -> state().
on({rm_change, OldRange, NewRange}, State) ->
    log:log("the range has changed: ~w -> ~w", [OldRange, NewRange]),
    log:log("state: ~w", [State]),
    case State of
        ok ->
            MissingRange = intervals:minus(NewRange, OldRange),
            log:log("missing range: ~w", [MissingRange]),
            #op{missing_range = MissingRange, found_leases = []};
        _ ->
            State
    end;

on({merge_after_leave, NewLease, OldLease, Result}, State) ->
    log:log("merge after finish done: ~w", [Result]),
    State.




