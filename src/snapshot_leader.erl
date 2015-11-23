%  @copyright 2012-2015 Zuse Institute Berlin

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

%% @author Stefan Keidel <keidel@informatik.hu-berlin.de>
%% @doc gen_component for (potential) leader of the snapshot algorithm
%% @version $Id$
-module(snapshot_leader).
-author('keidel@informatik.hu-berlin.de').
-vsn('$Id$').

%-define(TRACE(X, Y), io:format(X, Y)).
-define(TRACE(_X, _Y), ok).
-behaviour(gen_component).

%% functions for gen_component module and supervisor callbacks
-export([start_link/1, on/2, init/1]).

-include("scalaris.hrl").

% accepted messages of the snapshot_leader process
-type init_message() :: {init_snapshot, Client::comm:erl_local_pid()}.

-type result_message() :: {local_snapshot_done, From::comm:erl_local_pid(),
                           SnapNumber::non_neg_integer(),
                           DBRange::intervals:interval(),
                           Snapshot::db_dht:db_as_list()} |
    {local_snapshot_failed, From::comm:erl_local_pid(),
     SnapNumber::non_neg_integer(), Msg::string()}.

-type message() :: init_message() | result_message().

-export_type([result_message/0]).

-include("gen_component.hrl").

%% be startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2,
                             [], % parameters passed to init
                             [{pid_groups_join_as, DHTNodeGroup, snapshot_leader}]).

%% initialize: return initial state.
-spec init([]) -> snapshot_leader_state:state().
init([]) ->
    snapshot_leader_state:new().

-spec on(message(), snapshot_leader_state:state()) -> snapshot_leader_state:state().

on({init_snapshot, Client}, State) ->
    NewSnapNum = snapshot_leader_state:get_number(State) + 1,
    ?TRACE("snapshot_leader: init_snapshot with number ~p~n", [NewSnapNum]),
    % send init_snapshot to all dht_nodes
    bulkowner:issue_bulk_owner(uid:get_global_uid(), intervals:all(), {do_snapshot, NewSnapNum, comm:this()}),
    snapshot_leader_state:new(NewSnapNum, true, Client, State);

% TODO: too much redundant code below -> break this up into several functions

on({local_snapshot_done, _From, SnapNumber, Range, Snapshot}, State) ->
    case (snapshot_leader_state:is_in_progress(State)
         andalso SnapNumber =:= snapshot_leader_state:get_number(State)) of
        true ->
            ?TRACE("snapshot_leader: local_snapshot_done ~p from ~p for range ~p~n",
                   [SnapNumber, From, Range]),
            TmpState = snapshot_leader_state:add_interval(State, Range),
            NewState = snapshot_leader_state:add_snapshot(TmpState, Snapshot),
            case snapshot_leader_state:interval_union_is_all(NewState) of
                true -> % snapshot done, message client and "reset" local state
                    Data = snapshot_leader_state:get_global_snapshot(NewState),
                    ?TRACE("snapshot_leader: snapshot ~p is done. sending
                           data...~n", [SnapNumber]),
                    ErrorInterval = snapshot_leader_state:get_error_interval(NewState),
                    case intervals:is_empty(ErrorInterval) of
                        true ->
                            comm:send(snapshot_leader_state:get_client(NewState),
                                      {global_snapshot_done, Data});
                        _ ->
                            comm:send(snapshot_leader_state:get_client(NewState),
                                      {global_snapshot_done_with_errors, ErrorInterval, Data})
                    end,
                    snapshot_leader_state:new(SnapNumber, false, false, NewState);
                false ->
                    NewState
            end;
        false -> % late/random snapshot_done message -> ignore
            %% ?TRACE("snapshot_leader: local_snapshot_done ~p but stale ~n",
            %%        [SnapNumber]),
            State
    end;

on({local_snapshot_failed, _From, SnapNumber, Range, _Msg}, State) ->
    case (snapshot_leader_state:is_in_progress(State)
         andalso SnapNumber =:= snapshot_leader_state:get_number(State)) of
        true ->
            NewState = snapshot_leader_state:add_error_interval(State, Range),
            case snapshot_leader_state:interval_union_is_all(NewState) of
                true -> % snapshot done, message client and "reset" local state
                    Data = snapshot_leader_state:get_global_snapshot(NewState),
                    ?TRACE("snapshot_leader: Snapshot ~p failed sending data...~n", [SnapNumber]),
                    ErrorInterval = snapshot_leader_state:get_error_interval(NewState),
                    case intervals:is_empty(ErrorInterval) of
                        true ->
                            comm:send(snapshot_leader_state:get_client(NewState),
                                      {global_snapshot_done, Data});
                        _ ->
                            comm:send(snapshot_leader_state:get_client(NewState),
                                      {global_snapshot_done_with_errors, ErrorInterval, Data})
                    end,
                    snapshot_leader_state:new(SnapNumber, false, false, NewState);
                false ->
                    NewState
            end;
        false -> % late/random snapshot_failed message
            State
    end;

on(_, _) ->
    unknown_event.
