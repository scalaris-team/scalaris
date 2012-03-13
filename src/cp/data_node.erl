%  @copyright 2007-2012 Zuse Institute Berlin

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
%% @doc    data_node main file
%% @end
%% @version $Id$
-module(data_node).
-author('schuett@zib.de').
-vsn('$Id').

-include("scalaris.hrl").
-behaviour(gen_component).

-export([start_link/2, on/2, init/1]).

-ifdef(with_export_type_support).
-export_type([message/0]).
-endif.

% accepted messages of data_node processes
-type message() :: any().

%===============================================================================
%
% on handler
%
%===============================================================================

%% @doc message handler
-spec on(message(), data_node_state:state()) -> data_node_state:state() | kill.

on({read, SourcePid, SourceId, HashedKey}, State) ->
    % do not need to check lease
    Value = data_node_state:db_read(HashedKey),
    Msg = {read_reply, SourceId, HashedKey, Value},
    comm:send(SourcePid, Msg),
    State;

% messages concerning leases
on({lease_message, LeaseMessage}, State) ->
    data_node_lease:on(LeaseMessage, State);

on({lookup_fin, Key, Message}, State) ->
    case data_node_state:is_owner(State, Key) of
        true ->
            on(Message, State);
        false ->
            case pid_groups:get_my(router_node) of
                failed ->
                    ok;
                Pid ->
                    comm:send_local(Pid, {lookup_aux, Key, Message})
            end,
            State
    end.

%===============================================================================
%
% init and misc.
%
%===============================================================================

%% @doc joins this node in the ring and calls the main loop
-spec init(Options::[tuple()])
        -> dht_node_state:state() |
           {'$gen_component', [{on_handler, Handler::gen_component:handler()}],
            State::dht_node_join:join_state()}.
init(Options) ->
    true = is_first(Options),
    data_node_join:join_as_first(Options).

%% @doc spawns a scalaris node, called by the scalaris supervisor process
-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(DHTNodeGroup, Options) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, Options,
                             [{pid_groups_join_as, DHTNodeGroup, data_node},
                              wait_for_init]).

%% @doc Checks whether this VM is marked as first, e.g. in a unit test, and
%%      this is the first node in this VM.
-spec is_first([tuple()]) -> boolean().
is_first(Options) ->
    lists:member({first}, Options) andalso admin_first:is_first_vm().
