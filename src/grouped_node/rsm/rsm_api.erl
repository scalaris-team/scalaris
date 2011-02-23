%  @copyright 2007-2011 Zuse Institute Berlin

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
%% @doc    Replicated-state machine API
%% @end
%% @version $Id$
-module(rsm_api).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([start_link/3,
         deliver/2,
         get_single_state/1]).

-type(start_option() :: {join, rsm_state:rsm_pid_type()} | first).

-spec start_link(pid_groups:groupname(), start_option(), module()) -> {ok, pid()}.
start_link(GroupName, Options, AppModule) ->
    sup_rsm_node:start_link(GroupName, Options, AppModule).

%% @doc send a message to one node of a group and ask him to deliver
%% the message to the rsm. Note, that the rsm node could be dead. In
%% this case, the message gets lost.
-spec deliver(any(), rsm_state:rsm_pid_type()) -> any().
deliver(Message, RSMPid) when is_list(RSMPid) ->
    comm:send(hd(RSMPid), {deliver_to_rsm_request, Message, comm:this()});
deliver(Message, RSMPid) ->
    comm:send(RSMPid, {deliver_to_rsm_request, Message, comm:this()}).

-spec get_single_state(comm:mypid()) -> {rsm_view:view_type(), any()}.
get_single_state(Pid) ->
    comm:send(Pid, {rsm_get_single_state, comm:this()}),
    receive
        {rsm_get_single_state_response, View, AppState} ->
            {View, AppState}
    end.
