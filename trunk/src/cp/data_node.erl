%  @copyright 2012 Zuse Institute Berlin

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
%% @author Florian Schintke <schintke@zib.de>
%% @doc    data_node main file
%% @end
%% @version $Id$
-module(data_node).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id').

-include("scalaris.hrl").
-behaviour(gen_component).
-export([start_link/2, on/2, init/1]).

%% handling state of a data node
-export([new_state/0,
         get_leases/1, set_leases/2,
         get_db/1,
         get_rlease_mgmt/1, set_rlease_mgmt/2,
         % operations on the lease list
         add_lease_to_master_list/1, delete_lease_from_master_list/1,
         update_lease_in_master_list/1]).

-ifdef(with_export_type_support).
-export_type([msg/0, state/0]).
-endif.

% accepted messages of data_node processes
-type msg() ::
        data_node_leases:msg()
      | data_node_db:msg()
      | {?lookup_fin, ?RT:key(), comm:message()}.

-record(state, {
          %% my leases for working...
          leases   :: data_node_leases:state(),
          %% my database for user content
          db       :: data_node_db:state(),
          %% replicated lease storage and management of the lease_db
          rlease_mgmt :: rlease_mgmt:state()
         }).
-type state() :: #state{}.

%% @doc message handler
-spec on(msg(), state()) -> state().

%% messages concerning leases
on({lease, _LeaseMsg} = Msg, State) ->
    data_node_leases:on(Msg, State);

%% messages concerining db access
on({read, _SourcePid, _SourceId, _HashedKey} = Msg, State) ->
    % do not need to check lease
    data_node_db:on(Msg, State);

%% messages concerning routing
on({?lookup_fin, Key, Msg}, State) ->
    case data_node_leases:is_owner(State, Key) of
        true ->
            gen_component:post_op(State, Msg);
        false ->
            io:format("I am not owner~n"),
            case pid_groups:get_my(router_node) of
                failed -> ok; %% TODO: Delete this case, should not happen.
                Pid -> comm:send_local(Pid, {?lookup_aux, Key, Msg})
            end,
            State
    end.


%% @doc joins this node in the ring and calls the main loop
-spec init(Options::[tuple()]) -> state().
init(Options) ->
    true = is_first(Options),
    State = new_state(),
    data_node_join:join_as_first(State, Options).

%% @doc spawns a scalaris node, called by the scalaris supervisor process
-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(DataNodeGroup, Options) ->
    gen_component:start_link(
      ?MODULE, fun ?MODULE:on/2, Options,
      [{pid_groups_join_as, DataNodeGroup, data_node}, wait_for_init]).

%% @doc Checks whether this VM is marked as first, e.g. in a unit test, and
%%      this is the first node in this VM.
-spec is_first([tuple()]) -> boolean().
is_first(Options) ->
    lists:member({first}, Options) andalso admin_first:is_first_vm().


%% operations on the state
-spec new_state() -> state().
new_state() ->
    #state{db  = data_node_db:new_state(),
           leases = data_node_leases:new_state(),
           rlease_mgmt = rlease_mgmt:new_state()}.

-spec get_leases(state()) -> data_node_leases:state().
get_leases(#state{leases=Leases}) -> Leases.
-spec set_leases(state(), data_node_leases:state()) -> state().
set_leases(State, LState) -> State#state{leases = LState}.
-spec get_db(state()) -> data_node_db:state().
get_db(#state{db=DB}) -> DB.
-spec get_rlease_mgmt(state()) -> rlease_mgmt:state().
get_rlease_mgmt(#state{rlease_mgmt=RLMState}) -> RLMState.
-spec set_rlease_mgmt(state(), rlease_mgmt:state()) -> state().
set_rlease_mgmt(State, RLMState) -> State#state{rlease_mgmt = RLMState}.

%% operations on the lease list
add_lease_to_master_list(Lease) ->
    DataNode = pid_groups:get_my(data_node),
    comm:send_local(DataNode, {add_lease_to_master_list, Lease}).

delete_lease_from_master_list(Lease) ->
    DataNode = pid_groups:get_my(data_node),
    comm:send_local(DataNode, {delete_lease_from_master_list, Lease}).

update_lease_in_master_list(Lease) ->
    DataNode = pid_groups:get_my(data_node),
    comm:send_local(DataNode, {update_lease_in_master_list, Lease}).
