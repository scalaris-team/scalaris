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
-module(lease_ops).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id').

-include("scalaris.hrl").
-behaviour(gen_component).
-export([start_link/2, on/2, init/1]).

%% public API
-export([renew_lease/2, acquire_lease/2]).
-export([rack_validate/2, racc_validate/2]).

-ifdef(with_export_type_support).
-export_type([msg/0, state/0]).
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Todo:
%%
%% - define the contents of the owner field. It has to be a persistent
%%   pointer to my data_node? Move decision to leases module!
%% - what is the structure of learner_decide messages for qread resp. qwrite
%% - implement rack_validate/2 and racc_validate/2 for rbr-module
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% accepted messages of lease_ops processes
%-type msg() ::
%        data_node_leases:msg()
%      | data_node_db:msg()
%      | {?lookup_fin, ?RT:key(), comm:message()}.
%
-type msg() :: any().

-record(state, {
          %% state for embedded paxos learner...
          learner_state   :: pdb:table_id(),
          %% {lease_ops, Key} -> state map (progress)
          progress_state  :: pdb:table_id()
         }).
-type state() :: #state{}.

%% @doc message handler
-spec on(msg(), state()) -> state().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Lease Renewal
%%
%% Preconditions:
%% 1. there is no concurrent operation on the same key/lease id
%% 2. the lease has not beed merged with another range: L.aux != merged_with
%% 3. I am the owner of the current lease
%%
%% Steps:
%% 1. update timeout
%% 2. increment version
%% 3. qwrite(NewLease)
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({renew_lease, Lease, ClientPid} = _Msg,
   #state{progress_state = TableName} = State) ->
    Key              = leases:get_id(Lease),
    HasMergedWith    = has_merged(Lease),
    HasConcurrentOp  = has_concurrent_op(Lease, TableName),
    IamtheOwner      = leases:i_am_owner(Lease),
    if
        HasConcurrentOp ->
            comm:send(ClientPid, {renew_lease_response, Lease,
                                  {failed, have_concurrent_op}});
        HasMergedWith ->
            comm:send(ClientPid, {renew_lease_response, Lease,
                                  {failed, merged_with}});
        IamtheOwner ->
            NewLease = leases:inc_version(leases:update_timeout(Lease)),
            pdb:set({{lease_ops, Key}, renew_lease, Lease, NewLease,
                     ClientPid}, TableName),
            rbr:qwrite(comm:reply_as(comm:this(), 3,
                                     {renew_lease_qwrite_reply, Key, '_'}),
                       Key, NewLease);
        true ->
            comm:send(ClientPid, {renew_lease_response, Lease,
                                  {failed, unknown}})
    end,
    State;
%% does the learner_decide provide a version number/paxos id?
on({renew_lease_qwrite_reply, Key, LearnerResult} = _Msg,
   #state{progress_state = TableName} = State) ->
    PDBKey = {lease_ops, Key},
    {PDBKey, renew_lease, OldLease, NewLease, ClientPid} =
        pdb:get(PDBKey, TableName),
    pdb:delete(PDBKey, TableName),
    case LearnerResult of
        {learner_decide, _ClientCookie, NewLease} ->
            comm:send(ClientPid, {renew_lease_response, OldLease,
                                  ok}),
            data_node:update_lease_in_master_list(NewLease);
        {learner_decide, _ClientCookie, _Val} ->
            comm:send(ClientPid, {renew_lease_response, OldLease,
                                  {failed, unknown}});
        _ -> ups %% @todo
    end,
    State;

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Lease Acquire
%%
%% Preconditions for qwrite (second step):
%% 1. there is no concurrent operation on the same key/lease id
%% 2. the lease has not beed merged with another range: L.aux != merged_with
%% 3. the lease has timed out
%%
%% Steps:
%% 1. read current lease (qread)
%% 2. check whether we can acquire the lease
%% 3. set owner to self(), increase epoch, set version to 1, update timeout
%% 4. qwrite(NewLease)
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
on({acquire_lease, Key, ClientPid} = _Msg,
   #state{progress_state = _TableName} = State) ->
    ReplyTo = comm:reply_as(comm:this(), 4,
                            {acquire_lease_qread_reply, Key, ClientPid, '_'}),
    rbr:qread(ReplyTo, Key),
    State;
on({acquire_lease_qread_reply, Key, ClientPid, LearnerResult} = _Msg,
   #state{progress_state = TableName} = State) ->
    {learner_decide, _ClientCookie, Lease} = LearnerResult,
    HasMergedWith   = has_merged(Lease),
    HasConcurrentOp = has_concurrent_op(Lease, TableName),
    HasTimedOut     = has_timedout(Lease),
    if
        HasConcurrentOp ->
            comm:send(ClientPid, {acquire_lease_response, Lease,
                                  {failed, have_concurrent_op}});
        HasMergedWith ->
            comm:send(ClientPid, {acquire_lease_response, Lease,
                                  {failed, merged_with}});
        HasTimedOut ->
            NewLease = leases:update_timeout(leases:inc_epoch(leases:set_owner_to_self(Lease))),
            ReplyTo = comm:reply_as(comm:this(), 4,
                                    {acquire_lease_qwrite_reply, Key, ClientPid, '_'}),
            pdb:set({{lease_ops, Key}, acquire_lease, Lease, NewLease,
                     ClientPid}, TableName),
            rbr:qwrite(ReplyTo, Key, NewLease);
        true ->
            comm:send(ClientPid, {acquire_lease_response, Lease,
                                  {failed, unknown}})
    end,
    State;
on({acquire_lease_qwrite_reply, Key, LearnerResult} = _Msg,
   #state{progress_state = TableName} = State) ->
    PDBKey = {lease_ops, Key},
    {PDBKey, renew_lease, OldLease, NewLease, ClientPid} =
        pdb:get(PDBKey, TableName),
    pdb:delete(PDBKey, TableName),
    case LearnerResult of
        {learner_decide, _ClientCookie, NewLease} ->
            comm:send(ClientPid, {acquire_lease_response, OldLease,
                                  ok}),
            data_node:add_lease_to_master_list(NewLease);
        {learner_decide, _ClientCookie, _Val} ->
            comm:send(ClientPid, {acquire_lease_response, OldLease,
                                  {failed, unknown}});
        _ -> ups %% @todo
    end,
    State.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% initialization
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc initializes the state and calls the main loop
-spec init(Options::[tuple()]) -> state().
init(_Options) ->
    #state{learner_state  = pdb:new(list_to_atom(atom_to_list(?MODULE) ++ "_learner"),  [set, private]),
           progress_state = pdb:new(list_to_atom(atom_to_list(?MODULE) ++ "_progress"), [set, private])}.

%% @doc spawns a scalaris node, called by the scalaris supervisor process
-spec start_link(pid_groups:groupname(), [tuple()]) -> {ok, pid()}.
start_link(DataNodeGroup, Options) ->
    gen_component:start_link(
      ?MODULE, fun ?MODULE:on/2, Options,
      [{pid_groups_join_as, DataNodeGroup, lease_ops}, wait_for_init]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% public API
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% @doc renew a lease
-spec renew_lease(leases:lease(), comm:mypid()) -> ok.
renew_lease(Lease, ClientPid) ->
    LeaseOps = pid_groups:get_my(lease_ops),
    comm:send_local(LeaseOps, {renew_lease, Lease, ClientPid}).

%% @doc acquire a lease
-spec acquire_lease(rt_beh:key(), comm:mypid()) -> ok.
acquire_lease(Key, ClientPid) ->
    LeaseOps = pid_groups:get_my(lease_ops),
    comm:send_local(LeaseOps, {acquire_lease, Key, ClientPid}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% validate functions for round-based register
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec rack_validate(leases:lease(), leases:lease()) -> boolean().
rack_validate(_CurrentLease, _NewLease) ->
    true.

-spec racc_validate(leases:lease(), leases:lease()) -> boolean().
racc_validate(CurrentLease, NewLease) ->
    rack_validate(CurrentLease, NewLease).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% internal helpers
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

has_concurrent_op(Lease, TableName) ->
    undefined == pdb:get({lease_ops, leases:get_id(Lease)}, TableName).

-spec has_merged(leases:lease()) -> boolean().
has_merged(Lease) ->
    merged_with == element(1, leases:get_aux(Lease)).

-spec has_timedout(leases:lease()) -> boolean().
has_timedout(Lease) ->
    leases:is_valid(Lease).

%    rbr:qread(ClientPid, Key)
%    rbr:qwrite(ClientPid, Key, Version, Value)
%
%        add_lease_to_master_list(Lease)
%        delete_lease_from_master_list(Lease)
%        update_lease_in_master_list(Lease)
%

