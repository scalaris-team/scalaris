% @copyright 2007-2016 Zuse Institute Berlin

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

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).

-include("scalaris.hrl").
-include("record_helpers.hrl").

%% gen_component callbacks
-export([start_link/1, init/1, on/2]).

%% rm subscriptions
-export([rm_filter/3, rm_exec/5]).

% for unit tests
-export([get_takeovers/1]).

-include("gen_component.hrl").

-record(state, {
          takeovers     = ?required(state, takeovers) :: gb_trees:tree(?RT:key(),
                                                                       l_on_cseq:lease_t())
         }).

-type state() :: #state{}.

%% gen_component functions
%% @doc Starts the ring maintenence process
-spec start_link(pid_groups:groupname()) -> {ok, pid()} | ignore.
start_link(DHTNodeGroup) ->
    case config:read(leases) of
        true ->
            gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                                     [{wait_for_init}, {erlang_register, ?MODULE},
                                      {pid_groups_join_as, DHTNodeGroup, ?MODULE}]);
        _ ->
            ignore
    end.

%% @doc Initialises the module with an empty state.
-spec init([]) -> state().
init([]) ->
    rm_loop:subscribe(self(), ?MODULE,
                      fun ?MODULE:rm_filter/3,
                      fun ?MODULE:rm_exec/5, inf),
    #state{
       takeovers=gb_trees:empty()
      }.

-spec rm_filter(OldNeighbors::nodelist:neighborhood(),
                NewNeighbors::nodelist:neighborhood(),
                IsSlide::rm_loop:reason()) -> boolean().
rm_filter(Old, New, Reason) ->
    OldRange = nodelist:node_range(Old),
    NewRange = nodelist:node_range(New),
    case Reason of
        {slide_finished, _} ->
            false;
        {add_subscriber} ->
            false;
        {node_crashed, _} ->
            OldRange =/= NewRange;
        {node_discovery} ->
            OldRange =/= NewRange;
        {graceful_leave, _Node} -> % @todo ?
            false;
        {update_id_failed} -> % @todo ?
            false;
        {unknown} -> % @todo ?
            false
    end.

-spec rm_exec(pid(), Tag::?MODULE,
              OldNeighbors::nodelist:neighborhood(),
              NewNeighbors::nodelist:neighborhood(),
              Reason::rm_loop:reason()) -> ok.
rm_exec(Pid, _Tag, Old, New, _Reason) ->
  comm:send_local(Pid, {rm_change, nodelist:node_range(Old),
                        nodelist:node_range(New)}).

%% @private
-spec on(comm:message(), state()) -> state().
on({rm_change, OldRange, NewRange}, State) ->
    ?TRACE("the range has changed: ~w -> ~w", [OldRange, NewRange]),
    ?TRACE("state: ~w", [State]),
    This = comm:reply_as(comm:this(), 4, {compare_and_fix, OldRange, NewRange, '_'}),
    comm:send_local(pid_groups:get_my(dht_node), {get_state, This, [lease_list, my_range]}),
    State;

on({compare_and_fix, OldRange, NewRange,
    {get_state_response, [{lease_list, L}, {my_range, Range}]}}, State) ->
    compare_and_fix_rm_with_leases(State, OldRange, NewRange, L, Range);

on({read_after_rm_change, _MissingRange, Result}, State) ->
    ?TRACE("read_after_rm_change ~w", [Result]),
    case Result of
        {qread_done, _ReqId, _Round, _OldWriteRound, Lease} ->
            LeaseId = l_on_cseq:get_id(Lease),
            Pid = comm:reply_as(self(), 4, {takeover_after_rm_change, LeaseId, Lease, '_'}),
            %% log:log("rm_leases(~p): going to take over ~p~n", [self(), Lease]),
            l_on_cseq:lease_takeover(Lease, Pid),
            add_takeover(State, Lease);
        _ ->
            log:log("not so well-formed qread-response"),
            State
    end;

on({takeover_after_rm_change, LeaseId, _Lease, Result}, State) ->
    ?TRACE("takeover_after_rm_change ~w", [Result]),
    case Result of
        {takeover, failed, L, Error} ->
            case Error of
                {content_check_failed,lease_is_still_valid} ->
                    case is_current_takeover(State, L) of
                        {value, L2} ->
                            case l_on_cseq:get_timeout(L) =:= l_on_cseq:get_timeout(L2) of
                                true ->
                                    ?TRACE("retry ~s", [l_on_cseq:get_pretty_timeout(L)]),
                                    LeaseTimeout = l_on_cseq:get_timeout(L),
                                    Pid = comm:reply_as(self(), 4, {takeover_after_rm_change, LeaseId, L, '_'}),
                                    WaitTime = timer:now_diff(LeaseTimeout, os:timestamp()),
                                    ?TRACE("retry ~s ~w", [l_on_cseq:get_pretty_timeout(L), WaitTime]),
                                    case WaitTime < 500*1000 of
                                        true ->
                                            l_on_cseq:lease_takeover(L, Pid);
                                        false ->
                                            PostponeBy = trunc(0.5 + WaitTime / (1000*1000)),
                                            ?TRACE("delaying takeover by ~ws", [PostponeBy]),
                                            l_on_cseq:lease_takeover_after(PostponeBy, L, Pid)
                                    end,
                                    State;
                                false ->
                                    propose_new_neighbors(l_on_cseq:get_owner(L)),
                                    remove_takeover(State, LeaseId, L)
                            end;
                        none ->
                            propose_new_neighbors(l_on_cseq:get_owner(L)),
                            remove_takeover(State, LeaseId, L)
                    end;
                _ ->
                    case L of
                        prbr_bottom ->
                            ok;
                        _ ->
                            propose_new_neighbors(l_on_cseq:get_owner(L))
                    end,
                    %%log:log("unknown error in takeover_after_rm_change ~w", [Error]),
                    remove_takeover(State, LeaseId, L)
                end;
        {takeover, success, L2} ->
            ?TRACE("takeover_after_rm_change success", []),
            This = comm:reply_as(comm:this(), 5, {takeover_success, get_state, L2, LeaseId, '_'}),
            comm:send_local(pid_groups:get_my(dht_node), {get_state, This, lease_list}),
            State
    end;

on({takeover_success, get_state, L2, LeaseId, {get_state_response, LeaseList}}, State) ->
    ActiveLease = lease_list:get_active_lease(LeaseList),
    Pid = comm:reply_as(self(), 4, {merge_after_rm_change, L2, ActiveLease, '_'}),
    %% log:log("rm_leases: merging ~p and ~p~n", [L2, ActiveLease]),
    l_on_cseq:lease_merge(L2, ActiveLease, Pid),
    remove_takeover(State, LeaseId, L2);

on({merge_after_rm_change, _L2, _ActiveLease, Result}, State) ->
    ?TRACE("merge after rm_change: ~w", [Result]),
    case Result of
        {merge, success, __L2, _L1} ->
            State;
        {merge, fail, _Reason, _Step, __L1, __L2, _Current, _Next} ->
            %% @todo
            State
    end;

on({merge_after_leave, _NewLease, _OldLease, _Result}, State) ->
    ?TRACE("merge after finish done: ~w", [_Result]),
    State;

on({get_node_for_new_neighbor, {get_state_response, Node}}, State) ->
    rm_loop:propose_new_neighbors([Node]),
    State;

on({get_takeovers, Pid}, #state{takeovers=Takeovers} = State) ->
    comm:send(Pid, {get_takeovers_response, Takeovers}),
    State.

-spec compare_and_fix_rm_with_leases(state(), intervals:interval(), intervals:interval(),
                                     lease_list:lease_list(), intervals:interval())
                                     -> state().
compare_and_fix_rm_with_leases(State, OldRange, NewRange, LeaseList, MyRange) ->
    ?TRACE("lease list ~w", [LeaseList]),
    ActiveRange = lease_list:get_active_range(LeaseList),
    case intervals:is_empty(ActiveRange) of
        true ->
            % we lost our active lease; we do not participate in the ring anymore
            State;
        false ->
            case intervals:is_subset(OldRange, NewRange) of
                true ->
                    % NewRange > OldRange
                    % my range became larger -> do takeover
                    prepare_takeover(State, MyRange, ActiveRange);
                false ->
                    case intervals:is_subset(NewRange, OldRange) of
                        true ->
                            % OldRange > NewRange
                            % my range became smaller -> ignore
                            State;
                        false ->
                            State
                    end
            end
    end.

-spec prepare_takeover(state(), intervals:interval(), intervals:interval()) 
                                     -> state().
prepare_takeover(State, MyRange, ActiveRange) ->
    MissingRange = intervals:minus(MyRange, ActiveRange),
    case intervals:is_non_empty(MissingRange) 
        andalso
        intervals:is_left_of(MissingRange, ActiveRange) of
        false ->
            State;
        true ->
            %% log:log("missing range:~n missing=~p~n active=~p~n my_range=~p", 
            %%         [MissingRange, ActiveRange, MyRange]),
            %% log:log("missing range: ~w", [MissingRange]),
            LeaseId = l_on_cseq:id(MissingRange),
            Pid = comm:reply_as(self(), 3, {read_after_rm_change, MissingRange, '_'}),
            l_on_cseq:read(LeaseId, Pid),
            State
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% public helper functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec get_takeovers(comm:erl_local_pid()) -> 
                           gb_trees:tree(?RT:key(), l_on_cseq:lease_t()).
get_takeovers(RMLeasesPid) ->
    comm:send_local(RMLeasesPid, {get_takeovers, comm:this()}),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({get_takeovers_response, TakeOvers},% ->
            TakeOvers)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% state management
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec add_takeover(state(), l_on_cseq:lease_t()) -> state().
add_takeover(#state{takeovers=Takeovers} = State, Lease) ->
    Id = l_on_cseq:get_id(Lease),
    case gb_trees:lookup(Id, Takeovers) of
        {value, _Val} ->
            % @todo ?!?
            State;
        none ->
            NewTakeovers = gb_trees:insert(Id, Lease, Takeovers),
            State#state{takeovers=NewTakeovers}
    end.

-spec remove_takeover(state(), l_on_cseq:lease_id(), l_on_cseq:lease_t()) -> state().
remove_takeover(#state{takeovers=Takeovers} = State, Id, _Lease) ->
    NewTakeovers = gb_trees:delete_any(Id, Takeovers),
    State#state{takeovers=NewTakeovers}.

% @doc the given lease is the one we recorded earlier
-spec is_current_takeover(state(), l_on_cseq:lease_t()) -> {value, l_on_cseq:lease_t()} | none.
is_current_takeover(#state{takeovers=Takeovers}, L) ->
    Id = l_on_cseq:get_id(L),
    gb_trees:lookup(Id, Takeovers).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% utilities
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec propose_new_neighbors(comm:mypid() | nil) -> ok.
propose_new_neighbors(PidOrNil) ->
    ?TRACE("somebody else updated this lease", []),
    case PidOrNil of
        nil ->
            ok;
        Pid ->
            ReplyPid = comm:reply_as(comm:this(), 2, {get_node_for_new_neighbor, '_'}),
            comm:send(Pid, {get_state, ReplyPid, node}),
            ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% todo
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
