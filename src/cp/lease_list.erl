% @copyright 2012-2014, 2016 Zuse Institute Berlin,

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

%% @author Florian Schintke <schintke@zib.de>
%% @author Thorsten Schuett <schuett@zib.de>
%% @doc lease list in the dht_node_state.
%% @end
%% @version $Id$
-module(lease_list).
-author('schintke@zib.de').
-author('schuett@zib.de').
-vsn('$Id:$ ').

-include("scalaris.hrl").
-include("record_helpers.hrl").

-type active_lease_t() :: l_on_cseq:lease_t() | empty.

-type next_round_map() :: [{l_on_cseq:lease_id(), pr:pr()}].

-record(lease_list_t, {
          active         = ?required(lease_list_t, active )        :: active_lease_t(),
          passive        = ?required(lease_list_t, passive)        :: [l_on_cseq:lease_t()],
          next_round_map = ?required(lease_list_t, next_round_map) :: next_round_map()
         }).

-type lease_list() :: #lease_list_t{}.

-export_type([lease_list/0]).
-export_type([active_lease_t/0]).

-export([empty/0]).
-export([update_lease_in_dht_node_state/4]).
-export([remove_lease_from_dht_node_state/4]).
-export([make_lease_list/3]).
-export([get_active_lease/1]).
-export([get_passive_leases/1]).
-export([get_active_range/1]).
-export([get_next_round/2]).
-export([contains_lease/3]).
-export([update_next_round/3]).

-spec empty() -> lease_list().
empty() ->
    make_lease_list(empty, [], []).

-spec make_lease_list(active_lease_t(), [l_on_cseq:lease_t()], next_round_map()) ->
                             lease_list().
make_lease_list(Active, Passive, NextRounds) ->
    #lease_list_t{
       active = Active,
       passive   = Passive,
       next_round_map = NextRounds
      }.

-spec get_active_lease(lease_list()) -> active_lease_t().
get_active_lease(#lease_list_t{active=Active}) ->
    Active.

-spec get_active_range(lease_list()) -> intervals:interval().
get_active_range(#lease_list_t{active=Active}) ->
    case Active of
        empty ->
            intervals:empty();
        _ ->
            l_on_cseq:get_range(Active)
    end.

-spec get_passive_leases(lease_list()) -> list(l_on_cseq:lease_t()).
get_passive_leases(#lease_list_t{passive=Passive}) ->
    Passive.

-spec get_next_round(l_on_cseq:lease_id(), dht_node_state:state()) ->
                            pr:pr() | failed.
get_next_round(Id, State) ->
    #lease_list_t{next_round_map=NextRounds} = dht_node_state:get(State, lease_list),
    case lists:keyfind(Id, 1, NextRounds) of
        {Id, NextRound} ->
             NextRound;
        false ->
            failed
    end.

-spec update_next_round(l_on_cseq:lease_id(), pr:pr(), dht_node_state:state()) ->
                            dht_node_state:state().
update_next_round(Id, NextRound, State) ->
    LeaseList = dht_node_state:get(State, lease_list),
    NextRounds = LeaseList#lease_list_t.next_round_map,
    NewList = case lists:keyfind(Id, 1, NextRounds) of
                  {Id, _NextRound} ->
                      NewNextRounds = lists:keyreplace(Id, 1, NextRounds,
                                                       {Id, NextRound}),
                      LeaseList#lease_list_t{next_round_map=NewNextRounds};
                  false ->
                      LeaseList#lease_list_t{next_round_map=[{Id, NextRound}|NextRounds]}
              end,
    dht_node_state:set_lease_list(State, NewList).

-spec remove_next_round(l_on_cseq:lease_id(), dht_node_state:state()) ->
                               dht_node_state:state().
remove_next_round(Id, State) ->
    LeaseList = dht_node_state:get(State, lease_list),
    NextRounds = LeaseList#lease_list_t.next_round_map,
    NewNextRounds = lists:keydelete(Id, 1, NextRounds),
    dht_node_state:set_lease_list(State,
                                  LeaseList#lease_list_t{next_round_map=NewNextRounds}).

-spec update_lease_in_dht_node_state(l_on_cseq:lease_t(), dht_node_state:state(),
                                     active | passive,
                                     renew | received_lease | unittest | handover |
                                     takeover |
                                     recover |
                                     merge_reply_step1 | merge_reply_step2 |
                                     merge_reply_step3 |
                                     split_reply_step1 | split_reply_step2 |
                                     split_reply_step3 | split_reply_step4) ->
    dht_node_state:state().
update_lease_in_dht_node_state(Lease, State, Mode, Reason) ->
    LeaseList = dht_node_state:get(State, lease_list),
    case {Mode, Reason} of
        {_, received_lease} ->
            MyActiveLease = lease_list:get_active_lease(LeaseList),
            %log:log("received lease ~w~n~w", [Lease, MyActiveLease]),
            if
                MyActiveLease =:= empty ->
                    update_lease_in_dht_node_state(Lease, State, Mode);
                true ->
                    NewRange = l_on_cseq:get_range(Lease),
                    ActiveRange = l_on_cseq:get_range(MyActiveLease),
                    if
                        NewRange =:= ActiveRange ->
                            update_lease_in_dht_node_state(Lease, State, Mode);
                        true ->
                            IsAdjacent = intervals:is_adjacent(NewRange, ActiveRange),
                            IsLeftOf = intervals:is_left_of(NewRange, ActiveRange),
                            log:log("ranges ~w ~w", [NewRange, ActiveRange]),
                            log:log("received lease ~w ~w ~w ~w ~w", [intervals:is_adjacent(NewRange, ActiveRange),
                                                                      intervals:is_left_of(NewRange, ActiveRange),
                                                                      intervals:is_right_of(NewRange, ActiveRange),
                                                                      Lease, MyActiveLease]),
                            if
                                IsAdjacent andalso IsLeftOf ->
                                    DHTNode = pid_groups:get_my(rm_leases),
                                    Pid = comm:reply_as(DHTNode, 4,
                                                        {merge_after_leave,
                                                         Lease, MyActiveLease, '_'}),
                                    l_on_cseq:lease_merge(Lease, MyActiveLease, Pid),
                                    update_lease_in_dht_node_state(Lease, State, passive);
                                true ->
                                    log:log("shall merge non-adjacent leases"),
                                    ?DBG_ASSERT(false),
                                    State
                            end
                    end
            end;
        {_, _} ->
            update_lease_in_dht_node_state(Lease, State, Mode)
    end.

-spec contains_lease(l_on_cseq:lease_t(), dht_node_state:state(),
                     active | passive) -> boolean().
contains_lease(Lease, State, Mode) ->
    LeaseList = dht_node_state:get(State, lease_list),
    case Mode of
        active ->
            case get_active_lease(LeaseList) of
                empty ->
                    false;
                L ->
                    l_on_cseq:get_id(L) =:= l_on_cseq:get_id(Lease)
            end;
        passive ->
            PassiveLeases = get_passive_leases(LeaseList),
            lists:keyfind(l_on_cseq:get_id(Lease), 2, PassiveLeases) =/= false
    end.


-spec update_lease_in_dht_node_state(l_on_cseq:lease_t(), dht_node_state:state(),
                                     active | passive) ->
    dht_node_state:state().
update_lease_in_dht_node_state(Lease, State, Mode) ->
    LeaseList = dht_node_state:get(State, lease_list),
    case Mode of
        active ->
            dht_node_state:set_lease_list(State, update_active_lease(Lease, LeaseList));
        passive ->
            dht_node_state:set_lease_list(State, update_passive_lease(Lease, LeaseList))
    end.


-spec remove_active_lease_from_dht_node_state(l_on_cseq:lease_t(), l_on_cseq:lease_id(),
                                              dht_node_state:state()) ->
                                                     dht_node_state:state().
remove_active_lease_from_dht_node_state(Lease, Id, State) ->
    log:log("you are trying to remove an active lease via any?!? ~w", [Lease]),
    LeaseList = dht_node_state:get(State, lease_list),
    Active = LeaseList#lease_list_t.active,
    case l_on_cseq:get_id(Active) of
        Id ->
            lease_recover:restart_node(),
            dht_node_state:set_lease_list(remove_next_round(Id, State),
                                          LeaseList#lease_list_t{active=empty});
        _ ->
            State
    end.

-spec remove_passive_lease_from_dht_node_state(l_on_cseq:lease_t(), l_on_cseq:lease_id(),
                                              dht_node_state:state()) ->
                                                     dht_node_state:state().
remove_passive_lease_from_dht_node_state(_Lease, Id, State) ->
    LeaseList = dht_node_state:get(State, lease_list),
    Passive = LeaseList#lease_list_t.passive,
    NewPassive = lists:keydelete(Id, 2, Passive),
    dht_node_state:set_lease_list(remove_next_round(Id, State),
                                  LeaseList#lease_list_t{passive=NewPassive}).

-spec remove_lease_from_dht_node_state(l_on_cseq:lease_t(), l_on_cseq:lease_id(),
                                       dht_node_state:state(),
                                       active | passive | any) ->
                                              dht_node_state:state().
remove_lease_from_dht_node_state(Lease, Id, State, Mode) ->
    case Mode of
        passive ->
            remove_passive_lease_from_dht_node_state(Lease, Id, State);
        active ->
            log:log("you are trying to remove an active lease"),
            lease_recover:restart_node(),
            remove_active_lease_from_dht_node_state(Lease, Id, State);
        any ->
            remove_passive_lease_from_dht_node_state(Lease, Id,
              remove_active_lease_from_dht_node_state(Lease, Id, State))
    end.

-spec update_passive_lease(Lease::l_on_cseq:lease_t(), LeaseList::lease_list()) -> lease_list().
update_passive_lease(Lease, LeaseList = #lease_list_t{passive=Passive}) ->
    Id = l_on_cseq:get_id(Lease),
    NewPassive = case lists:keyfind(Id, 2, Passive) of
                     false ->
                         [Lease|Passive];
                     OldLease ->
                         case is_newer(Lease, OldLease) of
                             true ->
                                 lists:keyreplace(Id, 2, Passive, Lease);
                             false ->
                                 Passive
                         end
                 end,
    LeaseList#lease_list_t{passive=NewPassive}.

-spec update_active_lease(Lease::l_on_cseq:lease_t(), LeaseList::lease_list()) -> lease_list().
update_active_lease(Lease, LeaseList = #lease_list_t{active=Active}) ->
    case Lease of
        empty ->
            log:log("you are trying to remove an active lease in update"),
            lease_recover:restart_node(),
            ok;
        _ ->
            ok
    end,
    case Active of
        empty ->
            LeaseList#lease_list_t{active=Lease};
        _ ->
            case l_on_cseq:get_id(Lease) =/= l_on_cseq:get_id(Active) of
                true ->
                    log:log("bad update active lease (~w)~n~w~n~w~n", [self(), Lease, Active]),
                    log:log("~w ~w", [l_on_cseq:get_id(Lease), l_on_cseq:get_id(Active)]);
                false ->
                    ok
            end,
            ?DBG_ASSERT(l_on_cseq:get_id(Lease) =:= l_on_cseq:get_id(Active)),
            % @todo new lease should be newer than the old lease !!!
            case is_newer(Lease, Active) of
                true ->
                    LeaseList#lease_list_t{active=Lease};
                false ->
                    LeaseList
            end
    end.

-spec is_newer(New::l_on_cseq:lease_t(), Old::l_on_cseq:lease_t()) -> boolean().
is_newer(New, Old) ->
    NewEpoch = l_on_cseq:get_epoch(New),
    NewVersion = l_on_cseq:get_version(New),
    OldEpoch = l_on_cseq:get_epoch(Old),
    OldVersion = l_on_cseq:get_version(Old),
    ((NewEpoch =:= OldEpoch) andalso (NewVersion > OldVersion))
        orelse
          (NewEpoch > OldEpoch).
