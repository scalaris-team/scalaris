% @copyright 2012-2013 Zuse Institute Berlin,

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

-record(lease_list_t, {
          active  = ?required(lease, active ) :: active_lease_t(),
          passive = ?required(lease, passive) :: [l_on_cseq:lease_t()]
         }).

-type lease_list() :: #lease_list_t{}.

-ifdef(with_export_type_support).
-export_type([lease_list/0]).
-export_type([active_lease_t/0]).
-endif.

-export([empty/0]).
-export([update_lease_in_dht_node_state/4]).
-export([remove_lease_from_dht_node_state/3]).
%-export([remove_lease_from_dht_node_state/3]).
%-export([disable_lease_in_dht_node_state/2]).
-export([make_lease_list/2]).
-export([get_active_lease/1]).
-export([get_passive_leases/1]).

-spec empty() -> lease_list().
empty() ->
    make_lease_list(empty, []).

-spec make_lease_list(active_lease_t(), [l_on_cseq:lease_t()]) ->
                             lease_list().
make_lease_list(Active, Passive) ->
    #lease_list_t{
       active = Active,
       passive   = Passive
      }.

-spec get_active_lease(lease_list()) -> active_lease_t().
get_active_lease(#lease_list_t{active=Active}) ->
    Active.

-spec get_passive_leases(lease_list()) -> list(l_on_cseq:lease_t()).
get_passive_leases(#lease_list_t{passive=Passive}) ->
    Passive.

-spec update_lease_in_dht_node_state(l_on_cseq:lease_t(), dht_node_state:state(),
                                     active | passive,
                                     renew | received_lease | unittest | handover |
                                     takeover |
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
            log:log("received lease ~w~n~w", [Lease, MyActiveLease]),
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
                                    ?ASSERT(false),
                                    State
                            end
                    end
            end;
        {_, _} ->
            update_lease_in_dht_node_state(Lease, State, Mode)
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


-spec remove_active_lease_from_dht_node_state(l_on_cseq:lease_t(),
                                              dht_node_state:state()) ->
                                                     dht_node_state:state().
remove_active_lease_from_dht_node_state(Lease, State) ->
    LeaseList = dht_node_state:get(State, lease_list),
    Active = LeaseList#lease_list_t.active,
    Id = l_on_cseq:get_id(Lease),
    case Active of
        empty ->
            State;
        _ ->
            case l_on_cseq:get_id(Active) =:= Id of
                true ->
                    dht_node_state:set_lease_list(State,
                                                  LeaseList#lease_list_t{active=empty});
                false ->
                    State
            end
    end.

-spec remove_passive_lease_from_dht_node_state(l_on_cseq:lease_t(),
                                              dht_node_state:state()) ->
                                                     dht_node_state:state().
remove_passive_lease_from_dht_node_state(Lease, State) ->
    LeaseList = dht_node_state:get(State, lease_list),
    Passive = LeaseList#lease_list_t.passive,
    Id = l_on_cseq:get_id(Lease),
    NewPassive = lists:keydelete(Id, 2, Passive),
    dht_node_state:set_lease_list(State,
                                  LeaseList#lease_list_t{passive=NewPassive}).

-spec remove_lease_from_dht_node_state(l_on_cseq:lease_t(), dht_node_state:state(),
                                       active | passive | any) ->
                                              dht_node_state:state().
remove_lease_from_dht_node_state(Lease, State, Mode) ->
    case Mode of
        passive ->
            remove_passive_lease_from_dht_node_state(Lease, State);
        active ->
            log:log("you are trying to remove an active lease"),
            %ct:fail("nope"),
            remove_active_lease_from_dht_node_state(Lease, State);
        any ->
            remove_passive_lease_from_dht_node_state(Lease,
              remove_active_lease_from_dht_node_state(Lease, State))
    end.

%-spec disable_lease_in_dht_node_state(l_on_cseq:lease_t(), dht_node_state:state()) ->
%    dht_node_state:state().
%disable_lease_in_dht_node_state(Lease, State) ->
%    Id = l_on_cseq:get_id(Lease),
%    LeaseList = dht_node_state:get(State, lease_list),
%    NewActiveLeaseList = lists:keydelete(Id, 2, ActiveLeaseList),
%    NewPassiveLeaseList = update_passive_lease(Lease, PassiveLeaseList),
%    dht_node_state:set_lease_list(State, {NewActiveLeaseList, NewPassiveLeaseList}).

-spec update_passive_lease(Lease::l_on_cseq:lease_t(), LeaseList::lease_list()) -> lease_list().
update_passive_lease(Lease, LeaseList = #lease_list_t{passive=Passive}) ->
    Id = l_on_cseq:get_id(Lease),
    NewPassive = case lists:keyfind(Id, 2, Passive) of
                     false ->
                         [Lease|Passive];
                     _OldLease ->
                         lists:keyreplace(Id, 2, Passive, Lease)
                 end,
    LeaseList#lease_list_t{passive=NewPassive}.

-spec update_active_lease(Lease::l_on_cseq:lease_t(), LeaseList::lease_list()) -> lease_list().
update_active_lease(Lease, LeaseList = #lease_list_t{active=Active}) ->
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
            ?ASSERT(l_on_cseq:get_id(Lease) =:= l_on_cseq:get_id(Active)),
            % @todo new lease should be newer than the old lease !!!
            LeaseList#lease_list_t{active=Lease}
    end.

