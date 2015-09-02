%% @copyright 2012-2013 Zuse Institute Berlin

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
%% @doc    Unit tests for slide_leases
%% @end
%% @version $Id$
-module(lease_helper).
-author('schuett@zib.de').
-vsn('$Id').

-compile(export_all).

-include("scalaris.hrl").
-include("unittest.hrl").
-include("client_types.hrl").

-export([wait_for_correct_leases/1,
         wait_for_ring_size/1,
         wait_for_correct_ring/0,
         print_all_active_leases/0,
         print_all_passive_leases/0,
         intercept_lease_renew/1
        ]).

-spec wait_for_correct_leases(pos_integer()) -> ok.
wait_for_correct_leases(TargetSize) ->
    util:wait_for(fun () -> admin:check_leases(TargetSize) end, 1000),
    ct:pal("have correct leases"),
    util:wait_for(fun check_leases_per_node/0, 1000),
    ct:pal("have correct leases_per_node"),
    ok.

-spec wait_for_ring_size(pos_integer()) -> ok.
wait_for_ring_size(Size) ->
    util:wait_for(fun () ->
                          Nodes = api_vm:number_of_nodes(),
                          log:log("ring size: expected ~w, found ~w", [Size, Nodes]),
                          Nodes == Size end).

-spec wait_for_correct_ring() -> ok.
wait_for_correct_ring() ->
    util:wait_for(fun () -> ct:pal("->admin:check_ring_deep()"),
                            Res = admin:check_ring_deep(),
                            ct:pal("<-admin:check_ring_deep()"),
                            Res == ok
             end).

-spec wait_for_number_of_valid_active_leases(integer()) -> ok.
wait_for_number_of_valid_active_leases(Count) ->
    util:wait_for(fun () ->
                          AllLeases = get_all_leases(),
                          ActiveLeases = [ lease_list:get_active_lease(LL) || LL <- AllLeases ],
                          ValidLeases = [ L || L <- ActiveLeases, l_on_cseq:is_valid(L)],
                          NrOfValidLeases = length(ValidLeases),
                          log:log("active leases: expected ~w, found ~w", [Count, NrOfValidLeases]),
                          Count =:= NrOfValidLeases
                  end).

-spec print_all_active_leases() -> ok.
print_all_active_leases() ->
    AllLeases = get_all_leases(),
    ActiveLeases = [ lease_list:get_active_lease(LL) || LL <- AllLeases ],
    log:log("active leases: ~w", [ActiveLeases]),
    ok.

-spec print_all_passive_leases() -> ok.
print_all_passive_leases() ->
    AllLeases = get_all_leases(),
    PassiveLeases = [ lease_list:get_passive_leases(LL) || LL <- AllLeases ],
    log:log("passive leases: ~w", [PassiveLeases]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% wait helper
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_dht_node_state(Pid, What) ->
    comm:send_local(Pid, {get_state, comm:this(), What}),
    receive
        {get_state_response, Data} ->
            Data
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% message blocking
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec intercept_lease_renew(comm:erl_local_pid_plain()) -> comm:message().
intercept_lease_renew(DHTNode) ->
    %DHTNode = pid_groups:find_a(dht_node),
    % we wait for the next periodic trigger
    gen_component:bp_set_cond(DHTNode, block_renew(self()), block_renew),
    Msg = receive
              M = {l_on_cseq, renew, _Lease, _Mode} ->
                  M
          end,
    gen_component:bp_set_cond(DHTNode, block_trigger(self()), block_trigger),
    gen_component:bp_del(DHTNode, block_renew),
    Msg.

block_renew(Pid) ->
    fun (Message, _State) ->
            case Message of
                {l_on_cseq, renew, _Lease, _Mode} ->
                    comm:send_local(Pid, Message),
                    drop_single;
                _ ->
                    false
            end
    end.

block_trigger(Pid) ->
    fun (Message, _State) ->
            case Message of
                {l_on_cseq, renew_leases} ->
                    comm:send_local(Pid, Message),
                    drop_single;
                _ ->
                    false
            end
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% utility functions
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_all_leases() ->
    [ get_leases(DHTNode) || DHTNode <- pid_groups:find_all(dht_node) ].

get_leases(Pid) ->
    get_dht_node_state(Pid, lease_list).

check_leases_per_node() ->
    lists:all(fun (B) -> B end, [ check_local_leases(DHTNode) || DHTNode <- pid_groups:find_all(dht_node) ]).

-spec is_disjoint([intervals:interval()]) -> boolean().
is_disjoint([]) ->
    true;
is_disjoint([H | T]) ->
    is_disjoint(H, T) andalso
        is_disjoint(T).

-spec is_disjoint(intervals:interval(), [intervals:interval()]) -> boolean().
is_disjoint(_I, []) ->
    true;
is_disjoint(I, [H | T]) ->
    intervals:is_empty(intervals:intersection(I, H))
        andalso is_disjoint(I, T).


check_local_leases(DHTNode) ->
    LeaseList = get_dht_node_state(DHTNode, lease_list),
    ActiveLease = lease_list:get_active_lease(LeaseList),
    PassiveLeases = lease_list:get_passive_leases(LeaseList),
    %log:log("active lease: ~w", [ActiveLease]),
    ActiveInterval = case ActiveLease of
                         empty ->
                             intervals:empty();
                         _ ->
                             l_on_cseq:get_range(ActiveLease)
                     end,
    MyRange = get_dht_node_state(DHTNode, my_range),
    LocalCorrect = MyRange =:= ActiveInterval,
    length(PassiveLeases) == 0 andalso LocalCorrect.
