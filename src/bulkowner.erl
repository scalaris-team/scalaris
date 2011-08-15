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
%% @doc    Bulk owner operations (for now only broadcasting).

%% @version $Id$
%% @reference Ali Ghodsi, <em>Distributed k-ary System: Algorithms for Distributed Hash Tables</em>, PhD Thesis, page 129.
-module(bulkowner).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

% public API:
-export([issue_bulk_owner/3, issue_send_reply/4,
         send_reply/5, send_reply_failed/6]).

% only use inside the dht_node process:
-export([bulk_owner/5]).

%% @doc Start a bulk owner operation to send the message to all nodes in the
%%      given interval.
-spec issue_bulk_owner(Id::util:global_uid(), I::intervals:interval(), Msg::comm:message()) -> ok.
issue_bulk_owner(Id, I, Msg) ->
    DHTNode = pid_groups:find_a(dht_node),
    comm:send_local(DHTNode, {start_bulk_owner, Id, I, Msg}).

-spec issue_send_reply(Id::util:global_uid(), Target::comm:mypid(), Msg::comm:message(), Parents::[comm:mypid()]) -> ok.
issue_send_reply(Id, Target, Msg, Parents) ->
    DHTNode = pid_groups:find_a(dht_node),
    comm:send_local(DHTNode, {bulkowner_reply, Id, Target, Msg, Parents}).

-spec send_reply(Id::util:global_uid(), Target::comm:mypid(), Msg::comm:message(), Parents::[comm:mypid()], Shepherd::comm:erl_local_pid()) -> ok.
send_reply(Id, Target, {send_to_group_member, Proc, Msg}, [], Shepherd) ->
    comm:send_with_shepherd(Target, {send_to_group_member, Proc, {bulkowner_reply, Id, Msg}}, Shepherd);
send_reply(Id, Target, Msg, [], Shepherd) ->
    comm:send_with_shepherd(Target, {bulkowner_reply, Id, Msg}, Shepherd);
send_reply(Id, Target, Msg, [Parent | Rest], Shepherd) ->
    comm:send_with_shepherd(Parent, {bulkowner_reply, Id, Target, Msg, Rest}, Shepherd).

-spec send_reply_failed(Id::util:global_uid(), Target::comm:mypid(), Msg::comm:message(), Parents::[comm:mypid()], Shepherd::comm:erl_local_pid(), FailedPid::comm:mypid()) -> ok.
send_reply_failed(_Id, Target, Msg, [], _Shepherd, Target) ->
    log:log(warn, "[ ~p ] cannot send bulkowner_reply with message ~p (target node not available)",
            [pid_groups:pid_to_name(self()), Msg]);
send_reply_failed(Id, Target, Msg, Parents, Shepherd, _FailedPid) ->
    send_reply(Id, Target, Msg, Parents, Shepherd).

%% @doc main routine. It spans a broadcast tree over the nodes in I
-spec bulk_owner(State::dht_node_state:state(), Id::util:global_uid(), I::intervals:interval(), Msg::comm:message(), Parents::[comm:mypid()]) -> ok.
bulk_owner(State, Id, I, Msg, Parents) ->
%%     ct:pal("bulk_owner:~n self:~p,~n int :~p,~n rt  :~p~n", [dht_node_state:get(State, node), I, ?RT:to_list(State)]),
    Neighbors = dht_node_state:get(State, neighbors),
    SuccIntI = intervals:intersection(I, nodelist:succ_range(Neighbors)),
    case intervals:is_empty(SuccIntI) of
        true  -> ok;
        false ->
            comm:send(node:pidX(nodelist:succ(Neighbors)),
                      {bulkowner_deliver, Id, SuccIntI, Msg, Parents})
    end,
    case I =:= SuccIntI of
        true  -> ok;
        false ->
            MyNode = nodelist:node(Neighbors),
            NewParents = [node:pidX(MyNode) | Parents],
            RTList = lists:reverse(?RT:to_list(State)),
            bulk_owner_iter(RTList, Id, I, Msg, node:id(MyNode), NewParents)
    end.

%% @doc Iterates through the list of (unique) nodes in the routing table and
%%      sends them the according bulkowner messages for sub-intervals of I.
%%      The first call should have Limit=Starting_nodeid. The method will
%%      then go through the ReverseRTList (starting with the longest finger,
%%      ending with the node's successor) and send each node a bulk_owner
%%      message for the interval it is responsible for:
%%      I \cap (id(Node_in_reversertlist), Limit], e.g.
%%      node Nl from the longest finger is responsible for
%%      I \cap (id(Nl), id(Starting_node)].
%%      Note that the range (id(Starting_node), id(Succ_of_starting_node)]
%%      has already been covered by bulk_owner/3.
-spec bulk_owner_iter(ReverseRTList::nodelist:snodelist(),
                      Id::util:global_uid(),
                      I::intervals:interval(), Msg::comm:message(),
                      Limit::?RT:key(), Parents::[comm:mypid(),...]) -> ok.
bulk_owner_iter([], _Id, _I, _Msg, _Limit, _Parents) ->
    ok;
bulk_owner_iter([Head | Tail], Id, I, Msg, Limit, Parents) ->
    Interval_Head_Limit = node:mk_interval_between_ids(node:id(Head), Limit),
    Range = intervals:intersection(I, Interval_Head_Limit),
%%     ct:pal("send_bulk_owner_if: ~p ~p ~n", [I, Range]),
    NewLimit =
        case intervals:is_empty(Range) of
            false -> comm:send(node:pidX(Head), {bulk_owner, Id, Range, Msg, Parents}),
                     node:id(Head);
            true  -> Limit
        end,
    bulk_owner_iter(Tail, Id, I, Msg, NewLimit, Parents).
