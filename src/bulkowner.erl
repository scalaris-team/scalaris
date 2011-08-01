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

-export([issue_bulk_owner/2, bulk_owner/3]).

%% @doc Start a bulk owner operation to send the message to all nodes in the
%%      given interval.
-spec issue_bulk_owner(I::intervals:interval(), Msg::comm:message()) -> ok.
issue_bulk_owner(I, Msg) ->
    DHTNode = pid_groups:find_a(dht_node),
    comm:send_local(DHTNode, {start_bulk_owner, I, Msg}).

%% @doc main routine. It spans a broadcast tree over the nodes in I
-spec bulk_owner(State::dht_node_state:state(), I::intervals:interval(), Msg::comm:message()) -> ok.
bulk_owner(State, I, Msg) ->
%%     ct:pal("bulk_owner:~n self:~p,~n int :~p,~n rt  :~p~n", [dht_node_state:get(State, node), I, ?RT:to_list(State)]),
    Neighbors = dht_node_state:get(State, neighbors),
    SuccInt = intervals:intersection(I, nodelist:succ_range(Neighbors)),
    case intervals:is_empty(SuccInt) of
        true  -> ok;
        false ->
            comm:send(node:pidX(nodelist:succ(Neighbors)),
                      {bulkowner_deliver, SuccInt, Msg})
    end,
    case intervals:is_subset(I, SuccInt) of
        true  -> ok;
        false ->
            RTList = lists:reverse(?RT:to_list(State)),
            bulk_owner_iter(RTList, I, Msg, node:id(nodelist:node(Neighbors)))
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
                      I::intervals:interval(), Msg::comm:message(),
                      Limit::?RT:key()) -> ok.
bulk_owner_iter([], _I, _Msg, _Limit) ->
    ok;
bulk_owner_iter([Head | Tail], I, Msg, Limit) ->
    Interval_Head_Limit = node:mk_interval_between_ids(node:id(Head), Limit),
    Range = intervals:intersection(I, Interval_Head_Limit),
%%     ct:pal("send_bulk_owner_if: ~p ~p ~n", [I, Range]),
    NewLimit = case intervals:is_empty(Range) of
                   false ->
                       comm:send(node:pidX(Head), {bulk_owner, Range, Msg}),
                       node:id(Head);
                   true ->
                       Limit
               end,
    bulk_owner_iter(Tail, I, Msg, NewLimit).
