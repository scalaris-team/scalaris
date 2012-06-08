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
%% @doc    dht_node lookup algorithm (interacts with the dht_node process)
%% @end
%% @version $Id$
-module(dht_node_lookup).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([lookup_aux/4, lookup_aux_failed/3, lookup_fin_failed/3]).

%% userdevguide-begin dht_node_lookup:routing
%% @doc Find the node responsible for Key and send him the message Msg.
-spec lookup_aux(State::dht_node_state:state(), Key::intervals:key(),
                 Hops::non_neg_integer(), Msg::comm:message()) -> ok.
lookup_aux(State, Key, Hops, Msg) ->
    Neighbors = dht_node_state:get(State, neighbors),
    case intervals:in(Key, nodelist:succ_range(Neighbors)) of
        true -> % found node -> terminate
            P = node:pidX(nodelist:succ(Neighbors)),
            comm:send(P, {?lookup_fin, Key, Hops + 1, Msg}, [{shepherd, self()}]);
        _ ->
            P = ?RT:next_hop(State, Key),
            comm:send(P, {?lookup_aux, Key, Hops + 1, Msg}, [{shepherd, self()}])
    end.
%% userdevguide-end dht_node_lookup:routing

-spec lookup_aux_failed(dht_node_state:state(), Target::comm:mypid(),
                        Msg::comm:message()) -> ok.
lookup_aux_failed(State, _Target, {?lookup_aux, Key, Hops, Msg} = _Message) ->
    %io:format("lookup_aux_failed(State, ~p, ~p)~n", [_Target, _Message]),
    comm:send_local_after(100, self(), {?lookup_aux, Key, Hops + 1, Msg}),
    State.

-spec lookup_fin_failed(dht_node_state:state(), Target::comm:mypid(),
                        Msg::comm:message()) -> ok.
lookup_fin_failed(State, _Target, {?lookup_fin, Key, Hops, Msg} = _Message) ->
    %io:format("lookup_fin_failed(State, ~p, ~p)~n", [_Target, _Message]),
    comm:send_local_after(100, self(), {?lookup_aux, Key, Hops + 1, Msg}),
    State.
