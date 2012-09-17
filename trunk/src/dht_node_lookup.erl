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

-export([lookup_aux/4, lookup_fin/4,
         lookup_aux_failed/3, lookup_fin_failed/3]).

%% userdevguide-begin dht_node_lookup:routing
%% @doc Find the node responsible for Key and send him the message Msg.
-spec lookup_aux(State::dht_node_state:state(), Key::intervals:key(),
                 Hops::non_neg_integer(), Msg::comm:message()) -> ok.
lookup_aux(State, Key, Hops, Msg) ->
    Neighbors = dht_node_state:get(State, neighbors),
    WrappedMsg = ?RT:wrap_message(Msg),
    case intervals:in(Key, nodelist:succ_range(Neighbors)) of
        true -> % found node -> terminate
            P = node:pidX(nodelist:succ(Neighbors)),
            comm:send(P, {?lookup_fin, Key, Hops + 1, WrappedMsg}, [{shepherd, self()}]);
        _ ->
            P = ?RT:next_hop(State, Key),
            comm:send(P, {?lookup_aux, Key, Hops + 1, WrappedMsg}, [{shepherd, self()}])
    end.

%% @doc Find the node responsible for Key and send him the message Msg.
-spec lookup_fin(State::dht_node_state:state(), Key::intervals:key(),
                 Hops::non_neg_integer(), Msg::comm:message()) -> dht_node_state:state().
lookup_fin(State, Key, Hops, Msg) ->
    MsgFwd = dht_node_state:get(State, msg_fwd),
    FwdList = [P || {I, P} <- MsgFwd, intervals:in(Key, I)],
    case FwdList of
        []    ->
            case dht_node_state:is_db_responsible(Key, State) of
                true ->
                    comm:send_local(dht_node_state:get(State, monitor_proc),
                                    {lookup_hops, Hops}),
                    Unwrap = ?RT:unwrap_message(Msg, State),
                    gen_component:post_op(State, Unwrap);
                false ->
                    % it is possible that we received the message due to a
                    % forward while sliding and before the other node removed
                    % the forward -> do not warn then
                    SlidePred = dht_node_state:get(State, slide_pred),
                    SlideSucc = dht_node_state:get(State, slide_succ),
                    Neighbors = dht_node_state:get(State, neighbors),
                    case ((SlidePred =/= null andalso
                               slide_op:get_sendORreceive(SlidePred) =:= 'send' andalso
                               intervals:in(Key, slide_op:get_interval(SlidePred)))
                              orelse
                              (SlideSucc =/= null andalso
                                   slide_op:get_sendORreceive(SlideSucc) =:= 'send' andalso
                                   intervals:in(Key, slide_op:get_interval(SlideSucc)))
                              orelse
                              intervals:in(Key, nodelist:succ_range(Neighbors))) of
                        true -> ok;
                        false ->
                            DBRange = dht_node_state:get(State, db_range),
                            DBRange2 = [begin
                                            case intervals:is_continuous(Interval) of
                                                true -> {intervals:get_bounds(Interval), Id};
                                                _    -> {Interval, Id}
                                            end
                                        end || {Interval, Id} <- DBRange],
                            log:log(warn,
                                    "[ ~.0p ] Routing is damaged!! Trying again...~n  myrange:~p~n  db_range:~p~n  msgfwd:~p~n  Key:~p",
                                    [self(), intervals:get_bounds(nodelist:node_range(Neighbors)),
                                     DBRange2, MsgFwd, Key])
                    end,
                    lookup_aux(State, Key, Hops, Msg),
                    State
            end;
        [Pid] -> comm:send(Pid, {?lookup_fin, Key, Hops + 1, Msg}),
                 State
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
