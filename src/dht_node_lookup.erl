%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    dht_node_lookup.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    dht_node lookup algorithm
%%% @end
%%% Created : 3 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(dht_node_lookup).

-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([lookup_aux/4, get_key/4, set_key/5, delete_key/3]).

%logging on
%-define(LOG(S, L), io:format(S, L)).
%logging off
-define(LOG(S, L), ok).

lookup_aux(State, Key, Hops, Msg) ->
    Terminate = util:is_between(dht_node_state:get(State, node_id), Key, dht_node_state:get(State, succ_id)),
    if
        Terminate ->
            cs_send:send(dht_node_state:get(State, succ_pid), {lookup_fin, Hops + 1, Msg});
        true ->
            P = ?RT:next_hop(State, Key),
            cs_send:send(P, {lookup_aux, Key, Hops + 1, Msg})
    end.

get_key(State, Source_PID, HashedKey, Key) ->
    ?LOG("[ ~w | I | Node   | ~w ] get_key ~s~n",[calendar:universal_time(), self(), Key]),
    cs_send:send(Source_PID, {get_key_response, Key, ?DB:read(dht_node_state:get(State, db), HashedKey)}).

set_key(State, Source_PID, Key, Value, Versionnr) ->
    ?LOG("[ ~w | I | Node   | ~w ] set_key ~s ~s~n",[calendar:universal_time(), self(), Key, Value]),
    cs_send:send(Source_PID, {set_key_response, Key, Value, Versionnr}),
    DB = ?DB:write(dht_node_state:get(State, db), Key, Value, Versionnr),
    dht_node_state:set_db(State, DB).

delete_key(State, Source_PID, Key) ->
    {DB2, Result} = ?DB:delete(dht_node_state:get(State, db), Key),
    cs_send:send(Source_PID, {delete_key_response, Key, Result}),
    dht_node_state:set_db(State, DB2).
