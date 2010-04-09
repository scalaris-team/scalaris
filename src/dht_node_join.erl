%  @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    dht_node join procedure
%%% @end
%% @version $Id$
-module(dht_node_join).
-author('schuett@zib.de').
-vsn('$Id$ ').

-export([join_request/3, join_first/1]).

-include("../include/scalaris.hrl").

%% @doc handle the join request of a new node
%% @spec join_request(state:state(), pid(), Id) -> state:state()
%%   Id = term()

%% userdevguide-begin dht_node_join:join_request
-spec(join_request/3 :: (dht_node_state:state(), cs_send:mypid(), ?RT:key()) -> dht_node_state:state()).
join_request(State, Source_PID, Id) ->
    Pred = node:new(Source_PID, Id),
    {DB, HisData} = ?DB:split_data(dht_node_state:get_db(State), dht_node_state:id(State), Id),
    cs_send:send(Source_PID, {join_response, dht_node_state:pred(State), HisData}),
    rm_beh:update_preds([Pred]),
    dht_node_state:set_db(State, DB).
%% userdevguide-end dht_node_join:join_request

%%%------------------------------Join---------------------------------



join_first(Id) ->
    log:log(info,"[ Node ~w ] join as first ~w",[self(), Id]),
    Me = node:new(cs_send:this(), Id),
    rt_beh:initialize(Id, Me, Me),
    dht_node_state:new(?RT:empty(Me), Me, Me, Me, {Id, Id}, dht_node_lb:new(), ?DB:new(Id)).
%% userdevguide-end dht_node_join:join_ring
