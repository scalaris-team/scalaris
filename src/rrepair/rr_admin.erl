% @copyright 2012 Zuse Institute Berlin

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

%% @author Maik Lange <lakedaimon300@googlemail.com>
%% @doc Aministrative helper functions for replica repair
%% @version $Id:  $
-module(rr_admin).

-export([make_ring/2, 
         set_recon_method/1]).

-include("scalaris.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TYPES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type ring_type() :: symmetric | random.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec make_ring(ring_type(), pos_integer()) -> ok.
make_ring(Type, Size) ->
    %if ring exists kill all
    case Type of
        random -> 
            admin:add_node([{first}]),
            admin:add_nodes(Size -1);
        symmetric ->
            Ids = get_symmetric_ids(Size),
            admin:add_node([{first}, {{dht_node, id}, hd(Ids)}]),
            [admin:add_node_at_id(Id) || Id <- Ids]
    end,
    wait_for_stable_ring(),
    ok.

-spec set_recon_method(rep_upd_recon:method()) -> ok | {error, term()}.
set_recon_method(Method) ->
    config:write(rep_update_recon_method, Method),
    RM = config:read(rep_update_recon_method),
    case RM =:= Method of
        true -> ok;
        _ -> {error, set_failed}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

get_node_list() ->
    mgmt_server:node_list(),
    receive
        {get_list_response, List} -> List
    end.

get_symmetric_ids(NodeCount) ->
    [element(2, intervals:get_bounds(I)) || I <- intervals:split(intervals:all(), NodeCount)].

-spec wait_for_stable_ring() -> ok.
wait_for_stable_ring() ->
    util:wait_for(fun() ->
                          R = admin:check_ring(),
                          R =:= ok
                  end, 500).
