%  Copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : admin.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Supervisor for boot nodes
%%%
%%% Created : 03 Mar 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: admin.erl 463 2008-05-05 11:14:22Z schuett $
-module(admin).

-author('schuett@zib.de').
-vsn('$Id: admin.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([add_nodes/1, add_nodes/2, check_ring/0, nodes/0]).

%%====================================================================
%% API functions
%%====================================================================

%%--------------------------------------------------------------------
%% Function: add_nodes(int()) -> ok
%% Description: add new chordsharp nodes
%%--------------------------------------------------------------------
% @doc add new chordsharp nodes on the local node
% @spec add_nodes(int()) -> ok

add_nodes(Count) ->
    add_nodes(Count, 0).

% @spec add_nodes(int(), int()) -> ok
add_nodes(Count, Delay) ->
    randoms:init(),
    add_nodes_loop(Count, Delay).

add_nodes_loop(0, _) ->
    ok;
add_nodes_loop(Count, Delay) ->
    supervisor:start_child(main_sup, {randoms:getRandomId(),
				      {cs_sup_or, start_link, []},
				      permanent,
				      brutal_kill,
				      worker,
				      []}),
    timer:sleep(Delay),
    add_nodes_loop(Count - 1, Delay).

%%--------------------------------------------------------------------
%% Function: check_ring() -> term()
%% Description: contact boot server and check ring
%%--------------------------------------------------------------------
% @doc contact boot server and check ring
% @spec check_ring() -> term
check_ring() ->
    erlang:put(instance_id, process_dictionary:find_group(cs_node)),
    Nodes = statistics:get_ring_details(),
    case lists:foldl(fun check_ring_foldl/2, first, Nodes) of
	{error, Reason} ->
		{error, Reason};
	_X ->
		ok
    end.


check_ring_foldl({ok, Node}, first) ->
    get_id(hd(node_details:succlist(Node)));
check_ring_foldl({failed}, Last) ->
    Last;
check_ring_foldl(_, {error, Message}) ->
    {error, Message};
check_ring_foldl({ok, Node}, PredsSucc) ->
    MyId = get_id(node_details:me(Node)),
    if
	MyId == PredsSucc ->
	    get_id(hd(node_details:succlist(Node)));
	true ->
	    {error, lists:flatten(io_lib:format("~.16B didn't match ~.16B", [MyId, PredsSucc]))}
    end.


    
get_id(Node) ->
    IsNull = node:is_null(Node),
    if
        IsNull ->
            "null";
        true ->
            node:id(Node)
    end.

%%--------------------------------------------------------------------
%% Function: nodes() -> list()
%% Description: contact boot server and list the known ip addresses
%%--------------------------------------------------------------------
% @doc contact boot server and list the known ip addresses
% @spec nodes() -> list()
nodes() ->
    util:uniq([IP || {IP, _, _} <- lists:sort(boot_server:node_list())]).
