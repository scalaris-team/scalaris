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
%% @version $Id$
-module(admin).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([add_nodes/1, add_nodes/2, check_ring/0, nodes/0, start_link/0, start/0, 
	 get_dump/0, get_dump_bw/0, diff_dump/3]).

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
-spec(check_ring/0 :: () -> {error, string()} | ok).
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

%%===============================================================================
%% comm_layer:comm_logger functions
%%===============================================================================
get_dump() ->
    [cs_send:send(cs_send:get(admin_server, Server), {get_comm_layer_dump, cs_send:this()}) 
     || Server <- util:get_nodes()],
    % list({Map, StartTime})
    Dumps = [receive 
	 {get_comm_layer_dump_response, Dump} ->
	     Dump
     end || _ <- util:get_nodes()],
    StartTime = lists:min([Start || {_, Start} <- Dumps]),
    Keys = util:uniq(lists:sort(lists:flatten([gb_trees:keys(Map) || {Map, _} <- Dumps]))),
    {lists:foldl(fun (Tag, Map) -> 
			 gb_trees:enter(Tag, get_aggregate(Tag, Dumps), Map)
		 end, gb_trees:empty(), Keys), StartTime}.

get_dump_bw() ->
    {Map, StartTime} = get_dump(),
    RunTime = timer:now_diff(erlang:now(), StartTime),
    [{Tag, Size / RunTime, Count / RunTime} || {Tag, {Size, Count}} <- gb_trees:to_list(Map)].

get_aggregate(_Tag, []) ->
    {0, 0};
get_aggregate(Tag, [{Dump, _} | Rest]) ->
    case gb_trees:lookup(Tag, Dump) of
	none ->
	    get_aggregate(Tag, Rest);
	{value, {Size, Count}} ->
	    {AggSize, AggCount} = get_aggregate(Tag, Rest),
	    {AggSize + Size, AggCount + Count}
    end.
	    
diff_dump(BeforeDump, AfterDump, _RunTime) ->    
    Tags = util:uniq(lists:sort(lists:flatten([gb_trees:keys(BeforeDump), 
					       gb_trees:keys(AfterDump)]))),
    diff(Tags, BeforeDump, AfterDump).
    
diff([], _Before, _After) ->
    [];
diff([Tag | Rest], Before, After) ->
    {NewSize, NewCount} = gb_trees:get(Tag, After),
    case gb_trees:lookup(Tag, Before) of
	none ->
	    [{Tag, NewSize, NewCount} | diff(Rest, Before, After)];
	{value, {Size, Count}} ->
	    [{Tag, NewSize - Size, NewCount - Count} | diff(Rest, Before, After)]
    end.

%%===============================================================================
%% admin server functions
%%===============================================================================
start_link() ->
    {ok, spawn_link(?MODULE, start, [])}.

start() ->
    register(admin_server, self()),
    loop().

loop() ->
    receive
	{get_comm_layer_dump, Sender} ->
	    cs_send:send(Sender, {get_comm_layer_dump_response, 
				  comm_layer.comm_logger:dump()}),
	    loop()
    end.

%%--------------------------------------------------------------------
%% Function: nodes() -> list()
%% Description: contact boot server and list the known ip addresses
%%--------------------------------------------------------------------
% @doc contact boot server and list the known ip addresses
% @spec nodes() -> list()
nodes() ->
    util:uniq([IP || {IP, _, _} <- lists:sort(boot_server:node_list())]).
