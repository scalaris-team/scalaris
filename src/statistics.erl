%  Copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
%%% File    : statistics.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Statistics Module for Bootstrap server
%%%
%%% Created :  7 May 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(statistics).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([get_total_load/1, get_average_load/1, get_load_std_deviation/1, get_ring_details/0, get_average_rt_size/1, get_rt_size_std_deviation/1, get_memory_usage/1, get_max_memory_usage/1]).

get_total_load(Ring) ->
    FilteredRing = lists:filter(fun (X) -> is_valid(X) end, Ring),
    lists:foldl(fun (X, Sum) -> X + Sum end, 0, lists:map(fun get_load/1, FilteredRing)).

get_average_load(Ring) ->
    FilteredRing = lists:filter(fun (X) -> is_valid(X) end, Ring),
    get_total_load(FilteredRing) / length(FilteredRing).

get_memory_usage(Ring) ->
    FilteredRing = lists:filter(fun (X) -> is_valid(X) end, Ring),
    lists:foldl(fun (X, Sum) -> X + Sum end, 0, lists:map(fun get_memory/1, FilteredRing)) / length(FilteredRing).

get_max_memory_usage(Ring) ->
    FilteredRing = lists:filter(fun (X) -> is_valid(X) end, Ring),
    lists:foldl(fun (X, Sum) -> util:max(X, Sum) end, 0, lists:map(fun get_memory/1, FilteredRing)).
    

get_load_std_deviation(Ring) ->			
    FilteredRing = lists:filter(fun (X) -> is_valid(X) end, Ring),
    Average = get_average_load(FilteredRing),
    math:sqrt(lists:foldl(fun (Load, Acc) -> 
				  Acc + (Load - Average)*(Load - Average) 
			  end,
			  0, lists:map(fun get_load/1, FilteredRing)) / length(FilteredRing)).
    
get_load({ok, Details}) ->
    node_details:get(Details, load);
get_load({failed}) ->
    0.
get_memory({ok, Details}) ->
    node_details:get(Details, memory);
get_memory({failed}) ->
    0.

get_ring_details() ->
    boot_server:node_list(),
    Nodes = receive
        {get_list_response,N} -> 
            
            N
       after 2000 ->
            log:log(error,"ST: Timeout~n"),
           
            {failed}
    end,
    lists:sort(fun compare_node_details/2, lists:map(fun (Pid) -> get_node_details(Pid) end, Nodes)).
    


compare_node_details({ok, X}, {ok, Y}) ->
    node:id(node_details:get(X, node)) < node:id(node_details:get(Y, node));
compare_node_details({failed}, _) ->
    true;
compare_node_details({ok, _}, _) ->
    false.

get_node_details(Pid) ->
    cs_send:send(Pid, {get_node_details, cs_send:this_with_cookie(Pid)}),
    receive
	{{get_node_details_response, Details}, Pid} -> 
	    {ok, Details}
    after
        2000 ->
            log:log(error,"[ ST ]: 2sec Timeout by waiting on get_node_details_response form ~p~n",[Pid]),
            receive
                {{get_node_details_response, Details}, Pid} ->
                    {ok, Details}
            after
                4000 ->
                    log:log(error,"[ ST ]: 4sec Timeout by waiting on get_node_details_response form ~p~n",[Pid]),
                    {failed}
            end
    end.

%%%-------------------------------RT----------------------------------


get_total_rt_size(Ring) ->
    lists:foldl(fun (X, Sum) -> X + Sum end, 0, lists:map(fun get_rt/1, Ring)).

get_average_rt_size(Ring) ->
    FilteredRing = lists:filter(fun (X) -> is_valid(X) end, Ring),
    get_total_rt_size(FilteredRing) / length(FilteredRing).

get_rt_size_std_deviation(Ring) ->			
    FilteredRing = lists:filter(fun (X) -> is_valid(X) end, Ring),
    Average = get_average_rt_size(FilteredRing),
    math:sqrt(lists:foldl(fun (RTSize, Acc) -> 
				  Acc + (RTSize - Average)*(RTSize - Average) 
			  end,
			  0, lists:map(fun get_rt/1, FilteredRing)) / length(FilteredRing)).
    
get_rt({ok, Details}) ->
    node_details:get(Details, rt_size);
get_rt({failed}) ->
    0.

is_valid({ok, _}) ->
    true;
is_valid({failed}) ->
    false.
