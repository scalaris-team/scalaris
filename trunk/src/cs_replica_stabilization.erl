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
%%% File    : cs_replica_stabilization.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : bulk owner operation TODO
%%%
%%% Created :  18 Feb 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: cs_replica_stabilization.erl 463 2008-05-05 11:14:22Z schuett $
-module(cs_replica_stabilization).

-author('schuett@zib.de').
-vsn('$Id: cs_replica_stabilization.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([recreate_replicas/1]).

%% @doc recreates the replicas of the given key range
%% @spec recreate_replicas({string(), string()}) -> ok
recreate_replicas({From, To}) ->
    InstanceId = erlang:get(instance_id),
    ok.

%%     spawn(fun () -> 
%% 		  erlang:put(instance_id, InstanceId),
%% 		  Intervals = createIntervals(From, To),
%% 		  io:format("{~s, ~s}~n", [From, To]),
%% 		  printIntervals(Intervals),
%% 		  start_fetchers(1, Intervals),
%% 		  recreate_replicas_loop(length(Intervals), gb_sets:empty(), []) 
%% 	  end).

%%====================================================================
%% supervisor functions
%%====================================================================  

recreate_replicas_loop(Intervals, Done, Data) ->
    receive
	{fetched_data, Index, FetchedData} ->
	    case gb_sets:is_member(Index, Done) of
		false ->
		    recreate_replicas_loop(Intervals, gb_sets:add(Index, Done), [FetchedData | Data]);
		true ->
		    recreate_replicas_loop(Intervals, Done, Data)
	    end
    after 1000 ->
	    case gb_sets:size(Done) == Intervals of
		true ->
		    update_db(Data);
		false ->
		    recreate_replicas_loop(Intervals, Done, Data)
	    end
    end.

createIntervals([FromReplica | FromId], [ToReplica | ToId]) ->
    Prefixes = config:replicaPrefixes(),
    FromPrefix = util:find(FromReplica, Prefixes),
    ToPrefix = util:find(ToReplica, Prefixes),
    createIntervals(FromPrefix, FromId, ToPrefix, ToId, Prefixes,
		    length(config:replicaPrefixes()), length(config:replicaPrefixes())).
    
createIntervals(_FromPrefix, _FromId, _ToPrefix, _ToId, _Prefixes, _Replicas, 1) ->
    [];
    
createIntervals(FromPrefix, FromId, ToPrefix, ToId, Prefixes, Replicas, I) ->
    [{[lists:nth(FromPrefix rem Replicas + 1, Prefixes) | FromId], 
      [lists:nth(ToPrefix rem Replicas + 1, Prefixes) | ToId]} | 
     createIntervals(FromPrefix + 1, FromId, ToPrefix + 1, ToId, Prefixes, Replicas, I - 1)].
    
printIntervals([{From, To} | Tail]) ->
    io:format("{~s, ~s} ", [From, To]),
    printIntervals(Tail);
printIntervals([]) ->
    io:format("~n", []).

start_fetchers(_Index, []) ->
    ok;
start_fetchers(Index, [{From, To} | Tail]) ->
    Me = self(),
    spawn(fun () ->
		  fetch_interval(Me, Index, From, To)
	  end),
    start_fetchers(Index + 1, Tail).
    
%%====================================================================
%% fetch functions
%%====================================================================  

fetch_interval(Owner, Index, From, To) ->
    bulkowner:issue_bulk_owner(intervals:new(From, To), 
			       {bulk_read_with_version, cs_send:this(), From, To}),
    timer:send_after(5000, self(), {timeout}),
    fetch_interval_loop(Owner, Index, From, To, [], []).

fetch_interval_loop(Owner, Index, From, To, Done, FetchedData) ->
    case done(From, To, Done) of
	false ->
	    receive
		{timeout} ->
		    timer:send_after(5000, self(), {timeout}),
		    fetch_interval_loop(Owner, Index, From, To, Done, FetchedData);
		{bulk_read_with_version_response, LocalFrom, LocalTo, NewData} ->
		    fetch_interval_loop(Owner, Index, From, To, [{LocalFrom, LocalTo} | Done], 
					merge_data(FetchedData, NewData));
		X ->
		    io:format("unknown message ~w~n", [X]),
		    fetch_interval_loop(Owner, Index, From, To, Done, FetchedData)
	    end;
	true ->
	    Owner ! {fetched_data, Index, FetchedData}
    end.

%%====================================================================
%% TODO functions
%%====================================================================  

%% @TODO
done(From, To, Done) ->
    true.

%% @TODO
merge_data(FetchedData, NewData) ->
    [].

%%====================================================================
%% update database functions (TODO)
%%====================================================================  

%% @TODO
update_db(Data) ->
    ok.
