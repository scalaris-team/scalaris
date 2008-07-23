%  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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
%%% File    : webhelpers.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : 
%%%
%%% Created : 16 Apr 2007 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
%% @version $Id: webhelpers.erl 463 2008-05-05 11:14:22Z schuett $
-module(webhelpers).

-author('schuett@zib.de').
-vsn('$Id: webhelpers.erl 463 2008-05-05 11:14:22Z schuett $ ').

-export([getLoadRendered/0, getRingRendered/0, lookup/1, set_key/2, isPost/1]).

-include("yaws_api.hrl").

%% @doc checks whether the current request is a post operation
%% @spec isPost(A) -> bool()
%%   A = http_request
isPost(A) ->
    Method = (A#arg.req)#http_request.method,
    Method == 'POST'.

%%%-----------------------------Lookup/Put---------------------------

lookup(Key) ->
    timer:tc(transstore.transaction_api, quorum_read, [Key]).

set_key(Key, Value) ->
    timer:tc(transstore.transaction_api, single_write, [Key, Value]).

%%%-----------------------------Load----------------------------------

getLoad() ->
    Nodes = boot_server:node_list(),
    get_load(Nodes).
    
get_load([Head | Tail]) ->
    Head ! {get_load, self()},
    receive
	{get_load_response, Node, Value} -> [{ok, Node, Value} | get_load(Tail)]
    after
	2000 ->
	    [{failed, Head} | get_load(Tail)]
    end;
get_load([]) ->
    [].


%%%-----------------------------Load----------------------------------

getLoadRendered() ->
    Load = getLoad(),
    {table, [{bgcolor, "#cccccc"}, {border, "0"}, {cellpadding, "2"}, {cellspacing, "2"}, {width, "90%"}],
     [{tr, [],
       [
	{td, [{bgcolor, "#336699"}, {width, "48%"}], {font, [{color, "white"}], "Node"}},
	{td, [{bgcolor, "#336699"}, {valign, "top"}, {width, "16%"}], {font, [{color, "white"}], "Load"}}
       ]},
      renderLoad(Load)
     ]
    }.

renderLoad([{ok, Node, Value} | Tail]) ->
    [{tr, [], 
      [
       {td, [], io_lib:format('~p', [Node])},
       {td, [], io_lib:format('~p', [Value])}
      ]}, renderLoad(Tail)];
renderLoad([{failed, Node} | Tail]) ->
    [{tr, [], 
      [
       {td, [], io_lib:format('~p', [Node])},
       {td, [], "-"}
      ]}, renderLoad(Tail)];
renderLoad([]) ->
    [].


%%%-----------------------------Ring----------------------------------


getRingRendered() ->
    RealRing = statistics:get_ring_details(),
    Ring = lists:filter(fun (X) -> is_valid(X) end, RealRing),
    RingSize = util:lengthX(Ring),
    if
	RingSize == 0 ->
	    {p, [], "empty ring"};
	true ->
	    {p, [],
	      [
	      {table, [{bgcolor, '#CCDCEE'}, {width, "100%"}],
	       [
		{tr, [{bgcolor, '#000099'}],
		 [
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Total Load"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Average Load"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Load (std. deviation)"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Real Ring Size"}}}
		 ]
		},
		{tr, [],
		 [
		  {td, [], io_lib:format('~p', [statistics:get_total_load(Ring)])},
		  {td, [], io_lib:format('~p', [statistics:get_average_load(Ring)])},
		  {td, [], io_lib:format('~p', [statistics:get_load_std_deviation(Ring)])},
		  {td, [], io_lib:format('~p', [boot_server:node_list()])}
		 ]
		}
	       ]
	      },
	      {br, []},
	      {table, [{bgcolor, '#CCDCEE'}, {width, "100%"}],
	       [{tr, [{bgcolor, '#000099'}],
		 [
		  {td, [{align, "center"}, {width,"200px"}], {strong, [], {font, [{color, "white"}], "Host"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Pred"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Node"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Succ"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "RTSize"}}},
		  {td, [{align, "center"}], {strong, [], {font, [{color, "white"}], "Load"}}}
		 ]},
		lists:map(fun (Node) -> renderRing(Node) end, Ring)
	       ]
	      }
	     ]
	    }
    end.

renderRing({ok, Details}) ->
    Hostname = node_details:hostname(Details),
    Pred = node_details:pred(Details),
    Node = node_details:me(Details),
    SuccList = node_details:succlist(Details),
    RTSize = node_details:rt_size(Details),
    Load = node_details:load(Details),
    {tr, [], 
      [
       {td, [], [get_flag(Hostname), io_lib:format('~p', [Hostname])]},
       {td, [], io_lib:format('~p', [get_id(Pred)])},
       {td, [], io_lib:format('~p', [get_id(Node)])},
       {td, [], io_lib:format('~p', [lists:map(fun get_id/1, SuccList)])},
       {td, [], io_lib:format('~p', [RTSize])},
       {td, [], io_lib:format('~p', [Load])}
      ]};
renderRing({failed}) ->
    {tr, [], 
      [
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"},
       {td, [], "-"}
      ]}.

%%%-----------------------------Misc----------------------------------

get_id(Node) ->
    IsNull = node:is_null(Node),
    if
	IsNull ->
	    "null";
	true ->
	    node:id(Node)
    end.

get_flag(Hostname) ->
    Country = string:substr(Hostname, 1 + string:rchr(Hostname, $.)),
    URL = string:concat("icons/", string:concat(Country, ".gif")),
    {img, [{src, URL}, {width, 26}, {height, 16}], []}.

is_valid({ok, _}) ->
    true;
is_valid({failed}) ->
    false.
