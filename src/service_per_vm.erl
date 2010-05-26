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
%%% File    : service_per_vm.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description :
%%%
%%% Created : 20 Jan 2010 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @doc
%% @author Thorsten Schuett <schuett@zib.de>
%% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%% @version $Id$
-module(service_per_vm).

-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).

-export([dump_node_states/0, kill_nodes/1]).
-export([get_round_trip/2]).

-export([start_link/0, init/1, on/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% @doc ask all local nodes for their state
dump_node_states() ->
    [gen_component:get_state(Pid)
     || Pid <- process_dictionary:find_all_dht_nodes()].

kill_nodes(No) ->
    Childs = lists:sublist([X || X <- supervisor:which_children(main_sup),
                                 is_list(element(1, X))], No),
    [supervisor:terminate_child(main_sup, element(1, Child)) || Child <- Childs],
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Server process
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
    gen_component:start_link(?MODULE, [], [{register_native, service_per_vm}]).

init(_Arg) ->
    ok.

on({get_dht_nodes, Pid}, ok) ->
    case cs_send:is_valid(Pid) of
        true ->
            Nodes = get_live_dht_nodes(),
            cs_send:send(Pid, {get_dht_nodes_response, Nodes});
        false ->
            ok
    end,
    ok;

on(_, _State) ->
    unknown_event.

get_live_dht_nodes() ->
    [cs_send:make_global(Pid) || Pid <- process_dictionary:find_all_dht_nodes(), element(1, gen_component:get_state(Pid)) =:= state].

get_round_trip(GPid, Iterations) ->
    Start = erlang:now(),
    [ begin
          cs_send:send(GPid, {ping, cs_send:this()}),
          receive
              _Any -> ok
          end
      end
      || _ <- lists:seq(1, Iterations) ],
    End = erlang:now(),
    timer:now_diff(End, Start) / Iterations.
