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
%%% File    : unittest_helper.erl
%%% Author  : Thorsten Schuett <schuett@zib.de>
%%% Description : Helper functions for Unit tests 
%%%
%%% Created :  27 Aug 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
-module(unittest_helper).

-author('schuett@zib.de').
-vsn('$Id$ ').

-export([make_ring/1, stop_ring/1]).

make_ring(Size) ->
    error_logger:tty(true),
    Owner = self(),
    Pid = spawn(fun () ->
			timer:sleep(1000),
			process_dictionary:start_link_for_unittest(), 
			boot_sup:start_link(), 
			timer:sleep(1000),
			boot_server:connect(),
			admin:add_nodes(Size - 1, 1000),
			Owner ! {continue},
			timer:sleep(180000) 
		end),
    erlang:monitor(process, Pid),
    receive
	{'DOWN', _Ref, process, _Pid2, Reason} ->
	    ct:pal("process died: ~p ~n", [Reason]);
	{continue} -> 
	    ok
    end,
    check_ring_size(Size),
    wait_for_stable_ring(),
    timer:sleep(5000),
    Pid.

stop_ring(Pid) ->
    exit(Pid, kill).

wait_for_stable_ring() ->
    case admin:check_ring() of
	ok ->
	    ok;
	_ ->
	    timer:sleep(100),
	    wait_for_stable_ring()
    end.

check_ring_size(Size) ->
    erlang:put(instance_id, process_dictionary:find_group(cs_node)),
    case length(statistics:get_ring_details()) == Size of
	true ->
	    ok;
	_ ->
	    timer:sleep(100),
	    check_ring_size(Size)
    end.
