%  Copyright 2008, 2009 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
    undefined = ets:info(config_ets),
    Pid = spawn(fun () ->
                        %timer:sleep(1000),
                        ct:pal("Trying to build Scalaris~n"),
                        randoms:start(),
                        process_dictionary:start_link(),
                        %timer:sleep(1000),
                        boot_sup:start_link(),
                        %timer:sleep(1000),
                        boot_server:connect(),
                        admin:add_nodes(Size - 1),
                        Owner ! {continue},
                        receive
                            {done} ->
                                ok
                        end
                end),
    %erlang:monitor(process, Pid),
    receive
        {'DOWN', _Ref, process, _Pid2, Reason} ->
            ct:pal("process died: ~p ~n", [Reason]);
        {continue} ->
            ok
    end,
    timer:sleep(1000),
    check_ring_size(Size),
    wait_for_stable_ring(),
    check_ring_size(Size),
    ct:pal("Scalaris has booted~n"),
%    timer:sleep(30000),
    Pid.

stop_ring(Pid) ->
    try
        begin
            error_logger:tty(false),
            exit(Pid, kill),
            timer:sleep(1000),
            wait_for_process_to_die(Pid),
            wait_for_table_to_disappear(process_dictionary),
            timer:sleep(10000),
            ok
        end
    catch
        throw:Term ->
            ct:pal("exception in stop_ring: ~p~n", [Term]),
            throw(Term);
        exit:Reason ->
            ct:pal("exception in stop_ring: ~p~n", [Reason]),
            throw(Reason);
        error:Reason ->
            ct:pal("exception in stop_ring: ~p~n", [Reason]),
            throw(Reason)
    end.

wait_for_process_to_die(Pid) ->
    case is_process_alive(Pid) of
        true ->
            timer:sleep(500),
            wait_for_process_to_die(Pid);
        false ->
            ok
    end.

wait_for_table_to_disappear(Table) ->
    case ets:info(Table) of
        undefined ->
            ok;
        _ ->
            timer:sleep(500),
            wait_for_table_to_disappear(Table)
    end.

wait_for_stable_ring() ->
    R = admin:check_ring(),
    ct:pal("CheckRing: ~p~n",[R]),
    case R of
        ok ->
            ok;
        _ ->
            timer:sleep(1000),
            wait_for_stable_ring()
    end.

check_ring_size(Size) ->
    erlang:put(instance_id, process_dictionary:find_group(cs_node)),
    boot_server:number_of_nodes(),
    RSize = receive
        {get_list_length_response,L} ->
            L
    end,
    ct:pal("Size: ~p~n",[RSize]),
    case (RSize == Size) of
        true ->
            ok;
        _ ->
            timer:sleep(1000),
            check_ring_size(Size)
    end.
