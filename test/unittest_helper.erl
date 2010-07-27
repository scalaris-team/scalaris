%  Copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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
-vsn('$Id$').

-export([make_ring_with_ids/1, make_ring/1, stop_ring/1,

        check_ring_size/1, wait_for_stable_ring/0, wait_for_stable_ring_deep/0]).

-include("scalaris.hrl").

-spec make_ring_with_ids(list(?RT:key())) -> pid().
make_ring_with_ids(Ids) ->
    error_logger:tty(true),
    ct:pal("Starting unittest ~p", [ct:get_status()]),
    Owner = self(),
    undefined = ets:info(config_ets),
    Pid = spawn(fun () ->
                        %timer:sleep(1000),
                        ct:pal("Trying to build Scalaris~n"),
                        randoms:start(),
                        process_dictionary:start_link(),
                        %timer:sleep(1000),
                        sup_scalaris:start_link(boot,
                                                [{{idholder, id}, hd(Ids)}]),
                        %timer:sleep(1000),
                        boot_server:connect(),
                        [admin:add_node_at_id(Id) || Id <- tl(Ids)],
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
    check_ring_size(length(Ids)),
    wait_for_stable_ring(),
    check_ring_size(length(Ids)),
    ct:pal("Scalaris has booted~n"),
%    timer:sleep(30000),
    Pid.

-spec make_ring(pos_integer()) -> pid().
make_ring(Size) ->
    error_logger:tty(true),
    ct:pal("Starting unittest ~p", [ct:get_status()]),
    Owner = self(),
    undefined = ets:info(config_ets),
    Pid = spawn(fun () ->
                        %timer:sleep(1000),
                        ct:pal("Trying to build Scalaris~n"),
                        randoms:start(),
                        process_dictionary:start_link(),
                        %timer:sleep(1000),
                        sup_scalaris:start_link(boot),
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

-spec stop_ring(pid()) -> ok.
stop_ring(Pid) ->
    try
        begin
            error_logger:tty(false),
            exit(Pid, kill),
            timer:sleep(1000),
            wait_for_process_to_die(Pid),
            gen_component:kill(process_dictionary),
            wait_for_table_to_disappear(process_dictionary),
            timer:sleep(2000),
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

-spec wait_for_process_to_die(pid()) -> ok.
wait_for_process_to_die(Pid) ->
    case is_process_alive(Pid) of
        true ->
            timer:sleep(500),
            wait_for_process_to_die(Pid);
        false ->
            ok
    end.

-spec wait_for_table_to_disappear(tid() | atom()) -> ok.
wait_for_table_to_disappear(Table) ->
    case ets:info(Table) of
        undefined ->
            ok;
        _ ->
            timer:sleep(500),
            wait_for_table_to_disappear(Table)
    end.

-spec wait_for_stable_ring() -> ok.
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

-spec wait_for_stable_ring_deep() -> ok.
wait_for_stable_ring_deep() ->
    R = admin:check_ring_deep(),
    ct:pal("CheckRingDeep: ~p~n",[R]),
    case R of
        ok ->
            ok;
        _ ->
            timer:sleep(1000),
            wait_for_stable_ring_deep()
    end.

-spec check_ring_size(non_neg_integer()) -> ok.
check_ring_size(Size) ->
    erlang:put(instance_id, process_dictionary:find_group(dht_node)),
    boot_server:number_of_nodes(),
    RSize = receive
        {get_list_length_response,L} ->
            L
    end,
    ct:pal("Size: ~p~n",[RSize]),
    case (RSize =:= Size) of
        true ->
            ok;
        _ ->
            timer:sleep(1000),
            check_ring_size(Size)
    end.
