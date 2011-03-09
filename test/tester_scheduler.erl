%  @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%  @end
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
%%% File    tester.erl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    user-space scheduler
%%% @end
%%% Created :  4 Feb 2011 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$
-module(tester_scheduler).

-author('schuett@zib.de').
-vsn('$Id$').

-export([
         % instrumented calls
         gen_component_spawned/1,
         gen_component_initialized/1,
         gen_component_calling_receive/1,
         comm_send/2,
         comm_send_local/2,
         comm_send_local_after/3,

         % create scheduler
         start/1,

         % start scheduling
         start_scheduling/0,

         % instrument a module
         instrument_module/1
         ]).

% tester_scheduler cannot use gen_component because gen_component is
% instrumented!

-include("tester.hrl").
-include("unittest.hrl").

-record(state, {waiting_processes::any(),
                started::boolean(),
               white_list::list(tuple())}).

comm_send(Pid, Message) ->
    {RealPid, RealMessage} = comm:unpack_cookie(Pid,Message),
    usscheduler ! {comm_send, self(), RealPid, RealMessage},
    receive
        {comm_send, ack} ->
            ok
    end,
    % assume TCP
    comm_layer:send(RealPid, RealMessage),
    ok.

comm_send_local(Pid, Message) ->
    {RealPid, RealMessage} = comm:unpack_cookie(Pid,Message),
    usscheduler ! {comm_send_local, self(), RealPid, RealMessage},
    receive
        {comm_send_local, ack} ->
            ok
    end,
    RealPid ! RealMessage,
    ok.

comm_send_local_after(Delay, Pid, Message) ->
    {RealPid, RealMessage} = comm:unpack_cookie(Pid,Message),
    usscheduler ! {comm_send_local_after, self(), Delay, RealPid, RealMessage},
    receive
        {comm_send_local_after, ack} ->
            ok
    end,
    erlang:send_after(Delay, RealPid, RealMessage).

-spec gen_component_spawned(module()) -> ok.
gen_component_spawned(Module) ->
    usscheduler ! {gen_component_spawned, self(), Module},
    receive
        {gen_component_spawned, ack} ->
            ok
    end.

-spec gen_component_initialized(module()) -> ok.
gen_component_initialized(Module) ->
    usscheduler ! {gen_component_initialized, self(), Module},
    receive
        {gen_component_initialized, ack} ->
            ok
    end.

-spec gen_component_calling_receive(module()) -> ok.
gen_component_calling_receive(Module) ->
    usscheduler ! {gen_component_calling_receive, self(), Module},
    receive
        {gen_component_calling_receive, ack} ->
            ok
    end.

-spec start_scheduling() -> ok.
start_scheduling() ->
    usscheduler ! {start_scheduling},
    ok.

% @doc we assume the standard scalaris layout, i.e. we are currently
% in a ct_run... directory underneath a scalaris checkout. The ebin
% directory should be in ../ebin
instrument_module(Module) ->
    code:delete(Module),
    code:purge(Module),
    Src = get_file_for_module(Module),
    ct:pal("~p", [file:get_cwd()]),
    Options = get_compile_flags_for_module(Module),
    MyOptions = [return_errors,
                 {d, with_ct},
                 {parse_transform, tester_scheduler_parse_transform},
                 binary],
    %ct:pal("~p", [Options]),
    {ok, CurCWD} = file:get_cwd(),
    fix_cwd_scalaris(),
    case compile:file(Src, lists:append(MyOptions, Options)) of
        {ok,_ModuleName,Binary} ->
            ct:pal("Load binary: ~w", [code:load_binary(Module, Src, Binary)]),
            ct:pal("~p", [code:is_loaded(Module)]),
            ok;
        {ok,_ModuleName,Binary,Warnings} ->
            ct:pal("~p", [Warnings]),
            ct:pal("~w", [erlang:load_module(Module, Binary)]),
            ok;
        X ->
            ct:pal("1: ~p", [X]),
            ok
    end,
    file:set_cwd(CurCWD),
    ok.

% @doc main-loop of userspace scheduler
loop(#state{waiting_processes=Waiting, started=Started, white_list=WhiteList} = State) ->
    receive
        {gen_component_spawned, Pid, _Module} ->
            %ct:pal("spawned ~w in ~w", [Pid, Module]),
            Pid ! {gen_component_spawned, ack},
            loop(State);
        {gen_component_initialized, Pid, _Module} ->
            %ct:pal("initialized ~w in ~w", [Pid, Module]),
            Pid ! {gen_component_initialized, ack},
            loop(State);
        {gen_component_calling_receive, Pid, Module} ->
            case lists:member(Module, WhiteList) of
                true ->
                    Pid ! {gen_component_calling_receive, ack},
                    loop(State);
                false ->
                    case Started of
                        true ->
                            %Pid ! {gen_component_calling_receive, ack},
                            loop(schedule_next_task(State, Pid));
                        false ->
                            %ct:pal("stopped ~w in ~w", [Pid, Module]),
                            %Pid ! {gen_component_calling_receive, ack},
                            loop(State#state{waiting_processes=gb_sets:add(Pid, Waiting)})
                    end
            end;
        {comm_send, ReqPid, _Pid, _Message} ->
            ReqPid ! {comm_send, ack},
            loop(State);
        {comm_send_local, ReqPid, _Pid, _Message} ->
            ReqPid ! {comm_send_local, ack},
            loop(State);
        {comm_send_local_after, ReqPid, _Delay, _Pid, _Message} ->
            ReqPid ! {comm_send_local_after, ack},
            loop(State);
        {reschedule} ->
            loop(schedule_next_task(State));
        {start_scheduling} ->
            loop(State#state{started=true});
        X ->
            ct:pal("unknown message ~w", [X]),
            loop(State)
    end.

% @doc spawn userspace scheduler
-spec start(list(tuple())) -> {ok, pid()}.
start(Options) ->
    WhiteList = case lists:keyfind(white_list, 1, Options) of
                    {white_list, List} ->
                        List;
                    false ->
                        []
                end,
    State = #state{waiting_processes=gb_sets:new(), started=false, white_list=WhiteList},
    {ok, spawn(fun () -> seed(Options), loop(State) end)}.

% @doc find and schedule the next process
-spec schedule_next_task(#state{}, pid()) -> #state{}.
schedule_next_task(#state{waiting_processes=Waiting} = State, Pid) ->
    Waiting2 = gb_sets:add(Pid, Waiting),
    schedule_next_task(State#state{waiting_processes=Waiting2}).

-spec schedule_next_task(#state{}) -> #state{}.
schedule_next_task(#state{waiting_processes=Waiting} = State) ->
    case pick_next_runner(Waiting) of
        false ->
            erlang:send_after(sleep_delay(), self(), {reschedule}),
            loop(State);
        Pid ->
            %ct:pal("picked ~w", [Pid]),
            Pid ! {gen_component_calling_receive, ack},
            loop(State#state{waiting_processes=gb_sets:delete_any(Pid, Waiting)})
    end.

% @doc find the next process
pick_next_runner(Pids) ->
    Runnable = gb_sets:fold(fun (Pid, List) ->
                                    case erlang:process_info(Pid, message_queue_len) of
                                        {message_queue_len, 0} ->
                                            List;
                                        {message_queue_len, _} ->
                                            [Pid | List];
                                        _ ->
                                            List
                                    end
                            end, [], Pids),
    case Runnable of
        [] ->
            false;
        _ ->
            %util:randomelem(Runnable)
            seeded_randomelem(Runnable)
    end.

sleep_delay() -> 100.


-spec get_file_for_module(module()) -> {ok, string()}.
get_file_for_module(Module) ->
    % we have to be in $SCALARIS/ebin to find the beam file
    {ok, CurCWD} = file:get_cwd(),
    fix_cwd_ebin(),
    Res = beam_lib:chunks(Module, [compile_info]),
    file:set_cwd(CurCWD),
    case Res of
        {ok, {Module, [{compile_info, Options}]}} ->
            {source, Filename} = lists:keyfind(source, 1, Options),
            Filename;
        X ->
            ct:pal("~w ~p", [Module, X]),
            ct:pal("~p", [file:get_cwd()]),
            erlang:sleep(1000),
            ct:fail(unknown_module)
    end.

-spec get_compile_flags_for_module(module()) -> {ok, list()}.
get_compile_flags_for_module(Module) ->
    % we have to be in $SCALARIS/ebin to find the beam file
    {ok, CurCWD} = file:get_cwd(),
    fix_cwd_ebin(),
    Res = beam_lib:chunks(Module, [compile_info]),
    file:set_cwd(CurCWD),
    case Res of
        {ok, {Module, [{compile_info, Options}]}} ->
            {options, Opts} = lists:keyfind(options, 1, Options),
            Opts;
        X ->
            ct:pal("~w ~w", [Module, X]),
            erlang:sleep(1000),
            ct:fail(unknown_module),
            ok
    end.

% @doc set cwd to $SCALARIS/ebin
-spec fix_cwd_ebin() -> ok | {error, Reason::file:posix()}.
fix_cwd_ebin() ->
    case file:get_cwd() of
        {ok, CurCWD} ->
            case string:rstr(CurCWD, "/ebin") =/= (length(CurCWD) - 4 + 1) of
                true -> file:set_cwd("../ebin");
                _    -> ok
            end;
        Error -> Error
    end.

% @doc set cwd to $SCALARIS
-spec fix_cwd_scalaris() -> ok | {error, Reason::file:posix()}.
fix_cwd_scalaris() ->
    file:set_cwd("..").

seed(Options) ->
    case lists:keyfind(seed, 1, Options) of
        {seed, {A1,A2,A3}} ->
            random:seed(A1, A2, A3),
            ok;
        false ->
            {A1,A2,A3} = now(),
            ct:log("seed: ~p", [{A1,A2,A3}]),
            random:seed(A1, A2, A3),
            ok
    end.

%% @doc Returns a random element from the given (non-empty!) list according to
%%      a uniform distribution.
-spec seeded_randomelem(List::[X,...]) -> X.
seeded_randomelem(List)->
    Length = length(List),
    RandomNum = random:uniform(Length),
    lists:nth(RandomNum, List).

% @todo
% - start_scheduling could set a new/different white_list
