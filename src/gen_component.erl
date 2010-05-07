%% @copyright 2007-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc Generic component framework
%% @end
%% @version $Id$
-module(gen_component).
-include("scalaris.hrl").
-author('schuett@zib.de').
-vsn('$Id$ ').

%% breakpoint tracing
%-define(TRACE_BP(X,Y), io:format(X,Y)).
-define(TRACE_BP(X,Y), ok).

-export([behaviour_info/1]).
-export([start_link/2, start_link/3,
         start/4, start/2,
         start/3]).
-export([kill/1, sleep/2,
         get_state/1, change_handler/2]).
-export([set_breakpoint/3, set_breakpoint_cond/3, del_breakpoint/2]).
-export([breakpoint_step/1, breakpoint_cont/1, when_in_breakpoint/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% behaviour definition
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
behaviour_info(callbacks) ->
    [
     % init component
     {init, 1},
     % handle message
     {on, 2}
    ];
behaviour_info(_Other) ->
    undefined.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
kill(Pid) ->
    Pid ! {'$gen_component', kill}.

sleep(Pid, Time) ->
    Pid ! {'$gen_component', time, Time}.

get_state(Pid) ->
    Pid ! {'$gen_component', get_state, self()},
    receive
        {'$gen_component', get_state_response, State} ->
            State
    end.

%% @doc change the handler for handling messages
change_handler(State, Handler) when is_atom(Handler) ->
    {'$gen_component', [{on_handler, Handler}], State}.

%% requests regarding breakpoint processing
set_breakpoint(Pid, Msg_Tag, BPName) ->
    Pid ! {'$gen_component', bp, set_breakpoint, Msg_Tag, BPName}.

%% @doc Module:Function(Message, State, Params) will be evaluated to decide
%% whether a BP is reached. Params can be used as a payload.
set_breakpoint_cond(Pid, {_Module, _Function, _Params} = Cond, BPName) ->
    Pid ! {'$gen_component', bp, set_breakpoint_cond, Cond, BPName}.

del_breakpoint(Pid, BPName) ->
    Pid ! {'$gen_component', bp, del_breakpoint, BPName}.

breakpoint_step(Pid) ->
    Pid !  {'$gen_component', bp, breakpoint, step}.

breakpoint_cont(Pid) ->
    Pid !  {'$gen_component', bp, breakpoint, cont}.

%% @doc delay further breakpoint requests until a breakpoint actually occurs
when_in_breakpoint(Pid) ->
    Pid ! {'$gen_component', bp, when_in_breakpoint}.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% generic framework
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% register, InstanceId, Name
% profile
-spec(start_link/2 :: (module(), term()) -> {ok, pid()}).
start_link(Module, Args) ->
    start_link(Module, Args, []).

-spec(start_link/3 :: (module(), term(), list()) -> {ok, pid()}).
start_link(Module, Args, Options) ->
    Pid = spawn_link(?MODULE, start, [Module, Args, Options, self()]),
    receive
        {started, Pid} ->
            {ok, Pid}
    end.

-spec(start/2 :: (module(), term()) -> {ok, pid()}).
start(Module, Args) ->
    start(Module, Args, []).

-spec(start/3 :: (module(), term(), list()) -> {ok, pid()}).
start(Module, Args, Options) ->
    Pid = spawn(?MODULE, start, [Module, Args, Options, self()]),
    receive
        {started, Pid} ->
            {ok, Pid}
    end.

-spec(start/4 :: (module(), any(), list(), cs_send:erl_local_pid()) -> ok).
start(Module, Args, Options, Supervisor) ->
    %io:format("Starting ~p~n",[Module]),
    case lists:keysearch(register, 1, Options) of
        {value, {register, InstanceId, Name}} ->
            process_dictionary:register_process(InstanceId, Name, self()),
            ?DEBUG_REGISTER(list_to_atom(lists:flatten(io_lib:format("~p_~p",[Module,randoms:getRandomId()]))),self());
        false ->
            case lists:keysearch(register_native, 1, Options) of
            {value, {register_native,Name}} ->
                register(Name, self());
            false ->
                ?DEBUG_REGISTER(list_to_atom(lists:flatten(io_lib:format("~p_~p",[Module,randoms:getRandomId()]))),self()),
                ok
            end
    end,
    case lists:member(wait_for_init, Options) of
        true ->
            ok;
        false ->
            Supervisor ! {started, self()},
            ok
    end,
    try
        InitialComponentState = {Options, _Slowest = 0.0,
                                 {_BPs = [], _BPActive = false,
                                  _BP_HB_Q = []}},
        Handler = case Module:init(Args) of
                      {'$gen_component', Config, InitialState} ->
                          {value, {on_handler, NewHandler}} =
                              lists:keysearch(on_handler, 1, Config),
                          NewHandler;
                      InitialState ->
                          on
                  end,
        case lists:member(wait_for_init, Options) of
            false -> ok;
            true -> Supervisor ! {started, self()}, ok
        end,
        loop(Module, Handler, InitialState, InitialComponentState)
    catch
        % note: if init throws up, we will not send 'started' to the supervisor
        % this process will die and the supervisor will try to restart it
        throw:Term ->
            log:log(error,"exception in init/loop of ~p: ~p~n",
                    [Module, Term]),
            throw(Term);
        exit:Reason ->
            log:log(error,"exception in init/loop of ~p: ~p~n",
                    [Module, Reason]),
            throw(Reason);
        error:Reason ->
            log:log(error,"exception in init/loop of ~p: ~p~n",
                    [Module, {Reason, erlang:get_stacktrace()}]),
            throw(Reason)
    end.

loop(Module, On, State, {_Options, _Slowest, _BPState} = ComponentState) ->
    receive
        %%%%%%%%%%%%%%%%%%%%
        %% Attention!:
        %%   we actually manage two queues here:
        %%     user requests and breakpoint requests
        %%   FIFO order has to be maintained for both (separately)
        %%%%%%%%%%%%%%%%%%%%
        {'$gen_component', kill} ->
            ok;
        GenComponentMessage
          when is_tuple(GenComponentMessage),
               '$gen_component' =:= element(1, GenComponentMessage) ->
            NewComponentState =
                handle_gen_component_message(GenComponentMessage, State,
                                             ComponentState),
            loop(Module, On, State, NewComponentState);
        % handle failure detector messages
        {ping, Pid} ->
            cs_send:send(Pid, {pong}),
            loop(Module, On, State, ComponentState);
        %% forward a message to group member by its process name
        %% initiated via cs_send:send_to_group_member()
        {send_to_group_member, Processname, Msg} ->
            Pid = process_dictionary:get_group_member(Processname),
            case Pid of
                failed -> ok;
                _ -> cs_send:send_local(Pid , Msg)
            end,
            loop(Module, On, State, ComponentState);
        Message ->
            TmpComponentState =
                handle_breakpoint(Message, State, ComponentState),

            %Start = erlang:now(),
            case (try Module:On(Message, State)
                  catch
                      throw:Term -> {exception, Term};
                      exit:Reason -> {exception, Reason};
                      error:Reason -> {exception, {Reason,
                                                   erlang:get_stacktrace()}}
                  end) of
                {exception, Exception} ->
                    log:log(error,"Error: exception ~p during handling of ~p in module ~p in (~p)~n",
                              [Exception, Message, Module, State]),
                    loop(Module, On, State, TmpComponentState);
                unknown_event ->
                    {NewState, NewComponentState} =
                        handle_unknown_event(Message, State,
                                             TmpComponentState, Module, On),
                    loop(Module, On, NewState, NewComponentState);
                kill ->
                    ok;
                {'$gen_component', Config, NewState} ->
                    {value, {on_handler, NewHandler}} =
                        lists:keysearch(on_handler, 1, Config),
                    loop(Module, NewHandler, NewState, TmpComponentState);
                NewState ->
                    %%Stop = erlang:now(),
                    %%Span = timer:now_diff(Stop, Start),
                    %%if
                    %% Span > Slowest ->
                    %% io:format("slow message ~p (~p)~n", [Message, Span]),
                    %% loop(Module, NewState, {Options, Span});
                    %%true ->
                    loop(Module, On, NewState, TmpComponentState)
                    %%end
            end
    end.

handle_gen_component_message(Message, State, ComponentState) ->
    {_Options, _Slowest, {_BPs, _BPActive, Queue}} = ComponentState,
    case Message of
        {'$gen_component', bp, when_in_breakpoint} ->
            %% start holding back bp messages for next BP
            hold_bp_op_back(Message, ComponentState);
        {'$gen_component', bp, set_breakpoint_cond, Cond, BPName} ->
            case Queue of
                [] -> set_bp_cond(Cond, BPName, ComponentState);
                _ -> hold_bp_op_back(Message, ComponentState)
            end;
        {'$gen_component', bp, set_breakpoint, MsgTag, BPName} ->
            case Queue of
                [] -> set_bp(MsgTag, BPName, ComponentState);
                _ -> hold_bp_op_back(Message, ComponentState)
            end;
        {'$gen_component', bp, del_breakpoint, BPName} ->
            case Queue of
                [] -> del_bp(BPName, ComponentState);
                _ -> hold_bp_op_back(Message, ComponentState)
            end;
        {'$gen_component', bp, breakpoint, _StepOrCont} ->
            hold_bp_op_back(Message, ComponentState);

        {'$gen_component', sleep, Time} ->
            timer:sleep(Time),
            ComponentState;
        {'$gen_component', get_state, Pid} ->
            cs_send:send_local(
              Pid, {'$gen_component', get_state_response, State}),
            ComponentState
    end.

set_bp_cond(Cond, BPName, ComponentState) ->
    {Options, Slowest, {BPs, BPActive, Queue}} = ComponentState,
    NewBPs = [ {bp_cond, Cond, BPName} | BPs],
    {Options, Slowest, {NewBPs, BPActive, Queue}}.

set_bp(MsgTag, BPName, ComponentState) ->
    {Options, Slowest, {BPs, BPActive, Queue}} = ComponentState,
    NewBPs = [ {bp, MsgTag, BPName} | BPs],
    {Options, Slowest, {NewBPs, BPActive, Queue}}.

del_bp(BPName, ComponentState) ->
    {Options, Slowest, {BPs, BPActive, Queue}} = ComponentState,
    NewBPs = lists:keydelete(BPName, 3, BPs),
    {Options, Slowest, {NewBPs, BPActive, Queue}}.

hold_bp_op_back(Message, ComponentState) ->
    {Options, Slowest, {BPs, BPActive, Queue}} = ComponentState,
    NewQueue = lists:append(Queue, [Message]),
    ?TRACE_BP("Enqueued bp op ~p.~n", [Message]),
    {Options, Slowest, {BPs, BPActive, NewQueue}}.

handle_breakpoint(Message, State, ComponentState) ->
    wait_for_bp_leave(Message, State, ComponentState,
                      bp_active(Message, State, ComponentState)).

bp_active(_Message, _State,
          {_Options, _Slowest,
           {[] = _BPs, false = _BPActive, _HB_BP_Ops}} = _ComponentState) ->
    false;
bp_active(Message, State,
          {Options, Slowest,
           {BPs, BPActive, HB_BP_Ops}} = _ComponentState) ->
    [ ThisBP | RemainingBPs ] = BPs,
    BPActive
        orelse
        begin
            Decision = case ThisBP of
                           {bp, ThisTag, _BPName} ->
                               ThisTag =:= cs_send:get_msg_tag(Message);
                           {bp_cond, Cond, _BPName} when is_function(Cond) ->
                               Cond(Message, State);
                           {bp_cond, {Module, Fun, Params}, _BPName} ->
                               apply(Module, Fun, [Message, State, Params])
                       end,
            case Decision of
                true ->
                    ?TRACE_BP("Now in BP ~p via: ~p~n", [ThisBP, Message]),
                    Decision;
                false -> Decision
            end
        end
        orelse
        bp_active(Message, State,
                  {Options, Slowest, {RemainingBPs, BPActive, HB_BP_Ops}}).

wait_for_bp_leave(_Message, _State, ComponentState, _BP_Active = false) ->
    ComponentState;
wait_for_bp_leave(Message, State, ComponentState, _BP_Active = true) ->
    {Options, Slowest, {BPs, _BPActive, HB_BP_Ops}} = ComponentState,
    {Queue, FromQueue} = case HB_BP_Ops of
                [] ->
                    %% trigger a selective receive
                    ?TRACE_BP("wait for bp op by receive...", []),
                    receive
                        BPMsg when
                              is_tuple(BPMsg),
                              '$gen_component' =:= element(1, BPMsg),
                              bp =:= element(2, BPMsg) ->
                            ?TRACE_BP("got bp op by receive ~p.~n", [BPMsg]),
                            {[BPMsg], false}
                    end;
                _ ->
                    ?TRACE_BP("process queued bp op ~p.~n", [hd(HB_BP_Ops)]),
                    {HB_BP_Ops, true}
            end,
    handle_bp_request_in_bp(
      Message, State,
      {Options, Slowest, {BPs, true, tl(Queue)}}, hd(Queue), FromQueue).

handle_bp_request_in_bp(Message, State, ComponentState, BPMsg, FromQueue) ->
    {Options, Slowest, {BPs, _BPActive, Queue}} = ComponentState,
    case BPMsg of
        {'$gen_component', bp, breakpoint, step} ->
            {Options, Slowest, {BPs, true, Queue}};
        {'$gen_component', bp, breakpoint, cont} ->
            {Options, Slowest, {BPs, false, Queue}};

        {'$gen_component', bp, when_in_breakpoint} ->
            %% we are in breakpoint. Consume this bp message
            wait_for_bp_leave(Message, State, ComponentState, true);
        {'$gen_component', bp, set_breakpoint_cond, Cond, BPName} ->
            NextCompState =
                case {Queue, FromQueue} of
                    {[_H|_T], false} -> hold_bp_op_back(BPMsg, ComponentState);
                    _ -> set_bp_cond(Cond, BPName, ComponentState)
                end,
            wait_for_bp_leave(Message, State, NextCompState, true);
        {'$gen_component', bp, set_breakpoint, MsgTag, BPName} ->
            NextCompState =
                case {Queue, FromQueue} of
                    {[_H|_T], false} -> hold_bp_op_back(BPMsg, ComponentState);
                    _ -> set_bp(MsgTag, BPName, ComponentState)
                end,
            wait_for_bp_leave(Message, State, NextCompState, true);
        {'$gen_component', bp, del_breakpoint, BPName} ->
            NextCompState =
                case Queue of
                    {[_H|_T], false} -> hold_bp_op_back(BPMsg, ComponentState);
                    _ -> del_bp(BPName, ComponentState)
                end,
            wait_for_bp_leave(Message, State, NextCompState, true)
    end.

handle_unknown_event(UnknownMessage, State, ComponentState, Module, On) ->
   log:log(error,"unknown message: ~p in Module: ~p and handler ~p~n in State ~p ~n",[UnknownMessage,Module,On,State]),
    {State, ComponentState}.
