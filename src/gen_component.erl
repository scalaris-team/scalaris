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
%% @author Florian Schintke <schintke@zib.de>
%% @doc Generic component framework. This component and its usage
%%      are described in more detail in the 'User and Developers Guide'
%%      which can be found in user-dev-guide/main.pdf and online at
%%      [http://code.google.com/p/scalaris/wiki/UsersDevelopersGuide].
%% @end
%% @version $Id$
-module(gen_component).
-vsn('$Id$').

-include("scalaris.hrl").

%% breakpoint tracing
%-define(TRACE_BP(X,Y), io:format("~p", [self()]), io:format(X,Y)).
-define(TRACE_BP(X,Y), ok).
%% userdevguide-begin gen_component:trace_bp_steps
%-define(TRACE_BP_STEPS(X,Y), io:format(X,Y)). %% output on console
%-define(TRACE_BP_STEPS(X,Y), ct:pal(X,Y)).    %% output even if called by unittest
-define(TRACE_BP_STEPS(X,Y), ok).
%% userdevguide-end gen_component:trace_bp_steps

-export([behaviour_info/1]).
-export([start_link/2, start_link/3,
         start/4, start/2,
         start/3]).
-export([kill/1, sleep/2, runnable/1,
         get_state/1, get_state/2, change_handler/2]).
-export([bp_set/3, bp_set_cond/3, bp_del/2]).
-export([bp_step/1, bp_cont/1, bp_barrier/1]).

%% userdevguide-begin gen_component:behaviour
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {init, 1},     % initialize component
     {on, 2}        % handle a single message
                    % on(Msg, State) -> NewState | unknown_event | kill
    ];
%% userdevguide-end gen_component:behaviour
behaviour_info(_Other) ->
    undefined.

%%% API
-spec kill(Pid::pid() | port() | atom()) -> ok.
kill(Pid) ->        Pid ! {'$gen_component', kill}, ok.
-spec sleep(Pid::pid() | port() | atom(), TimeInMs::integer() | infinity) -> ok.
sleep(Pid, Time) -> Pid ! {'$gen_component', sleep, Time}, ok.

-spec runnable(Pid::pid()) -> boolean().
runnable(Pid) ->
    {message_queue_len, MQLen} = erlang:process_info(Pid, message_queue_len),
    MQResult =
        case MQLen of
            0 -> false;
            _ ->
                %% are there messages which are not gen_component messages?
                {messages, Msgs} = erlang:process_info(Pid, messages),
                lists:any(fun(X) -> element(1, X) =/= '$gen_component' end,
                          Msgs)
        end,
    case MQResult of
        false ->
            Pid ! {'$gen_component', bp, msg_in_bp_waiting, self()},
            receive
                {'$gen_component', bp, msg_in_bp_waiting_response, Runnable} ->
                    Runnable
            end;
        true -> true
    end.

-spec get_state(Pid::pid()) -> term().
get_state(Pid) ->
    Pid ! {'$gen_component', get_state, self()},
    receive
        {'$gen_component', get_state_response, State} -> State
    end.

-spec get_state(Pid::pid(), Timeout::non_neg_integer()) -> term().
get_state(Pid, Timeout) ->
    Pid ! {'$gen_component', get_state, self()},
    receive
        {'$gen_component', get_state_response, State} -> State
    after Timeout -> failed
    end.

%% @doc change the handler for handling messages
-spec change_handler(State, Handler::atom())
        -> {'$gen_component', [{on_handler, Handler::atom()}], State}.
change_handler(State, Handler) when is_atom(Handler) ->
    {'$gen_component', [{on_handler, Handler}], State}.

%% requests regarding breakpoint processing
-spec bp_set(Pid::comm:erl_local_pid(), MsgTag::comm:message_tag(), BPName::any()) -> ok.
bp_set(Pid, MsgTag, BPName) ->
    Pid ! {'$gen_component', bp, bp_set, MsgTag, BPName},
    ok.

%% @doc Module:Function(Message, State, Params) will be evaluated to decide
%% whether a BP is reached. Params can be used as a payload.
-spec bp_set_cond(Pid::comm:erl_local_pid(),
                  Cond::{module(), atom(), 2} | fun((comm:message(), State::any()) -> boolean()),
                  BPName::any()) -> ok.
bp_set_cond(Pid, {_Module, _Function, _Params = 2} = Cond, BPName) ->
    Pid ! {'$gen_component', bp, bp_set_cond, Cond, BPName},
    ok;
bp_set_cond(Pid, Cond, BPName) when is_function(Cond) ->
    Pid ! {'$gen_component', bp, bp_set_cond, Cond, BPName},
    ok.

-spec bp_del(Pid::comm:erl_local_pid(), BPName::any()) -> ok.
bp_del(Pid, BPName) ->
    Pid ! {'$gen_component', bp, bp_del, BPName},
    ok.

-spec bp_step(Pid::comm:erl_local_pid()) -> {module(), On::atom(), comm:message()}.
bp_step(Pid) ->
    Pid !  {'$gen_component', bp, breakpoint, step, self()},
    receive {'$gen_component', bp, breakpoint, step_done,
             _GCPid, Module, On, Message} ->
            ?TRACE_BP_STEPS("~p ~p:~p/2 handled message ~w~n",
                            [_GCPid, Module, On, Message]),
            {Module, On, Message}
    end.

-spec bp_cont(Pid::comm:erl_local_pid()) -> ok.
bp_cont(Pid) ->
    Pid !  {'$gen_component', bp, breakpoint, cont},
    ok.

%% @doc delay further breakpoint requests until a breakpoint actually occurs
-spec bp_barrier(Pid::comm:erl_local_pid()) -> ok.
bp_barrier(Pid) ->
    Pid ! {'$gen_component', bp, barrier},
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% generic framework
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% profile
-spec start_link(module(), term()) -> {ok, pid()}.
start_link(Module, Args) ->
    start_link(Module, Args, []).

-spec start_link(module(), term(), list()) -> {ok, pid()}.
start_link(Module, Args, Options) ->
    Pid = spawn_link(?MODULE, start, [Module, Args, Options, self()]),
    receive
        {started, Pid} ->
            {ok, Pid}
    end.

-spec start(module(), term()) -> {ok, pid()}.
start(Module, Args) ->
    start(Module, Args, []).

-spec start(module(), term(), list()) -> {ok, pid()}.
start(Module, Args, Options) ->
    Pid = spawn(?MODULE, start, [Module, Args, Options, self()]),
    receive
        {started, Pid} ->
            {ok, Pid}
    end.

-spec start(module(), term(), list(), comm:erl_local_pid()) -> ok.
start(Module, Args, Options, Supervisor) ->
    case lists:keysearch(pid_groups_join_as, 1, Options) of
        {value, {pid_groups_join_as, GroupId, PidName}} ->
            pid_groups:join_as(GroupId, PidName),
            log:log(info, "[ gen_component ] ~p started ~p:~p as ~s:~p", [Supervisor, self(), Module, GroupId, PidName]),
            ?DEBUG_REGISTER(list_to_atom(lists:flatten(io_lib:format("~p_~p",[Module,randoms:getRandomId()]))),self());
        false ->
            log:log(info, "[ gen_component ] ~p started ~p:~p", [Supervisor, self(), Module])
    end,
    case lists:keysearch(erlang_register, 1, Options) of
        {value, {erlang_register, Name}} ->
            case whereis(Name) of
                undefined -> ok;
                _ -> catch(unregister(Name)) %% unittests may leave garbage
            end,
            catch(register(Name, self()));
        false ->
            ?DEBUG_REGISTER(list_to_atom(lists:flatten(io_lib:format("~p_~p",[Module,randoms:getRandomId()]))),self()),
            ok
    end,
    case lists:member(wait_for_init, Options) of
        true -> ok;
        false -> Supervisor ! {started, self()}
    end,
    try
        InitialComponentState = {Options, _Slowest = 0.0, bp_state_new()},
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
            log:log(error,"exception in init/loop of ~p: ~.0p",
                    [Module, Term]),
            throw(Term);
        exit:Reason ->
            log:log(error,"exception in init/loop of ~p: ~.0p",
                    [Module, Reason]),
            throw(Reason);
        error:Reason ->
            log:log(error,"exception in init/loop of ~p: ~.0p",
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
            comm:send(Pid, {pong}),
            loop(Module, On, State, ComponentState);
        %% forward a message to group member by its process name
        %% initiated via comm:send_to_group_member()
        {send_to_group_member, Processname, Msg} ->
            Pid = pid_groups:get_my(Processname),
            case Pid of
                failed -> ok;
                _ -> comm:send_local(Pid , Msg)
            end,
            loop(Module, On, State, ComponentState);
        Message ->
            TmpComponentState =
                handle_breakpoint(Message, State, ComponentState),
            %% Start = erlang:now(),
            case (try Module:On(Message, State)
                  catch
                      throw:Term -> {exception, Term};
                      exit:Reason -> {exception, Reason};
                      error:Reason -> {exception, {Reason,
                                                   erlang:get_stacktrace()}}
                  end) of
                {exception, {function_clause, [{Module, on, [Message, State]} | _]}} ->
                    % equivalent to unknown_event, but it resulted in an exception
                    {NewState, NewComponentState} =
                        handle_unknown_event(Message, State,
                                             TmpComponentState, Module, On),
                    NextComponentState = bp_step_done(Module, On, Message, NewComponentState),
                    loop(Module, On, NewState, NextComponentState);
                {exception, Exception} ->
                    log:log(error,"Error: exception ~p during handling of ~.0p in module ~p in (~.0p)",
                            [Exception, Message, Module, State]),
                    NextComponentState = bp_step_done(Module, On, Message, TmpComponentState),
                    loop(Module, On, State, NextComponentState);
                unknown_event ->
                    {NewState, NewComponentState} =
                        handle_unknown_event(Message, State,
                                             TmpComponentState, Module, On),
                    NextComponentState = bp_step_done(Module, On, Message, NewComponentState),
                    loop(Module, On, NewState, NextComponentState);
                kill -> ok;
                {'$gen_component', Config, NewState} ->
                    {value, {on_handler, NewHandler}} =
                        lists:keysearch(on_handler, 1, Config),
                    %% This is not counted as a bp_step
                    loop(Module, NewHandler, NewState, TmpComponentState);
                NewState ->
                    %%Stop = erlang:now(),
                    %%Span = timer:now_diff(Stop, Start),
                    %%if
                    %% Span > Slowest ->
                    %% io:format("slow message ~p (~p)~n", [Message, Span]),
                    %% loop(Module, NewState, {Options, Span});
                    %%true ->
                    NextComponentState = bp_step_done(Module, On, Message, TmpComponentState),
                    loop(Module, On, NewState, NextComponentState)
                    %%end
                end
    end.

handle_gen_component_message(Message, State, ComponentState) ->
    {_Options, _Slowest, BPState} = ComponentState,
    case Message of
        {'$gen_component', bp, barrier} ->
            %% start holding back bp messages for next BP
            gc_state_bp_hold_back(ComponentState, Message);
        {'$gen_component', bp, bp_set_cond, Cond, BPName} ->
            case bp_state_get_queue(BPState) of
                [] -> gc_state_bp_set_cond(ComponentState, Cond, BPName);
                _ -> gc_state_bp_hold_back(ComponentState, Message)
            end;
        {'$gen_component', bp, bp_set, MsgTag, BPName} ->
            case bp_state_get_queue(BPState) of
                [] -> gc_state_bp_set(ComponentState, MsgTag, BPName);
                _ -> gc_state_bp_hold_back(ComponentState, Message)
            end;
        {'$gen_component', bp, bp_del, BPName} ->
            case bp_state_get_queue(BPState) of
                [] -> gc_state_bp_del(ComponentState, BPName);
                _ -> gc_state_bp_hold_back(ComponentState, Message)
            end;
        {'$gen_component', bp, breakpoint, step, _Stepper} ->
            gc_state_bp_hold_back(ComponentState, Message);
        {'$gen_component', bp, breakpoint, cont} ->
            gc_state_bp_hold_back(ComponentState, Message);

        {'$gen_component', bp, msg_in_bp_waiting, Pid} ->
            Pid ! {'$gen_component', bp, msg_in_bp_waiting_response, false},
            ComponentState;
        {'$gen_component', sleep, Time} ->
            timer:sleep(Time),
            ComponentState;
        {'$gen_component', get_state, Pid} ->
            comm:send_local(
              Pid, {'$gen_component', get_state_response, State}),
            ComponentState
    end.

gc_state_bp_set_cond({Options, Slowest, BPState}, Cond, BPName) ->
    {Options, Slowest, bp_state_bp_set_cond(BPState, Cond, BPName)}.

gc_state_bp_set({Options, Slowest, BPState}, MsgTag, BPName) ->
    {Options, Slowest, bp_state_bp_set(BPState, MsgTag, BPName)}.

gc_state_bp_del({Options, Slowest, BPState}, BPName) ->
    {Options, Slowest, bp_state_bp_del(BPState, BPName)}.

gc_state_bp_hold_back({Options, Slowest, BPState}, Message) ->
    ?TRACE_BP("Enqueued bp op ~p -> ~p~n", [Message,
    {Options, Slowest, bp_state_hold_back(BPState, Message)}]),
    {Options, Slowest, bp_state_hold_back(BPState, Message)}.
%    NewQueue = lists:append(Queue, [Message]),

handle_breakpoint(Message, State, ComponentState) ->
    BPActive = bp_active(Message, State, ComponentState),
    wait_for_bp_leave(Message, State, ComponentState, BPActive).

bp_active(_Message, _State,
          {_Options, _Slowest,
           {[] = _BPs, false = _BPActive, _HB_BP_Ops, false = _BPStepped, unknown}}
          = _ComponentState) ->
    false;
bp_active(Message, State,
          {Options, Slowest,
           {BPs, BPActive, HB_BP_Ops, BPStepped, StepperPid}} = _ComponentState) ->
    [ ThisBP | RemainingBPs ] = BPs,
    BPActive
        orelse
        begin
            Decision = case ThisBP of
                           {bp, ThisTag, _BPName} ->
                               ThisTag =:= comm:get_msg_tag(Message);
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
                  {Options, Slowest, {RemainingBPs, BPActive, HB_BP_Ops, BPStepped, StepperPid}}).

wait_for_bp_leave(_Message, _State, ComponentState, _BP_Active = false) ->
    ComponentState;
wait_for_bp_leave(Message, State, ComponentState, _BP_Active = true) ->
    {Options, Slowest, {BPs, _BPActive, HB_BP_Ops, BPStepped, StepperPid}} = ComponentState,
    ?TRACE_BP("In wait for bp leave~n", []),
    {Queue, FromQueue} = case HB_BP_Ops of
                [] ->
                    %% trigger a selective receive
                    ?TRACE_BP("~p wait for bp op by receive...", [self()]),
                    receive
                        BPMsg when
                              is_tuple(BPMsg),
                              '$gen_component' =:= element(1, BPMsg),
                              bp =:= element(2, BPMsg) ->
                            ?TRACE_BP("got bp op by receive ~p.~n", [BPMsg]),
                            {[BPMsg], false}
                    end;
                _ ->
                    ?TRACE_BP("~p process queued bp op ~p.~n", [self(), hd(HB_BP_Ops)]),
                    {HB_BP_Ops, true}
            end,
    handle_bp_request_in_bp(
      Message, State,
      {Options, Slowest, {BPs, true, tl(Queue), BPStepped, StepperPid}}, hd(Queue), FromQueue).

handle_bp_request_in_bp(Message, State, ComponentState, BPMsg, FromQueue) ->
    ?TRACE_BP("Handle bp request in bp ~p ~n", [BPMsg]),
    {Options, Slowest, BPState} = ComponentState,
    case BPMsg of
        {'$gen_component', bp, breakpoint, step, StepperPid} ->
            TmpBPState = bp_state_set_bpstepped(BPState, true),
            NewBPState = bp_state_set_bpstepper(TmpBPState, StepperPid),
            {Options, Slowest, NewBPState};
        {'$gen_component', bp, breakpoint, cont} ->
            T1BPState = bp_state_set_bpactive(BPState, false),
            T2BPState = bp_state_set_bpstepped(T1BPState, false),
            NewBPState = bp_state_set_bpstepper(T2BPState, unknown),
            {Options, Slowest, NewBPState};
        {'$gen_component', bp, msg_in_bp_waiting, Pid} ->
            Pid ! {'$gen_component', bp, msg_in_bp_waiting_response, true},
            wait_for_bp_leave(Message, State, ComponentState, true);
        {'$gen_component', bp, barrier} ->
            %% we are in breakpoint. Consume this bp message
            wait_for_bp_leave(Message, State, ComponentState, true);
        {'$gen_component', bp, bp_set_cond, Cond, BPName} ->
            NextCompState =
                case {bp_state_get_queue(BPState), FromQueue} of
                    {[_H|_T], false} -> gc_state_bp_hold_back(ComponentState, BPMsg);
                    _ -> gc_state_bp_set_cond(ComponentState, Cond, BPName)
                end,
            wait_for_bp_leave(Message, State, NextCompState, true);
        {'$gen_component', bp, bp_set, MsgTag, BPName} ->
            NextCompState =
                case {bp_state_get_queue(BPState), FromQueue} of
                    {[_H|_T], false} -> gc_state_bp_hold_back(ComponentState, BPMsg);
                    _ -> gc_state_bp_set(ComponentState, MsgTag, BPName)
                end,
            wait_for_bp_leave(Message, State, NextCompState, true);
        {'$gen_component', bp, bp_del, BPName} ->
            NextCompState =
                case bp_state_get_queue(BPState) of
                    {[_H|_T], false} -> gc_state_bp_hold_back(ComponentState, BPMsg);
                    _ -> gc_state_bp_del(ComponentState, BPName)
                end,
            wait_for_bp_leave(Message, State, NextCompState, true)
    end.

handle_unknown_event(UnknownMessage, State, ComponentState, Module, On) ->
   log:log(error,"unknown message: ~.0p~n in Module: ~p and handler ~p~n in State ~.0p",[UnknownMessage,Module,On,State]),
    {State, ComponentState}.

bp_state_new() ->
    {_BPs = [], _BPActive = false,
     _BP_HB_Q = [], _BPStepped = false,
     _BPStepperPid = unknown}.
bp_state_get_bps(BPState) -> element(1, BPState).
bp_state_set_bps(BPState, Val) -> setelement(1, BPState, Val).
bp_state_get_bpactive(BPState) -> element(2, BPState).
bp_state_set_bpactive(BPState, Val) -> setelement(2, BPState, Val).
bp_state_get_queue(BPState) -> element(3, BPState).
bp_state_set_queue(BPState, Val) -> setelement(3, BPState, Val).
bp_state_get_bpstepped(BPState) -> element(4, BPState).
bp_state_set_bpstepped(BPState, Val) -> setelement(4, BPState, Val).
bp_state_get_bpstepper(BPState) -> element(5, BPState).
bp_state_set_bpstepper(BPState, Val) -> setelement(5, BPState, Val).
bp_state_bp_set_cond(BPState, Cond, BPName) ->
    NewBPs = [ {bp_cond, Cond, BPName} | bp_state_get_bps(BPState)],
    bp_state_set_bps(BPState, NewBPs).
bp_state_bp_set(BPState, MsgTag, BPName) ->
    NewBPs = [ {bp, MsgTag, BPName} | bp_state_get_bps(BPState)],
    bp_state_set_bps(BPState, NewBPs).
bp_state_bp_del(BPState, BPName) ->
    NewBPs = lists:keydelete(BPName, 3, bp_state_get_bps(BPState)),
    bp_state_set_bps(BPState, NewBPs).
bp_state_hold_back(BPState, Message) ->
    NewQueue = lists:append(bp_state_get_queue(BPState), [Message]),
    bp_state_set_queue(BPState, NewQueue).

%% @doc release the bp_step function when we executed a bp_step
bp_step_done(Module, On, Message, ComponentState) ->
    {Options, Slowest, BPState} = ComponentState,
    case {bp_state_get_bpactive(BPState),
          bp_state_get_bpstepped(BPState)} of
        {true, true} ->
            comm:send_local(bp_state_get_bpstepper(BPState),
                            {'$gen_component', bp, breakpoint, step_done,
                             self(), Module, On, Message}),
            TmpBPState = bp_state_set_bpstepped(BPState, false),
            NextBPState = bp_state_set_bpstepper(TmpBPState, unknown),
            {Options, Slowest, NextBPState};
        _ -> {Options, Slowest, BPState}
    end.
