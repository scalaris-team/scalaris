%% @copyright 2007-2012 Zuse Institute Berlin

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
%-define(TRACE_BP(X,Y), ct:pal(X,Y)).
-define(TRACE_BP(X,Y), ok).
%% userdevguide-begin gen_component:trace_bp_steps
%-define(TRACE_BP_STEPS(X,Y), io:format(X,Y)).     %% output on console
%-define(TRACE_BP_STEPS(X,Y), ct:pal(X,Y)).        %% output even if called by unittest
%-define(TRACE_BP_STEPS(X,Y), io:format(user,X,Y)). %% clean output even if called by unittest
-define(TRACE_BP_STEPS(X,Y), ok).
%% userdevguide-end gen_component:trace_bp_steps

-ifdef(with_ct).
-define(SPAWNED(MODULE), tester_scheduler:gen_component_spawned(MODULE)).
-define(INITIALIZED(MODULE), tester_scheduler:gen_component_initialized(MODULE)).
-define(CALLING_RECEIVE(MODULE), tester_scheduler:gen_component_calling_receive(MODULE)).
-else.
-define(SPAWNED(MODULE), ok).
-define(INITIALIZED(MODULE), ok).
-define(CALLING_RECEIVE(MODULE), ok).
-endif.

-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.
-export([start_link/3, start_link/4,
         start/3, start/4, start/5]).
-export([kill/1, sleep/2, is_gen_component/1, runnable/1,
         get_state/1, get_state/2,
         get_component_state/1, get_component_state/2,
         change_handler/2, post_op/2]).
-export([bp_set/3, bp_set_cond/3, bp_del/2]).
-export([bp_step/1, bp_cont/1, bp_barrier/1]).

-ifdef(with_export_type_support).
-export_type([handler/0]).
-endif.

-type bp_name() :: atom().

-type bp_message() ::
          {'$gen_component', bp, breakpoint, step, pid()}
        | {'$gen_component', bp, breakpoint, cont}
        | {'$gen_component', bp, msg_in_bp_waiting, pid()}
        | {'$gen_component', bp, barrier}
        | {'$gen_component', bp, bp_set_cond, fun(), bp_name()}
        | {'$gen_component', bp, bp_set, comm:message_tag(), bp_name()}
        | {'$gen_component', bp, bp_del, bp_name()}.

-type bp() ::
        {bp, MsgTag :: comm:message_tag(), bp_name()}
      | {bp_cond, Condition :: fun(), bp_name()}.

-type bp_state() :: {[bp()],
                     boolean(),
                     [bp_message()],
                     boolean(),
                     pid() | unknown}.

-type component_state() ::
        {Options :: list(), Slowest :: float(), bp_state()}.

-type handler() :: fun((comm:message(), State) -> State).

%% userdevguide-begin gen_component:behaviour
-ifdef(have_callback_support).
-callback init(Args::term()) -> State::term().
-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {init, 1}     % initialize component
     % note: can use arbitrary on-handler, but by default on/2 is used:
%%      {on, 2}        % handle a single message
%%                     % on(Msg, State) -> NewState | unknown_event | kill
    ];
behaviour_info(_Other) ->
    undefined.
-endif.
%% userdevguide-end gen_component:behaviour

%%% API
-spec kill(Pid::pid() | port() | atom()) -> ok.
kill(Pid) ->        Pid ! {'$gen_component', kill}, ok.
-spec sleep(Pid::pid() | port() | atom(), TimeInMs::integer() | infinity) -> ok.
sleep(Pid, Time) -> Pid ! {'$gen_component', sleep, Time}, ok.

-spec is_gen_component(Pid::pid()) -> boolean().
is_gen_component(Pid) ->
    Call = element(2, erlang:process_info(Pid, initial_call)),
    gen_component =:= element(1, Call).

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

-spec receive_state_if_alive(Pid::pid(),
        MsgTag::get_state_response | get_component_state_response)
            -> term() | failed.
receive_state_if_alive(Pid, MsgTag) ->
    case erlang:is_process_alive(Pid) of
        true ->
            receive
                {'$gen_component', MsgTag, State} -> State
                after 100 ->
                    receive_state_if_alive(Pid, MsgTag)
            end;
        _ -> failed
    end.

-spec receive_state_if_alive(Pid::pid(),
        MsgTag::get_state_response | get_component_state_response,
        Timeout::non_neg_integer()) -> term() | failed.
receive_state_if_alive(Pid, MsgTag, Timeout) when Timeout >= 0->
    case erlang:is_process_alive(Pid) of
        true ->
            receive
                {'$gen_component', MsgTag, State} -> State
                after 100 ->
                    receive_state_if_alive(Pid, MsgTag, Timeout - 100)
            end;
        _ -> failed
    end;
receive_state_if_alive(_Pid, _MsgTag, _Timeout) -> failed.

-spec get_state(Pid::pid()) -> term() | failed.
get_state(Pid) ->
    Pid ! {'$gen_component', get_state, self()},
    receive_state_if_alive(Pid, get_state_response).

-spec get_state(Pid::pid(), Timeout::non_neg_integer()) -> term() | failed.
get_state(Pid, Timeout) ->
    Pid ! {'$gen_component', get_state, self()},
    receive_state_if_alive(Pid, get_state_response, Timeout).

-spec get_component_state(Pid::pid()) -> term().
get_component_state(Pid) ->
    Pid ! {'$gen_component', get_component_state, self()},
    receive_state_if_alive(Pid, get_component_state_response).

-spec get_component_state(Pid::pid(), Timeout::non_neg_integer()) -> {Module::module(), Handler::atom(), ComponentState::term()} | failed.
get_component_state(Pid, Timeout) ->
    Pid ! {'$gen_component', get_component_state, self()},
    receive_state_if_alive(Pid, get_component_state_response, Timeout).

%% @doc change the handler for handling messages
-spec change_handler(State, Handler::handler())
        -> {'$gen_component', [{on_handler, Handler::handler()}], State}.
change_handler(State, Handler) when is_function(Handler, 2) ->
    {'$gen_component', [{on_handler, Handler}], State}.

%% @doc perform a post op, i.e. handle a message directly after another
-spec post_op(State, comm:message())
        -> {'$gen_component', [{post_op, comm:message()}], State}.
post_op(State, Message) ->
    {'$gen_component', [{post_op, Message}], State}.

%% requests regarding breakpoint processing
-spec bp_set(pid(), comm:message_tag(), bp_name()) -> ok.
bp_set(Pid, MsgTag, BPName) ->
    Pid ! {'$gen_component', bp, bp_set, MsgTag, BPName},
    ok.

%% @doc Module:Function(Message, State, Params) will be evaluated to decide
%% whether a BP is reached. Params can be used as a payload.
-spec bp_set_cond(pid(),
                  Cond::{module(), atom(), 2}
                      | fun((comm:message(), State::any()) -> boolean()),
                  bp_name()) -> ok.
bp_set_cond(Pid, {_Module, _Function, _Params = 2} = Cond, BPName) ->
    Pid ! {'$gen_component', bp, bp_set_cond, Cond, BPName},
    ok;
bp_set_cond(Pid, Cond, BPName) when is_function(Cond) ->
    Pid ! {'$gen_component', bp, bp_set_cond, Cond, BPName},
    ok.

-spec bp_del(pid(), bp_name()) -> ok.
bp_del(Pid, BPName) ->
    Pid ! {'$gen_component', bp, bp_del, BPName},
    ok.

-spec bp_step(pid()) -> {module(), On::atom(), comm:message()}.
bp_step(Pid) ->
    Pid !  {'$gen_component', bp, breakpoint, step, self()},
    receive {'$gen_component', bp, breakpoint, step_done,
             _GCPid, Module, On, Message} ->
            ?TRACE_BP_STEPS("    Handler: ~p:~p/2~n"
                            "*** Handling done.~n",
                            [Module, On]),
            {Module, On, Message}
    end.

-spec bp_cont(pid()) -> ok.
bp_cont(Pid) ->
    Pid !  {'$gen_component', bp, breakpoint, cont},
    ok.

%% @doc delay further breakpoint requests until a breakpoint actually occurs
-spec bp_barrier(pid()) -> ok.
bp_barrier(Pid) ->
    Pid ! {'$gen_component', bp, barrier},
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% generic framework
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% profile
-spec start_link(module(), handler(), term()) -> {ok, pid()}.
start_link(Module, Handler, Args) ->
    start_link(Module, Handler, Args, []).

-spec start_link(module(), handler(), term(), list()) -> {ok, pid()}.
start_link(Module, Handler, Args, Options) ->
    Pid = spawn_link(?MODULE, start, [Module, Handler, Args, Options, self()]),
    receive
        {started, Pid} ->
            {ok, Pid}
    end.

-spec start(module(), handler(), term()) -> {ok, pid()}.
start(Module, Handler, Args) ->
    start(Module, Handler, Args, []).

-spec start(module(), handler(), term(), list()) -> {ok, pid()}.
start(Module, Handler, Args, Options) ->
    Pid = spawn(?MODULE, start, [Module, Handler, Args, Options, self()]),
    receive
        {started, Pid} ->
            {ok, Pid}
    end.

-spec start(module(), handler(), term(), list(), pid()) -> no_return() | ok.
start(Module, DefaultHandler, Args, Options, Supervisor) ->
    %?SPAWNED(Module),
    case util:app_get_env(verbose, false) of
        false -> ok;
        _ -> io:format("Starting ~p with pid ~.0p.~n",
                       [Module, self()])
    end,
    case lists:keyfind(pid_groups_join_as, 1, Options) of
        {pid_groups_join_as, GroupId, PidName} ->
            pid_groups:join_as(GroupId, PidName),
            log:log(info, "[ gen_component ] ~p started ~p:~p as ~s:~p", [Supervisor, self(), Module, GroupId, PidName]),
            ?DEBUG_REGISTER(list_to_atom(atom_to_list(Module) ++ "_"
                                         ++ randoms:getRandomString()), self()),
            case lists:keyfind(erlang_register, 1, Options) of
                false when is_atom(PidName) ->
                    %% we can give this process a better name for
                    %% debugging, for example for etop.
                    EName = list_to_atom(GroupId ++ "-"
                                         ++ atom_to_list(PidName)),
                    catch(erlang:register(EName, self()));
                false ->
                    EName = list_to_atom(GroupId ++ "-" ++ PidName),
                    catch(erlang:register(EName, self()));
                _ -> ok
            end;
        false ->
            log:log(info, "[ gen_component ] ~p started ~p:~p", [Supervisor, self(), Module])
    end,
    _ = case lists:keyfind(erlang_register, 1, Options) of
            {erlang_register, Name} ->
                _ = case whereis(Name) of
                        undefined -> ok;
                        _ -> catch(erlang:unregister(Name)) %% unittests may leave garbage
                    end,
                catch(erlang:register(Name, self()));
            false ->
                ?DEBUG_REGISTER(list_to_atom(atom_to_list(Module) ++ "_"
                                             ++ randoms:getRandomString()), self()),
                ok
        end,
    _ = case lists:member(wait_for_init, Options) of
            true -> ok;
            false -> Supervisor ! {started, self()}
        end,
    try
        InitialComponentState = {Options, _Slowest = 0.0, bp_state_new()},
        Handler = case Module:init(Args) of
                      {'$gen_component', Config, InitialState} ->
                          {on_handler, NewHandler} =
                              lists:keyfind(on_handler, 1, Config),
                          NewHandler;
                      InitialState ->
                          DefaultHandler
                  end,
        case lists:member(wait_for_init, Options) of
            false -> ok;
            true -> Supervisor ! {started, self()}, ok
        end,
        ?INITIALIZED(Module),
        try_loop(Module, Handler, InitialState, InitialComponentState)
    catch
        % note: if init throws up, we will not send 'started' to the supervisor
        % this process will die and the supervisor will try to restart it
        Level:Reason ->
            log:log(error,"Error: exception ~p:~p in init of ~p:  ~.0p",
                    [Level, Reason, Module, erlang:get_stacktrace()]),
            erlang:Level(Reason)
    end.

-spec try_loop(module(), handler(), term(), component_state()) -> no_return() | ok.
try_loop(Module, Handler, State, {_Options, _Slowest, _BPState} = ComponentState) ->
    try loop(Module, Handler, State, ComponentState)
    catch Level:Reason ->
              log:log(error, "Error: exception ~p:~p in loop of module ~p "
                          "in (~.0p) with ~p - stacktrace: ~.0p",
                      [Level, Reason, Module, State, Handler,
                       erlang:get_stacktrace()]),
              try_loop(Module, Handler, State, ComponentState)
    end.

-spec loop(module(), handler(), term(), component_state()) -> no_return() | ok.
loop(Module, Handler, State, {_Options, _Slowest, _BPState} = ComponentState) ->
    ?CALLING_RECEIVE(Module),
    receive Msg -> loop(Module, Handler, Msg, State, ComponentState)
    end.

-spec loop(module(), handler(), comm:message(), term(), component_state()) -> no_return() | ok.
loop(Module, Handler, ReceivedMsg, State, {_Options, _Slowest, _BPState} = ComponentState) ->
    case ReceivedMsg of
        %%%%%%%%%%%%%%%%%%%%
        %% Attention!:
        %%   we actually manage two queues here:
        %%     user requests and breakpoint requests
        %%   FIFO order has to be maintained for both (separately)
        %%%%%%%%%%%%%%%%%%%%
        {'$gen_component', kill} ->
            log:log(info, "[ gen_component ] ~.0p killed (~.0p:~.0p/2):",
                    [self(), Module, Handler]),
            ok;
        GenComponentMessage
          when is_tuple(GenComponentMessage) andalso
               '$gen_component' =:= element(1, GenComponentMessage) ->
            NewComponentState =
                handle_gen_component_message(GenComponentMessage, Module, Handler,
                                             State, ComponentState),
            loop(Module, Handler, State, NewComponentState);
        % handle failure detector messages
        {ping, Pid} ->
            comm:send(Pid, {pong}, [{channel, prio}]),
            loop(Module, Handler, State, ComponentState);
        %% forward a message to group member by its process name
        %% initiated via comm:send/3 with group_member
        {send_to_group_member, Processname, Msg} ->
            Pid = pid_groups:get_my(Processname),
            case Pid of
                failed -> ok;
                _      -> comm:send_local(Pid, Msg)
            end,
            loop(Module, Handler, State, ComponentState);
        Message ->
            TmpComponentState =
                handle_breakpoint(Message, State, ComponentState),
            %% Start = erlang:now(),
            case (try Handler(Message, State)
                  catch Level:Reason ->
                            Stacktrace = erlang:get_stacktrace(),
                            case Stacktrace of
                                [{Module, Handler, [Message, State]} | _] % for erlang < R15
                                  when Reason =:= function_clause andalso
                                           Level =:= error ->
                                    unknown_event;
                                [{Module, Handler, [Message, State], _} | _] % erlang >= R15
                                  when Reason =:= function_clause andalso
                                           Level =:= error ->
                                    unknown_event;
                                _ ->
                                    log:log(error,
                                            "~n** Exception:~n ~.0p:~.0p~n"
                                            "** Current message:~n ~.0p~n"
                                            "** Module:~n ~.0p~n"
                                            "** Source linetrace (enable in scalaris.hrl):~n ~.0p~n"
                                            "** State:~n ~.0p~n"
                                            "** Handler:~n ~.0p~n"
                                            "** Stacktrace:~n ~.0p~n",
                                            [Level, Reason,
                                             Message,
                                             Module,
                                             erlang:get(test_server_loc),
                                             State,
                                             Handler,
                                             Stacktrace]),
                                    '$exception'
                            end
                  end) of
                '$exception' ->
                    NextComponentState = bp_step_done(Module, Handler, Message, TmpComponentState),
                    loop(Module, Handler, State, NextComponentState);
                unknown_event ->
                    {NewState, NewComponentState} =
                        handle_unknown_event(Message, State,
                                             TmpComponentState, Module, Handler),
                    NextComponentState = bp_step_done(Module, Handler, Message, NewComponentState),
                    loop(Module, Handler, NewState, NextComponentState);
                kill ->
                    log:log(info, "[ gen_component ] ~.0p killed (~.0p:~.0p/2):",
                            [self(), Module, Handler]),
                    ok;
                {'$gen_component', [{post_op, Msg1}], NewState} ->
                    handle_post_op(Msg1, Module, Handler, NewState, TmpComponentState);
                {'$gen_component', Commands, NewState} ->
                    %% This is not counted as a bp_step
                    case lists:keyfind(on_handler, 1, Commands) of
                        {on_handler, NewHandler} ->
                            loop(Module, NewHandler, NewState, TmpComponentState);
                        false ->
                            case lists:keyfind(post_op, 1, Commands) of
                                {post_op, Msg1} ->
                                    handle_post_op(Msg1, Module, Handler, NewState, TmpComponentState);
                                false ->
                                    % let's fail since the Config list was either
                                    % empty or contained an invalid entry
                                    log:log(warn, "[ gen_component ] unknown command(s): ~.0p",
                                            [Commands]),
                                    erlang:throw('unknown gen_component command')
                            end
                    end;
                NewState ->
                    %%Stop = erlang:now(),
                    %%Span = timer:now_diff(Stop, Start),
                    %%if
                    %% Span > Slowest ->
                    %% io:format("slow message ~p (~p)~n", [Message, Span]),
                    %% loop(Module, NewState, {Options, Span});
                    %%true ->
                    NextComponentState = bp_step_done(Module, Handler, Message, TmpComponentState),
                    loop(Module, Handler, NewState, NextComponentState)
                    %%end
                end
    end.

-spec handle_post_op(Message::comm:message(), Module::module(), Handler::handler(),
                     State::term(), component_state()) -> no_return() | ok.
handle_post_op(Message, Module, Handler, State, ComponentState) ->
    {_Opts, _Slowest, BPState} = ComponentState,
    case bp_state_get_bpactive(BPState) of
        true ->
            ?TRACE_BP_STEPS("~n"
                            "*** Trigger post-op...~n"
                            "    Process: ~p (~p)~n"
                            "    Handler: ~p:~p/2~n"
                            "    Message: ~.0p~n",
                            [self(), pid_groups:group_and_name_of(self()),
                             Module, Handler, Msg1]),
            self() ! {'$gen_component', bp, breakpoint, step,
                      bp_state_get_bpstepper(BPState)},
            ok;
        false -> ok
    end,
    loop(Module, Handler, Message, State, ComponentState).

-spec handle_gen_component_message(Message::comm:message(), Module::module(),
                                   Handler::handler(), State::term(), component_state())
        -> component_state().
handle_gen_component_message(Message, Module, Handler, State, ComponentState) ->
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
            ComponentState;
        {'$gen_component', get_component_state, Pid} ->
            comm:send_local(
              Pid, {'$gen_component', get_component_state_response, {Module, Handler, ComponentState}}),
            ComponentState
    end.

-spec gc_state_bp_set_cond(component_state(),fun(),atom())
                          -> component_state().
gc_state_bp_set_cond({Options, Slowest, BPState}, Cond, BPName) ->
    ?TRACE_BP("Set bp ~p with name ~p~n", [Cond, BPName]),
    {Options, Slowest, bp_state_bp_set_cond(BPState, Cond, BPName)}.

-spec gc_state_bp_set(component_state(),comm:message_tag(),atom())
                     -> component_state().
gc_state_bp_set({Options, Slowest, BPState}, MsgTag, BPName) ->
    ?TRACE_BP("Set bp ~p with name ~p~n", [MsgTag, BPName]),
    {Options, Slowest, bp_state_bp_set(BPState, MsgTag, BPName)}.

-spec gc_state_bp_del(component_state(), atom())
                     -> component_state().
gc_state_bp_del({Options, Slowest, BPState}, BPName) ->
    ?TRACE_BP("Del bp ~p~n", [BPName]),
    {Options, Slowest, bp_state_bp_del(BPState, BPName)}.

-spec gc_state_bp_hold_back(component_state(), bp_message())
                           -> component_state().
gc_state_bp_hold_back({Options, Slowest, BPState}, Message) ->
    ?TRACE_BP("Enqueued bp op ~p -> ~p~n", [Message,
    {Options, Slowest, bp_state_hold_back(BPState, Message)}]),
    {Options, Slowest, bp_state_hold_back(BPState, Message)}.
%    NewQueue = lists:append(Queue, [Message]),

-spec handle_breakpoint(comm:message(), any(), component_state())
                           -> component_state().
handle_breakpoint(_Message, _State, {_,_,BPState} = ComponentState)
  when element(1, BPState) =:= []
       andalso element(2, BPState) =:= false ->
    ComponentState;
handle_breakpoint(Message, State, ComponentState) ->
    BPActive = bp_active(Message, State, ComponentState),
    wait_for_bp_leave(Message, State, ComponentState, BPActive).

-spec bp_active(comm:message(), any(), component_state()) -> boolean().
bp_active(_Message, _State,
          {_Options, _Slowest,
           {[] = _BPs, false = _BPActive, _HB_BP_Ops, _BPStepped, _StepperPid}}
          = _ComponentState) ->
    false;
bp_active(Message, State,
          {Options, Slowest,
           {BPs, BPActive, HB_BP_Ops, BPStepped, StepperPid}} = _ComponentState) ->
    BPActive
        orelse
        begin
            [ ThisBP | RemainingBPs ] = BPs,
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

-spec wait_for_bp_leave(comm:message(), any(), component_state(), boolean())
                       -> component_state().
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
                            {[BPMsg], false};
                        GetCompStateMsg when
                              is_tuple(GetCompStateMsg),
                              '$gen_component' =:= element(1, GetCompStateMsg),
                              get_component_state =:= element(2, GetCompStateMsg) ->
                            {[GetCompStateMsg], false}
                    end;
                _ ->
                    ?TRACE_BP("~p process queued bp op ~p.~n", [self(), hd(HB_BP_Ops)]),
                    {HB_BP_Ops, true}
            end,
    handle_bp_request_in_bp(
      Message, State,
      {Options, Slowest, {BPs, true, tl(Queue), BPStepped, StepperPid}}, hd(Queue), FromQueue).

-spec handle_bp_request_in_bp(comm:message(), any(), component_state(),
                              bp_message(), boolean())
                             -> component_state().
handle_bp_request_in_bp(Message, State, ComponentState, BPMsg, FromQueue) ->
    ?TRACE_BP("Handle bp request in bp ~p ~n", [BPMsg]),
    {Options, Slowest, BPState} = ComponentState,
    case BPMsg of
        {'$gen_component', bp, breakpoint, step, StepperPid} ->
            TmpBPState = bp_state_set_bpstepped(BPState, true),
            NewBPState = bp_state_set_bpstepper(TmpBPState, StepperPid),
            ?TRACE_BP_STEPS("~n"
                            "*** Start handling message...~n"
                            "    Process: ~p (~p)~n"
                            "    Message: ~.0p~n",
                            [self(), pid_groups:group_and_name_of(self()),
                             Message]),
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
                case {bp_state_get_queue(BPState), FromQueue} of
                    {[_H|_T], false} -> gc_state_bp_hold_back(ComponentState, BPMsg);
                    _ -> gc_state_bp_del(ComponentState, BPName)
                end,
            wait_for_bp_leave(Message, State, NextCompState, true);
        {'$gen_component', get_component_state, Pid} ->
            comm:send_local(
              Pid, {'$gen_component', get_component_state_response, State}),
            wait_for_bp_leave(Message, State, ComponentState, true)
    end.

-spec handle_unknown_event(Message::tuple(), any(), component_state(),
                           module(), handler()) -> {any(), component_state()}.
handle_unknown_event({web_debug_info, Requestor}, State, ComponentState, Module, Handler) ->
    GenCompInfo = lists:flatten(io_lib:format("~p", [State])),
    comm:send_local(Requestor, {web_debug_info_reply,
                                [{"generic info from gen_component:", ""},
                                 {"module", Module}, {"handler", Handler},
                                 {"state", GenCompInfo}]}),
    {State, ComponentState};
handle_unknown_event(UnknownMessage, State, ComponentState, Module, Handler) ->
   log:log(error, "unknown message: ~.0p~n in Module: ~p and handler ~p"
           " in pid ~p ~.0p~n in State ~.0p",
           [UnknownMessage,Module,Handler,self(), pid_groups:group_and_name_of(self()), State]),
    {State, ComponentState}.

-spec bp_state_new() -> bp_state().
bp_state_new() ->
    {_BPs = [], _BPActive = false,
     _BP_HB_Q = [], _BPStepped = false,
     _BPStepperPid = unknown}.
-spec bp_state_get_bps(bp_state()) -> [bp()].
bp_state_get_bps(BPState) -> element(1, BPState).
-spec bp_state_set_bps(bp_state(), [bp()]) -> bp_state().
bp_state_set_bps(BPState, Val) -> setelement(1, BPState, Val).
-spec bp_state_get_bpactive(bp_state()) -> boolean().
bp_state_get_bpactive(BPState) -> element(2, BPState).
-spec bp_state_set_bpactive(bp_state(), boolean()) -> bp_state().
bp_state_set_bpactive(BPState, Val) -> setelement(2, BPState, Val).
-spec bp_state_get_queue(bp_state()) -> [bp_message()].
bp_state_get_queue(BPState) -> element(3, BPState).
-spec bp_state_set_queue(bp_state(), [bp_message()]) -> bp_state().
bp_state_set_queue(BPState, Val) -> setelement(3, BPState, Val).
-spec bp_state_get_bpstepped(bp_state()) -> boolean().
bp_state_get_bpstepped(BPState) -> element(4, BPState).
-spec bp_state_set_bpstepped(bp_state(), boolean()) -> bp_state().
bp_state_set_bpstepped(BPState, Val) -> setelement(4, BPState, Val).
-spec bp_state_get_bpstepper(bp_state()) -> pid() | 'unknown'.
bp_state_get_bpstepper(BPState) -> element(5, BPState).
-spec bp_state_set_bpstepper(bp_state(), pid() | 'unknown') -> bp_state().
bp_state_set_bpstepper(BPState, Val) -> setelement(5, BPState, Val).
-spec bp_state_bp_set_cond(bp_state(), fun(), bp_name()) -> bp_state().
bp_state_bp_set_cond(BPState, Cond, BPName) ->
    NewBPs = [ {bp_cond, Cond, BPName} | bp_state_get_bps(BPState)],
    bp_state_set_bps(BPState, NewBPs).

-spec bp_state_bp_set(bp_state(), comm:message_tag(), bp_name()) -> bp_state().
bp_state_bp_set(BPState, MsgTag, BPName) ->
    NewBPs = [ {bp, MsgTag, BPName} | bp_state_get_bps(BPState)],
    bp_state_set_bps(BPState, NewBPs).
-spec bp_state_bp_del(bp_state(), bp_name()) -> bp_state().
bp_state_bp_del(BPState, BPName) ->
    NewBPs = lists:keydelete(BPName, 3, bp_state_get_bps(BPState)),
    bp_state_set_bps(BPState, NewBPs).
-spec bp_state_hold_back(bp_state(), bp_message()) -> bp_state().
bp_state_hold_back(BPState, Message) ->
    NewQueue = lists:append(bp_state_get_queue(BPState), [Message]),
    bp_state_set_queue(BPState, NewQueue).

%% @doc release the bp_step function when we executed a bp_step
-spec bp_step_done(module(), handler(), comm:message(), component_state())
                  -> component_state().
bp_step_done(Module, Handler, Message, ComponentState) ->
    {Options, Slowest, BPState} = ComponentState,
    case bp_state_get_bpactive(BPState) andalso
          bp_state_get_bpstepped(BPState) of
        true ->
            comm:send_local(bp_state_get_bpstepper(BPState),
                            {'$gen_component', bp, breakpoint, step_done,
                             self(), Module, Handler, Message}),
            TmpBPState = bp_state_set_bpstepped(BPState, false),
            NextBPState = bp_state_set_bpstepper(TmpBPState, unknown),
            {Options, Slowest, NextBPState};
        _ -> ComponentState
    end.
