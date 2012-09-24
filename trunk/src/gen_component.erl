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
-vsn('$Id$ ').

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
-export([start_link/4, start/4, start/5]).
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

-type bp_msg() ::
          {'$gen_component', bp, breakpoint, step, pid()}
        | {'$gen_component', bp, breakpoint, cont}
        | {'$gen_component', bp, msg_in_bp_waiting, pid()}
        | {'$gen_component', bp, barrier}
        | {'$gen_component', bp, bp_set_cond, fun(), bp_name()}
        | {'$gen_component', bp, bp_set, comm:msg_tag(), bp_name()}
        | {'$gen_component', bp, bp_del, bp_name()}.

-type gc_msg() ::
          bp_msg()
        | {'$gen_component', sleep, pos_integer()}
        | {'$gen_component', get_state, pid()}
        | {'$gen_component', get_component_state, pid()}
        | {'$gen_component', trace_mpath, trace_mpath:passed_state(),
           From::term(), To::term(), comm:message()}.

-type bp() ::
        {bp, MsgTag :: comm:msg_tag(), bp_name()}
      | {bp_cond, Condition :: fun(), bp_name()}
      | {bp_cond, {module(), atom(), pos_integer()}, bp_name()}.

-type user_state() :: term().
-type handler() :: fun((comm:message(), user_state()) -> user_state()).
-type gc_state() ::
        { module(),
          handler(),
          [bp()],             %% registered breakpoints
          boolean(),          %% bp active?
          [bp_msg()],         %% queued bp messages
          boolean(),          %% bp stepped?
          pid() | unknown     %% bp stepper
        }.

%% define macros for the tuple positions to use them also in guards.
-define(MOD,         1).
-define(HAND,        2).
%-define(OPTS,        3).
%-define(SLOWEST,     4).
-define(BPS,         3).
-define(BP_ACTIVE,   4).
-define(BP_QUEUE,    5).
-define(BP_STEPPED,  6).
-define(BP_STEPPER,  7).

%% userdevguide-begin gen_component:behaviour
-ifdef(have_callback_support).
-callback init(Args::term()) -> user_state().
-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {init, 1} %% initialize component
     %% note: can use arbitrary on-handler, but by default on/2 is used:
     %% {on, 2} %% handle a single message
     %% on(Msg, UserState) -> NewUserState | unknown_event | kill
    ];
behaviour_info(_Other) -> undefined.
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

-spec get_state(Pid::pid()) -> user_state() | failed.
get_state(Pid) ->
    Pid ! {'$gen_component', get_state, self()},
    receive_state_if_alive(Pid, get_state_response).

-spec get_state(Pid::pid(), Timeout::non_neg_integer()) -> user_state() | failed.
get_state(Pid, Timeout) ->
    Pid ! {'$gen_component', get_state, self()},
    receive_state_if_alive(Pid, get_state_response, Timeout).

-spec get_component_state(Pid::pid()) -> gc_state().
get_component_state(Pid) ->
    Pid ! {'$gen_component', get_component_state, self()},
    receive_state_if_alive(Pid, get_component_state_response).

-spec get_component_state(pid(), Timeout::non_neg_integer()) -> gc_state() | failed.
get_component_state(Pid, Timeout) ->
    Pid ! {'$gen_component', get_component_state, self()},
    receive_state_if_alive(Pid, get_component_state_response, Timeout).

%% @doc change the handler for handling messages
-spec change_handler(user_state(), Handler::handler())
        -> {'$gen_component', [{on_handler, Handler::handler()}], user_state()}.
change_handler(UState, Handler) when is_function(Handler, 2) ->
    {'$gen_component', [{on_handler, Handler}], UState}.

%% @doc perform a post op, i.e. handle a message directly after another
-spec post_op(user_state(), comm:message())
        -> {'$gen_component', [{post_op, comm:message()}], user_state()}.
post_op(UState, Msg) ->
    {'$gen_component', [{post_op, Msg}], UState}.

%% requests regarding breakpoint processing
-spec bp_set(pid(), comm:msg_tag(), bp_name()) -> ok.
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
bp_set_cond(Pid, Cond, BPName) when is_function(Cond, 2) ->
    Pid ! {'$gen_component', bp, bp_set_cond, Cond, BPName},
    ok.

-spec bp_del(pid(), bp_name()) -> ok.
bp_del(Pid, BPName) ->
    Pid ! {'$gen_component', bp, bp_del, BPName},
    ok.

-spec bp_step(pid()) -> {module(), On::atom(), comm:message()}.
bp_step(Pid) ->
    ?TRACE_BP_STEPS("Do step ~p ~p~n", [Pid, catch pid_groups:group_and_name_of(Pid)]),
    Pid !  {'$gen_component', bp, breakpoint, step, self()},
    receive {'$gen_component', bp, breakpoint, step_done,
             _GCPid, Module, On, Msg} ->
            ?TRACE_BP_STEPS("    Handler: ~p:~p/2~n"
                            "*** Handling done.~n",
                            [Module, On]),
            {Module, On, Msg}
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
-spec start_link(module(), handler(), term(), list()) -> {ok, pid()}.
start_link(Module, Handler, Args, Options) ->
    Pid = spawn_link(?MODULE, start, [Module, Handler, Args, Options, self()]),
    receive {started, Pid} -> {ok, Pid} end.

-spec start(module(), handler(), term(), list()) -> {ok, pid()}.
start(Module, Handler, Args, Options) ->
    Pid = spawn(?MODULE, start, [Module, Handler, Args, Options, self()]),
    receive {started, Pid} -> {ok, Pid} end.

-spec start(module(), handler(), term(), list(), pid()) -> no_return() | ok.
start(Module, DefaultHandler, Args, Options, Supervisor) ->
    %?SPAWNED(Module),
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
    WaitForInit = lists:member(wait_for_init, Options),
    _ = case WaitForInit of
            true -> ok;
            false -> Supervisor ! {started, self()}
        end,
    {NewUState, GCState} =
        try
            T1State = gc_new(Module, DefaultHandler),
            Handler = case Module:init(Args) of
                          {'$gen_component', Config, UState} ->
                              {on_handler, NewHandler} =
                                  lists:keyfind(on_handler, 1, Config),
                              NewHandler;
                          UState ->
                              DefaultHandler
                      end,
            {UState, gc_set_hand(T1State, Handler)}
        catch
            % If init throws up, send 'started' to the supervisor but exit.
            % The supervisor will try to restart the process as it is watching
            % this PID.
            Level:Reason ->
                log:log(error,"Error: exception ~p:~p in init of ~p:  ~.0p",
                        [Level, Reason, Module, erlang:get_stacktrace()]),
                erlang:Level(Reason)
        after
            case WaitForInit of
                false -> ok;
                true -> Supervisor ! {started, self()}, ok
            end
        end,
    ?INITIALIZED(Module),
    loop(NewUState, GCState).

-spec loop(user_state(), gc_state()) -> no_return() | ok.
loop(UState, GCState) ->
    ?CALLING_RECEIVE(Module),
    receive Msg ->
%%            try
                on(Msg, UState, GCState)
            %% catch Level:Reason ->
            %%         Stacktrace = erlang:get_stacktrace(),
            %%         log:log(error,
            %%                 "Error: exception ~p:~p in loop of ~.0p~n ",
            %%                 [Level, Reason, {UState, GCState}]),
            %%         on_exception(Msg, Level, Reason, Stacktrace,
            %%                      UState, GCState)
            %% end
    end.


%%%%%%%%%%%%%%%%%%%%
%% Attention!:
%%   we actually manage two queues here:
%%     user requests and breakpoint requests
%%   FIFO order has to be maintained for both (separately).
%%   This is done by a hold back queue for breakpoint messages, while
%%   not in a breakpoint. A selective receive of breakpoint messages,
%%   provides further bp instructions when needed inside a bp.
%%%%%%%%%%%%%%%%%%%%
-spec on(comm:message(), user_state(), gc_state())
        -> ok.
on(GCMsg, UState, GCState)
  when is_tuple(GCMsg)
       andalso '$gen_component' =:= element(1, GCMsg) ->
    on_gc_msg(GCMsg, UState, GCState);
on({ping, Pid}, UState, GCState) ->
    %% handle failure detector messages
    comm:send(Pid, {pong}, [{channel, prio}]),
    loop(UState, GCState);
on({send_to_group_member, Processname, Msg}, UState, GCState) ->
    %% forward a message to group member by its process name
    %% initiated via comm:send/3 with group_member
    Pid = pid_groups:get_my(Processname),
    case Pid of
        failed -> ok;
        _      -> comm:send_local(Pid, Msg)
    end,
    loop(UState, GCState);
on(Msg, UState, GCState) ->
    T1GCState = on_bp(Msg, UState, GCState),
    Module  = gc_mod(T1GCState),
    Handler = gc_hand(T1GCState),
    try Handler(Msg, UState) of
        kill ->
            log:log(info, "[ gen_component ] ~.0p killed (~.0p:~.0p/2):",
                    [self(), Module, Handler]),
            ok;
        {'$gen_component', [{post_op, Msg1}], NewUState} ->
            on_post_op(Msg1, NewUState, T1GCState);
        {'$gen_component', Commands, NewUState} ->
            %% This is not counted as a bp_step
            case lists:keyfind(on_handler, 1, Commands) of
                {on_handler, NewHandler} ->
                    loop(NewUState, gc_set_hand(T1GCState, NewHandler));
                false ->
                    case lists:keyfind(post_op, 1, Commands) of
                        {post_op, Msg1} ->
                            on_post_op(Msg1, NewUState, T1GCState);
                        false ->
                            %% let's fail since the Config list was either
                            %% empty or contained an invalid entry
                            log:log(warn, "[ gen_component ] unknown command(s): ~.0p",
                                    [Commands]),
                            erlang:throw('unknown gen_component command')
                    end
            end;
        unknown_event ->
            %% drop T2State, as it contains the error message
            on_unknown_event(Msg, UState, T1GCState),
            case gc_bpactive(T1GCState) andalso gc_bpstepped(T1GCState) of
                false -> loop(UState, T1GCState);
                true -> loop(UState, bp_step_done(Msg, T1GCState))
            end;
        NewUState ->
            case gc_bpactive(T1GCState) andalso gc_bpstepped(T1GCState) of
                false -> loop(NewUState, T1GCState);
                true -> loop(NewUState, bp_step_done(Msg, T1GCState))
            end
    catch Level:Reason ->
              Stacktrace = erlang:get_stacktrace(),
              case Stacktrace of
                  %% erlang < R15 : {Module, Handler, [Msg, State]}
                  %% erlang >= R15: {Module, Handler, [Msg, State], _}
                  [T | _] when
                    erlang:element(1, T) =:= Module andalso
                        %% erlang:element(2, T) =:= Handler andalso
                        erlang:element(3, T) =:= [Msg, UState] andalso
                        Reason =:= function_clause andalso
                        Level =:= error ->
                      on_unknown_event(Msg, UState, T1GCState),
                      loop(UState, T1GCState);
                  _ ->
                      on_exception(Msg, Level, Reason, Stacktrace,
                                   UState, T1GCState),
                      loop(UState, T1GCState)
              end
    end.

-spec on_traced_msg(comm:message(), user_state(), gc_state())
                   -> ok.
on_traced_msg(GCMsg, UState, GCState)
  when is_tuple(GCMsg)
       andalso '$gen_component' =:= element(1, GCMsg) ->
    on_gc_msg(GCMsg, UState, GCState);
on_traced_msg({ping, Pid}, UState, GCState) ->
    %% handle failure detector messages
    comm:send(Pid, {pong}, [{channel, prio}]),
    MsgTag = erlang:erase('$gen_component_trace_mpath_msg_tag'),
    trace_mpath:log_info(self(), {gc_on_done, MsgTag}),
    trace_mpath:stop(),
    loop(UState, GCState);
on_traced_msg({send_to_group_member, Processname, Msg}, UState, GCState) ->
    %% forward a message to group member by its process name
    %% initiated via comm:send/3 with group_member
    Pid = pid_groups:get_my(Processname),
    case Pid of
        failed -> ok;
        _      -> comm:send_local(Pid, Msg)
    end,
    MsgTag = erlang:erase('$gen_component_trace_mpath_msg_tag'),
    trace_mpath:log_info(self(), {gc_on_done, MsgTag}),
    trace_mpath:stop(),
    loop(UState, GCState);
on_traced_msg(Msg, UState, GCState) ->
    T1GCState = on_bp(Msg, UState, GCState),
    Module  = gc_mod(T1GCState),
    Handler = gc_hand(T1GCState),
    try Handler(Msg, UState) of
        kill ->
            log:log(info, "[ gen_component ] ~.0p killed (~.0p:~.0p/2):",
                    [self(), Module, Handler]),
            ok;
        {'$gen_component', [{post_op, Msg1}], NewUState} ->
            on_post_op(Msg1, NewUState, T1GCState);
        {'$gen_component', Commands, NewUState} ->
            %% This is not counted as a bp_step
            case lists:keyfind(on_handler, 1, Commands) of
                {on_handler, NewHandler} ->
                    MsgTag = erlang:erase('$gen_component_trace_mpath_msg_tag'),
                    trace_mpath:log_info(self(), {gc_on_done, MsgTag}),
                    trace_mpath:stop(),
                    loop(NewUState, gc_set_hand(T1GCState, NewHandler));
                false ->
                    case lists:keyfind(post_op, 1, Commands) of
                        {post_op, Msg1} ->
                            on_post_op(Msg1, NewUState, T1GCState);
                        false ->
                            %% let's fail since the Config list was either
                            %% empty or contained an invalid entry
                            log:log(warn, "[ gen_component ] unknown command(s): ~.0p",
                                    [Commands]),
                            erlang:throw('unknown gen_component command')
                    end
            end;
        unknown_event ->
            %% drop T2State, as it contains the error message
            on_unknown_event(Msg, UState, T1GCState),
            MsgTag = erlang:erase('$gen_component_trace_mpath_msg_tag'),
            trace_mpath:log_info(self(), {gc_on_done, MsgTag}),
            trace_mpath:stop(),
            case gc_bpactive(T1GCState) andalso gc_bpstepped(T1GCState) of
                false -> loop(UState, T1GCState);
                true -> loop(UState, bp_step_done(Msg, T1GCState))
            end;
        NewUState ->
            MsgTag = erlang:erase('$gen_component_trace_mpath_msg_tag'),
            trace_mpath:log_info(self(), {gc_on_done, MsgTag}),
            trace_mpath:stop(),
            case gc_bpactive(T1GCState) andalso gc_bpstepped(T1GCState) of
                false -> loop(NewUState, T1GCState);
                true -> loop(NewUState, bp_step_done(Msg, T1GCState))
            end
    catch Level:Reason ->
              Stacktrace = erlang:get_stacktrace(),
              case Stacktrace of
                  %% erlang < R15 : {Module, Handler, [Msg, State]}
                  %% erlang >= R15: {Module, Handler, [Msg, State], _}
                  [T | _] when
                    erlang:element(1, T) =:= Module andalso
                        %% erlang:element(2, T) =:= Handler andalso
                        erlang:element(3, T) =:= [Msg, UState] andalso
                        Reason =:= function_clause andalso
                        Level =:= error ->
                      on_unknown_event(Msg, UState, T1GCState),
                      MsgTag = erlang:erase('$gen_component_trace_mpath_msg_tag'),
                      trace_mpath:log_info(self(), {gc_on_done, MsgTag}),
                      trace_mpath:stop(),
                      loop(UState, T1GCState);
                  _ ->
                      on_exception(Msg, Level, Reason, Stacktrace,
                                   UState, T1GCState),
                      MsgTag = erlang:erase('$gen_component_trace_mpath_msg_tag'),
                      trace_mpath:log_info(self(), {gc_on_done, MsgTag}),
                      trace_mpath:stop(),
                      loop(UState, T1GCState)
              end
    end.

-spec on_unknown_event(comm:message(), user_state(), gc_state())
                      -> ok.
on_unknown_event({web_debug_info, Requestor}, UState, GCState) ->
    comm:send_local(Requestor, {web_debug_info_reply,
                                [{"generic info from gen_component:", ""},
                                 {"module", webhelpers:safe_html_string("~.0p", [gc_mod(GCState)])},
                                 {"handler", webhelpers:safe_html_string("~.0p", [gc_hand(GCState)])},
                                 {"state", webhelpers:safe_html_string("~.0p", [UState])}]});
on_unknown_event(UnknownMessage, UState, GCState) ->
    log:log(error,
            "~n** Unknown message:~n ~.0p~n"
            "** Module:~n ~.0p~n"
            "** Handler:~n ~.0p~n"
            "** Pid:~n ~p ~.0p~n"
            "** State:~n ~.0p~n",
            [UnknownMessage,
             gc_mod(GCState),
             gc_hand(GCState),
             self(), catch pid_groups:group_and_name_of(self()),
             {UState, GCState}]),
    ok.

on_exception(Msg, Level, Reason, Stacktrace, UState, GCState) ->
    log:log(error,
            "~n** Exception:~n ~.0p:~.0p~n"
            "** Current message:~n ~.0p~n"
            "** Module:~n ~.0p~n"
            "** Handler:~n ~.0p~n"
            "** Pid:~n ~p ~.0p~n"
            "** Source linetrace (enable in scalaris.hrl):~n ~.0p~n"
            "** State:~n ~.0p~n"
            "** Stacktrace:~n ~.0p~n",
            [Level, Reason,
             Msg,
             gc_mod(GCState),
             gc_hand(GCState),
             self(), catch pid_groups:group_and_name_of(self()),
             erlang:get(test_server_loc),
             {UState, GCState},
             Stacktrace]),
    ok.

-spec on_post_op(comm:message(), user_state(), gc_state())
                -> ok.
on_post_op(Msg, UState, GCState) ->
    case gc_bpactive(GCState) of
        true ->
            ?TRACE_BP_STEPS("~n"
                            "*** Trigger post-op...~n"
                            "    Process: ~p (~p)~n"
                            "    Handler: ~p:~p/2~n"
                            "    Message: ~.0p~n",
                            [self(), catch pid_groups:group_and_name_of(self()),
                             gc_mod(GCState), gc_hand(GCState), Msg]),
            self() ! {'$gen_component', bp, breakpoint, step,
                      gc_bpstepper(GCState)},
            ok;
        false -> ok
    end,
    case erlang:get(trace_mpath) of
        undefined -> on(Msg, UState, GCState);
        Logger    -> trace_mpath:log_info(Logger, self(), Msg),
                     on_traced_msg(Msg, UState, GCState)
    end.

-spec on_gc_msg(gc_msg(), user_state(), gc_state()) -> ok.
on_gc_msg({'$gen_component', trace_mpath, PState, From, To, Msg}, UState, GCState) ->
    trace_mpath:log_recv(PState, From, To, Msg),
    erlang:put('$gen_component_trace_mpath_msg_tag', element(1, Msg)),
    trace_mpath:start(PState),
    on_traced_msg(Msg, UState, GCState);
on_gc_msg({'$gen_component', kill}, _UState, GCState) ->
    log:log(info, "[ gen_component ] ~.0p killed (~.0p:~.0p/2):",
            [self(), gc_mod(GCState), gc_hand(GCState)]),
    ok;
on_gc_msg({'$gen_component', bp, msg_in_bp_waiting, Pid}, UState, GCState) ->
    Pid ! {'$gen_component', bp, msg_in_bp_waiting_response, false},
    loop(UState, GCState);
on_gc_msg({'$gen_component', sleep, Time}, UState, GCState) ->
    timer:sleep(Time),
    loop(UState, GCState);
on_gc_msg({'$gen_component', get_state, Pid}, UState, GCState) ->
    comm:send_local(
      Pid, {'$gen_component', get_state_response, UState}),
    loop(UState, GCState);
on_gc_msg({'$gen_component', get_component_state, Pid}, UState, GCState) ->
    comm:send_local(
      Pid, {'$gen_component', get_component_state_response,
            {gc_mod(GCState), gc_hand(GCState), GCState}}),
    loop(UState, GCState);
on_gc_msg({'$gen_component', bp, bp_set_cond, Cond, BPName} = Msg, UState, GCState) ->
    NewGCState =
        case gc_bpqueue(GCState) of
            [] -> gc_bp_set_cond(GCState, Cond, BPName);
            _ -> gc_bp_hold_back(GCState, Msg)
        end,
    loop(UState, NewGCState);
on_gc_msg({'$gen_component', bp, bp_set, MsgTag, BPName} = Msg, UState, GCState) ->
    NewGCState =
        case gc_bpqueue(GCState) of
            [] -> gc_bp_set(GCState, MsgTag, BPName);
            _ -> gc_bp_hold_back(GCState, Msg)
        end,
    loop(UState, NewGCState);
on_gc_msg({'$gen_component', bp, bp_del, BPName} = Msg, UState, GCState) ->
    NewGCState =
        case gc_bpqueue(GCState) of
            [] -> gc_bp_del(GCState, BPName);
            _ -> gc_bp_hold_back(GCState, Msg)
        end,
    loop(UState, NewGCState);
on_gc_msg({'$gen_component', bp, barrier} = Msg, UState, GCState) ->
    NewGCState = gc_bp_hold_back(GCState, Msg),
    loop(UState, NewGCState);
on_gc_msg({'$gen_component', bp, breakpoint, step, _Stepper} = Msg,
          UState, GCState) ->
    NewGCState = gc_bp_hold_back(GCState, Msg),
    loop(UState, NewGCState);
on_gc_msg({'$gen_component', bp, breakpoint, cont} = Msg,
          UState, GCState) ->
    NewGCState = gc_bp_hold_back(GCState, Msg),
    loop(UState, NewGCState).


-spec on_bp(comm:message(), user_state(), gc_state()) -> gc_state().
on_bp(_Msg, _UState, GCState)
  when (false =:= element(?BP_ACTIVE, GCState))
       andalso ([] =:= element(?BPS, GCState)) ->
   GCState;
on_bp(Msg, UState, GCState) ->
    BPActive = bp_active(Msg, UState, GCState),
    wait_for_bp_leave(Msg, GCState, BPActive).

-spec bp_active(comm:message(), user_state(), gc_state()) -> boolean().
bp_active(_Msg, _UState, GCState)
  when (false =:= element(?BP_ACTIVE, GCState))
       andalso ([] =:= element(?BPS, GCState))->
    false;
bp_active(Msg, UState, GCState) ->
    gc_bpactive(GCState)
        orelse
        begin
            [ ThisBP | RemainingBPs ] = gc_bps(GCState),
            Decision = case ThisBP of
                           {bp, ThisTag, _BPName} ->
                               ThisTag =:= comm:get_msg_tag(Msg);
                           {bp_cond, Cond, _BPName} when is_function(Cond) ->
                               Cond(Msg, UState);
                           {bp_cond, {Module, Fun, Params}, _BPName} ->
                               apply(Module, Fun, [Msg, UState, Params])
                       end,
            case Decision of
                true ->
                    ?TRACE_BP("~p Now in BP ~p via: ~p~n", [self(), ThisBP, Msg]),
                    Decision;
                false -> Decision
            end
        end
        orelse
        bp_active(Msg, UState, gc_set_bps(GCState, RemainingBPs)).

-spec wait_for_bp_leave(comm:message(), gc_state(), boolean()) -> gc_state().
wait_for_bp_leave(_Msg, State, _BP_Active = false) -> State;
wait_for_bp_leave(Msg, State, _BP_Active = true) ->
    ?TRACE_BP("~p In wait for bp leave~n", [self()]),
    {Queue, IsFromQueue} =
        case gc_bpqueue(State) of
            [] ->
                %% trigger a selective receive
                ?TRACE_BP("~p wait for bp op by receive...~n", [self()]),
                receive
                    BPMsg when
                          is_tuple(BPMsg),
                          '$gen_component' =:= element(1, BPMsg),
                          bp =:= element(2, BPMsg) ->
                        ?TRACE_BP("~p got bp op by receive ~p.~n", [self(), BPMsg]),
                        {[BPMsg], false};
                    GetCompStateMsg when
                          is_tuple(GetCompStateMsg),
                          '$gen_component' =:= element(1, GetCompStateMsg),
                          get_component_state =:= element(2, GetCompStateMsg) ->
                        {[GetCompStateMsg], false}
                end;
            _ ->
                ?TRACE_BP("~p process queued bp op ~p.~n",
                          [self(), hd(gc_bpqueue(State))]),
                {gc_bpqueue(State), true}
        end,
    T1State = gc_set_bpqueue(State, tl(Queue)),
    T2State = gc_set_bpactive(T1State, true),
    ?TRACE_BP("~p Handle bp request in bp ~p ~n", [self(), hd(Queue)]),
    on_bp_req_in_bp(Msg, T2State, hd(Queue), IsFromQueue).

-spec on_bp_req_in_bp(comm:message(), gc_state(), bp_msg(), boolean())
                     -> gc_state().
on_bp_req_in_bp(_Msg, State,
                {'$gen_component', bp, breakpoint, step, StepperPid},
                _IsFromQueue) ->
    ?TRACE_BP_STEPS("rec step req in bp ~p~n", [self()]),
    T1State = gc_set_bpstepped(State, true),
    T2State = gc_set_bpstepper(T1State, StepperPid),
    ?TRACE_BP_STEPS("~n"
                    "*** Start handling message...~n"
                    "    Process: ~p (~p)~n"
                    "    Message: ~.0p~n",
                    [self(), catch pid_groups:group_and_name_of(self()), _Msg]),
    T2State;
on_bp_req_in_bp(_Msg, State,
                {'$gen_component', bp, breakpoint, cont},
                _IsFromQueue) ->
    T1State = gc_set_bpstepper(State, unknown),
    T2State = gc_set_bpstepped(T1State, false),
    gc_set_bpactive(T2State, false);
on_bp_req_in_bp(Msg, State,
                {'$gen_component', bp, msg_in_bp_waiting, Pid},
                _IsFromQueue) ->
    Pid ! {'$gen_component', bp, msg_in_bp_waiting_response, true},
    wait_for_bp_leave(Msg, State, true);
on_bp_req_in_bp(Msg, State,
                {'$gen_component', bp, barrier}, _IsFromQueue) ->
    %% we are in breakpoint. Consume this bp message
    wait_for_bp_leave(Msg, State, true);
on_bp_req_in_bp(Msg, State,
                {'$gen_component', bp, bp_set_cond, Cond, BPName} = BPMsg,
                IsFromQueue) ->
    NextState =
        case gc_bpqueue(State) of
            [_H|_T] when not IsFromQueue -> gc_bp_hold_back(State, BPMsg);
            _ -> gc_bp_set_cond(State, Cond, BPName)
        end,
    wait_for_bp_leave(Msg, NextState, true);
on_bp_req_in_bp(Msg, State,
                {'$gen_component', bp, bp_set, MsgTag, BPName} = BPMsg,
                IsFromQueue) ->
    NextState =
        case gc_bpqueue(State) of
            [_H|_T] when not IsFromQueue -> gc_bp_hold_back(State, BPMsg);
            _ -> gc_bp_set(State, MsgTag, BPName)
        end,
    wait_for_bp_leave(Msg, NextState, true);
on_bp_req_in_bp(Msg, State,
                {'$gen_component', bp, bp_del, BPName}= BPMsg,
                IsFromQueue)->
    NextState =
        case gc_bpqueue(State) of
            [_H|_T] when not IsFromQueue -> gc_bp_hold_back(State, BPMsg);
            _ -> gc_bp_del(State, BPName)
        end,
    wait_for_bp_leave(Msg, NextState, true);
on_bp_req_in_bp(Msg, State,
                {'$gen_component', get_component_state, Pid},
                _IsFromQueue) ->
    comm:send_local(
      Pid, {'$gen_component', get_component_state_response, State}),
    wait_for_bp_leave(Msg, State, true).


%% @doc release the bp_step function when we executed a bp_step
-spec bp_step_done(comm:message(), gc_state()) -> gc_state().
bp_step_done(_, State)
  when not element(?BP_ACTIVE, State)
       orelse not element(?BP_STEPPED, State) ->
    State;
bp_step_done(Msg, State) ->
    ?TRACE_BP_STEPS("step done ~.0p~n", [Msg]),
    comm:send_local(gc_bpstepper(State),
                    {'$gen_component', bp, breakpoint, step_done,
                     self(), gc_mod(State), gc_hand(State), Msg}),
    T1State = gc_set_bpstepper(State, unknown),
    gc_set_bpstepped(T1State, false).


-spec gc_new(module(), handler()) -> gc_state().
gc_new(Module, Handler) ->
    {Module, Handler,
%     Options, _Slowest = 0.0,
     _BPs = [], _BPActive = false,
     _BPHoldbackQueue = [], _BPStepped = false,
     _BPStepperPid = unknown
    }.
gc_mod(State) ->                element(?MOD, State).
gc_hand(State) ->               element(?HAND, State).
gc_set_hand(State, Handler) ->  setelement(?HAND, State, Handler).
%% opts
%% slowest
-spec gc_bps(gc_state()) -> [bp()].
gc_bps(State) ->                element(?BPS, State).
-spec gc_set_bps(gc_state(), [bp()]) -> gc_state().
gc_set_bps(State, Val) ->       setelement(?BPS, State, Val).
-spec gc_bpactive(gc_state()) -> boolean().
gc_bpactive(State) ->           element(?BP_ACTIVE, State).
-spec gc_set_bpactive(gc_state(), boolean()) -> gc_state().
gc_set_bpactive(State, Val) ->  setelement(?BP_ACTIVE, State, Val).
-spec gc_bpqueue(gc_state()) -> [bp_msg()].
gc_bpqueue(State) ->            element(?BP_QUEUE, State).
-spec gc_set_bpqueue(gc_state(), [bp_msg()]) -> gc_state().
gc_set_bpqueue(State, Val) ->   setelement(?BP_QUEUE, State, Val).
-spec gc_bpstepped(gc_state()) -> boolean().
gc_bpstepped(State) -> element(?BP_STEPPED, State).
-spec gc_set_bpstepped(gc_state(), boolean()) -> gc_state().
gc_set_bpstepped(State, Val) -> setelement(?BP_STEPPED, State, Val).
-spec gc_bpstepper(gc_state()) -> pid() | 'unknown'.
gc_bpstepper(State) ->          element(?BP_STEPPER, State).
-spec gc_set_bpstepper(gc_state(), pid() | 'unknown') -> gc_state().
gc_set_bpstepper(State, Val) -> setelement(?BP_STEPPER, State, Val).

-spec gc_bp_hold_back(gc_state(), bp_msg()) -> gc_state().
gc_bp_hold_back(State, Msg) ->
    ?TRACE_BP("Enqueuing bp op ~p -> ~p~n", [Msg, State]),
    NewQueue = gc_bpqueue(State) ++ [Msg],
    gc_set_bpqueue(State, NewQueue).

-spec gc_bp_set_cond(gc_state(), fun(), bp_name()) -> gc_state().
gc_bp_set_cond(State, Cond, BPName) ->
    ?TRACE_BP("Set bp ~p with name ~p~n", [Cond, BPName]),
    NewBPs = [ {bp_cond, Cond, BPName} | gc_bps(State)],
    gc_set_bps(State, NewBPs).

-spec gc_bp_set(gc_state(),comm:msg_tag(),atom()) -> gc_state().
gc_bp_set(State, MsgTag, BPName) ->
    ?TRACE_BP("Set bp ~p with name ~p~n", [MsgTag, BPName]),
    NewBPs = [ {bp, MsgTag, BPName} | gc_bps(State)],
    gc_set_bps(State, NewBPs).

-spec gc_bp_del(gc_state(), atom()) -> gc_state().
gc_bp_del(State, BPName) ->
    ?TRACE_BP("~p Del bp ~p~n", [self(), BPName]),
    NewBPs = lists:keydelete(BPName, 3, gc_bps(State)),
    gc_set_bps(State, NewBPs).
