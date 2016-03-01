%% @copyright 2007-2016 Zuse Institute Berlin

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
%%      [http://scalaris.zib.de].
%% @end
%% @version $Id$
-module(gen_component).
-vsn('$Id$').

-compile({inline, [gc_mod/1, gc_hand/1, gc_set_hand/2,
                   gc_bps/1, gc_set_bps/2,
                   gc_bpactive/1, gc_set_bpactive/2,
                   gc_bpqueue/1, gc_set_bpqueue/2,
                   gc_bpstepped/1, gc_set_bpstepped/2,
                   gc_bpstepper/1, gc_set_bpstepper/2,
                   handle_message/3
                  ]}).

-include("scalaris.hrl").

%% breakpoint tracing
%-define(TRACE_BP(X,Y), log:pal(X,Y)).
-define(TRACE_BP(X,Y), ok).
%% userdevguide-begin gen_component:trace_bp_steps
%-define(TRACE_BP_STEPS(X,Y), io:format(X,Y)).     %% output on console
%-define(TRACE_BP_STEPS(X,Y), log:pal(X,Y)).        %% output even if called by unittest
%-define(TRACE_BP_STEPS(X,Y), io:format(user,X,Y)). %% clean output even if called by unittest
-define(TRACE_BP_STEPS(X,Y), ok).
%% userdevguide-end gen_component:trace_bp_steps

-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.
-export([start_link/4, start/4, start/5]).
-export([kill/1, sleep/2, is_gen_component/1, runnable/1,
         get_state/1, get_state/2,
         get_component_state/1, get_component_state/2,
         change_handler/2, post_op/2]).
-export([bp_set/3, bp_set_cond/3, bp_set_cond_async/3,
         bp_del/2, bp_del_async/2]).
-export([bp_step/1, bp_cont/1, bp_barrier/1]).
-export([bp_about_to_kill/1,
         monitor/1, demonitor/1, demonitor/2]).

-export_type([handler/0, option/0]).
-export_type([bp_name/0]). %% for unittests

-type bp_name() :: atom().

-type bp_msg() ::
          {'$gen_component', bp, breakpoint, step, pid()}
        | {'$gen_component', bp, breakpoint, cont}
        | {'$gen_component', bp, msg_in_bp_waiting, pid()}
        | {'$gen_component', bp, barrier}
        | {'$gen_component', bp, bp_set_cond, fun(), bp_name(), pid() | none}
        | {'$gen_component', bp, bp_set, comm:msg_tag(), bp_name(), pid() | none}
        | {'$gen_component', bp, bp_del, bp_name(), pid() | none}.

-type gc_msg() ::
          bp_msg()
        | {'$gen_component', sleep, pos_integer()}
        | {'$gen_component', about_to_kill, pos_integer(), pid()}
        | {'$gen_component', get_state, pid(), Cookie::pos_integer()}
        | {'$gen_component', get_component_state, pid(), Cookie::pos_integer()}
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
          pid() | unknown,    %% bp stepper
          [comm:mypid()]      %% exception subscribers
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
-define(EXC_SUBS,    8).

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
    case erlang:process_info(Pid, initial_call) of
        undefined -> false; % process not alive
        {initial_call, {Module, Function, _Arity}} ->
                gen_component =:= Module orelse
                    start_gen_component =:= Function
    end.

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

-spec receive_state_if_alive(
        Pid::pid(), MsgTag::get_state_response | get_component_state_response,
        Cookie::pos_integer()) -> term() | failed.
receive_state_if_alive(Pid, MsgTag, Cookie) ->
    case erlang:is_process_alive(Pid) of
        true ->
            receive
                {'$gen_component', MsgTag, Cookie, State} ->
                    State;
                {'$gen_component', MsgTag, _OldCookie, _State} ->
                    % remove unrelated/old incarnations of this message
                    receive_state_if_alive(Pid, MsgTag, Cookie)
            after 100 ->
                receive_state_if_alive(Pid, MsgTag, Cookie)
            end;
        _ ->
            receive
                {'$gen_component', MsgTag, State} -> State
            after 0 ->
                failed
            end
    end.

-spec receive_state_if_alive(
        Pid::pid(), MsgTag::get_state_response | get_component_state_response,
        Cookie::pos_integer(), Timeout::non_neg_integer()) -> term() | failed.
receive_state_if_alive(Pid, MsgTag, Cookie, Timeout) when Timeout >= 0->
    case erlang:is_process_alive(Pid) of
        true ->
            receive
                {'$gen_component', MsgTag, Cookie, State} ->
                    State;
                {'$gen_component', MsgTag, _OldCookie, _State} ->
                    % remove unrelated/old incarnations of this message
                    receive_state_if_alive(Pid, MsgTag, Cookie, Timeout)
            after 100 ->
                receive_state_if_alive(Pid, MsgTag, Cookie, Timeout - 100)
            end;
        _ ->
            receive
                {'$gen_component', MsgTag, Cookie, State} -> State
            after 0 ->
                failed
            end
    end;
receive_state_if_alive(_Pid, MsgTag, Cookie, _Timeout) ->
    % one last try looking into the message box only
    receive
        {'$gen_component', MsgTag, Cookie, State} ->
            State;
        {'$gen_component', MsgTag, _OldCookie, _State} ->
            % remove unrelated/old incarnations of this message
            failed
    after 0 ->
        failed
    end.

-spec get_state(Pid::pid()) -> user_state() | failed.
get_state(Pid) ->
    Cookie = randoms:getRandomInt(),
    Pid ! {'$gen_component', get_state, self(), Cookie},
    receive_state_if_alive(Pid, get_state_response, Cookie).

-spec get_state(Pid::pid(), Timeout::non_neg_integer()) -> user_state() | failed.
get_state(Pid, Timeout) ->
    Cookie = randoms:getRandomInt(),
    Pid ! {'$gen_component', get_state, self(), Cookie},
    receive_state_if_alive(Pid, get_state_response, Cookie, Timeout).

-spec get_component_state(Pid::pid()) -> gc_state().
get_component_state(Pid) ->
    Cookie = randoms:getRandomInt(),
    Pid ! {'$gen_component', get_component_state, self(), Cookie},
    receive_state_if_alive(Pid, get_component_state_response, Cookie).

-spec get_component_state(pid(), Timeout::non_neg_integer()) -> gc_state() | failed.
get_component_state(Pid, Timeout) ->
    Cookie = randoms:getRandomInt(),
    Pid ! {'$gen_component', get_component_state, self(), Cookie},
    receive_state_if_alive(Pid, get_component_state_response, Cookie, Timeout).

%% @doc change the handler for handling messages
-spec change_handler(user_state(), Handler::handler())
        -> {'$gen_component', [{on_handler, Handler::handler()},...], user_state()}.
change_handler(UState, Handler) when is_function(Handler, 2) ->
    {'$gen_component', [{on_handler, Handler}], UState}.

%% @doc perform a post op, i.e. handle a message directly after another
-spec post_op(comm:message(), user_state())
        -> {'$gen_component', [{post_op, comm:message()},...], user_state()}.
post_op(Msg, UState) ->
    {'$gen_component', [{post_op, Msg}], UState}.

%% requests regarding breakpoint processing
-spec bp_set(pid(), comm:msg_tag(), bp_name()) -> ok.
bp_set(Pid, MsgTag, BPName) ->
    Pid ! Msg = {'$gen_component', bp, bp_set, MsgTag, BPName, self()},
    receive Msg -> ok end,
    ok.

%% @doc Module:Function(Message, State, Params) will be evaluated to decide
%% whether a BP is reached. Params can be used as a payload.
-spec bp_set_cond(pid(),
                  Cond::{module(), atom(), 2}
                      | fun((comm:message(), State::any()) -> boolean()),
                  bp_name()) -> ok.
bp_set_cond(Pid, {_Module, _Function, _Params = 2} = Cond, BPName) ->
    Pid ! Msg = {'$gen_component', bp, bp_set_cond, Cond, BPName, self()},
    receive Msg -> ok end,
    ok;
bp_set_cond(Pid, Cond, BPName) when is_function(Cond, 2) ->
    Pid ! Msg = {'$gen_component', bp, bp_set_cond, Cond, BPName, self()},
    receive Msg -> ok end,
    ok.

-spec bp_set_cond_async(pid(),
                        Cond::{module(), atom(), 2}
                            | fun((comm:message(), State::any()) -> boolean()),
                        bp_name()) -> ok.
bp_set_cond_async(Pid, {_Module, _Function, _Params = 2} = Cond, BPName) ->
    Pid ! {'$gen_component', bp, bp_set_cond, Cond, BPName, none},
    ok;
bp_set_cond_async(Pid, Cond, BPName) when is_function(Cond, 2) ->
    Pid ! {'$gen_component', bp, bp_set_cond, Cond, BPName, none},
    ok.

-spec bp_del(pid(), bp_name()) -> ok.
bp_del(Pid, BPName) ->
    Pid ! Msg = {'$gen_component', bp, bp_del, BPName, self()},
    receive Msg -> ok end,
    ok.

-spec bp_del_async(pid(), bp_name()) -> ok.
bp_del_async(Pid, BPName) ->
    Pid ! {'$gen_component', bp, bp_del, BPName, none},
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

%% @doc Brings the given gen_component into a state that is paused in
%%      preparation of a graceful shutdown of all children of a supervisor.
%%      Note: A monitor is used to safe-guard the (synchronous) creation of the
%%      breakpoint in cases of another interfering shutdown process.
%% @see sup:sup_terminate_childs/1
-spec bp_about_to_kill(pid()) -> ok.
bp_about_to_kill(Pid) ->
    Self = self(),
    ?DBG_ASSERT(Pid =/= Self),
    % if infected, remove all monitors, send infected DOWN messages ourself
    % note: need to get the monitors before we add ours below
    MonitoredBy =
        case trace_mpath:infected() of
            true ->
                {monitored_by, X} = erlang:process_info(Pid, monitored_by),
                X;
            _ ->
                []
        end,

    %% about_to_kill handling
    MonitorRef = erlang:monitor(process, Pid),
    % suspend gen_component independently from break points being set/active
    % (60s should be enough for the sub-sequent kill)
    % see sup:sup_terminate_childs/1
    Pid ! Msg = {'$gen_component', about_to_kill, 60000, self()},
    receive
        Msg -> erlang:demonitor(MonitorRef, [flush]), ok;
        {'DOWN', MonitorRef, process, Pid, _Info1} -> ok
    after 2000 ->
            erlang:demonitor(MonitorRef, [flush]),
            log:pal("Failed to pause ~p (~p) within 2s~n~.2p",
                    [Pid, pid_groups:group_and_name_of(Pid),
                     erlang:process_info(Pid, [registered_name, initial_call, current_function])])
    end,

    %% continued infected 'DOWN' message handling (after pausing the processes)
    % TODO: add infected 'EXIT' messages from links?
    % note: the proto_sched process also sets a monitor but this should not be
    %       the process to kill since processes do not kill themselves!
    _ = [begin
             ?DBG_ASSERT(PidX =/= Self),
             % we can only handly gen_components here, other monitors cause uncontrolled side effects
             ?DBG_ASSERT(is_gen_component(PidX)),
             MonitorX = erlang:monitor(process, PidX),
             PidX ! {'$gen_component', remove_monitor, Pid, Self},
             receive
                 {'$gen_component', monitor, MonRef} when is_reference(MonRef) ->
                     erlang:demonitor(MonitorX, [flush]),
                     %log:pal("about_to_kill: ~p monitors ~p with ref ~p", [PidX, Pid, MonRef]),
                     % send infected DOWN message:
                     comm:send_local(PidX, {'DOWN', MonRef, process, Pid, about_to_kill}),
                     ok;
                 {'$gen_component', monitor, undefined} ->
                     % not monitoring any more
                     erlang:demonitor(MonitorX, [flush]),
                     %log:pal("about_to_kill: ~p monitors ~p with ref ~p", [PidX, Pid, no_ref]),
                     ok;
                 {'DOWN', MonitorX, process, PidX, _Info2} ->
                     % monitoring PID is unavailable
                     ok
             after 2000 ->
                 erlang:demonitor(MonitorX, [flush]),
                 log:pal("Failed to remove uninfected monitor at ~p (~p) for ~p within 2s~n~.2p",
                         [PidX, pid_groups:group_and_name_of(PidX), Pid,
                          erlang:process_info(PidX, [registered_name, initial_call, current_function])])
             end
         end || PidX <- MonitoredBy,
                % exclude already paused pids:
                erlang:process_info(PidX, priority) =/= {priority, low}],
    
    ok.

%% @doc Sets an erlang monitor using erlang:monitor/2 in a gen_component
%%      process and stores info to support killing gen_components with
%%      trace_mpath/proto_sched.
%% @see bp_about_to_kill/1
-spec monitor(comm:erl_local_pid_plain()) -> reference().
monitor(Pid) ->
    ?DBG_ASSERT(is_gen_component(self())),
    MonRef = erlang:monitor(process, Pid),
    %log:pal("monitor: ~p monitors ~p with ref ~p", [self(), Pid, MonRef]),
    % we need the real pid in the process dictionary for bp_about_to_kill/1 to work
    RealPid = if is_atom(Pid) ->
                     case whereis(Pid) of
                         undefined ->
                             % this process is not alive anymore -> store the
                             % registered name instead so the check in
                             % demonitor/2 does not hit
                             Pid;
                         X when is_pid(X) ->
                             X
                     end;
                 is_pid(Pid) ->
                     Pid
              end,
    % only a single monitor allowed:,
    undefined = erlang:put({'$gen_component_monitor_ref', RealPid}, MonRef),
    undefined = erlang:put({'$gen_component_monitor_pid', MonRef}, RealPid),

    % if the process did not exist yet, convert the DOWN message to an infected message:
    case trace_mpath:infected() of
        true ->
            receive
                {'DOWN', MonRef, process, _Pid, noproc = _Reason} = Msg ->
                    comm:send_local(self(), Msg)
            after 0 -> ok
            end;
        _ ->
            ok
    end,
    MonRef.

-spec demonitor(MonitorRef::reference()) -> true.
demonitor(MonitorRef) ->
    ?MODULE:demonitor(MonitorRef, []).

-spec demonitor(MonitorRef::reference(), OptionList::[flush | info]) -> boolean().
demonitor(MonitorRef, OptionList) ->
    ?DBG_ASSERT(is_gen_component(self())),
    %log:pal("demonitor: ~p demonitors ref ~p", [self(), MonitorRef]),
    Res = erlang:demonitor(MonitorRef, OptionList),
    % previous monitor must exist:
    Pid = erlang:erase({'$gen_component_monitor_pid', MonitorRef}),
    ?ASSERT2(Pid =/= undefined, Pid),
    MonitorRef = erlang:erase({'$gen_component_monitor_ref', Pid}),
    Res.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% generic framework
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% profile

-type spawn_option() ::
            link | monitor
          | {priority, Level::normal | high | max}
          | {fullsweep_after, Number::non_neg_integer()}
          | {min_heap_size, Size::non_neg_integer()}
          | {min_bin_vheap_size, VSize::non_neg_integer()}.
-type option() ::
            {pid_groups_join_as, pid_groups:groupname(), pid_groups:pidname()}
          | {pid_groups_join_as, pid_groups:groupname(), {short_lived, pid_groups:pidname()}}
          | {erlang_register, Name::atom()}
          | {spawn_opts, [spawn_option()]}
          | {wait_for_init}.

-spec start_link(module(), handler(), term(), [option()]) -> {ok, pid()}.
start_link(Module, Handler, Args, Options) ->
    case lists:keytake(spawn_opts, 1, Options) of
        {value, {spawn_opts, SpawnOpt}, Options1} -> ok;
        false -> Options1 = Options,
                 SpawnOpt = [],
                 ok
    end,
    Pid = case erlang:function_exported(Module, start_gen_component, 5) of
              true ->
                  spawn_opt(Module, start_gen_component,
                                  [Module, Handler, Args, Options1, self()],
                                  [link | SpawnOpt]);
              false ->
                  log:log("[ gen_component ] the module ~p does not provide its own start/5 function (please include gen_component.hrl)",
                          [Module]),
                  spawn_opt(?MODULE, start,
                                  [Module, Handler, Args, Options1, self()],
                                  [link | SpawnOpt])
    end,
    receive {started, Pid} -> {ok, Pid} end.

-spec start(module(), handler(), term(), [option()]) -> {ok, pid()}.
start(Module, Handler, Args, Options) ->
    Pid = spawn(?MODULE, start, [Module, Handler, Args, Options, self()]),
    receive {started, Pid} -> {ok, Pid} end.

-spec start(module(), handler(), term(), [option()], pid()) -> no_return() | ok.
start(Module, DefaultHandler, Args, Options, Supervisor) ->
    % note: register globally first (disables the registered name from a
    %       potential DEBUG_REGISTER below)
    Registered =
        case lists:keyfind(erlang_register, 1, Options) of
            {erlang_register, Name} ->
                _ = case whereis(Name) of
                        undefined -> ok;
                        _ -> catch(erlang:unregister(Name)) %% unittests may leave garbage
                    end,
                erlang:register(Name, self()),
                true;
            false ->
                false
        end,
    case lists:keyfind(pid_groups_join_as, 1, Options) of
        {pid_groups_join_as, GroupId, {short_lived, PidName}} ->
            % if short-lived processes are spawned multiple times, we cannot
            % create a new atom (for process registration) for each spawn or
            % we will run out of atoms!
            % -> no DEBUG_REGISTER!
            pid_groups:join_as(GroupId, PidName),
            log:log(info, "[ gen_component ] ~p started ~p:~p as ~s:~p",
                    [Supervisor, self(), Module, GroupId, PidName]),
            ok;
        {pid_groups_join_as, GroupId, PidName} ->
            pid_groups:join_as(GroupId, PidName),
            log:log(info, "[ gen_component ] ~p started ~p:~p as ~s:~p",
                    [Supervisor, self(), Module, GroupId, PidName]),
            if Registered -> ok;
               is_atom(PidName) ->
                   %% we can give this process a better name for
                   %% debugging, for example for etop.
                   ?DEBUG_REGISTER(list_to_atom(pid_groups:group_to_string(GroupId)
                                               ++ "-" ++ atom_to_list(PidName)),
                                   self()),
                   ok;
               true ->
                   ?DEBUG_REGISTER(list_to_atom(pid_groups:group_to_string(GroupId)
                                               ++ "-" ++ PidName),
                                   self()),
                   ok
            end;
        false when Registered ->
            log:log(info, "[ gen_component ] ~p started ~p:~p", [Supervisor, self(), Module]);
        false ->
            ?DEBUG_REGISTER(list_to_atom(atom_to_list(Module) ++ "_" ++ randoms:getRandomString()),
                            self()),
            log:log(info, "[ gen_component ] ~p started ~p:~p", [Supervisor, self(), Module])
    end,
    WaitForInit = lists:member({wait_for_init}, Options),
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
            T2State = case lists:keyfind(exception_subscription, 1, Options) of
                          {exception_subscription, Subs} ->
                              gc_set_exc_subs(T1State, Subs);
                          _ ->
                              T1State
                      end,
            {UState, gc_set_hand(T2State, Handler)}
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
    loop(NewUState, GCState).

-spec loop(user_state(), gc_state()) -> no_return() | ok.
loop(UState, GCState) ->
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_in_loop, self()}),
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

%% @doc Helper for on/3 and on_traced_msg/3 handling gen_component-provided
%%      default messages as well as user-messages.
-spec handle_message(comm:message() | comm:group_message(), user_state(), handler())
        -> user_state() | kill | unknown_event |
           {'$gen_component', Commands::[{on_handler, Handler::handler()} |
                                         {post_op, comm:message()}], user_state()}.
handle_message({ping, Pid}, UState, _Handler) ->
    %% generic ping message
    comm:send(Pid, {pong, pid_groups:my_pidname()}, [{channel, prio}]),
    UState;
handle_message({?send_to_group_member, Processname, Msg}, UState, _Handler) ->
    %% forward a message to group member by its process name
    %% initiated via comm:send/3 with group_member
    comm:forward_to_group_member(Processname, Msg),
    UState;
handle_message({?send_to_registered_proc, Processname, Msg}, UState, _Handler) ->
    %% forward a message to a registered process
    %% initiated via comm:send/3 with registered_proc
    comm:forward_to_registered_proc(Processname, Msg),
    UState;
handle_message(Msg, UState, Handler) ->
    Handler(Msg, UState).

%%%%%%%%%%%%%%%%%%%%
%% Attention!:
%%   we actually manage two queues here:
%%     user requests and breakpoint requests
%%   FIFO order has to be maintained for both (separately).
%%   This is done by a hold back queue for breakpoint messages, while
%%   not in a breakpoint. A selective receive of breakpoint messages,
%%   provides further bp instructions when needed inside a bp.
%%%%%%%%%%%%%%%%%%%%
-spec on(comm:message() | comm:group_message(), user_state(), gc_state())
        -> ok.
on(GCMsg, UState, GCState)
  when is_tuple(GCMsg) andalso '$gen_component' =:= element(1, GCMsg) ->
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_in_on, self(), GCMsg}),
    on_gc_msg(GCMsg, UState, GCState);
on(Msg, UState, GCState) ->
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_in_on, self(), Msg}),
    T1GCState = on_bp(Msg, UState, GCState),
    case T1GCState of
        {drop_single, T2GCState} ->
            ?DBG_ASSERT2(not trace_mpath:infected(), {infected_after_on, self(), Msg}),
            loop(UState, T2GCState);
        _ ->
            Module  = gc_mod(T1GCState),
            Handler = gc_hand(T1GCState),
            try handle_message(Msg, UState, Handler) of
                kill ->
                    log:log(info, "[ gen_component ] ~.0p killed (~.0p:~.0p/2):",
                            [self(), Module, Handler]),
                    ok;
                {'$gen_component', [{post_op, Msg1}], NewUState} ->
                    on_post_op(Msg1, NewUState, T1GCState);
                {'$gen_component', [{on_handler, NewHandler}], NewUState} ->
                    %% This is not counted as a bp_step
                    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_after_on, self(), Msg}),
                    loop(NewUState, gc_set_hand(T1GCState, NewHandler));
                {'$gen_component', Commands, _NewUState} ->
                    %% let's fail since the Config list was either
                    %% empty or contained an invalid entry
                    log:log(warn, "[ gen_component ] unknown command(s): ~.0p",
                            [Commands]),
                    erlang:throw('unknown gen_component command');
                unknown_event ->
                    %% drop T2State, as it contains the error message
                    on_unknown_event(Msg, UState, T1GCState),
                    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_after_on, self(), Msg}),
                    case gc_bpactive(T1GCState) andalso gc_bpstepped(T1GCState) of
                        false -> loop(UState, T1GCState);
                        true -> loop(UState, bp_step_done(Msg, T1GCState))
                    end;
                NewUState ->
                    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_after_on, self(), Msg}),
                    case gc_bpactive(T1GCState) andalso gc_bpstepped(T1GCState) of
                        false -> loop(NewUState, T1GCState);
                        true -> loop(NewUState, bp_step_done(Msg, T1GCState))
                    end
            catch Level:Reason ->
                    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_after_on, self(), Msg}),
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
            end
    end.

-spec on_traced_msg(comm:message(), user_state(), gc_state())
                   -> ok.
on_traced_msg(GCMsg, UState, GCState)
  when is_tuple(GCMsg) andalso '$gen_component' =:= element(1, GCMsg) ->
    ?DBG_ASSERT2(trace_mpath:infected(), {not_infected_in_traced_msg, self(), GCMsg}),
    on_gc_msg(GCMsg, UState, GCState);
on_traced_msg(Msg, UState, GCState) ->
    ?DBG_ASSERT2(trace_mpath:infected(), {not_infected_in_traced_msg, self(), Msg}),
    T1GCState = on_bp(Msg, UState, GCState),
    Module  = gc_mod(T1GCState),
    Handler = gc_hand(T1GCState),
    try handle_message(Msg, UState, Handler) of
        kill ->
            log:log(info, "[ gen_component ] ~.0p killed (~.0p:~.0p/2):",
                    [self(), Module, Handler]),
            MsgTag = erlang:erase('$gen_component_trace_mpath_msg_tag'),
            trace_mpath:log_info(self(), {gc_on_done, MsgTag}),
            trace_mpath:stop(),
            ok;
        {'$gen_component', [{post_op, Msg1}], NewUState} ->
            on_post_op(Msg1, NewUState, T1GCState);
        {'$gen_component', [{on_handler, NewHandler}], NewUState} ->
            %% This is not counted as a bp_step
            MsgTag = erlang:erase('$gen_component_trace_mpath_msg_tag'),
            trace_mpath:log_info(self(), {gc_on_done, MsgTag}),
            trace_mpath:stop(),
            loop(NewUState, gc_set_hand(T1GCState, NewHandler));
        {'$gen_component', Commands, _NewUState} ->
            %% let's fail since the Config list was either
            %% empty or contained an invalid entry
            log:log(warn, "[ gen_component ] unknown command(s): ~.0p",
                    [Commands]),
            erlang:throw('unknown gen_component command');
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
                                 {"state", webhelpers:html_pre("~50p", [UState])}]});
on_unknown_event(UnknownMessage, UState, GCState) ->
    DbgMsg = "~n** Unknown message:~n ~.0p~n"
               "** Module:~n ~.0p~n"
               "** Handler:~n ~.0p~n"
               "** Pid:~n ~p ~.0p~n"
               "** State:~n ~.0p~n",
    DbgVal = [setelement(1, UnknownMessage, util:extint2atom(element(1, UnknownMessage))),
              gc_mod(GCState),
              gc_hand(GCState),
              self(), catch pid_groups:group_and_name_of(self()),
              {UState, GCState}],
    log_or_fail(DbgMsg, DbgVal, unknown_message).

on_exception(Msg, Level, Reason, Stacktrace, UState, GCState) ->
    DbgMsg = "~n** Exception:~n ~.0p:~.0p~n"
               "** Current message:~n ~.0p~n"
               "** Module:~n ~.0p~n"
               "** Handler:~n ~.0p~n"
               "** Pid:~n ~p ~.0p~n"
               "** Source linetrace (enable in scalaris.hrl):~n ~.0p~n"
               "** State:~n ~.0p~n"
               "** Stacktrace:~n ~.0p~n",
    Mod = gc_mod(GCState),
    DbgVal = [Level, Reason,
              setelement(1, Msg, util:extint2atom(element(1, Msg))),
              Mod,
              gc_hand(GCState),
              self(), catch pid_groups:group_and_name_of(self()),
              erlang:get(test_server_loc),
              {UState, GCState},
              Stacktrace],
    inform_exc_subs(gc_exc_subs(GCState), self(), Mod, {Level, Reason, Msg}),
    log_or_fail(DbgMsg, DbgVal, exception_throw).

-spec inform_exc_subs([comm:mypid()], pid(), atom(), term()) -> ok.
inform_exc_subs([], _Source, _Mod, _DbgMsg) -> ok;
inform_exc_subs([Pid | Tail], Source, Mod, DbgMsg) ->
    comm:send(Pid, {'DOWN', exc_subs, Mod, Source, DbgMsg}),
    inform_exc_subs(Tail, Source, Mod, DbgMsg).

-spec log_or_fail(DbgMsg::string(), DbgVal::[term()],
                  Reason::unknown_message | exception_throw) -> ok.
log_or_fail(DbgMsg, DbgVal, Reason) ->
    case util:is_unittest() of
        true ->
            % use ct:pal here as logging may have been stopped already
            ct:pal(DbgMsg, DbgVal),
            catch tester_global_state:log_last_calls(),
            ct:abort_current_testcase(Reason);
        _ ->
            log:log(error, DbgMsg, DbgVal)
    end,
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
        Logger    -> trace_mpath:log_info(
                       Logger, self(),
                       {gc_post_op, trace_mpath:get_msg_tag(Msg)}),
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
on_gc_msg({'$gen_component', bp, msg_in_bp_waiting, Pid} = _Msg, UState, GCState) ->
    Pid ! {'$gen_component', bp, msg_in_bp_waiting_response, false},
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), _Msg}),
    loop(UState, GCState);
on_gc_msg({'$gen_component', sleep, Time} = _Msg, UState, GCState) ->
    timer:sleep(Time),
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), _Msg}),
    loop(UState, GCState);
on_gc_msg({'$gen_component', about_to_kill, Time, Pid} = Msg, UState, GCState) ->
    comm:send_local(Pid, Msg),
    process_flag(priority, low),
    timer:sleep(Time),
    process_flag(priority, normal),
    log:log(warn, "[ ~p ] about_to_kill timeout hit without being killed", [self()]),
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), Msg}),
    loop(UState, GCState);
on_gc_msg({'$gen_component', remove_monitor, Pid, Source} = _Msg, UState, GCState) ->
    MonRef = erlang:get({'$gen_component_monitor_ref', Pid}),
    erlang:demonitor(MonRef),
    comm:send_local(Source, {'$gen_component', monitor, MonRef}),
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), _Msg}),
    loop(UState, GCState);
on_gc_msg({'$gen_component', get_state, Pid, Cookie} = _Msg, UState, GCState) ->
    comm:send_local(
      Pid, {'$gen_component', get_state_response, Cookie, UState}),
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), _Msg}),
    loop(UState, GCState);
on_gc_msg({'$gen_component', get_component_state, Pid, Cookie} = _Msg, UState, GCState) ->
    comm:send_local(
      Pid, {'$gen_component', get_component_state_response, Cookie,
            {gc_mod(GCState), gc_hand(GCState), GCState}}),
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), _Msg}),
    loop(UState, GCState);
on_gc_msg({'$gen_component', bp, bp_set_cond, Cond, BPName, Pid} = Msg, UState, GCState) ->
    NewGCState =
        case gc_bpqueue(GCState) of
            [] ->
                case Pid of none -> ok; _ -> Pid ! Msg, ok end,
                gc_bp_set_cond(GCState, Cond, BPName);
            _ -> gc_bp_hold_back(GCState, Msg)
        end,
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), Msg}),
    loop(UState, NewGCState);
on_gc_msg({'$gen_component', bp, bp_set, MsgTag, BPName, Pid} = Msg, UState, GCState) ->
    NewGCState =
        case gc_bpqueue(GCState) of
            [] ->
                Pid ! Msg,
                gc_bp_set(GCState, MsgTag, BPName);
            _ -> gc_bp_hold_back(GCState, Msg)
        end,
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), Msg}),
    loop(UState, NewGCState);
on_gc_msg({'$gen_component', bp, bp_del, BPName, Pid} = Msg, UState, GCState) ->
    NewGCState =
        case gc_bpqueue(GCState) of
            [] ->
                case Pid of none -> ok; _ -> Pid ! Msg, ok end,
                gc_bp_del(GCState, BPName);
            _ -> gc_bp_hold_back(GCState, Msg)
        end,
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), Msg}),
    loop(UState, NewGCState);
on_gc_msg({'$gen_component', bp, barrier} = Msg, UState, GCState) ->
    NewGCState = gc_bp_hold_back(GCState, Msg),
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), Msg}),
    loop(UState, NewGCState);

on_gc_msg({'$gen_component', bp, breakpoint, step, _Stepper} = Msg,
          UState, GCState) ->
    NewGCState = gc_bp_hold_back(GCState, Msg),
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), Msg}),
    loop(UState, NewGCState);
on_gc_msg({'$gen_component', bp, breakpoint, cont} = Msg,
          UState, GCState) ->
    NewGCState = gc_bp_hold_back(GCState, Msg),
    ?DBG_ASSERT2(not trace_mpath:infected(), {infected_gc_msg, self(), Msg}),
    loop(UState, NewGCState).


-spec on_bp(comm:message(), user_state(), gc_state()) ->
                   gc_state() | {drop_single, gc_state()}.
on_bp(_Msg, _UState, GCState)
  when (false =:= element(?BP_ACTIVE, GCState))
       andalso ([] =:= element(?BPS, GCState)) ->
   GCState;
on_bp(Msg, UState, GCState) ->
    BPActive = bp_active(Msg, UState, GCState),
    wait_for_bp_leave(Msg, GCState, BPActive).

-spec bp_active(comm:message(), user_state(), gc_state()) ->
                       boolean() | drop_single.
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
                false ->
                    bp_active(Msg, UState, gc_set_bps(GCState, RemainingBPs));
                drop_single ->
                    ?TRACE_BP("~p Now in BP ~p via: ~p~n", [self(), ThisBP, Msg]),
                    Decision
            end
        end.

-spec wait_for_bp_leave(comm:message(), gc_state(), boolean() | drop_single) ->
                               gc_state() | {drop_single, gc_state()}.
wait_for_bp_leave(_Msg, State, _BP_Active = false) -> State;
wait_for_bp_leave(_Msg, State, _BP_Active = drop_single) -> {drop_single, State};
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
            BPQueue ->
                ?TRACE_BP("~p process queued bp op ~p.~n",
                          [self(), hd(BPQueue)]),
                {BPQueue, true}
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
                {'$gen_component', bp, bp_set_cond, Cond, BPName, Pid} = BPMsg,
                IsFromQueue) ->
    NextState =
        case gc_bpqueue(State) of
            [_H|_T] when not IsFromQueue -> gc_bp_hold_back(State, BPMsg);
            _ ->
                case Pid of none -> ok; _ -> Pid ! Msg, ok end,
                gc_bp_set_cond(State, Cond, BPName)
        end,
    wait_for_bp_leave(Msg, NextState, true);
on_bp_req_in_bp(Msg, State,
                {'$gen_component', bp, bp_set, MsgTag, BPName, Pid} = BPMsg,
                IsFromQueue) ->
    NextState =
        case gc_bpqueue(State) of
            [_H|_T] when not IsFromQueue -> gc_bp_hold_back(State, BPMsg);
            _ ->
                Pid ! BPMsg,
                gc_bp_set(State, MsgTag, BPName)
        end,
    wait_for_bp_leave(Msg, NextState, true);
on_bp_req_in_bp(Msg, State,
                {'$gen_component', bp, bp_del, BPName, Pid}= BPMsg,
                IsFromQueue)->
    NextState =
        case gc_bpqueue(State) of
            [_H|_T] when not IsFromQueue -> gc_bp_hold_back(State, BPMsg);
            _ ->
                case Pid of none -> ok; _ -> Pid ! BPMsg, ok end,
                gc_bp_del(State, BPName)
        end,
    wait_for_bp_leave(Msg, NextState, true);
on_bp_req_in_bp(Msg, State,
                {'$gen_component', get_component_state, Pid, Cookie},
                _IsFromQueue) ->
    comm:send_local(
      Pid, {'$gen_component', get_component_state_response, Cookie, State}),
    wait_for_bp_leave(Msg, State, true);
on_bp_req_in_bp(Msg, State, {'$gen_component', remove_monitor, Pid, Source},
                _IsFromQueue) ->
    MonRef = erlang:get({'$gen_component_monitor_ref', Pid}),
    erlang:demonitor(MonRef),
    comm:send_local(Source, {'$gen_component', monitor, MonRef}),
    wait_for_bp_leave(Msg, State, true);
on_bp_req_in_bp(Msg, State,
                {'$gen_component', about_to_kill, Time, Pid} = SleepMsg,
                _IsFromQueue) ->
    comm:send_local(Pid, SleepMsg),
    process_flag(priority, low),
    timer:sleep(Time),
    process_flag(priority, normal),
    log:log(warn, "[ ~p ] about_to_kill timeout hit without being killed", [self()]),
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
     _BPStepperPid = unknown,
     _ExcSubs = []
    }.
gc_mod(State) ->                element(?MOD, State).
gc_hand(State) ->               element(?HAND, State).
gc_set_hand(State, Handler) ->  setelement(?HAND, State, Handler).
gc_exc_subs(State) ->           element(?EXC_SUBS, State).
gc_set_exc_subs(State, Subs) -> setelement(?EXC_SUBS, State, Subs).
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
