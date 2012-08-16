% @copyright 2012 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc Trace what a message triggers in the system by tracing all
%% generated subsequent messages.
%% @version $Id$
-module(trace_mpath).
-author('schintke@zib.de').
-vsn('$Id$ ').

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% 1. call trace_mpath:start(your_trace_id)
%% 2. perform a request like api_tx:read("a")
%% 3. call trace_mpath:stop() %% trace_id is taken from the calling
%%                               process implicitly
%% 4. call trace_mpath:get_trace(your_trace_id) to retrieve the trace,
%%    when you think everything is recorded
%% 5. call trace_mpath:cleanup(your_trace_id) to free the memory
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("scalaris.hrl").
-behaviour(gen_component).

%% client functions
-export([start/0, start/1, start/2, stop/0]).
-export([get_trace/0, get_trace/1, get_trace_raw/1, cleanup/0, cleanup/1]).

%% trace analysis
-export([send_histogram/1]).

%% report tracing events from other modules
-export([log_send/4]).
-export([log_info/3]).
-export([log_recv/4]).
-export([epidemic_reply_msg/4]).

%% gen_component behaviour
-export([start_link/1, init/1]).
-export([on/2]). %% internal message handler as gen_component

-type logger()       :: io_format                       %% | ctpal
                      | {log_collector, comm:mypid()}.
-type pidinfo()      :: {comm:mypid(), {pid_groups:groupname(),
                                        pid_groups:pidname()}}.
-type anypid()       :: pid() | comm:mypid() | pidinfo().
-type trace_id()     :: atom().
-type send_event()   :: {log_send, erlang_timestamp(), trace_id(),
                         Source::pidinfo(), Dest::pidinfo(), comm:message()}.
-type info_event()   :: {log_info, erlang_timestamp(), trace_id(),
                         pidinfo(), comm:message()}.
-type recv_event()   :: {log_recv, erlang_timestamp(), trace_id(),
                         Source::pidinfo(), Dest::pidinfo(), comm:message()}.
-type trace_event()  :: send_event() | info_event() | recv_event().
-type trace()        :: [trace_event()].
-type msg_map_fun()  :: fun((comm:message()) -> comm:message()).
-type passed_state() :: {trace_id(), logger(), msg_map_fun()}.
-type gc_mpath_msg() :: {'$gen_component', trace_mpath, passed_state(),
                         Source::pidinfo(), Dest::pidinfo(), comm:message()}.

-ifdef(with_export_type_support).
-export_type([logger/0]).
-export_type([pidinfo/0]).
-export_type([passed_state/0]).
-endif.

-spec start() -> ok.
start() -> start(default).

-spec start(trace_id() | passed_state()) -> ok.
start(TraceId) when is_atom(TraceId) ->
    LoggerPid = pid_groups:find_a(trace_mpath),
    start(TraceId, comm:make_global(LoggerPid), fun(Msg) -> Msg end);
start(PState) when is_tuple(PState) ->
    start(passed_state_trace_id(PState), 
          passed_state_logger(PState),
          passed_state_msg_map_fun(PState)).

-spec start(trace_id(), logger() | comm:mypid() | msg_map_fun()) -> ok.
start(TraceId, MsgMapFun) when is_function(MsgMapFun) ->
    LoggerPid = pid_groups:find_a(trace_mpath),    
    start(TraceId, comm:make_global(LoggerPid), MsgMapFun);
start(TraceId, Logger) ->
    start(TraceId, Logger, fun(Msg) -> Msg end).

-spec start(trace_id(), logger() | comm:mypid(), msg_map_fun()) -> ok.
start(TraceId, _Logger, MsgMapFun) ->
    Logger = case comm:is_valid(_Logger) of
                 true -> {log_collector, _Logger}; %% just a pid was given                     
                 false -> _Logger
             end,
    PState = passed_state_new(TraceId, Logger, MsgMapFun),
    own_passed_state_put(PState).

-spec stop() -> ok.
stop() ->
    %% stop sending epidemic messages
    erlang:erase(trace_mpath),
    ok.

-spec get_trace() -> trace().
get_trace() -> get_trace(default).

-spec get_trace(trace_id()) -> trace().
get_trace(TraceId) ->
    LogRaw = get_trace_raw(TraceId),
    [case Event of
         {SendOrRcv, Time, TraceId, Source, Dest, {Tag, Key, Hops, Msg}}
           when Tag =:= ?lookup_aux orelse Tag =:= ?lookup_fin ->
             {SendOrRcv, Time, TraceId, Source, Dest,
              convert_msg({Tag, Key, Hops, convert_msg(Msg)})};
         {SendOrRcv, Time, TraceId, Source, Dest, Msg} ->
             {SendOrRcv, Time, TraceId, Source, Dest, convert_msg(Msg)};
         {log_info, _Time, _TraceId, _Pid, _Msg} = X -> X
     end || Event <- LogRaw].

-spec convert_msg(Msg::comm:message()) -> comm:message().
convert_msg(Msg) when is_tuple(Msg) ->
    setelement(1, Msg, util:extint2atom(element(1, Msg))).

-spec get_trace_raw(trace_id()) -> trace().
get_trace_raw(TraceId) ->
    LoggerPid = pid_groups:find_a(trace_mpath),
    comm:send_local(LoggerPid, {get_trace, comm:this(), TraceId}),
    receive
        ?SCALARIS_RECV({get_trace_reply, Log}, Log)
    end.

-spec cleanup() -> ok.
cleanup() -> cleanup(default).

-spec cleanup(trace_id()) -> ok.
cleanup(TraceId) ->
    LoggerPid = pid_groups:find_a(trace_mpath),
    comm:send_local(LoggerPid, {cleanup, TraceId}),
    ok.

%% Functions for trace analysis
-spec send_histogram(trace()) -> list().
send_histogram(Trace) ->
    %% only send events
    Sends = [ X || X <- Trace, element(1, X) =:= log_send],
    %% only message tags
    Tags = [ element(1,element(6,X)) || X <- Sends],
    SortedTags = lists:sort(Tags),
    %% reduce tags
    CountedTags = lists:foldl(fun(X, Acc) ->
                                      case Acc of
                                          [] -> [{X, 1}];
                                          [{Y, Count} | Tail] ->
                                              case X =:= Y of
                                                  true ->
                                                      [{Y, Count + 1} | Tail];
                                                  false ->
                                                      [{X, 1}, {Y, Count} | Tail]
                                              end
                                      end
                              end,
                              [], SortedTags),
    lists:reverse(lists:keysort(2, CountedTags)).

%% Functions used to report tracing events from other modules
-spec epidemic_reply_msg(passed_state(), anypid(), anypid(), comm:message()) ->
                                gc_mpath_msg().
epidemic_reply_msg(PState, FromPid, ToPid, Msg) ->
    From = normalize_pidinfo(FromPid),
    To = normalize_pidinfo(ToPid),
    {'$gen_component', trace_mpath, PState, From, To, Msg}.

-spec log_send(passed_state(), anypid(), anypid(), comm:message()) ->
                      gc_mpath_msg().
log_send(PState, FromPid, ToPid, Msg) ->
    From = normalize_pidinfo(FromPid),
    To = normalize_pidinfo(ToPid),
    Now = os:timestamp(),
    MsgMapFun = passed_state_msg_map_fun(PState),
    case passed_state_logger(PState) of
        io_format ->
            io:format("~p send ~.0p -> ~.0p:~n  ~.0p.~n",
                      [util:readable_utc_time(Now), From, To, MsgMapFun(Msg)]);
        {log_collector, LoggerPid} ->
            TraceId = passed_state_trace_id(PState),
            send_log_msg(LoggerPid, {log_send, Now, TraceId, From, To, MsgMapFun(Msg)})
    end,
    epidemic_reply_msg(PState, From, To, Msg).

-spec log_info(passed_state(), anypid(), term()) -> ok.
log_info(PState, FromPid, Info) ->
    From = normalize_pidinfo(FromPid),
    Now = os:timestamp(),    
    case passed_state_logger(PState) of
        io_format ->
            io:format("~p info ~.0p:~n  ~.0p.~n",
                      [util:readable_utc_time(Now), From, Info]);
        {log_collector, LoggerPid} ->
            TraceId = passed_state_trace_id(PState),
            send_log_msg(LoggerPid, {log_info, Now, TraceId, From, Info})
    end,
    ok.

-spec log_recv(passed_state(), anypid(), anypid(), comm:message()) -> ok.
log_recv(PState, FromPid, ToPid, Msg) ->
    From = normalize_pidinfo(FromPid),
    To = normalize_pidinfo(ToPid),
    Now = os:timestamp(),
    MsgMapFun = passed_state_msg_map_fun(PState),
    case  passed_state_logger(PState) of
        io_format ->
            io:format("~p recv ~.0p -> ~.0p:~n  ~.0p.~n",
                      [util:readable_utc_time(Now), From, To, MsgMapFun(Msg)]);
        {log_collector, LoggerPid} ->
            TraceId = passed_state_trace_id(PState),
            send_log_msg(LoggerPid, {log_recv, Now, TraceId, From, To, MsgMapFun(Msg)})
    end,
    ok.

-spec send_log_msg(comm:mypid(), trace_event()) -> ok.
send_log_msg(LoggerPid, Msg) ->
    %% don't log the sending of log messages ...
    RestoreThis = own_passed_state_get(),
    stop(),
    comm:send(LoggerPid, Msg),
    own_passed_state_put(RestoreThis).

-spec normalize_pidinfo(anypid()) -> pidinfo().
normalize_pidinfo(Pid) ->
    case is_pid(Pid) of
        true -> {comm:make_global(Pid), pid_groups:group_and_name_of(Pid)};
        false ->
            case comm:is_valid(Pid) of
                true ->
                    case comm:is_local(Pid) of
                        true -> {Pid,
                                 pid_groups:group_and_name_of(
                                   comm:make_local(Pid))};
                        false -> {Pid, non_local_pid_name_unknown}
                    end;
                false -> %% already a pidinfo()
                    Pid
            end
    end.

-type state() :: [{trace_id(), trace()}].

-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(ServiceGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2, [],
                             [{erlang_register, trace_mpath},
                              {pid_groups_join_as, ServiceGroup, ?MODULE}]).

-spec init(any()) -> state().
init(_Arg) -> [].

-spec on(trace_event() | comm:message(), state()) -> state().
on({log_send, _Time, TraceId, _From, _To, _UMsg} = Msg, State) ->
    state_add_log_event(State, TraceId, Msg);
on({log_recv, _Time, TraceId, _From, _To, _UMsg} = Msg, State) ->
    state_add_log_event(State, TraceId, Msg);
on({log_info, _Time, TraceId, _From, _UMsg} = Msg, State) ->
    state_add_log_event(State, TraceId, Msg);

on({get_trace, Pid, TraceId}, State) ->
    case lists:keyfind(TraceId, 1, State) of
        false ->
            comm:send(Pid, {get_trace_reply, no_trace_found});
        {TraceId, Msgs} ->
            comm:send(Pid, {get_trace_reply, lists:reverse(Msgs)})
    end,
    State;
on({cleanup, TraceId}, State) ->
    case lists:keytake(TraceId, 1, State) of
        {value, _Tuple, TupleList2} -> TupleList2;
        false                       -> State
    end.

passed_state_new(TraceId, Logger, MsgMapFun) -> 
    {TraceId, Logger, MsgMapFun}.

passed_state_trace_id(State)      -> element(1, State).
passed_state_logger(State)        -> element(2, State).
passed_state_msg_map_fun(State)   -> element(3, State).

own_passed_state_put(State)       -> erlang:put(trace_mpath, State), ok.
own_passed_state_get()            -> erlang:get(trace_mpath).

state_add_log_event(State, TraceId, Msg) ->
    NewEntry = case lists:keyfind(TraceId, 1, State) of
                   false ->
                       {TraceId, [Msg]};
                   {TraceId, OldTrace} ->
                       {TraceId, [Msg | OldTrace]}
               end,
    lists:keystore(TraceId, 1, State, NewEntry).
