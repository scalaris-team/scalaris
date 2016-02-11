% @copyright 2009-2015 Zuse Institute Berlin,
%            2009 onScale solutions GmbH
% @end

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
%% @doc Cheap message delay.
%%      Instead of using send_after, which is slow in Erlang, as it
%%      performs a system call, this module allows for a weaker
%%      message delay.
%%      You can specify the minimum message delay in seconds and the
%%      component will send the message sometime afterwards.
%%      Only local messages inside a VM are supported.
%%      Internally it triggers itself periodically to schedule sending.
%% @end
%% @version $Id$
-module(msg_delay).
-author('schintke@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(_X,_Y), ok).
-behaviour(gen_component).

%% public interface for delayed messages
-export([send_local/3, send_local/4,
         send_local_as_client/3, send_local_as_client/4]).

%% public interface for self trigger messages using msg_delay
-export([send_trigger/2]).

%% functions for gen_component module and supervisor callbacks
-export([start_link/1, on/2, init/1]).

-include("gen_component.hrl").

% accepted messages of the msg_delay process
-type message() ::
    {msg_delay_req, Seconds::pos_integer(), Dest::comm:erl_local_pid(),
     Msg::comm:message(), comm:send_local_options()} |
    {msg_delay_periodic}.

% internal state
-type state() :: {TimeTable :: pdb:tableid(), Round :: non_neg_integer()}.

-spec send_local_as_client(Seconds::non_neg_integer(),
                           Dest::comm:erl_local_pid(),
                           Msg::comm:message()) -> ok.
send_local_as_client(Seconds, Dest, Msg) ->
    send_local_as_client(Seconds, Dest, Msg, []).

-spec send_local_as_client(Seconds::non_neg_integer(),
                           Dest::comm:erl_local_pid(),
                           Msg::comm:message(), comm:send_local_options())
        -> ok.
send_local_as_client(Seconds, Dest, Msg, Options) ->
    Delayer = pid_groups:pid_of(clients_group, msg_delay),
    %% TODO: if infected with proto_sched logging, immediately log msg
    %% to proto_sched for the 'delayed' messages pool (which is not
    %% implemented yet) and do *not* deliver to msg_delay process.
    comm:send_local(Delayer, {msg_delay_req, Seconds, Dest, Msg, Options}).

-spec send_local(Seconds::non_neg_integer(), Dest::comm:erl_local_pid(),
                 Msg::comm:message()) -> ok.
send_local(Seconds, Dest, Msg) ->
    send_local(Seconds, Dest, Msg, []).

-spec send_local(Seconds::non_neg_integer(), Dest::comm:erl_local_pid(),
                 Msg::comm:message(), comm:send_local_options()) -> ok.
send_local(Seconds, Dest, Msg, Options) ->
    Delayer = pid_groups:find_a(msg_delay),
    %% TODO: if infected with proto_sched logging, immediately deliver
    %% to proto_sched for the 'delayed' messages pool (which is not
    %% implemented yet) and do *not* deliver to msg_delay process.
    comm:send_local(Delayer, {msg_delay_req, Seconds, Dest, Msg, Options}).

%% be startable via supervisor, use gen_component
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(DHTNodeGroup) ->
    gen_component:start_link(?MODULE, fun ?MODULE:on/2,
                             [], % parameters passed to init
                             [{pid_groups_join_as, DHTNodeGroup, msg_delay}]).

-spec send_trigger(Seconds::non_neg_integer(), Msg::comm:message()) -> ok.
send_trigger(Seconds, Msg) ->
    %% (1) Triggers are typically periodic messages. We do not want to
    %%     infect a system forever, so we forbid trace_mpath here.
    %% (2) We do not need a reply_as for self() in this case, as one can
    %%     easily put all infos in the message itself, when inside the
    %%     same process.
    ?DBG_ASSERT2(erlang:get(trace_mpath) =:= undefined,
                 send_trigger_infected_with_trace_mpath_or_proto_sched),
    send_local(Seconds, self(), Msg, [{?quiet}]).

%% userdevguide-begin gen_component:sample
%% initialize: return initial state.
-spec init([]) -> state().
init([]) ->
    ?TRACE("msg_delay:init for pid group ~p~n", [pid_groups:my_groupname()]),
    %% For easier debugging, use a named table (generates an atom)
    %%TableName = erlang:list_to_atom(pid_groups:group_to_filename(pid_groups:my_groupname()) ++ "_msg_delay"),
    %%TimeTable = pdb:new(TableName, [set, protected, named_table]),
    %% use random table name provided by ets to *not* generate an atom
    TimeTable = pdb:new(?MODULE, [set]),
    comm:send_local(self(), {msg_delay_periodic}),
    _State = {TimeTable, _Round = 0}.

-spec on(message(), state()) -> state().
on({msg_delay_req, Seconds, Dest, Msg, Options} = _FullMsg,
   {TimeTable, Counter} = State) ->
    ?TRACE("msg_delay:on(~.0p, ~.0p)~n", [_FullMsg, State]),
    Future = trunc(Counter + Seconds),
    EMsg = case erlang:get(trace_mpath) of
               undefined -> Msg;
               PState -> trace_mpath:epidemic_reply_msg(PState, comm:this(), Dest, Msg)
           end,
    case pdb:get(Future, TimeTable) of
        undefined ->
            pdb:set({Future, [{Dest, EMsg, Options}]}, TimeTable);
        {_, MsgQueue} ->
            pdb:set({Future, [{Dest, EMsg, Options} | MsgQueue]}, TimeTable)
    end,
    State;

%% periodic trigger
on({msg_delay_periodic} = Trigger, {TimeTable, Counter} = _State) ->
    ?TRACE("msg_delay:on(~.0p, ~.0p)~n", [Trigger, State]),
    % triggers are not allowed to be infected!
    ?DBG_ASSERT2(not trace_mpath:infected(), trigger_infected),
    _ = case pdb:take(Counter, TimeTable) of
        undefined -> ok;
        {_, MsgQueue} ->
            _ = [ case Msg of
                      {'$gen_component', trace_mpath, PState, _From, _To, OrigMsg} ->
                          case element(2, PState) of %% element 2 is the logger
                              {proto_sched, _} ->
                                  log:log("msg_delay: proto_sched not ready for delayed messages, so dropping this message: ~p", [OrigMsg]),
                                  %% these messages should not be
                                  %% accepted to the database of
                                  %% msg_delay anyway, but instead
                                  %% should be immediately delivered
                                  %% to proto_sched for the 'delayed'
                                  %% messages pool (which is not
                                  %% implemented yet) (see send_local,
                                  %% send_local_as_client)
                                  %% erlang:throw(redirect_proto_sched_msgs_at_submission_please)
                                  ok;
                              _ ->
                                  trace_mpath:start(PState),
                                  comm:send_local(Dest, OrigMsg, Options),
                                  trace_mpath:stop()
                          end;
                      _ ->
                          ?DBG_ASSERT2(not trace_mpath:infected(), infected_with_uninfected_msg),
                          comm:send_local(Dest, Msg, Options)
                  end || {Dest, Msg, Options} <- MsgQueue ]
    end,
    _ = comm:send_local_after(1000, self(), Trigger),
    {TimeTable, Counter + 1};

on({web_debug_info, Requestor}, {TimeTable, Counter} = State) ->
    KeyValueList =
        [{"queued messages (in 0-10s, messages):", ""} |
         [begin
              Future = trunc(Counter + Seconds),
              Queue = case pdb:get(Future, TimeTable) of
                          undefined -> none;
                          {_, Q}    -> Q
                      end,
              {webhelpers:safe_html_string("~p", [Seconds]),
               webhelpers:safe_html_string("~p", [Queue])}
          end || Seconds <- lists:seq(0, 10)]],
    comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.
%% userdevguide-end gen_component:sample
