%  @copyright 2007-2015 Zuse Institute Berlin

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
%% @doc    Message Sending.
%%
%%  Messages consist of a tuple of which the first element is the message's
%%  tag, i.e. an atom. Process identifiers depend on local and global target
%%  and can also wrap up an reply envelope that can be used to
%%  rewrite reply messages.
%%
%%  Sending messages to so-enveloped process identifiers works
%%  seamlessly, e.g.  a server receiving message {tag, SourcePid} can
%%  reply with comm:send(SourcePid, {tag_response}). On the receiving
%%  side (a client), the reply message is embedded into the envelope
%%  tuple at the specified position. Pids
%%  with envelopes  can be created using reply_as/3.  @end
%%  @version $Id$
-module(comm).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

%% Sending messages
-export([send/2, send/3, send_local/2, send_local/3, send_local_after/3,
         forward_to_group_member/2, forward_to_registered_proc/2]).

%% Pid manipulation
-export([make_global/1, make_local/1]).
-export([this/0, get/2]).
-export([reply_as/3]).
-export([is_valid/1, is_local/1]).
-export([get_ip/1, get_port/1]).

%% Message manipulation
-export([get_msg_tag/1]).
-export([unpack_cookie/2, get_plain_pid/1]).

%% initialization
-export([init_and_wait_for_valid_IP/0]).

-export_type([message/0, group_message/0, msg_tag/0,
              mypid/0, mypid_plain/0,
              erl_local_pid/0, erl_local_pid_plain/0,
              erl_local_pid_with_reply_as/0,
              send_options/0, send_local_options/0, channel/0]).

-type msg_tag() :: atom() | byte(). %% byte() in case of compact external atoms. See include/atom_ext.hrl

%% there is no variable length-tuple definition for types
%% -> declare messages with up to 15 parameters here:
-type envelope() ::
        {msg_tag(), any()} |
        {msg_tag(), any(), any()} |
        {msg_tag(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()} |
        {msg_tag(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()}.
-type message() ::
        {msg_tag()} | envelope().

-type channel() :: main | prio.

-type reg_name()                  :: atom().
-type erl_local_pid_plain()       :: pid() | reg_name().
-type mypid_plain() :: {inet:ip_address(),
                        comm_server:tcp_port(),
                        erl_local_pid_plain()}.

-type group_message() :: {?send_to_group_member | ?send_to_registered_proc, atom(), message() | group_message()}.
% envelopes:
-type erl_local_pid_with_reply_as() ::
        {erl_local_pid_plain(),         e, pos_integer(), envelope()}
      | {erl_local_pid_with_reply_as(), e, pos_integer(), envelope()}.
-type mypid_with_reply_as() ::
        {mypid_plain(),         e, pos_integer(), envelope()}
      | {mypid_with_reply_as(), e, pos_integer(), envelope()}.

-type erl_local_pid() :: erl_local_pid_plain() | erl_local_pid_with_reply_as().
-type mypid()         :: mypid_plain() | mypid_with_reply_as().

%-type plain_pid() :: mypid_plain() | erl_local_pid_plain().

-type send_options() :: [{shepherd, Pid::erl_local_pid()} |
                         {group_member, Process::atom()} |
                         {channel, channel()} | {?quiet} |
                         {no_keep_alive}].

-type send_local_options() :: [{?quiet}].

-dialyzer({no_contracts, [unpack_cookie/2, get_plain_pid/1]}).

%% @doc Sends a message to a process given by its pid.
-spec send(mypid(), message() | group_message()) -> ok.
send(Pid, Msg) -> send(Pid, Msg, []).

%% @doc Send a message to an arbitrary process with the given options.
%%      If a shepherd is given, it will be informed when the sending fails;
%%      with a message of the form:
%%       {send_error, Pid, Msg, Reason}.
%%      If a group_member is given, the message is send to an arbitrary process
%%      of another node instructing it to forward the message to a process in
%%      its group with the given name.
-spec send(mypid(), message() | group_message(), send_options()) -> ok.
send(Pid, Msg, Options) ->
    {RealPid, RealMsg1} = unpack_cookie(Pid, Msg),
    {RealMsg, RealOpts} = pack_group_member(RealMsg1, Options),
    case erlang:get(trace_mpath) of
        undefined ->
            comm_server:send(RealPid, RealMsg, RealOpts);
        Logger ->
            RealNumericPid =
                case comm_server:is_local(RealPid) of
                    true ->
                        LocalPidPart = make_local(RealPid),
                        LocalNumericPid = case is_atom(LocalPidPart) of
                                              true -> whereis(LocalPidPart);
                                              false -> LocalPidPart
                                          end,
                        make_global(LocalNumericPid);
                    false ->
                        % TODO: convert named process on remote node?
                        RealPid
                end,
            Deliver = trace_mpath:log_send(Logger, self(),
                                           RealNumericPid, RealMsg, global,
                                           Options),
            case Deliver of
                false -> ok;
                true ->
                    %% send infected message to destination
                    LogEpidemicMsg = trace_mpath:epidemic_reply_msg(
                                       Logger, self(), RealPid, RealMsg),
                    comm_server:send(RealPid, LogEpidemicMsg, RealOpts)
            end
    end,
    ok.

-ifdef(enable_debug).
-define(SEND_LOCAL_CHECK_PID(Pid, Msg, Options),
        if is_atom(Pid) ->
               case whereis(Pid) of
                   undefined ->
                       case lists:member({?quiet}, Options) of
                           false ->
                               log:log(warn, "~p (name: ~.0p) Send to ~.0p failed, "
                                       "drop message ~.0p due to ~p",
                                       [self(), pid_groups:my_pidname(), RealPid,
                                        RealMsg, local_target_not_alive]);
                           _ -> ok
                       end;
                   _ -> ok
               end;
           is_pid(Pid) ->
               case is_process_alive(Pid) andalso
                        erlang:process_info(Pid, priority) =/= {priority, low} of
                   true -> ok;
                   false ->
                       case lists:member({?quiet}, Options) of
                           false ->
                               log:log(warn, "~p (name: ~.0p) Send to ~.0p failed, "
                                       "drop message ~.0p due to ~p",
                                       [self(), pid_groups:my_pidname(), RealPid,
                                        RealMsg, local_target_not_alive]);
                           _ -> ok
                       end
               end
        end).
-else.
-define(SEND_LOCAL_CHECK_PID(Pid, Msg, Options), ok).
-endif.

%% @doc Sends a message to a local process given by its local pid
%%      (as returned by self()).
-spec send_local(erl_local_pid(), message() | group_message()) -> ok.
send_local(Pid, Msg) ->
    send_local(Pid, Msg, []).

%% @doc Sends a message to a local process given by its local pid
%%      (as returned by self()).
-spec send_local(erl_local_pid(), message() | group_message(),
                 send_local_options()) -> ok.
send_local(Pid, Msg, Options) ->
    {RealPid, RealMsg} = unpack_cookie(Pid, Msg),
    _ = case erlang:get(trace_mpath) of
            undefined ->
                ?SEND_LOCAL_CHECK_PID(RealPid, RealMsg, Options),
                RealPid ! RealMsg;
            Logger ->
                RealNumericPid = case is_atom(RealPid) of
                                     true -> whereis(RealPid);
                                     false -> RealPid
                                 end,
                Deliver = trace_mpath:log_send(Logger, self(),
                                               RealNumericPid, RealMsg, local,
                                               Options),
                case Deliver of
                    false -> ok;
                    true ->
                        LogEpidemicMsg = trace_mpath:epidemic_reply_msg(
                                           Logger, self(), RealPid, RealMsg),
                        ?SEND_LOCAL_CHECK_PID(RealPid, LogEpidemicMsg, Options),
                        RealPid ! LogEpidemicMsg
                end
        end,
    ok.

%% @doc Sends a message to a local process given by its local pid
%%      (as returned by self()) after the given delay in milliseconds.
-spec send_local_after(Delay::0..4294967295, erl_local_pid(), message() | group_message()) -> reference().
send_local_after(Delay, Pid, Msg) ->
    {RealPid, RealMsg} = unpack_cookie(Pid, Msg),
    case erlang:get(trace_mpath) of
        undefined ->
            erlang:send_after(Delay, RealPid, RealMsg);
        Logger ->
            %% TODO: put RealMsg into the delayed pool of proto_sched
            %%       by using new a new send type 'local_after'. Have
            %%       also to adapt trace_mpath then.
            Deliver = trace_mpath:log_send(Logger, self(),
                                           RealPid, RealMsg, local_after, []),
            %% should we also deliver using the normal way?
            %% (is it a trace (=true) or a proto_sched (=false)).
            case Deliver of
                false ->
                    %% to keep the -spec we return a reference().  In
                    %% contrast to erlang:send_after() one cannot
                    %% cancel this message with a correspondig
                    %% erlang:cancel_timer(). It still will be
                    %% delivered after the corresponding attempt to
                    %% cancel it.  We cannot solve this better, as we
                    %% cannot track the corresponding cancel_timer().
                    erlang:make_ref();
                true ->
                    LogEpidemicMsg = trace_mpath:epidemic_reply_msg(
                                       Logger, self(), RealPid, RealMsg),
                    erlang:send_after(Delay, RealPid, LogEpidemicMsg)
            end
    end.

%% @doc Convert a local or global erlang pid to a global pid of type
%%      mypid() for use in send/2.
-spec make_global(erl_local_pid() | mypid()) -> mypid().
make_global({Pid, e, Nth, Cookie}) ->
    {make_global(Pid), e, Nth, Cookie};
make_global(Pid) when is_pid(Pid) -> get(Pid, this());
make_global(Pid) when is_atom(Pid) -> get(Pid, this());
make_global(GlobalPid) -> GlobalPid.

%% @doc Convert a global mypid() of the current node to a local erlang pid.
-spec make_local(erl_local_pid() | mypid()) -> erl_local_pid().
make_local({Pid, e, Nth, Cookie}) when is_pid(Pid) orelse is_atom(Pid) ->
    {Pid, e, Nth, Cookie};
make_local({Pid, e, Nth, Cookie}) ->
    {make_local(Pid), e, Nth, Cookie};
make_local(Pid) when is_pid(Pid) orelse is_atom(Pid) -> Pid;
make_local(Pid) -> comm_server:make_local(Pid).

%% @doc Returns the global pid of the current process.
-spec this() -> mypid_plain().
this() -> comm_server:this().

%% @doc Creates the plain PID a process with name Name would have on node Node.
-spec get(Name::erl_local_pid_plain(), Node::mypid()) -> mypid_plain().
get(Name, {Pid, e, _Nth, _Envelope} = _Node) ->
    get(Name, Pid);
get(Name, {IP, Port, _Pid} = _Node) -> {IP, Port, Name}.


%% @doc Encapsulates the given pid (local or global) with the reply_as
%%      request, so a send/2 to the generated target will put a reply
%%      message at the Nth position of the given envelope.
-spec reply_as(erl_local_pid() | mypid(), 2..16, envelope()) ->
               mypid_with_reply_as() | erl_local_pid_with_reply_as().
reply_as(Target, Nth, Envelope) ->
    ?DBG_ASSERT('_' =:= element(Nth, Envelope)),
    {Target, e, Nth, Envelope}.

%% @doc Check whether the given pid is well formed.
-spec is_valid(mypid() | any()) -> boolean().
is_valid({Pid, e, _Nth, _Cookie}) -> is_valid(Pid);
is_valid(Pid) -> comm_server:is_valid(Pid).

%% @doc Check whether a global mypid() can be converted to a local
%%      pid of the current node.
-spec is_local(mypid()) -> boolean().
is_local(Pid) ->
    comm_server:is_local(get_plain_pid(Pid)).

%% @doc Gets the IP address of the given (global) mypid().
-spec get_ip(mypid()) -> inet:ip_address().
get_ip(Pid) ->
    comm_server:get_ip(get_plain_pid(Pid)).

%% @doc Gets the port of the given (global) mypid().
-spec get_port(mypid()) -> non_neg_integer().
get_port(Pid) ->
    comm_server:get_port(get_plain_pid(Pid)).


%% @doc Gets the tag of a message (the first element of its tuple - should be an
%%      atom).
-spec get_msg_tag(message() | group_message()) -> msg_tag().
get_msg_tag({Msg, _Cookie})
  when is_tuple(Msg) andalso (is_atom(erlang:element(1, Msg)) orelse is_integer(erlang:element(1, Msg))) ->
    get_msg_tag(Msg);
get_msg_tag({?send_to_group_member, _ProcessName, Msg})
  when is_tuple(Msg) andalso (is_atom(erlang:element(1, Msg)) orelse is_integer(erlang:element(1, Msg))) ->
    get_msg_tag(Msg);
get_msg_tag({?send_to_registered_proc, _ProcessName, Msg})
  when is_tuple(Msg) andalso (is_atom(erlang:element(1, Msg)) orelse is_integer(erlang:element(1, Msg))) ->
    get_msg_tag(Msg);
get_msg_tag(Msg)
  when is_tuple(Msg) andalso (is_atom(erlang:element(1, Msg)) orelse is_integer(erlang:element(1, Msg))) ->
    erlang:element(1, Msg).

-spec unpack_cookie(mypid(), message()) -> {mypid_plain(), message()};
                   (erl_local_pid(), message()) -> {erl_local_pid_plain(), message()}.
unpack_cookie({Pid, e, Nth, Envelope}, Msg) ->
    unpack_cookie(Pid, setelement(Nth, Envelope, Msg));
unpack_cookie(Pid, Msg) ->
    {Pid, Msg}.

-spec get_plain_pid(mypid()) -> mypid_plain();
                   (erl_local_pid()) -> erl_local_pid_plain().
get_plain_pid({Pid, e, _Nth, _Envelope}) ->
    get_plain_pid(Pid);
get_plain_pid(Pid) ->
    Pid.

%% @doc Creates a group member message and filter out the send options for the
%%      comm_server process.
-spec pack_group_member(message() | group_message(), send_options()) -> {message() | group_message(), send_options()}.
pack_group_member(Msg, [] = Opts)                      -> {Msg, Opts};
pack_group_member(Msg, [{shepherd, _Shepherd}] = Opts) -> {Msg, Opts};
pack_group_member(Msg, Opts)                           ->
    case lists:keytake(group_member, 1, Opts) of
        false ->
            case lists:keytake(registered_proc, 1, Opts) of
                false ->
                    {Msg, Opts};
                {value, {registered_proc, Process}, Opts2} ->
                    {{?send_to_registered_proc, Process, Msg}, Opts2}
            end;
        {value, {group_member, Process}, Opts2} ->
            {{?send_to_group_member, Process, Msg}, Opts2}
    end.

%% @doc Forwards a message to a group member by its process name.
%%      NOTE: Does _not_ warn if the group member does not exist (should only
%%            happen during node leave operations).
-spec forward_to_group_member(atom(), message()) -> ok.
forward_to_group_member(Processname, Msg) ->
    case pid_groups:get_my(Processname) of
        failed -> ok;
        Pid    -> comm:send_local(Pid, Msg)
    end.

%% @doc Forwards a message to a registered process.
%%      NOTE: Does _not_ warn if the process does not exist (should only
%%            happen during node leave operations).
-spec forward_to_registered_proc(atom(), message()) -> ok.
forward_to_registered_proc(Processname, Msg) ->
    case whereis(Processname) of
        undefined            -> ok;
        Pid when is_pid(Pid) -> comm:send_local(Pid, Msg)
    end.

%% @doc Initializes the comm layer by sending a message to
%%      known_hosts. A valid IP (and comm:mypid()) for comm:this/0
%%      will be available afterwards.  (ugly hack to get a valid
%%      ip-address into the comm-layer)
-spec init_and_wait_for_valid_IP() -> ok.
init_and_wait_for_valid_IP() ->
    case is_valid(this()) of
        true -> ok;
        false ->
            KnownHosts1 = config:read(known_hosts),
            % maybe the list of known nodes is empty and we have a mgmt_server?
            MgmtServer = config:read(mgmt_server),
            KnownHosts = case is_valid(MgmtServer) of
                             true -> [MgmtServer | KnownHosts1];
                             _ -> KnownHosts1
                         end,
            % note, comm:this() may be invalid at this moment
            _ = [send(KnownHost, {comm_says_hi}, [{group_member, service_per_vm}, {?quiet}])
                   || KnownHost <- KnownHosts],
            timer:sleep(100),
            init_and_wait_for_valid_IP()
    end.
