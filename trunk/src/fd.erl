% @copyright 2007-2011 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%            2009 onScale solutions GmbH

%  Licensed under the Apache License, Version 2.0 (the "License");
%  you may not use this file except in compliance with the License.
%  You may obtain a copy of the License at
%
%      http://www.apache.org/licenses/LICENSE-2.0
%
%  Unless required by applicable law or agreed to in writing, software
%  distributed under the License is distributed on an "AS IS" BASIS,
%  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%  See the License for the specific language governing permissions and
%  limitations under the License.

%% @author Thorsten Schuett <schuett@zib.de>
%% @doc    Failure detector based on Guerraoui.
%% @end
%% @version $Id$
-module(fd).
-author('schuett@zib.de').
-vsn('$Id$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(_X,_Y), ok).
-behaviour(gen_component).
-include("scalaris.hrl").

-ifdef(with_export_type_support).
-export_type([cookie/0]).
-endif.

-export([subscribe/1, subscribe/2]).
-export([unsubscribe/1, unsubscribe/2]).
-export([update_subscriptions/2]).
%% gen_server & gen_component callbacks
-export([start_link/1, init/1, on/2]).

-type(cookie() :: '$fd_nil' | any()).
-type(state() :: ok).

%% @doc generates a failure detector for the calling process on the given pid.
-spec subscribe(comm:mypid() | [comm:mypid()]) -> ok.
subscribe(GlobalPids) -> subscribe(GlobalPids, '$fd_nil').

%% @doc generates a failure detector for the calling process and cookie on the
%%      given pid.
-spec subscribe(comm:mypid() | [comm:mypid()], cookie()) -> ok.
subscribe([], _Cookie)         -> ok;
subscribe(GlobalPids, Cookie) when is_list(GlobalPids) ->
    _ = [begin
             HBPid = get_hbs(Pid),
             comm:send_local(HBPid, {add_subscriber, self(), Pid, Cookie})
         end
         || Pid <- GlobalPids],
    ok;
subscribe(GlobalPid, Cookie) -> subscribe([GlobalPid], Cookie).

%% @doc deletes the failure detector for the given pid.
-spec unsubscribe(comm:mypid() | [comm:mypid()]) -> ok.
unsubscribe(GlobalPids)-> unsubscribe(GlobalPids, '$fd_nil').

%% @doc deletes the failure detector for the given pid and cookie.
-spec unsubscribe(comm:mypid() | [comm:mypid()], cookie()) -> ok.
unsubscribe([], _Cookie)         -> ok;
unsubscribe(GlobalPids, Cookie) when is_list(GlobalPids) ->
    _ = [begin
             HBPid = get_hbs(Pid),
             comm:send_local(HBPid, {del_subscriber, self(), Pid, Cookie})
         end
         || Pid <- GlobalPids],
    ok;
unsubscribe(GlobalPid, Cookie) -> unsubscribe([GlobalPid], Cookie).

%% @doc Unsubscribes from the pids in OldPids but not in NewPids and subscribes
%%      to the pids in NewPids but not in OldPids.
-spec update_subscriptions([comm:mypid()], [comm:mypid()]) -> ok.
update_subscriptions(OldPids, NewPids) ->
    {OnlyOldPids, _Same, OnlyNewPids} = util:split_unique(OldPids, NewPids),
    unsubscribe(OnlyOldPids),
    subscribe(OnlyNewPids).

%% gen_component functions
%% @doc Starts the failure detector server
-spec start_link(pid_groups:groupname()) -> {ok, pid()}.
start_link(ServiceGroup) ->
    gen_component:start_link(?MODULE, [],
      [wait_for_init, {erlang_register, ?MODULE},
       {pid_groups_join_as, ServiceGroup, ?MODULE}]).

%% @doc Initialises the module with an empty state.
-spec init([]) -> state().
init([]) ->
    % local heartbeat processes
    _ = ets:new(fd_hbs, [set, protected, named_table]),
    ok.

%% @private
-spec on(comm:message(), state()) -> state().
on({create_hbs, Pid, ReplyTo}, State) ->
    NewHBS = start_and_register_hbs(Pid),
    comm:send_local(ReplyTo, {create_hbs_reply, NewHBS}),
    State;

on({hbs_finished, RemoteWatchedPid}, State) ->
    ets:delete(fd_hbs, comm:get(fd, RemoteWatchedPid)),
    State;

on({subscribe_heartbeats, Subscriber, TargetPid}, State) ->
    ?TRACE("FD: subscribe_heartbeats~n", []),
    HBPid =
        case ets:lookup(fd_hbs, comm:get(fd, Subscriber)) of
            [] -> % synchronously create new hb process
                start_and_register_hbs(Subscriber);
            [Res] -> element(2, Res)
        end,
    comm:send_local(HBPid, {add_watching_of, TargetPid}),
    comm:send(Subscriber, {update_remote_hbs_to, comm:make_global(HBPid)}),
    State;

on({pong, Subscriber}, State) ->
    ?TRACE("FD: pong, ~p~n", [Subscriber]),
    forward_to_hbs(Subscriber, {pong_via_fd, Subscriber}),
    State;

on({update_remote_hbs_to, Pid}, State) ->
    ?TRACE("FD: update_remote_hbs_to p~n", [Pid]),
    forward_to_hbs(Pid, {update_remote_hbs_to, Pid}),
    State;

on({add_watching_of, Pid}, State) ->
    ?TRACE("FD: add_watching_of ~p~n", [Pid]),
    forward_to_hbs(Pid, {add_watching_of, Pid}),
    State;

on({del_watching_of, Pid}, State) ->
    ?TRACE("FD: del_watching_of ~p~n", [Pid]),
    forward_to_hbs(Pid, {del_watching_of, Pid}),
    State;

on({unittest_report_down, Pid}, State) ->
    ?TRACE("FD: unittest_report_down p~n", [Pid]),
    forward_to_hbs(
      Pid, {'DOWN', no_ref, process, comm:make_local(Pid), unittest_down}),
    State;

on({web_debug_info, _Requestor}, State) ->
    ?TRACE("FD: web_debug_info~n", []),
%% TODO: reimplement for new fd.
%%     Subscriptions = fd_db:get_subscriptions(),
%%     % resolve (local and remote) pids to names:
%%     S2 = [begin
%%               case comm:is_local(TargetPid) of
%%                   true -> {Subscriber,
%%                            {pid_groups:pid_to_name(comm:make_local(TargetPid)), Cookie}};
%%                   _ ->
%%                       comm:send(comm:get(pid_groups, TargetPid),
%%                                 {group_and_name_of, TargetPid, comm:this()}),
%%                       receive
%%                           {group_and_name_of_response, Name} ->
%%                               {Subscriber, {pid_groups:pid_to_name2(Name), Cookie}}
%%                       after 2000 -> X
%%                       end
%%               end
%%           end || X = {Subscriber, {TargetPid, Cookie}} <- Subscriptions],
%%     KeyValueList =
%%         [{"subscriptions", length(Subscriptions)},
%%          {"subscriptions (subscriber, {target, cookie}):", ""} |
%%          [{pid_groups:pid_to_name(Pid),
%%            lists:flatten(io_lib:format("~p", [X]))} || {Pid, X} <- S2]],
%%     comm:send_local(Requestor, {web_debug_info_reply, KeyValueList}),
    State.

%%% Internal functions
-spec my_fd_pid() -> pid() | failed.
my_fd_pid() ->
    case whereis(?MODULE) of
        undefined ->
            log:log(error, "[ FD ] call of my_fd_pid undefined"),
            failed;
        PID -> PID
    end.

-spec get_hbs(comm:mypid()) -> pid().
get_hbs(Pid) ->
    %% normalize for the table entry (just distinguish nodes)
    FDPid = comm:get(fd, Pid),
    case ets:lookup(fd_hbs, FDPid) of
        [] -> % synchronously create new hb process
            comm:send_local(my_fd_pid(), {create_hbs, Pid, self()}),
            receive {create_hbs_reply, NewHBS} -> NewHBS end;
        [Res] -> element(2, Res)
    end.

-spec start_and_register_hbs(comm:mypid()) -> pid().
start_and_register_hbs(Pid) ->
    FDPid = comm:get(fd, Pid),
    case ets:lookup(fd_hbs, FDPid) of
        [] ->
            NewHBS = element(2, fd_hbs:start_link(Pid)),
            ets:insert(fd_hbs, {comm:get(fd, Pid), NewHBS}),
            NewHBS;
        [Res] -> element(2, Res)
    end.

-spec forward_to_hbs(comm:mypid(), comm:message()) -> ok.
forward_to_hbs(Pid, Msg) ->
    case ets:lookup(fd_hbs, comm:get(fd,Pid)) of
        %% [] -> thats_crazy; %% let gen_component report it
        [Entry] ->
            HBSPid = element(2, Entry),
            comm:send_local(HBSPid, Msg)
    end, ok.
