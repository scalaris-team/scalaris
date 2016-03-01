% @copyright 2012-2016 Zuse Institute Berlin

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
-vsn('$Id$').

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
%% Optional Startup for reduced memory consumption:
%%   1.1 Use message map fun to normalize or shrink messages
%%       call: trace_mpath:start(your_trace_id, [{map_fun, your_map_fun()}])
%%   1.2 Use filter fun to log only messages your are interested in
%%       call: trace_mpath:start(your_trace_id, [{filter_fun, your_filter_fun()}])
%%       attention: filter_fun operates on mapped messages if map_fun() is used
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-include("scalaris.hrl").
-behaviour(gen_component).

%% client functions
-export([start/0, start/1, start/2, stop/0]).
-export([infected/0]).
-export([clear_infection/0, restore_infection/0]).
-export([get_trace/0, get_trace/1, get_trace/2,
         get_trace_raw/1, get_trace_raw/2,
         cleanup/0, cleanup/1]).
-export([thread_yield/0]).

%% trace analysis
-export([send_histogram/1]).
-export([time_delta/1]).
-export([to_texfile/2, to_texfile/3,
         to_texfile_no_time/2, to_texfile_no_time/3]).

%% report tracing events from other modules
-export([log_send/6]).
-export([log_info/2, log_info/3]).
-export([log_recv/4]).
-export([epidemic_reply_msg/4]).
-export([get_msg_tag/1]).

%% useful in e.g. filter funs
-export([normalize_pidinfo/1]).

%% gen_component behaviour
-export([start_link/1, init/1]).
-export([on/2]). %% internal message handler as gen_component

-type time()         :: erlang_timestamp() | non_neg_integer().
-type logger()       :: io_format                       %% | ctpal
                      | {log_collector, comm:mypid()}
                      | {proto_sched, comm:mypid()}.
-type pidinfo()      :: {comm:mypid(),
                         {pid_groups:groupname(), pid_groups:pidname()} |
                             no_pid_name |
                             non_local_pid_name_unknown}.
-type anypid()       :: pid() | comm:mypid().
-type trace_id()     :: atom().
-type send_event0()  :: {log_send, time(), trace_id(), %internal
                         Source::anypid(), Dest::anypid(), comm:message(),
                         local | global}.
-type send_event()   :: {log_send, time(), trace_id(),
                         Source::pidinfo(), Dest::pidinfo(), comm:message(),
                         local | global}.
-type info_event0()  :: {log_info, time(), trace_id(), %internal
                         anypid(), comm:message()}.
-type info_event()   :: {log_info, time(), trace_id(),
                         pidinfo(), comm:message()}.
-type recv_event0()  :: {log_recv, time(), trace_id(), %internal
                         Source::anypid(), Dest::anypid(), comm:message()}.
-type recv_event()   :: {log_recv, time(), trace_id(),
                         Source::pidinfo(), Dest::pidinfo(), comm:message()}.
-type trace_event0() :: send_event0() | info_event0() | recv_event0(). %internal
-type trace_event()  :: send_event() | info_event() | recv_event().
-type trace()        :: [trace_event()].
-type msg_map_fun()  :: fun((comm:message(), Source::pid() | comm:mypid(),
                             Dest::pid() | comm:mypid()) -> comm:message()).
-type filter_fun()   :: fun((trace_event()) -> boolean()).
-type passed_state1():: {trace_id(), logger(), msg_map_fun(), filter_fun()}.
-type passed_state() :: passed_state1()
                        | {trace_id(), logger()}.
-type gc_mpath_msg() :: {'$gen_component', trace_mpath, passed_state(),
                         Source::anypid(), Dest::anypid(), comm:message()}.
-type options()      :: [{logger, logger() | comm:mypid()} |
                         {map_fun, msg_map_fun()} |
                         {filter_fun, filter_fun()}].

-export_type([trace/0, trace_event/0]).
-export_type([pidinfo/0, logger/0, passed_state/0]).

-include("gen_component.hrl").

-spec start() -> ok.
start() -> start(default).

-spec start(trace_id() | passed_state()) -> ok.
start(TraceId) when is_atom(TraceId) ->
    start(TraceId, []);
start({TraceId, Logger} = _PState) ->
    proto_sched:start(TraceId, Logger);
start({TraceId, Logger, MsgMapFun, FilterFun} = _PState) ->
    start(TraceId, Logger, MsgMapFun, FilterFun).

-spec start(trace_id(), logger() | comm:mypid() | options()) -> ok.
start(TraceId, Options) when is_list(Options) ->
    InLogger = proplists:get_value(logger, Options, nil),
    Logger = if InLogger =:= nil ->
                    LoggerPid = pid_groups:find_a(?MODULE),
                    comm:make_global(LoggerPid);
                true -> InLogger
             end,
    MsgFun = proplists:get_value(map_fun, Options, fun(Msg, _Src, _Dest) -> Msg end),
    FilterFun = proplists:get_value(filter_fun, Options, fun(_) -> true end),
    start(TraceId, Logger, MsgFun, FilterFun);
start(TraceId, Logger) ->
    start(TraceId, [{logger, Logger}]).

-spec start(trace_id(), logger() | comm:mypid(), msg_map_fun(), filter_fun()) -> ok.
start(TraceId, InLogger, MsgMapFun, FilterFun) ->
    Logger = case comm:is_valid(InLogger) of
                 true -> {log_collector, InLogger}; %% just a pid was given
                 false -> InLogger
             end,
    PState = passed_state_new(TraceId, Logger, MsgMapFun, FilterFun),
    own_passed_state_put(PState).

-spec stop() -> ok.
stop() ->
    % assert infection when stop/0 is called
    ?DBG_ASSERT(erlang:get(trace_mpath) =/= undefined),
    %% stop sending epidemic messages
    erlang:erase(trace_mpath),
    ok.

-spec infected() -> boolean().
infected() ->
    case erlang:get(trace_mpath) of
        {_TraceId, _Logger} -> true;
        {_TraceId, _Logger, _MsgMapFun, _FilterFun} -> true;
        _                   -> false
    end.

-spec clear_infection() -> ok.
clear_infection() ->
    case erlang:erase(trace_mpath) of
        undefined ->
            %% remove _bak entry as otherwise an old _bak may be
            %% restored in restore_infection
            erlang:erase(trace_mpath_bak);
        PState    ->
            erlang:put(trace_mpath_bak, PState)
    end,
    ok.

-spec restore_infection() -> ok.
restore_infection() ->
    case erlang:get(trace_mpath_bak) of
        undefined -> ok;
        PState    -> erlang:put(trace_mpath, PState),
                     ok
    end.

-spec get_trace() -> trace().
get_trace() -> get_trace(default).

-spec get_trace(trace_id()) -> trace().
get_trace(TraceId) ->
    get_trace(TraceId, none).

-spec get_trace(trace_id(), Option::cleanup | none) -> trace().
get_trace(TraceId, Option) ->
    LogRaw = get_trace_raw(TraceId, Option),
    Trace =
        [case Event of
             {log_send, Time, TraceId, Source, Dest, {Tag, Key, Hops, Msg}, LorG}
               when Tag =:= ?lookup_aux orelse Tag =:= ?lookup_fin ->
                 {log_send, Time, TraceId,
                  normalize_pidinfo(Source),
                  normalize_pidinfo(Dest),
                  convert_msg({Tag, Key, Hops, convert_msg(Msg)}), LorG};
             {log_send, Time, TraceId, Source, Dest, Msg, LorG} ->
                 {log_send, Time, TraceId,
                  normalize_pidinfo(Source),
                  normalize_pidinfo(Dest), convert_msg(Msg), LorG};
             {log_recv, Time, TraceId, Source, Dest, {Tag, Key, Hops, Msg}}
               when Tag =:= ?lookup_aux orelse Tag =:= ?lookup_fin ->
                 {log_recv, Time, TraceId,
                  normalize_pidinfo(Source),
                  normalize_pidinfo(Dest),
                  convert_msg({Tag, Key, Hops, convert_msg(Msg)})};
             {log_recv, Time, TraceId, Source, Dest, Msg} ->
                 {log_recv, Time, TraceId,
                  normalize_pidinfo(Source),
                  normalize_pidinfo(Dest), convert_msg(Msg)};
             {log_info, Time, TraceId, Pid, Msg} ->
                 case Msg of
                     {gc_on_done, Tag} ->
                         {log_info, Time, TraceId,
                          normalize_pidinfo(Pid),
                          {gc_on_done, util:extint2atom(Tag)}};
                     _ ->
                         {log_info, Time, TraceId,
                          normalize_pidinfo(Pid), convert_msg(Msg)}
                 end
         end || Event <- LogRaw],
    resolve_remote_pids(Trace).

-spec resolve_remote_pids(Trace::trace()) -> trace().
resolve_remote_pids(Trace) ->
    % remote pids are present in two forms (with each of them present!):
    % (a) fully resolved pidinfo() and (b) non-resolved pidinfo()
    % -> find (a) and replace (b):
    DictResolved =
        dict:from_list(
          lists:flatmap(
            fun(Event) ->
                    Pid1 = element(4, Event),
                    Pid2 = ?IIF(element(1, Event) =:= log_info, unknown, element(5, Event)),
                    % known pids
                    [{PidX, X} || X = {PidX, InfoX} <- [Pid1, Pid2], is_tuple(InfoX)]
            end, Trace)),
    [begin
         Pid1 = element(4, Event),
         Pid2 = ?IIF(element(1, Event) =:= log_info, unknown, element(5, Event)),
         lists:foldl(fun({I, PidX}, EventX) ->
                             case dict:find(PidX, DictResolved) of
                                 error -> EventX;
                                 {ok, Res} -> setelement(I, EventX, Res)
                             end
                     end, Event,
                     % unknown pids:
                     [{I, PidX} || {I, {PidX, InfoX}} <- [{4, Pid1}, {5, Pid2}],
                                   is_atom(InfoX)])
     end || Event <- Trace].

-spec convert_msg(Msg::comm:message()) -> comm:message().
convert_msg(Msg) when is_tuple(Msg)
                      andalso tuple_size(Msg) =:= 3
                      andalso f =:= element(2, Msg) ->
    %% lookup envelope
    setelement(1, Msg, util:extint2atom(element(1,element(3, Msg))));
convert_msg(Msg) when is_tuple(Msg) andalso tuple_size(Msg) >= 1 ->
    setelement(1, Msg, util:extint2atom(element(1, Msg)));
convert_msg(Msg) -> Msg.

-spec get_trace_raw(trace_id()) -> trace().
get_trace_raw(TraceId) ->
    get_trace_raw(TraceId, none).

-spec get_trace_raw(trace_id(), Option::cleanup | none) -> trace().
get_trace_raw(TraceId, Option) ->
    LoggerPid = pid_groups:find_a(trace_mpath),
    comm:send_local(LoggerPid, {get_trace, comm:this(), TraceId, Option}),
    trace_mpath:thread_yield(),
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

%% for proto_sched and to mark end of client processing before
%% receives
-spec thread_yield() -> ok.
thread_yield() ->
    %% report done for proto_sched to go on...
    log_info(self(), {gc_on_done, scalaris_recv}).
    %% clear_infection().
    %% we need to remain infected for receive after N clauses.


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

-spec time_delta(trace()) -> trace().
time_delta(Trace) ->
    SortedTrace = lists:keysort(2, Trace),
    StartTime = element(2, hd(SortedTrace)),
    [ setelement(2, X, timer:now_diff(element(2, X), StartTime))
      || X <- SortedTrace].

-spec notime_delta(trace()) -> trace().
notime_delta(Trace) ->
    SortedTrace = lists:keysort(2, Trace),
    util:map_with_nr(fun(X, I) -> setelement(2, X, I*10) end, SortedTrace, 0).

%% sample call sequence to get a tx trace:
%% tex traces have to be relatively short, so paste all at once to the
%% erlang shell.
%% api_tx:write("b", 1). trace_mpath:start(). api_tx:write("a", 1). trace_mpath:stop(). timer:sleep(10). T = trace_mpath:get_trace(). trace_mpath:to_texfile(T, "trace.tex").
%%  P = pid_groups:find_a(trace_mpath). gen_component:bp_set_cond(P, fun(_,_) -> true end, mybp). api_tx:write("b", 1). trace_mpath:start(). api_tx:write("a", 1). trace_mpath:stop(). timer:sleep(1). gen_component:bp_del(P, mybp). gen_component:bp_cont(P). timer:sleep(10).  T = trace_mpath:get_trace(). trace_mpath:to_texfile(T, "trace.tex").
%% P = pid_groups:find_a(trace_mpath). gen_component:bp_set_cond(P, fun(_,_)-> true end, mybp). rbrcseq:qread(self(), "b"). receive _ -> ok end. trace_mpath:start(). rbrcseq:qread(self(), "a"). receive _ -> ok end. trace_mpath:stop(). gen_component:bp_del(P, mybp). gen_component:bp_cont(P). T = trace_mpath:get_trace(). trace_mpath:to_texfile(T, "trace.tex").

%% @doc Write the trace to a LaTeX file (20 microseconds in the trace are 1 cm
%%      in the plot).
-spec to_texfile(trace(), file:name())
        -> ok | {error, file:posix() | badarg | terminated}.
to_texfile(Trace, Filename) ->
    to_texfile(Trace, Filename, 20).

%% @doc Write the trace to a LaTeX file (ScaleX microseconds in the trace are 1
%%      cm in the plot).
-spec to_texfile(trace(), file:name(), ScaleX::pos_integer())
        -> ok | {error, file:posix() | badarg | terminated}.
to_texfile(Trace, Filename, ScaleX) ->
    to_texfile(Trace, Filename, fun time_delta/1, true, ScaleX).

%% @doc Write the trace to a LaTeX file (20 microseconds in the trace are 1 cm
%%      in the plot). The representation will not have a time representation
%%      but rather only show successive messages.
-spec to_texfile_no_time(trace(), file:name())
        -> ok | {error, file:posix() | badarg | terminated}.
to_texfile_no_time(Trace, Filename) ->
    to_texfile_no_time(Trace, Filename, 20).

%% @doc Write the trace to a LaTeX file (ScaleX microseconds in the trace are 1
%%      cm in the plot). The representation will not have a time representation
%%      but rather only show successive messages.
-spec to_texfile_no_time(trace(), file:name(), ScaleX::pos_integer())
        -> ok | {error, file:posix() | badarg | terminated}.
to_texfile_no_time(Trace, Filename, ScaleX) ->
    %% we do not need the gc_on_done messages
    F = fun(X) ->
                case X of
                    {log_info, _TimeStamp, _TraceId, _From, {gc_on_done, _MsgTag}} -> false;
                    _ -> true
                end
        end,
    FilteredTrace = [X || X <- Trace, F(X)],
    to_texfile(FilteredTrace, Filename, fun notime_delta/1, false, ScaleX).

%% @doc Write the trace to a LaTeX file (ScaleX microseconds in the trace are
%%      1 cm in the plot).
-spec to_texfile(trace(), file:name(), DeltaFun::fun((trace()) -> trace()),
                 HaveRealTime::boolean(), ScaleX::pos_integer())
        -> ok | {error, file:posix() | badarg | terminated}.
to_texfile(Trace, Filename, DeltaFun, HaveRealTime, ScaleX0) ->
    {ok, File} = file:open(Filename, [write]),
    io:format(File,
      "\\documentclass[10pt]{article}~n"
      "\\usepackage[paperwidth=\\maxdimen,paperheight=\\maxdimen]{geometry}~n"
      "\\usepackage{tikz}~n"
      "\\usetikzlibrary{arrows}~n"
      "\\usepackage[T1]{fontenc}~n"
      "\\usepackage{lmodern}~n"
      "\\usepackage[tightpage,active]{preview}~n"
      "\\PreviewEnvironment{tikzpicture}~n"
      "\\begin{document}~n"
      "\\makeatletter~n"
      "\\renewcommand{\\tiny}{\\@setfontsize\\miniscule{4}{5}}~n"
      "\\makeatother~n"
      "\\pagestyle{empty}\\sf\\scriptsize"
      "\\begin{tikzpicture}~n",
              []),
    %% create nodes for processes:
    %% all processes in that order, but only once

    NodesR = lists:foldl(
               fun(X, Acc) ->
                       Pid = case element(1, X) of
                                 log_send -> element(4, X);
                                 log_recv -> element(5, X);
                                 log_info -> element(4, X)
                             end,
                       case lists:member(Pid, Acc) of
                           true -> Acc;
                           false -> [ Pid | Acc ]
                       end
               end,
               [], Trace),
    Nodes = lists:reverse(NodesR),
    DrawTrace = DeltaFun(Trace),

    EndTime = element(2, lists:last(DrawTrace)),

    ScaleX =
        if (EndTime div ScaleX0) > 565 ->
               NewScale = util:ceil(EndTime / 5650) * 10,
               io:format("Warning: adapting scale to 1cm : ~Bus to fit "
                         "the maximum LaTeX width of 565cm.~n", [NewScale]),
               NewScale;
           true ->
               ScaleX0
        end,
    TicsFreq = ScaleX, %% draw x-tics every TicsFreq microseconds

    %% draw nodes and timelines
    _ = lists:foldl(
          fun(X, Acc) ->
                  LatexNode = term_to_latex_string(X),
                  io:format(File,
                            "\\draw (0, -~f) node[anchor=east] {~s};~n",
                            [length(Acc)/2, LatexNode]),
                  io:format(File,
                            "\\draw[color=gray,very thin] (0, -~f) -- (~fcm, -~f);~n",
                            [length(Acc)/2, (EndTime+10)/ScaleX, length(Acc)/2]),
                  case EndTime > 600 of
                      true ->
                          io:format(File,
                                    "\\foreach \\x in {~B,~B,...,~B}~n"
                                    "  \\node[anchor=south east,gray,inner sep=0pt] at (\\x, -~f) {\\tiny ~s};~n",
                                    [15, 30,
                                     ((trunc((EndTime+10)) div 15) * 15) div ScaleX,
                                     length(Acc)/2, LatexNode]);
                      false -> ok
                  end,
                  [X | Acc]
          end,
          [], Nodes),
    %% draw key
    EndSlot = (EndTime div TicsFreq),
    util:for_to(
      1, EndSlot,
      fun(I) ->
              io:format(File,
              "  \\draw[color=gray, very thin] (~fcm, 0.5)"
              "  node[above] {~B" ++ ?IIF(HaveRealTime, "$\\mu$s", "") ++ "} --"
              " (~f, -~f);~n",
              [I*TicsFreq/ScaleX,
               I*TicsFreq,
               I*TicsFreq/ScaleX, length(Nodes)/2])
      end),

    io:format(File,
              "  \\draw[color=green!30!black,->] (-4cm, 0.5)"
              "  -- (-3.5cm, 0.5) node[anchor=west] {local send};~n"
              "  \\draw[color=red!50!black,->] (-2cm, 0.5)"
              "  -- (-1.5cm, 0.5) node[anchor=west] {global send};~n", []),

    draw_messages(File, Nodes, ScaleX, HaveRealTime, DrawTrace),

    io:format(
      File,
      "\\end{tikzpicture}~n"
      "\\end{document}~n",
      []),
    file:close(File).

term_to_latex_string(Term) ->
    quote_latex(lists:flatten(io_lib:format("~0.0p", [Term]))).

quote_latex([]) -> [];
quote_latex([Char | Tail]) ->
    NewChars =
        case Char of
            $_ ->  "\\_";
            ${ ->  "\\{";
            $} ->  "\\}";
%%            $[ ->  "\\[";
%%            $] ->  "\\]";
            $# ->  "\\#";
            %% $< ->  lists:reverse("$\lt$");
            %% $> ->  lists:reverse("$\gt$");
            _ -> [Char]
        end,
    NewChars ++ quote_latex(Tail).

%% @doc Gets the message tag of the given message. Special dht_node messages of
%%      embedded processes get translated into a tuple of two message tags.
-spec get_msg_tag(Msg::comm:message()) -> atom() | {atom(), atom()}.
get_msg_tag(Msg) when tuple_size(Msg) =< 1 ->
    element(1, Msg);
get_msg_tag(Msg) ->
    case element(1, Msg) of
        TagX when (TagX =:= join orelse TagX =:= move orelse
                           TagX =:= l_on_cseq orelse TagX =:= rm) ->
            Element2 = element(2, Msg),
            case is_tuple(Element2) andalso tuple_size(Element2) >= 1
                andalso is_atom(element(1, Element2)) of
                true  ->
                    {TagX, element(1, Element2)};
                false when is_atom(Element2) ->
                    {TagX, Element2};
                false ->
                    TagX
            end;
        ?send_to_group_member_atom ->
            {?send_to_group_member_atom, _PidName, EmbeddedMsg} = Msg,
            {send_to_group_member, get_msg_tag(EmbeddedMsg)};
        ?send_to_registered_proc_atom ->
            {?send_to_registered_proc_atom, _PidName, EmbeddedMsg} = Msg,
            {send_to_registered_proc, get_msg_tag(EmbeddedMsg)};
        TagX when TagX =:= ?lookup_aux_atom orelse
                      TagX =:= ?lookup_fin_atom ->
            {TagX, _Key, _Hops, WrappedMsg} = Msg,
            {TagX, get_msg_tag(WrappedMsg)};
        TagX ->
            TagX
    end.

draw_messages(_File, _Nodes, _ScaleX, _HaveRealTime, []) -> ok;
draw_messages(File, Nodes, ScaleX, HaveRealTime, [X | DrawTrace]) ->
    RemainingTrace =
    case element(1, X) of
        log_send ->
            %% search for corresponding receive and draw a line
            SrcNum = length(lists:takewhile(
                              fun(Y) -> element(4, X) =/= Y end, Nodes)),
            DestNum = length(lists:takewhile(
                               fun(Y) -> element(5, X) =/= Y end, Nodes)),
            Recv = [ Y || Y <- DrawTrace,
                          log_recv =:= element(1, Y),
                          element(4, X) =:= element(4, Y),
                          element(5, X) =:= element(5, Y),
                          %% element(6, X) =:= element(6, Y), %% we have fifo
                          element(2, X) =< element(2, Y)
                   ],
            SendTime = element(2, X),
            SendMsg = element(6, X),
            RecvEvent =
                case Recv of
                    [] -> setelement(2, X, SendTime + 10);
                    _ -> hd(Recv)
                end,
            RecvTime = element(2, RecvEvent),
            SendTag = term_to_latex_string(get_msg_tag(SendMsg)),
            RecvTag = term_to_latex_string(get_msg_tag(element(6, RecvEvent))),
            Color = case element(7, X) of
                        local -> "green!30!black";
                        global -> "red!50!black"
                    end,
            MsgSizeBytes = erlang:byte_size(erlang:term_to_binary(SendMsg, [{minor_version, 1}])),
            MsgSize =
                case math:log(MsgSizeBytes)/math:log(2) of
                    L when L < 10.0 ->
                        term_to_latex_string(MsgSizeBytes) ++ "\\,B";
                    L when L < 20.0 ->
                        term_to_latex_string(MsgSizeBytes div 1024) ++ "\\,KB";
                    L when L < 30.0 ->
                        term_to_latex_string(
                          MsgSizeBytes div 1024 div 1024) ++ "\\,MB";
                    L when L < 40.0 ->
                        term_to_latex_string(
                          MsgSizeBytes div 1024 div 1024 div 1024) ++ "\\,GB"
                end,

            case SrcNum of
                SrcNum when (SrcNum < DestNum) ->
                    MsgTag =
                        case SendTag =:= RecvTag of
                            true -> SendTag;
                            false -> SendTag ++ "\\\\[-0.5em]\\tiny " ++ RecvTag
                        end,
                    io:format(File,
                              "\\draw[->, color=~s] (~fcm, -~f)"
                              " to node[inner sep=1pt, anchor=west,sloped,rotate=90,align=left] "
                              "{\\tiny ~s} (~fcm, -~f) "
                              "node [anchor=north, inner sep=1pt] {\\tiny ~s};~n",
                              [Color, SendTime/ScaleX, SrcNum/2,
                               MsgTag,
                               RecvTime/ScaleX, DestNum/2,
                               MsgSize]);
                SrcNum when (SrcNum > DestNum) ->
                    MsgTag =
                        case SendTag =:= RecvTag of
                            true -> SendTag;
                            false -> RecvTag ++ "\\\\[-0.5em]\\tiny " ++ SendTag
                        end,
                    io:format(File,
                              "\\draw[->, color=~s] (~fcm, -~f)"
                              " to node[inner sep=1pt, anchor=west,sloped,rotate=-90, align=left] "
                              "{\\tiny ~s} (~fcm, -~f) "
                              "node [anchor=west, inner sep=1pt, rotate=60] {\\tiny ~s};~n",
                              [Color, SendTime/ScaleX, SrcNum/2,
                               MsgTag,
                               RecvTime/ScaleX, DestNum/2,
                               MsgSize]);
                SrcNum when (SrcNum =:= DestNum) ->
                    MsgTag =
                        case SendTag =:= RecvTag of
                            true -> SendTag;
                            false -> RecvTag ++ "\\\\[-0.5em]\\tiny " ++ SendTag
                        end,
                    io:format(File,
                              "\\draw[->, color=~s] (~fcm, -~f)"
                              " .. controls +(~fcm,-0.3) .."
                              " node[inner sep=1pt,anchor=west,sloped,rotate=-90, align=left]"
                              "{\\tiny ~s} (~fcm, -~f) "
                              "node [anchor=west, inner sep=1pt, rotate=60] {\\tiny ~s};~n",
                              [Color, element(2, X)/ScaleX, SrcNum/2,
                               (RecvTime - element(2, X))/ScaleX/2,
                               MsgTag,
                               RecvTime/ScaleX, DestNum/2,
                               MsgSize])
            end,
            NewDrawTrace =
                case Recv of
                    [] -> DrawTrace;
                    _ -> lists:delete(hd(Recv), DrawTrace)
                end,
            case Recv of
                [] -> DrawTrace;
                _ ->
                    %% draw process busy until gc_on_done log_info event
                    DoneEvents = [ Y || Y <- NewDrawTrace,
                                        log_info =:= element(1, Y),
                                        element(2, Y) >= RecvTime,
                                        element(4, Y) =:= element(5, hd(Recv)),
                                        element(1, element(5, Y)) =:= gc_on_done
                                ],
                    case DoneEvents of
                        [] -> NewDrawTrace;
                        _ ->
                            DoneTime = element(2, hd(DoneEvents)),
                            if HaveRealTime ->
                                   io:format(File,
                                             "\\draw[semithick] (~fcm, -~f)"
                                                 " -- "
                                                 " (~fcm, -~f) node[inner sep=1pt, anchor=south] {\\tiny ~B};~n",
                                             [RecvTime/ScaleX, DestNum/2,
                                              DoneTime/ScaleX, DestNum/2,
                                              DoneTime - RecvTime]);
                               true -> ok
                            end,
                            lists:delete(hd(DoneEvents), NewDrawTrace)
                    end
            end;
        log_recv ->
            %% found a receive without a send?
            %% not yet implemented
            io:format("Found receive without send?~n"),
          _ = element(5, X),
            DrawTrace;
        log_info ->
            %% print info somewhere
            SrcNum = length(lists:takewhile(
                              fun(Y) -> element(4, X) =/= Y end, Nodes)),
            EventTime = element(2, X),
            io:format(
              File, "\\draw [color=blue] (~fcm, -~f) ++(0, 0.1cm) node[rotate=60, anchor=west, inner sep=1pt] {\\tiny ~s}-- ++(0, -0.2cm);~n",
              [EventTime/ScaleX, SrcNum/2, term_to_latex_string(element(5, X))]),
            DrawTrace
    end,
    draw_messages(File, Nodes, ScaleX, HaveRealTime, RemainingTrace).

%% Functions used to report tracing events from other modules
-spec epidemic_reply_msg(passed_state(), anypid(), anypid(), comm:message()) ->
                                gc_mpath_msg().
epidemic_reply_msg(PState, FromPid, ToPid, Msg) ->
    {'$gen_component', trace_mpath, PState, comm:make_global(FromPid), ToPid, Msg}.

-spec log_send(passed_state(), anypid(), anypid(), comm:message(),
               local | global | local_after, comm:send_options())
        -> DeliverAlsoDirectly :: boolean().
log_send(PState, FromPid, ToPid, Msg, LocalOrGlobal, SendOptions) ->
    Now = os:timestamp(),
    case passed_state_logger(PState) of
        io_format ->
            MsgMapFun = passed_state_msg_map_fun(PState),
            io:format("~p send ~.0p -> ~.0p:~n  ~.0p.~n",
                      [util:readable_utc_time(Now),
                       normalize_pidinfo(FromPid),
                       normalize_pidinfo(ToPid), MsgMapFun(Msg, FromPid, ToPid)]),
            true;
        {log_collector, LoggerPid} ->
            MsgMapFun = passed_state_msg_map_fun(PState),
            TraceId = passed_state_trace_id(PState),
            case comm:is_local(LoggerPid) of
                true ->
                    % normalise names later...
                    FromPid1 = FromPid, ToPid1 = ToPid, ok;
                false ->
                    % normalise names now (non-local loggers cannot resolve them)
                    FromPid1 = normalize_pidinfo(FromPid),
                    ToPid1 = normalize_pidinfo(ToPid),
                    ok
            end,
            send_log_msg(
              PState,
              LoggerPid,
              {log_send, Now, TraceId, FromPid1, ToPid1, MsgMapFun(Msg, FromPid, ToPid),
               ?IIF(LocalOrGlobal =:= local_after, local, LocalOrGlobal)}),
            true;
        {proto_sched, _} when LocalOrGlobal =:= local_after ->
            %% Do delivery via proto_sched and *not* via
            %% erlang:send_after (returning false here). Otherwise the
            %% msg gets delivered a second time shortly after outside
            %% the schedule.
            %% TODO: see comm:send_local_after
            %% TODO: Should be queued to another queue in the proto_sched to
            %% allow violation of FIFO ordering
            proto_sched:log_send(PState, FromPid, ToPid, Msg, LocalOrGlobal,
                                 SendOptions),
            false;
        {proto_sched, _} ->
            proto_sched:log_send(PState, FromPid, ToPid, Msg, LocalOrGlobal,
                                 SendOptions),
            false
    end.

-spec log_info(anypid(), comm:message()) -> ok.
log_info(FromPid, Info) ->
    case own_passed_state_get() of
        undefined -> ok;
        PState -> log_info(PState, FromPid, Info)
    end.
-spec log_info(passed_state(), anypid(), comm:message()) -> ok.
log_info(PState, FromPid, Info) ->
    Now = os:timestamp(),
    case passed_state_logger(PState) of
        io_format ->
            io:format("~p info ~.0p:~n  ~.0p.~n",
                      [util:readable_utc_time(Now),
                       normalize_pidinfo(FromPid),
                       Info]);
        {log_collector, LoggerPid} ->
            TraceId = passed_state_trace_id(PState),
            case comm:is_local(LoggerPid) of
                true ->
                    % normalise names later...
                    FromPid1 = FromPid, ok;
                false ->
                    % normalise names now (non-local loggers cannot resolve them)
                    FromPid1 = normalize_pidinfo(FromPid),
                    ok
            end,
            send_log_msg(PState, LoggerPid, {log_info, Now, TraceId, FromPid1, Info});
        {proto_sched, LoggerPid} ->
            case Info of
                {gc_on_done, Tag} ->
                    TraceId = passed_state_trace_id(PState),
                    send_log_msg(PState, LoggerPid,
                                 {on_handler_done, TraceId, Tag,
                                  element(1, normalize_pidinfo(FromPid))});
                _ ->
                    proto_sched:log_info(PState, FromPid, Info)
            end
    end,
    ok.

-spec log_recv(passed_state(), anypid(), anypid(), comm:message()) -> ok.
log_recv(PState, FromPid, ToPid, Msg) ->
    Now = os:timestamp(),
    case passed_state_logger(PState) of
        io_format ->
            MsgMapFun = passed_state_msg_map_fun(PState),
            io:format("~p recv ~.0p -> ~.0p:~n  ~.0p.~n",
                      [util:readable_utc_time(Now),
                       normalize_pidinfo(FromPid),
                       normalize_pidinfo(ToPid),
                       MsgMapFun(Msg, FromPid, ToPid)]);
        {log_collector, LoggerPid} ->
            MsgMapFun = passed_state_msg_map_fun(PState),
            TraceId = passed_state_trace_id(PState),
            case comm:is_local(LoggerPid) of
                true ->
                    % normalise names later...
                    FromPid1 = FromPid, ToPid1 = ToPid, ok;
                false ->
                    % normalise names now (non-local loggers cannot resolve them)
                    FromPid1 = normalize_pidinfo(FromPid),
                    ToPid1 = normalize_pidinfo(ToPid),
                    ok
            end,
            send_log_msg(
              PState,
              LoggerPid,
              {log_recv, Now, TraceId, FromPid1, ToPid1, MsgMapFun(Msg, FromPid, ToPid)});
        {proto_sched, _} ->
            ok
    end,
    ok.

-spec send_log_msg(passed_state(), comm:mypid(),
                   trace_event0() |
                       {on_handler_done, trace_id(),
                        MsgTag::atom() | integer(), anypid()})
        -> ok.
send_log_msg(RestoreThis, LoggerPid, Msg) ->
    %% don't log the sending of log messages ...
    case passed_state_logger(RestoreThis) of
        {proto_sched, _} ->
            clear_infection(),
            comm:send(LoggerPid, Msg),
            own_passed_state_put(RestoreThis);
        _ ->
            FilterFun = passed_state_filter_fun(RestoreThis),
            case FilterFun(Msg) of
                true ->
                    clear_infection(),
                    comm:send(LoggerPid, Msg),
                    own_passed_state_put(RestoreThis);
                false -> ok
            end
    end.

-spec normalize_pidinfo(anypid() | pidinfo()) -> pidinfo().
normalize_pidinfo(Pid) ->
    case is_pid(Pid) of
        true ->
            ?ASSERT(node(Pid) =:= node()),
            PidName = try pid_groups:group_and_name_of(Pid) of
                          failed -> no_pid_name;
                          Name -> Name
                      catch _:_ -> no_pid_name
                      end,
            {comm:make_global(Pid), PidName};
        false ->
            case comm:is_valid(Pid) of
                true ->
                    case comm:is_local(Pid) of
                        true ->
                            PidName =
                                try pid_groups:group_and_name_of(
                                       comm:make_local(Pid)) of
                                    failed -> no_pid_name;
                                    Name -> Name
                                catch _:_ -> no_pid_name
                                end,
                            {Pid, PidName};
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
on({log_send, _Time, TraceId, _From, _To, _UMsg, _LorG} = Msg, State) ->
    state_add_log_event(State, TraceId, Msg);
on({log_recv, _Time, TraceId, _From, _To, _UMsg} = Msg, State) ->
    state_add_log_event(State, TraceId, Msg);
on({log_info, _Time, TraceId, _From, _UMsg} = Msg, State) ->
    state_add_log_event(State, TraceId, Msg);

on({get_trace, Pid, TraceId, cleanup}, State) ->
    case lists:keytake(TraceId, 1, State) of
        false ->
            comm:send(Pid, {get_trace_reply, no_trace_found}),
            State;
        {value, {TraceId, Msgs}, TupleList2} ->
            comm:send(Pid, {get_trace_reply, lists:reverse(Msgs)}),
            TupleList2
    end;
on({get_trace, Pid, TraceId, none}, State) ->
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

-spec passed_state_new(trace_id(), logger(), msg_map_fun(), filter_fun())
        -> passed_state1().
passed_state_new(TraceId, Logger, MsgMapFun, FilterFun) ->
    {TraceId, Logger, MsgMapFun, FilterFun}.

-spec passed_state_trace_id(passed_state()) -> trace_id().
passed_state_trace_id(State)      -> element(1, State).
-spec passed_state_logger(passed_state()) -> logger().
passed_state_logger(State)        -> element(2, State).
-spec passed_state_msg_map_fun(passed_state1()) -> msg_map_fun().
passed_state_msg_map_fun(State)   -> element(3, State).
-spec passed_state_filter_fun(passed_state1()) -> filter_fun().
passed_state_filter_fun(State)    -> element(4, State).

-spec own_passed_state_put(passed_state()) -> ok.
own_passed_state_put(State)       -> erlang:put(trace_mpath, State), ok.

-spec own_passed_state_get() -> passed_state() | undefined.
own_passed_state_get()            -> erlang:get(trace_mpath).

state_add_log_event(State, TraceId, Msg) ->
    NewEntry = case lists:keyfind(TraceId, 1, State) of
                   false ->
                       {TraceId, [Msg]};
                   {TraceId, OldTrace} ->
                       {TraceId, [Msg | OldTrace]}
               end,
    lists:keystore(TraceId, 1, State, NewEntry).
