%  @copyright 2008-2017 Zuse Institute Berlin

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
%% @doc    Globally included file (sets the modules being used in cases where
%%         multiple choices exist)
%% @end
%% @version $Id$

%% Version of Scalaris
-define(SCALARIS_VERSION, "0.9.0+git").

%% The RT macro is defined by the --with-rt configure parameter and determines
%% which kind of routingtable is used.
-include("rt.hrl").

% special parameters to be passed to ets:new/2 (not only used on db_ets!)
% note: keep access level at least at protected for dht_node_db_cache to work!
% note: named_table is not allowed any more and may result in exceptions due to non-unique table names
-define(DB_ETS_ADDITIONAL_OPS, [protected]).
%% userdevguide-end scalaris:db

%% when defined, use new tx protocol (which is not yet complete)
%% can be enabled with ./configure --enable-txnew
-ifdef(enable_txnew).
-define(TXNEW, true).
-endif.

%% userdevguide-begin scalaris:rm
%%Standard chord ring maintenance
%-define(RM, rm_chord).

%% ring maintenance by T-man
-define(RM, rm_tman).

%% ring maintenance by T-man-Sharp
%-define(RM, rm_tmansharp).
%% userdevguide-end scalaris:rm

% enable logging of message statistics at the granularity of socket messages
-define(LOG_MESSAGE_SOCK(SNDRCV, MESSAGE, SIZE, CHANNEL), ok).
%% -define(LOG_MESSAGE_SOCK(SNDRCV, BINARYMESSAGE, SIZE, CHANNEL), comm_logger:log(SNDRCV, {CHANNEL, socket_bin}, SIZE)).

% enable logging of message statistics at the granularity of erlang messages
-define(LOG_MESSAGE(SNDRCV, MESSAGE, CHANNEL), ok).
%% -define(LOG_MESSAGE(SNDRCV, MESSAGE, CHANNEL), comm_logger:log(SNDRCV, {CHANNEL, comm:get_msg_tag(MESSAGE)}, erlang:byte_size(erlang:term_to_binary(MESSAGE, [{minor_version, 1}])))).


% Back-end of the pdb module
-define(PDB_ERL, true). % erlang put and get (process dictionary)
%-define(PDB_ETS, true). % may have performance issues with msg_delay

%%%%%%%%%%%%%%%%%%%%%%

-define(implies(A, B), ((not (A)) orelse (B))).

% for debugging:
-ifdef(have_ctline_support).
% allows the retrieval of the current function and line number a process is in
% (process dictionary, key test_server_loc)
-compile({parse_transform, ct_line}).
-endif.

-define(ASSERT(X), true = X).
-define(ASSERT2(X, Y), case X of
                           true -> ok;
                           _ -> util:do_throw(Y)
                       end).
%% debug mode (enable with ./configure --enable-debug)
-ifdef(enable_debug).
-define(DBG_ASSERT(X), ?ASSERT(X)).
-define(DBG_ASSERT2(X, Y), ?ASSERT2(X, Y)).
% enable native register for all processes in gen_component
% useful for debug (etop, appmon), but lets memory usage grow over time
-define(DEBUG_REGISTER(PROCESS, PID), catch(erlang:register(PROCESS, PID))).
-else.
-define(DBG_ASSERT(X), ok).
-define(DBG_ASSERT2(X, Y), ok).
-define(DEBUG_REGISTER(PROCESS, PID), ok).
-endif.

% disable compression (the overhead is too high, at least for GbE)
-define(COMM_COMPRESS_MSG(DeliverMsg, State),
        term_to_binary(DeliverMsg, [{minor_version, 1}])
       ).
% do not compress big messages (assumes that those messages contain already-compressed values)
%% -define(COMM_COMPRESS_MSG(DeliverMsg, State),
%%         DeliverMsgSize = erlang:byte_size(erlang:term_to_binary(DeliverMsg, [{minor_version, 1}])),
%%         MsgQueueLen = erlang:max(1, msg_queue_len(State)),
%%         CompressionLvl = if (DeliverMsgSize / MsgQueueLen) > 1024 -> 0;
%%                             true -> 2
%%                          end,
%%         term_to_binary(DeliverMsg, [{compressed, CompressionLvl}, {minor_version, 1}])
%%        ).
-define(COMM_DECOMPRESS_MSG(DeliverMsg, State), binary_to_term(DeliverMsg)).

% using snappy-erlang-nif:
%% -define(COMM_COMPRESS_MSG(DeliverMsg, State),
%%         {ok, SnappyMsg} = snappy:compress(term_to_binary(DeliverMsg, [{minor_version, 1}])),
%%         SnappyMsg
%%        ).
%% -define(COMM_DECOMPRESS_MSG(DeliverMsg, State),
%%         {ok, SnappyMsg} = snappy:decompress(DeliverMsg),
%%         binary_to_term(SnappyMsg)
%%        ).

-define(SCALARIS_RECV_PRODUCTION(X,Y),
        {'$gen_component', trace_mpath,
         _ScalPState, _ScalFrom, _ScalTo, X = _ScalMsg} ->
               trace_mpath:log_recv(_ScalPState, _ScalFrom, _ScalTo, _ScalMsg),
               case erlang:get(trace_mpath) of
                   undefined ->
                       trace_mpath:start(_ScalPState),
                       ok;
                   _ ->
                       trace_mpath:log_info(_ScalPState, _ScalTo, {scalaris_recv})
               end,
               Y;
            X -> Y
        ).

-define(SCALARIS_RECV_DEBUG(X,Y),
        {'$gen_component', trace_mpath,
         _ScalPState, _ScalFrom, _ScalTo, X = _ScalMsg} ->
               trace_mpath:log_recv(_ScalPState, _ScalFrom, _ScalTo, _ScalMsg),
               case erlang:get(trace_mpath) of
                   undefined ->
                       trace_mpath:start(_ScalPState),
                       ok;
                   _ ->
                       trace_mpath:log_info(_ScalPState, _ScalTo, {scalaris_recv, "SCALARIS_RECV at client process (pid, module, line)~n", _ScalTo, ?MODULE, ?LINE})
               end,
               case gen_component:is_gen_component(self())
                   andalso ({current_function, {pid_groups, add, 3}}
                            =/=  process_info(self(), current_function)) of
                   false -> ok;
                   true ->
                       log:log("?SCALARIS_RECV not allowed inside a gen_component ~p",
                               [process_info(self(), current_function)]),
                       erlang:error(scalaris_recv_in_gen_component)
               end,
               Y;
            X = _ScalMsg->
               ?ASSERT2(not trace_mpath:infected(), {noninfected_message_in_infected_process, _ScalMsg}),
               case gen_component:is_gen_component(self())
                   andalso ({current_function, {pid_groups, add, 3}}
                            =/=  process_info(self(), current_function)) of
                   false -> ok;
                   true ->
                       log:log("?SCALARIS_RECV not allowed inside a gen_component ~p",
                               [process_info(self(), current_function)]),
                       erlang:error(scalaris_recv_in_gen_component)
               end,
               Y
        ).

-ifdef(enable_debug).
-define(SCALARIS_RECV(X,Y), ?SCALARIS_RECV_DEBUG(X, Y)).
-else.
-define(SCALARIS_RECV(X,Y), ?SCALARIS_RECV_PRODUCTION(X, Y)).
-endif.

-define(IIF(C, A, B), case C of
                          true -> A;
                          _ -> B
                      end).

-ifdef(with_crypto_hash).
-define(CRYPTO_MD5(V), crypto:hash(md5, V)).
-define(CRYPTO_SHA(V), crypto:hash(sha, V)).
-else.
-define(CRYPTO_MD5(V), crypto:md5(V)).
-define(CRYPTO_SHA(V), crypto:sha(V)).
-endif.

-define(TRACE_MPATH_SAFE(Expression),
        begin
            %% in case proto_sched or trace_mpath infected this msg
            trace_mpath:clear_infection(),
            Expression,
            %% restore infection if infected
            trace_mpath:restore_infection()
        end).

-include("types.hrl").
-include("atom_ext.hrl").
