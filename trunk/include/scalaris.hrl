%  @copyright 2008-2012 Zuse Institute Berlin
%  @end
%
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
%%%-------------------------------------------------------------------
%%% File    scalaris.hrl
%%% @author Thorsten Schuett <schuett@zib.de>
%%% @doc    Globally included file (sets the modules being used in cases where
%%%         multiple choices exist)
%%% @end
%%% Created : 10 Apr 2008 by Thorsten Schuett <schuett@zib.de>
%%%-------------------------------------------------------------------
%% @version $Id$

%% Version of Scalaris
-define(SCALARIS_VERSION, "0.5.0+svn").

%% userdevguide-begin scalaris:rt
%%The RT macro determines which kind of routingtable is used. Uncomment the
%%one that is desired.

%%Standard Chord routingtable
-define(RT, rt_chord).
% first valid key:
-define(MINUS_INFINITY, 0).
-define(MINUS_INFINITY_TYPE, 0).
% first invalid key:
-define(PLUS_INFINITY, 16#100000000000000000000000000000000).
-define(PLUS_INFINITY_TYPE, 16#100000000000000000000000000000000).

%%Simple routingtable
%-define(RT, rt_simple).
%% userdevguide-end scalaris:rt

%%Flexible routingtable
%-define(RT, rt_frtchord).

%%Grouped Flexible Routing Table
%-define(RT, rt_gfrtchord).

%% userdevguide-begin scalaris:db
%%Standard database backend
%-define(DB, db_toke).
%-define(DB, db_verify_use). % for testing only!
-define(DB, db_ets).
% special parameters to be passed to ets:new/2 (not only used on db_ets!)
%-define(DB_ETS_ADDITIONAL_OPS, [private]). % better, faster for production use
-define(DB_ETS_ADDITIONAL_OPS, [protected, named_table]). % better for debugging
%% userdevguide-end scalaris:db


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
%% -define(LOG_MESSAGE(SNDRCV, MESSAGE, CHANNEL), comm_logger:log(SNDRCV, {CHANNEL, comm:get_msg_tag(MESSAGE)}, erlang:external_size(MESSAGE))).


% enable native register for all processes in gen_component or disable
% useful 4 debug (etop, appmon), but let memory usage grow over the time
% enable:
%-define(DEBUG_REGISTER(PROCESS,PID),erlang:register(PROCESS,PID)).
% disable:
-define(DEBUG_REGISTER(PROCESS,PID),ok).

% Replica Repair
-define(REP_BLOOM, bloom). % bloom filter implemenation selection
-define(REP_HFS, hfs_lhsp). %HashFunctionSet selection for usage by bloom filter

% Back-end of the pdb module
-define(PDB_ERL, true). % erlang put and get (process dictionary)
%-define(PDB_ETS, true). % may have performance issues with msg_delay

% for debugging:
-ifdef(have_ctline_support).
% allows the retrieval of the current function and line number a process is in
% (process dictionary, key test_server_loc)
%-compile({parse_transform, ct_line}).
-endif.

% disable compression (the overhead is too high, at least for GbE)
-define(COMM_COMPRESS_MSG(DeliverMsg, State),
        term_to_binary(DeliverMsg, [{minor_version, 1}])
       ).
% do not compress big messages (assumes that those messages contain already-compressed values)
%% -define(COMM_COMPRESS_MSG(DeliverMsg, State),
%%         DeliverMsgSize = erlang:external_size(DeliverMsg, [{minor_version, 1}]),
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


-define(SCALARIS_RECV(X,Y),
        {'$gen_component', trace_mpath,
         _ScalPState, _ScalFrom, _ScalTo, X = _ScalMsg} ->
               trace_mpath:log_recv(_ScalPState, _ScalFrom, _ScalTo, _ScalMsg),
               case erlang:get(trace_mpath) of
                   undefined ->
                       trace_mpath:log_info(_ScalPState, _ScalTo, {tracing_ends, "Tracing ends at client process (pid, module, line)~n", _ScalTo, ?MODULE, ?LINE});
                   _ -> ok
               end,
               Y;
            X -> Y
        ).

-define(IIF(C, A, B), case C of
                          true -> A;
                          _ -> B
                      end).

-include("types.hrl").
-include("atom_ext.hrl").
