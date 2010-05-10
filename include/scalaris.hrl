%  @copyright 2008-2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
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

%% userdevguide-begin scalaris:rt
%%The RT macro determines which kind of routingtable is used. Uncomment the
%%one that is desired.

%%Standard Chord routingtable
-define(RT, rt_chord).

%%Simple routingtable
%-define(RT, rt_simple).
%% userdevguide-end scalaris:rt


%% userdevguide-begin scalaris:db
%%Standard database backend
%-define(DB, db_gb_trees).
%-define(DB, db_tcerl).
-define(DB, db_ets).
%% userdevguide-end scalaris:db


%% userdevguide-begin scalaris:rm
%%Standard chord ring maintenance
%-define(RM, rm_chord).

%% ring maintenance by T-man
-define(RM, rm_tman).

%% ring maintenance by T-man-Sharp
%-define(RM, rm_tmansharp).
%% userdevguide-end scalaris:rm


-define(TCP_LAYER, true). % TCP communication
%-define(BUILTIN, true).   % distributed Erlang native communication
%-define(SIMULATION, true).


% enable logging of message statistics
-define(LOG_MESSAGE(MESSAGE, SIZE), ok).
%% -define(LOG_MESSAGE(MESSAGE, SIZE), comm_logger:log(cs_send:get_msg_tag(MESSAGE), SIZE).


% enable native register for all processes in gen_component or disable
% useful 4 debug (etop, appmon), but let memory usage grow over the time
% enable:
%-define(DEBUG_REGISTER(PROCESS,PID),register(PROCESS,PID)).
% disable:
-define(DEBUG_REGISTER(PROCESS,PID),ok).


% enable detailed time logging in cs_api (jsonrpc)
%-define(LOG_CS_API(Timer, Time), monitor_timing:log(Timer, Time)).
-define(LOG_CS_API(Timer, Time), ok).


-include("types.hrl").
