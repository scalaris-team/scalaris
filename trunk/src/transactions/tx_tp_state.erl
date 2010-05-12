%% @copyright 2009, 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin
%%    and onScale solutions GmbH

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

%% @author Florian Schintke <schintke@onscale.de>
%% @doc : Part of generic transaction implementation -
%%           The state for a transaction in a tp (transaction participant).
%% @version $Id$
-module(tx_tp_state).
%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-author('schintke@onscale.de').

%% Operations on tx_tm_state
%% -export([new/5, new/6]).

%% tx_tm_state: {TxId,
%%               Client,
%%               ClientsId,
%%               RTMs, (should we store the RTMs once more
%%                      to make RTMs not churning during a Tx?)
%%               tx_tm_helper_behaviour to use? needed? for what?,
%%               timeout before the first RTM takes over
%%               [{TLogEntry, [{PaxosID, state}]}]
%%               Majority of RTMs,
%%               NumIds,
%%               NumPrepared,
%%               NumAbort,
%%               }



