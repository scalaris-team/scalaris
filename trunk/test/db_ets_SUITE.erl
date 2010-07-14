% @copyright 2010 Konrad-Zuse-Zentrum fuer Informationstechnik Berlin

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

%%% @author Nico Kruber <kruber@zib.de>
%%% @doc    Unit tests for src/db_ets.erl.
%%% @end
%% @version $Id$
-module(db_ets_SUITE).

-author('kruber@zib.de').
-vsn('$Id$').

-compile(export_all).

-define(TEST_DB, db_ets).

-include("db_SUITE.hrl").

all() -> tests_avail().
