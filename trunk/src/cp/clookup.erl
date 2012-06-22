%  @copyright 2012 Zuse Institute Berlin

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
%% @author Florian Schintke <schintke@zib.de>
%% @doc    lookup with given consistency model consistent or best_effort.
%% @end
%% @version $Id$
-module(clookup).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id ').

-include("scalaris.hrl").

-export([lookup/3, lookup/4]).

-ifdef(with_export_type_support).
-export_type([need_consistency/0]).
-export_type([got_consistency/0]).
-endif.

-type need_consistency() :: proven | best_effort.
-type got_consistency()  :: was_consistent | was_not_consistent.

-spec lookup(?RT:key(), comm:message(), need_consistency()) -> ok.
lookup(Key, Msg, NeedConsistency) ->
    lookup(pid_groups:find_a(router_node), Key, Msg, NeedConsistency).

-spec lookup(comm:erl_local_pid(), ?RT:key(),
             comm:message(), need_consistency()) -> ok.
lookup(Pid, Key, Msg, NeedConsistency) ->
    comm:send_local(Pid, {?lookup_aux, Key, Msg, NeedConsistency}).










