%  @copyright 2010-2012 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Type definitions of non-existing, opaque or custom types which can
%%         be used independently from the erlang version.
%% @end
%% @version $Id$

-ifdef(tid_not_builtin).
-type tid() :: ets:tid().
-else.
% Erlang 17.0 has tid() as a builtin type but allows overriding it:
-ifndef(etstid_not_available).
-type tid() :: ets:tid().
-endif.
-endif.

-ifdef(have_erlang_timestamp).
-type erlang_timestamp() :: erlang:timestamp().
-else.
-type erlang_timestamp() :: {MegaSecs::non_neg_integer(),
                             Secs::non_neg_integer(),
                             MicroSecs::non_neg_integer()}.
-endif.
