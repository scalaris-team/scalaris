%  @copyright 2007-2014 Zuse Institute Berlin

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

%% @author Jens Fischer <jensvfischer@gmail.com>
%% @doc    Header for macros used in lookups
%% @end
%% @version $Id$

-author('jensvfischer@gmail.com').
-vsn('$Id$').

-ifdef(enable_debug).
% add source information to debug routing damaged messages
-define(HOPS_TO_DATA(Hops), {comm:this(), Hops}).
-define(HOPS_FROM_DATA(Data), element(2, Data)).
-type data() :: {Source::comm:mypid(), Hops::non_neg_integer()}.
-else.
-define(HOPS_TO_DATA(Hops), Hops).
-define(HOPS_FROM_DATA(Data), Data).
-type data() :: Hops::non_neg_integer().
-endif.



