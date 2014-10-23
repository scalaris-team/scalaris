% @copyright 2013 Zuse Institute Berlin

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

%% @author Maximilian Michels <michels@zib.de>
%% @doc Behaviour for various operations in a cloud environment, e.g. for
%%      scaling.
%% @end
%% @version $Id$
-module(cloud_beh).
-author('michels@zib.de').
-vsn('$Id$').

-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-ifdef(have_callback_support).
-include("scalaris.hrl").

-callback init() -> failed | ok.
-callback get_number_of_vms() -> failed | non_neg_integer().
-callback add_vms(Count::non_neg_integer()) -> failed | ok.
-callback remove_vms(Count::non_neg_integer()) -> failed | ok.

-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {init, 0},
     {get_number_of_vms, 0},
     {add_vms, 1},
     {remove_vms, 1}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.
