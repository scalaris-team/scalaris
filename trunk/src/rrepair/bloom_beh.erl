% @copyright 2010-2011 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    Bloom Filter Behaviour
%% @end
%% @version $Id$

-module(bloom_beh).

-ifndef(have_callback_support).
-export([behaviour_info/1]).
-endif.

-ifdef(have_callback_support).
-include("scalaris.hrl").
-type bloom_filter() :: term().
-type key() :: binary() | integer() | float() | boolean() | atom() | tuple() | ?RT:key().

-callback new(integer(), float()) -> bloom_filter().
-callback new(integer(), float(), ?REP_HFS:hfs()) -> bloom_filter().
-callback add(bloom_filter(), key() | [key()]) -> bloom_filter().
-callback is_element(bloom_filter(), key()) -> boolean().
-callback equals(bloom_filter(), bloom_filter()) -> boolean().
-callback join(bloom_filter(), bloom_filter()) -> bloom_filter().
-callback print(bloom_filter()) -> [{atom(), any()}].
-else.
-spec behaviour_info(atom()) -> [{atom(), arity()}] | undefined.
behaviour_info(callbacks) ->
    [
     {new, 2}, {new, 3},
     {add, 2},
     {is_element, 2}, {equals, 2}, {join, 2},
     {print, 1}
    ];
behaviour_info(_Other) ->
    undefined.
-endif.
