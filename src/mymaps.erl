%% @copyright 2016 Zuse Institute Berlin

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
%% @doc Thin wrapper around the maps module falling back to gb_trees if maps
%%      are not available (Erlang versions below 17.0).
%%      (extend as needed)
%% @version $Id$
-module(mymaps).
-author('kruber@zib.de').
-vsn('$Id$').

-export([new/0,
         get/2, get/3, find/2, is_key/2,
         put/3, remove/2, update/3,
         from_list/1, to_list/1,
         keys/1, values/1,
         size/1]).

-export_type([mymap/0]).

-ifdef(with_maps).
-type mymap() :: map().
-else.
-type mymap() :: gb_trees:tree().
-endif.

-spec get(Key::term(), Map::mymap()) -> Value::term().
-ifdef(with_maps).
get(Key, Map) -> maps:get(Key, Map).
-else.
get(Key, Map) ->
    case gb_trees:lookup(Key, Map) of
        {value, Value} -> Value;
        none -> erlang:error({badkey, Key})
    end.
-endif.


-spec get(Key::term(), Map::mymap(), Default) -> Value | Default
        when is_subtype(Value, term()), is_subtype(Default, term()).
-ifdef(with_maps).
get(Key, Map, Default) -> maps:get(Key, Map, Default).
-else.
get(Key, Map, Default) ->
    case gb_trees:lookup(Key, Map) of
        {value, Value} -> Value;
        none -> Default
    end.
-endif.

-spec find(Key::term(), Map::mymap()) -> {ok, Value::term()} | error.
-ifdef(with_maps).
find(Key, Map) -> maps:find(Key, Map).
-else.
find(Key, Map) ->
    case gb_trees:lookup(Key, Map) of
        {value, Value} -> {ok, Value};
        none -> error
    end.
-endif.

-spec from_list([{Key::term(), Value::term()}]) -> Map::mymap().
-ifdef(with_maps).
from_list(List) -> maps:from_list(List).
-else.
from_list(List) -> gb_trees:from_orddict(orddict:from_list(List)).
-endif.

-spec is_key(Key::term(), Map::mymap()) -> boolean().
-ifdef(with_maps).
is_key(Key, Map) -> maps:is_key(Key, Map).
-else.
is_key(Key, Map) -> gb_trees:is_defined(Key, Map).
-endif.

-spec keys(Map::mymap()) -> [Key::term()].
-ifdef(with_maps).
keys(Map) -> maps:keys(Map).
-else.
keys(Map) -> gb_trees:keys(Map).
-endif.

-spec new() -> mymap().
-ifdef(with_maps).
new() -> maps:new().
-else.
new() -> gb_trees:empty().
-endif.

-spec put(Key::term(), Value::term(), Map1::mymap()) -> Map2::mymap().
-ifdef(with_maps).
put(Key, Value, Map) -> maps:put(Key, Value, Map).
-else.
put(Key, Value, Map) -> gb_trees:enter(Key, Value, Map).
-endif.

-spec remove(Key::term(), Map1::mymap()) -> Map2::mymap().
-ifdef(with_maps).
remove(Key, Map) -> maps:remove(Key, Map).
-else.
remove(Key, Map) -> gb_trees:delete_any(Key, Map).
-endif.

-spec to_list(Map::mymap()) -> [{Key::term(), Value::term()}].
-ifdef(with_maps).
to_list(Map) -> maps:to_list(Map).
-else.
to_list(Map) -> gb_trees:to_list(Map).
-endif.

-spec update(Key::term(), Value::term(), Map1::mymap()) -> Map2::mymap().
-ifdef(with_maps).
update(Key, Value, Map) -> maps:update(Key, Value, Map).
-else.
update(Key, Value, Map) -> gb_trees:update(Key, Value, Map).
-endif.

-spec values(Map::mymap()) -> [Value::term()].
-ifdef(with_maps).
values(Map) -> maps:values(Map).
-else.
values(Map) -> gb_trees:values(Map).
-endif.

-spec size(Map::mymap()) -> non_neg_integer().
-ifdef(with_maps).
size(Map) -> maps:size(Map).
-else.
size(Map) -> gb_trees:size(Map).
-endif.
