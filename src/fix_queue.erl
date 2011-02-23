%  @copyright 2009-2011 Zuse Institute Berlin

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

%% @author Christian Hennig <hennig@zib.de>
%% @doc    Queue implementation with a fixed maximum length.
%% @end
%% @version $Id$
-module(fix_queue).
-author('hennig@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-ifdef(with_export_type_support).
-export_type([fix_queue/0]).
-endif.

-export([new/1, add/2, add_unique_head/4, map/2, remove/3,
         length/1, max_length/1, queue/1]).

-opaque(fix_queue()::{MaxLength :: pos_integer(),
                      Length    :: non_neg_integer(),
                      Queue     :: queue()}).

%% @doc Creates a new fixed-size queue.
-spec new(MaxLength::pos_integer()) -> fix_queue().
new(MaxLength) ->
    {MaxLength, 0, queue:new()}.

%% @doc Adds an element to the given queue. 
-spec add(Element::term(), Queue::fix_queue()) -> fix_queue().
add(Elem, {MaxLength, Length, Queue}) ->
    {NewLength, NewQueue} =
        case Length =:= MaxLength of
            true -> {Length, queue:in(Elem, queue:drop(Queue))};
            _    -> {Length + 1, queue:in(Elem, Queue)}
        end,
    {MaxLength, NewLength, NewQueue}.

%% @doc Adds an element to the given queue. If there is already an equal
%%      element at the "head" (rear) of the queue, it will be replaced by the
%%      element selected by SelectFun.
%%      Note that this is much cheaper than checking all elements!
-spec add_unique_head(Element, Queue::fix_queue(),
        EqFun::fun((Old::Element, New::Element) -> boolean()),
        SelectFun::fun((Old::Element, New::Element) -> Element)) -> fix_queue().
add_unique_head(Elem, {MaxLength, Length, Queue}, EqFun, SelectFun) ->
    {NewL1, NewQ1} =
        case queue:peek_r(Queue) of
            {value, Item} ->
                case EqFun(Item, Elem) of
                    true -> {Length, queue:in(SelectFun(Item, Elem), queue:drop_r(Queue))};
                    _    -> {Length + 1, queue:in(Elem, Queue)}
                end;
            empty -> {1, queue:in(Elem, Queue)}
        end,
    {NewLength, NewQueue} =
        case NewL1 > MaxLength of
            true -> {MaxLength, queue:drop(NewQ1)};
            _    -> {Length, NewQ1}
        end,
    {MaxLength, NewLength, NewQueue}.

%% @doc Maps a function to all elements of the given queue.
-spec map(fun((term()) -> E), Queue::fix_queue()) -> [E].
map(Fun, {_MaxLength, _Length, Queue}) ->
    lists:map(Fun, queue:to_list(Queue)).

-spec remove(Element, Queue::fix_queue(),
        EqFun::fun((Element, Element) -> boolean())) -> fix_queue().
remove(Elem, {MaxLength, _Length, Queue}, EqFun) ->
    NewQueue = queue:filter(fun(X) -> not EqFun(X, Elem) end, Queue),
    NewLength = queue:len(NewQueue),
    {MaxLength, NewLength, NewQueue}.

-spec length(fix_queue()) -> non_neg_integer().
length({_MaxLength, Length, _Queue}) -> Length.

-spec max_length(fix_queue()) -> pos_integer().
max_length({MaxLength, _Length, _Queue}) -> MaxLength.

-spec queue(fix_queue()) -> queue().
queue({_MaxLength, _Length, Queue}) -> Queue.
