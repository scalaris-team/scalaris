%  @copyright 2010-2011 Zuse Institute Berlin

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
%% @doc    Implements helper functions for queuing messages and sending them
%%         in the order they have been queued.
%% @end
%% @version $Id$
-module(msg_queue).
-author('kruber@zib.de').
-vsn('$Id$').

-export_type([msg_queue/0]).

-export([new/0, is_empty/1, add/2, send/1]).

-include("scalaris.hrl").

-type msg_queue() :: [comm:message()].

%% @doc Creates a new message queue.
-spec new() -> msg_queue().
new() -> [].

%% @doc Checks whether the message queue is empty.
-spec is_empty(msg_queue()) -> boolean().
is_empty(QueuedMessages) ->
    QueuedMessages =:= [].

%% @doc Adds a message to a given queue.
-spec add(msg_queue(), comm:message()) -> msg_queue().
add(QueuedMessages, NewMessage) ->
    [NewMessage | QueuedMessages].

%% @doc Sends queued messages to the process calling the method, i.e. self(), in
%%      the order the messages have been queued.
-spec send(msg_queue()) -> ok.
send(QueuedMessages) ->
    lists:foldr(fun(Msg, _) ->
                        comm:send_local(self(), Msg)
                end, ok, QueuedMessages).
