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

%% @author Jan Fajerski <fajerski@zib.de>
%% @doc state for one map reduce master
%% @version $Id$
-module(mr_master_state).
-author('fajerski@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-type outstanding() :: false | snapshot.

-record(state, {id             = 0 :: ?RT:key(),
                acked = intervals:empty() :: intervals:interval(),
                round  = 0 :: non_neg_integer(),
                outstanding    = false :: outstanding(),
                job            = [] :: mr_state:job_description(),
                client         = false :: false | comm:mypid()
               }).

-type state() :: #state{}.

-export_type([state/0]).

-export([new/3
        , get/2
        , set/2
        , get_slide_delta/2]).

-spec new(Id::?RT:key(), Job::mr_state:job_description(), Client::comm:mypid()) ->
    state().
new(Id, Job, Client) ->
    #state{id          = Id,
           outstanding =  snapshot,
           job         = Job,
           client      = Client}.

%% TODO check if this is better vs dht_node_state
-spec get(id, state()) -> ?RT:key();
         (acked, state()) -> intervals:interval();
         (round, state()) -> non_neg_integer();
         (outstanding, state()) -> outstanding();
         (job, state()) -> mr_state:job_description();
         (client, state()) -> comm:mypid().
get(id, #state{id = X}) ->
    X;
get(acked, #state{acked = X}) ->
    X;
get(round, #state{round = X}) ->
    X;
get(outstanding, #state{outstanding = X}) ->
    X;
get(job, #state{job = X}) ->
    X;
get(client, #state{client = X}) ->
    X.

-spec set(state(), [{id, ?RT:key()} |
                        {acked, intervals:interval()} |
                        {round, non_neg_integer()} |
                        {outstanding, outstanding()} |
                        {client, comm:mypid()}]) -> state().
set(State, []) ->
    State;
set(State, [{id, Id} | T]) ->
    set(State#state{id = Id}, T);
set(State, [{acked, Acked} | T]) ->
    set(State#state{acked = Acked}, T);
set(State, [{round, Round} | T]) ->
    set(State#state{round = Round}, T);
set(State, [{outstanding, Out} | T]) ->
    set(State#state{outstanding = Out}, T);
set(State, [{client, Client} | T]) ->
    set(State#state{client = Client}, T).

-spec get_slide_delta(state(), intervals:interval()) -> {boolean(), state()}.
get_slide_delta(State, Interval) ->
    {intervals:in(get(id, State), Interval), State}.
