%  @copyright 2011-2014 Zuse Institute Berlin

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
%% @doc    API functions for the Map-Reduce system
%%
%%         The Map-Reduce system will snapshot the database and apply the the
%%         supplied job to that data. Since snapshots work on the overlay level
%%         only the hashed keys are available.
%%         Since hashed keys are not useful in this context, the MR system will
%%         only considers values of the following structure as inputs:
%%         ```
%%         {Key::string(), Value::term()}
%%         or
%%         {Tag::atom(), Key:string(), Value::term()}
%%         '''
%%         Either all 2-tuples are considered to be input or, if
%%         `{tag, some_tag}' is found in the options list, all 3-tuples with `some_tag'
%%         as the first element are.
%%
%%         A job description is a 2-tuple where the first element is a list of
%%         phase specifications and the second element a list of options.
%%         A simple example job could look like this:
%%         ```
%%            Map = fun({_Key, Line}) ->
%%                Tokens = string:tokens(Line, " \n,.;:?!()\"'-_"),
%%                [{string:to_lower(X),1} || X <- Tokens]
%%            end,
%%            Reduce = fun(KVList) ->
%%                    lists:map(fun({K, V}) ->
%%                                      {K, lists:sum(V)}
%%                                end, KVList)
%%            end,
%%            api_mr:start_job({[{map, erlanon, Map},
%%                               {reduce, erlanon, Reduce}],
%%                              []}).
%%         '''
%%         It considers all `{string(), string()}' as input and returns the word count of all
%%         values.
%%
%% @end
%% @version $Id$
-module(api_mr).
-author('fajerski@zib.de').
-vsn('$Id$').

%% -define(TRACE(X, Y), io:format(X, Y)).
-define(TRACE(X, Y), ok).

-export([start_job/1]).

-include("scalaris.hrl").

%% @doc synchronous call to start a map reduce job.
%%      it will return the results of the job.
-spec start_job(mr_state:job_description()) -> [any()].
start_job(Job) ->
    Id = randoms:getRandomString(),
    api_dht_raw:unreliable_lookup(api_dht:hash_key(Id), {mr_master, init, comm:this(), Id, Job}),
    wait_for_results([], intervals:empty(), Id).

-spec wait_for_results([any()], intervals:interval(), mr_state:jobid()) -> [any()].
wait_for_results(Data, Interval, Id) ->
    {NewData, NewInterval} =
        begin
            trace_mpath:thread_yield(),
            receive
                ?SCALARIS_RECV({mr_results, PartData, PartInterval, Id},
                               case PartData of
                                   {error, Reason} ->
                                       {[{error, Reason}], intervals:all()};
                                   PartData ->
                                       {[PartData | Data], intervals:union(PartInterval, Interval)}
                               end)
                end
        end,
    ?TRACE("mr_api: received data for job ~p: ~p~n", [Id, hd(NewData)]),
    case intervals:is_all(NewInterval) of
        true ->
            lists:append(NewData);
        _ ->
            wait_for_results(NewData, NewInterval, Id)
    end.
