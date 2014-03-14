% @copyright 2007-2014 Zuse Institute Berlin

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
%% @doc Load generator
%% @end
%% @version $Id$
-module(bench_load).
-author('schuett@zib.de').
-vsn('$Id$').

-export([start/1]).

-include("scalaris.hrl").

-spec start(Gap::pos_integer()) -> ok.
start(Gap) ->
    loop(Gap, []).

-spec loop(Gap::pos_integer(), [pid()]) -> ok.
loop(Gap, Pids) ->
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV(
            {load_stop}, %%->
            begin
                _ = [erlang:exit(Pid, kill) || Pid <- Pids],
                ok
            end
          )
    after Gap ->
            Pid = spawn_new(),
            loop(Gap, [Pid | Pids])
    end.

spawn_new() ->
    io:format("spawn~n", []),
    spawn(fun() ->
                 worker()
          end).

worker() ->
    Key1 = randoms:getRandomString(),
    Key2 = randoms:getRandomString(),
    read_read_iter(Key1, Key2, 1000, 0),
    worker().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% read_read (reads two items in one transaction)
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
read_read_tx(Key1, Key2) ->
    {_, Result} = api_tx:req_list([{read, Key1}, {read, Key2}, {commit}]),
    Result.

read_read_iter(_Key1, _Key2, 0, Aborts) ->
    Aborts;
read_read_iter(Key1, Key2, Iterations, Aborts) ->
    case read_read_tx(Key1, Key2) of
        [{ok, 0},{ok,0},{ok}] -> read_read_iter(Key1, Key2, Iterations - 1, Aborts);
        _ -> read_read_iter(Key1, Key2, Iterations, Aborts + 1)
    end.

