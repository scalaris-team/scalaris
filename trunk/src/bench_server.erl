% @copyright 2007-2011 Zuse Institute Berlin

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
%% @doc This is a small server for running benchmarks
%% @end
-module(bench_server).
-author('schuett@zib.de').
-vsn('$Id$').

-behaviour(gen_component).
-export([start_link/0, init/1, on/2]).

%% public interface
-export([run_increment/2, run_read/2]).

-export([run_threads/2]).

-include("scalaris.hrl").

%% @doc run an increment benchmark (i++) on all nodes
-spec run_increment(ThreadsPerVM::pos_integer(), Iterations::pos_integer()) -> ok.
run_increment(ThreadsPerVM, Iterations) ->
    Msg = {bench_increment, ThreadsPerVM, Iterations, comm:this()},
    manage_run(ThreadsPerVM, Iterations, [verbose], Msg).

%% @doc run an read benchmark on all nodes
-spec run_read(ThreadsPerVM::pos_integer(), Iterations::pos_integer()) -> ok.
run_read(ThreadsPerVM, Iterations) ->
    Msg = {bench_read, ThreadsPerVM, Iterations, comm:this()},
    manage_run(ThreadsPerVM, Iterations, [verbose], Msg).

%% @doc spread run over the available VMs and collect results
%% (executed in clients context)
-spec manage_run(ThreadsPerVM::pos_integer(), Iterations::pos_integer(), Options::[locally | verbose | profile | {copies, non_neg_integer()}], Message::comm:message()) -> ok.
manage_run(ThreadsPerVM, Iterations, Options, Message) ->
    ServerList = util:get_proc_in_vms(bench_server),
    %% io:format("~p~n", [ServerList]),
    {BeforeDump, _} = admin:get_dump(),
    Before = erlang:now(),
    _ = [comm:send(Server, Message) || Server <- ServerList],
    io:format("Collecting results... ~n"),
    Times = [receive {done, X, Time} -> io:format("BS: ~p @ ~p~n",[Time, X]),Time end || _Server <- ServerList],
    After = erlang:now(),
    case lists:member(verbose, Options) of
        true ->
            {AfterDump, _} = admin:get_dump(),
            RunTime = timer:now_diff(After, Before),
            DiffDump = admin:diff_dump(BeforeDump, AfterDump, RunTime),
            io:format("servers: ~p threads/vm: ~p iterations: ~p~n",
                      [length(ServerList), ThreadsPerVM, Iterations]),
            io:format("total time: ~p~n", [RunTime / 1000000.0]),
            io:format("1/s: ~p~n",
                      [length(ServerList) * ThreadsPerVM * Iterations / RunTime * 1000000.0]),
            Throughput = [ThreadsPerVM * Iterations / Time * 1000000.0 || Time <- Times],
            io:format("~p~n", [Throughput]),
            io:format("High load avg. latency: ~p ms~n", [ RunTime / 1000.0 / Iterations ]),
            io:format("Message statistics (message name, bytes, how often): ~p~n", [DiffDump]);
        false -> ok
    end,
    ok.

-spec run_threads(non_neg_integer(),
                  fun((Parent::comm:erl_local_pid()) -> any())) -> ok.
run_threads(Threads, Bench) ->
    Self = self(),
    _ = [ spawn(fun () -> Bench(Self) end) || _ <- lists:seq(1, Threads) ],
    _ = [ receive {done, _} -> ok end || _ <- lists:seq(1, Threads) ],
    ok.

%% read benchmark
-spec bench_read(Parent::comm:erl_local_pid(), Key::string(), Iterations::non_neg_integer(), FailedReads::non_neg_integer()) -> ok.
bench_read(Owner, _Key, 0, _Fail) ->
    %% io:format("repeated requests: ~p~n", [_Fail]),
    comm:send_local(Owner , {done, ok});
bench_read(Owner, Key, Iterations, Fail) ->
    case api_tx:read(Key) of
        {fail, _Reason} -> bench_read(Owner, Key, Iterations, Fail + 1);
        {ok, _Value}    -> bench_read(Owner, Key, Iterations - 1, Fail)
    end.


-spec on(comm:message(), ok) -> ok.
on({bench_increment, Threads, Iterations, Owner}, State) ->
    Bench = fun (Parent) ->
                    Key = get_and_init_key(),
                    bench_increment:process(Parent, Key, Iterations)
            end,
    {Time, _} = util:tc(?MODULE, run_threads, [Threads, Bench]),
    comm:send(Owner, {done, comm_server:get_local_address_port(), Time}),
    State;

on({bench_read, Threads, Iterations, Owner}, State) ->
    Bench = fun (Parent) ->
                    Key = get_and_init_key(),
                    bench_read(Parent, Key, Iterations, 0)
            end,
    {Time, _} = util:tc(?MODULE, run_threads, [Threads, Bench]),
    comm:send(Owner, {done, comm_server:get_local_address_port(), Time}),
    State.

-spec init([]) -> ok.
init([]) -> ok.

%% @doc spawns a bench_server
-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_component:start_link(?MODULE, [], [{erlang_register, bench_server}]).

%% helper functions
-spec get_and_init_key() -> string().
get_and_init_key() ->
    Key = randoms:getRandomId(),
    case get_and_init_key(Key, _Retries = 10) of
        fail ->
            io:format("geT_and_init_key choosing new key and retrying~n"),
            timer:sleep(1000 * randoms:rand_uniform(1, 10)),
            get_and_init_key();
        Key -> Key
    end.

get_and_init_key(_Key, 0)    -> fail;
get_and_init_key(Key, Count) ->
    case api_tx:write(Key, 0) of
        {ok} ->
            Key;
        {fail, abort} ->
            io:format("geT_and_init_key 1 failed, retrying~n", []),
            get_and_init_key(Key, Count - 1);
        {fail, timeout} ->
            io:format("geT_and_init_key 2 timeout, retrying~n", []),
            timer:sleep(1000 * randoms:rand_uniform(1, 10-Count)),
            get_and_init_key(Key, Count - 1)
    end.
