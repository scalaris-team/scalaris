%% @copyright 2011, 2012, 2014 Zuse Institute Berlin

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

%% @author Florian Schintke <schintke@zib.de>,
%% @author Nico Kruber <kruber@zib.de>
%% @doc API for transactional access to replicated DHT items.
%% For single request/single item operations, we provide read/1,
%% write/2, and test_and_set/3 functions that directly commit.
%%
%% For compound transactions a transaction log has to be passed
%% through all operations and finally has to be committed. This is
%% supported by the functions new_tlog/0, read/2, write/3, req_list/2,
%% and commit/1.
%% @end
%% @version $Id$

%% Perform a chain of operations (passing a transaction log) and
%% finally commit the transaction.
-export([new_tlog/0, req_list/1, req_list/2,
         read/2, write/3, add_del_on_list/4, add_on_nr/3, test_and_set/4, commit/1]).

%% Perform single operation transactions.
-export([req_list_commit_each/1,
         read/1, write/2, add_del_on_list/3, add_on_nr/2, test_and_set/3,
         get_system_snapshot/0]).

-include("scalaris.hrl").
-include("client_types.hrl").

% Public Interface
-type read_request() :: {read, client_key()}.
-type read_random_from_list_request() :: {read, client_key(), random_from_list}.
-type read_sublist_request() :: {read, client_key(), {sublist, Start::pos_integer() | neg_integer(), Len::integer()}}.
-type write_request() :: {write, client_key(), client_value()}.
-type write_request_enc() :: {write, client_key(), rdht_tx:encoded_value()}.
-type add_del_on_list_request() ::
          {add_del_on_list, client_key(),
           client_value(),  %% abort when not ToAdd::[client_value()],
           client_value()}.  %% abort when not ToRemove::[client_value()]}
-type add_del_on_list_request_enc() ::
          {add_del_on_list, client_key(),
           rdht_tx:encoded_value(),   %% abort when not decoded to [client_value()],
           rdht_tx:encoded_value()}.  %% abort when not decoded to [client_value()]}
-type add_on_nr_request() ::
          {add_on_nr, client_key(),
           client_value()}. %% abort when not number()}
-type add_on_nr_request_enc() ::
          {add_on_nr, client_key(),
           rdht_tx:encoded_value()}. %% abort when not decoded to number()}
-type test_and_set_request() ::
          {test_and_set, client_key(),
           Old::client_value(), New::client_value()}.
-type test_and_set_request_enc() ::
          {test_and_set, client_key(),
           Old::rdht_tx:encoded_value(), New::rdht_tx:encoded_value()}.

-type request_on_key() ::
          read_request()
        | read_random_from_list_request()
        | read_sublist_request()
        | write_request()
        | add_del_on_list_request()
        | add_on_nr_request()
        | test_and_set_request().
-type request_on_key_enc() ::
          read_request()
        | read_random_from_list_request()
        | read_sublist_request()
        | write_request_enc()
        | add_del_on_list_request_enc()
        | add_on_nr_request_enc()
        | test_and_set_request_enc().
-type request() :: request_on_key() | {commit}.
-type request_enc() :: request_on_key_enc() | {commit}.

-type read_result() :: {ok, client_value()} | {fail, not_found}.
-type read_random_from_list_result() :: {ok, RandomValue_ListLen_MaybeEncoded::client_value()} | {fail, not_found | empty_list | not_a_list}.
-type read_sublist_result() :: {ok, SubList_ListLen_MaybeEncoded::client_value()} | {fail, not_found | not_a_list}.
-type write_result() :: {ok}.
-type listop_result() :: write_result() | {fail, not_a_list}.
-type numberop_result() :: write_result() | {fail, not_a_number}.
-type commit_result() :: {ok} | {fail, abort, [client_key()]}.
-type testandset_result() ::
          write_result()
        | {fail, not_found | {key_changed, RealOldValue::client_value()}}.
-type result() ::
          read_result()
        | read_random_from_list_result()
        | read_sublist_result()
        | write_result()
        | listop_result()
        | numberop_result()
        | testandset_result()
        | commit_result().

%% @doc Get an empty transaction log to start a new transaction.
-spec new_tlog() -> tx_tlog:tlog_ext().
new_tlog() -> tx_tlog:empty().

%% @doc Perform several requests starting a new transaction.
-spec req_list([request()]) -> {tx_tlog:tlog_ext(), [result()]}.
req_list(ReqList) ->
    req_list(new_tlog(), ReqList).

%% @doc Perform a read inside a transaction.
-spec read(tx_tlog:tlog_ext(), client_key())
          -> {tx_tlog:tlog_ext(), read_result()}.
read(TLog, Key) ->
    {NewTLog, [Result]} = req_list(TLog, [{read, Key}]),
    {NewTLog, Result}.

%% @doc Perform a write inside a transaction.
-spec write(tx_tlog:tlog_ext(), client_key(), client_value())
           -> {tx_tlog:tlog_ext(), write_result()}.
write(TLog, Key, Value) ->
    {NewTLog, [Result]} = req_list(TLog, [{write, Key, Value}]),
    {NewTLog, Result}.

%% @doc Perform a add_del_on_list operation inside a transaction.
-spec add_del_on_list(tx_tlog:tlog_ext(), client_key(), ToAdd::[client_value()], ToRemove::[client_value()])
           -> {tx_tlog:tlog_ext(), listop_result()}.
add_del_on_list(TLog, Key, ToAdd, ToRemove) ->
    {NewTLog, [Result]} = req_list(TLog, [{add_del_on_list, Key, ToAdd, ToRemove}]),
    {NewTLog, Result}.

%% @doc Perform a add_del_on_list operation inside a transaction.
-spec add_on_nr(tx_tlog:tlog_ext(), client_key(), ToAdd::number())
           -> {tx_tlog:tlog_ext(), numberop_result()}.
add_on_nr(TLog, Key, ToAdd) ->
    {NewTLog, [Result]} = req_list(TLog, [{add_on_nr, Key, ToAdd}]),
    {NewTLog, Result}.

%% @doc Atomically compare and set a value for a key inside a transaction.
%%      If the value stored at Key is the same as OldValue, then NewValue will
%%      be stored.
-spec test_and_set(tx_tlog:tlog_ext(), Key::client_key(), OldValue::client_value(), NewValue::client_value())
        -> {tx_tlog:tlog_ext(), testandset_result()}.
test_and_set(TLog, Key, OldValue, NewValue) ->
    {NewTLog, [Result]} = req_list(TLog, [{test_and_set, Key, OldValue, NewValue}]),
    {NewTLog, Result}.

%% @doc Finish a transaction and materialize it atomically on the DHT.
-spec commit(tx_tlog:tlog_ext()) -> commit_result().
commit(TLog) ->
    {_NewTLog, [Result]} = req_list(TLog, [{commit}]),
    Result.

%% @doc Atomically read a single key (not as part of a transaction).
-spec read(client_key()) -> read_result().
read(Key) ->
    ReqList = [{read, Key}],
    {_TLog, [Res]} = req_list(tx_tlog:empty(), ReqList),
    Res.

%% @doc Atomically write and commit a single key (not as part of a transaction).
-spec write(client_key(), client_value()) -> commit_result().
write(Key, Value) ->
    ReqList = [{write, Key, Value}, {commit}],
    {_TLog, [_Res1, Res2]} = req_list(tx_tlog:empty(), ReqList),
    Res2.

%% @doc Atomically perform a add_del_on_list operation and a commit (not as part of a transaction).
-spec add_del_on_list(client_key(), ToAdd::[client_value()], ToRemove::[client_value()])
           -> listop_result() | {fail, abort, [client_key()]}.
add_del_on_list(Key, ToAdd, ToRemove) ->
    ReqList = [{add_del_on_list, Key, ToAdd, ToRemove}, {commit}],
    {_TLog, [Res1, Res2]} = req_list(tx_tlog:empty(), ReqList),
    case Res1 of
        X when erlang:is_tuple(X) andalso erlang:element(1, X) =:= fail -> X;
        _ -> Res2
    end.

%% @doc Atomically perform a add_del_on_list operation and a commit (not as part of a transaction).
-spec add_on_nr(client_key(), ToAdd::number())
           -> numberop_result() | {fail, abort, [client_key()]}.
add_on_nr(Key, ToAdd) ->
    ReqList = [{add_on_nr, Key, ToAdd}, {commit}],
    {_TLog, [Res1, Res2]} = req_list(tx_tlog:empty(), ReqList),
    case Res1 of
        X when erlang:is_tuple(X) andalso erlang:element(1, X) =:= fail -> X;
        _ -> Res2
    end.

%% @doc Atomically compare and set a value for a key (not as part of a transaction).
%%      If the value stored at Key is the same as OldValue, then NewValue will
%%      be stored.
-spec test_and_set(Key::client_key(), OldValue::client_value(), NewValue::client_value())
        -> testandset_result() | {fail, abort, [client_key()]}.
test_and_set(Key, OldValue, NewValue) ->
    ReqList = [{test_and_set, Key, OldValue, NewValue}, {commit}],
    {_TLog, [Res1, Res2]} = req_list(tx_tlog:empty(), ReqList),
    case Res1 of
        X when erlang:is_tuple(X) andalso erlang:element(1, X) =:= fail -> X;
        _ -> Res2
    end.

-spec commit_req(request_on_key()) -> result().
commit_req(Request) ->
    case Request of
        {read, Key} -> read(Key);
        {write, Key, Value} -> write(Key, Value);
        {add_del_on_list, Key, ToAdd, ToRemove} -> add_del_on_list(Key, ToAdd, ToRemove);
        {add_on_nr, Key, ToAdd} -> add_on_nr(Key, ToAdd);
        {test_and_set, Key, Old, New} -> test_and_set(Key, Old, New);
        {read, _Key, _Op} = Req ->
            ReqList = [Req],
            {_TLog, [Res]} = req_list(tx_tlog:empty(), ReqList),
            Res;
        _ -> {fail, abort, []}
    end.

%% @doc Perform several requests in parallel and commit each.
%%      The execution order of multiple requests on the same key is undefined!
-spec req_list_commit_each([request_on_key()]) -> [result()].
req_list_commit_each(ReqList) ->
    util:par_map(fun commit_req/1, ReqList, 50).

%% @doc Get system snapshot
-spec get_system_snapshot() -> [tuple()] | {snapshot_failed,
                                            intervals:interval(),
                                            [tuple()]}.
get_system_snapshot() ->
    Key = ?RT:hash_key(randoms:getRandomString()),
    api_dht_raw:unreliable_lookup(Key, {?send_to_group_member, snapshot_leader,
                                       {init_snapshot, comm:this()}}),
    trace_mpath:thread_yield(),
    receive
        ?SCALARIS_RECV({global_snapshot_done, Data}, Data);
        ?SCALARIS_RECV({global_snapshot_done_with_errors, ErrorInterval, Data},
                       {snapshot_failed, ErrorInterval, Data})
    end.
