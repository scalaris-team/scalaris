% @copyright 2012-2013 Zuse Institute Berlin,

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

%% @author Florian Schintke <schintke@zib.de>
%% @doc txid store based on rbrcseq.
%% @end
%% @version $Id:$
-module(txid_on_cseq).
-author('schintke@zib.de').
-vsn('$Id:$').

%-define(TRACE(X,Y), io:format(X,Y)).
-define(TRACE(X,Y), ok).
-include("scalaris.hrl").
-include("client_types.hrl").

-export([read/2]).
-export([new/3, new_feeder/3]).
-export([decide/5]).
-export([delete/2]).

%% filters and checks for rbr_cseq operations consistency
-export([is_valid_new/3]).
-export([is_valid_decide/3]).
-export([is_valid_delete/3]).

%% write filters
-export([wf_decide/3]).

-type txid() :: ?RT:key().

-type txid_entry() ::
        { txid(),
          comm:mypid(),         %% client
          [client_key()],       %% involved keys
          erlang_timestamp(),   %% creation time
          open | commit | abort %% status
        }.

-ifdef(with_export_type_support).
-export_type([txid/0]).
-endif.

-spec read(txid(), comm:erl_local_pid()) -> ok.
read(Key, ReplyTo) ->
    %% decide which db is responsible, ie. if the key is from
    %% the first quarter of the ring, use lease_db1, if from 2nd
    %% quarter -> use lease_db2, ...
    DB = get_db_for_id(Key),
    %% perform qread
    rbrcseq:qread(DB, ReplyTo, Key).

-spec new_feeder(txid(), [client_key()], ok) ->
                 {txid(), [client_key()], comm:erl_local_pid()}.
new_feeder(Key, InvolvedKeys, ok) ->
    {Key, InvolvedKeys, self()}.

-spec new(txid(), [client_key()], comm:erl_local_pid()) -> ok.
new(Key, InvolvedKeys, ReplyTo) ->
    DB = get_db_for_id(Key),
    Value = new_entry(Key, InvolvedKeys, comm:make_global(ReplyTo)),
    RBRCseqPid = comm:make_global(pid_groups:find_a(DB)),
    rbrcseq:qwrite_fast(DB, ReplyTo, Key,
                        fun txid_on_cseq:is_valid_new/3, Value,
                        prbr:smallest_round(RBRCseqPid), prbr_bottom).

-spec decide(txid(), commit | abort, comm:erl_local_pid(),
             prbr:r_with_id(), any()) -> ok.
decide(Key, Decision, ReplyTo, Round, OldVal) ->
    DB = get_db_for_id(Key),
    rbrcseq:qwrite_fast(DB, ReplyTo, Key,
                        fun txid_on_cseq:is_valid_decide/3, Decision,
                        Round, OldVal).

-spec delete(txid(), comm:erl_local_pid()) -> ok.
delete(Key, ReplyTo) ->
    DB = get_db_for_id(Key),
    rbrcseq:qwrite(DB, ReplyTo, Key,
                   fun txid_on_cseq:is_valid_delete/3, prbr_bottom).

%% content checks
-spec is_valid_new(prbr_bottom | txid_entry(),
                   prbr:write_filter(), New::txid_entry()) ->
                          {boolean(), null}.
is_valid_new(prbr_bottom, _WriteFilter, _NewTxIdEntry) ->
    {true, null};
is_valid_new(ExistingEntry, _WriteFilter, ExistingEntry) ->
    %% recreating same entry is ok (write through may happened)
    {true, null};
is_valid_new(_ExistingEntry, _WriteFilter, _DifferingNewEntry) ->
    {false, null}.

-spec is_valid_decide(prbr_bottom | txid_entry(),
                      prbr:write_filter(), commit | abort) ->
                               {boolean(), null}.
is_valid_decide(prbr_bottom, _WriteFilter, _Decision) ->
    {false, null};
is_valid_decide(ExistingEntry, _WriteFilter, Decision) ->
    case status(ExistingEntry) of
        open -> {true, null};
        %% recreating same entry is ok (write through may happened)
        Decision -> {true, null};
        _ ->
            log:log("You try to overwrite a tx with another decision.~n"),
            {false, null}
    end.

-spec is_valid_delete(prbr_bottom | txid_entry(),
                      prbr:write_filter(), New::txid_entry()) ->
                               {boolean(), null}.
is_valid_delete(prbr_bottom, _WriteFilter, prbr_bottom) ->
    {true, null};
is_valid_delete(ExistingEntry, _WriteFilter, ExistingEntry) ->
    %% recreating same entry is ok (write through may happened)
    {false, null};
is_valid_delete(ExistingEntry, _WriteFilter, prbr_bottom) ->
    case status(ExistingEntry) of
        open -> {false, null};
        _ -> {true, null} %% comit | abort
    end.

%% write filters
%% WF(old_dbdata(), UpdateInfo, value()) -> dbdata().
-spec wf_decide(txid_entry(), null, commit | abort) -> txid_entry().
wf_decide(Old, null, Decision) ->
    set_status(Old, Decision).


-spec get_db_for_id(?RT:key()) -> atom().
get_db_for_id(Id) ->
    erlang:list_to_existing_atom(
      lists:flatten(
        io_lib:format("txid_db~p", [?RT:get_key_segment(Id)]))).

%% abstract data type: txid_entry
-spec new_entry(?RT:key(), [client_key()], comm:mypid()) -> txid_entry().
new_entry(TxId, Keys, Client) ->
    {TxId, Client, Keys, os:timestamp(), open}.

-spec status(txid_entry()) -> open | commit | abort.
status(Entry) -> element(5, Entry).

-spec set_status(txid_entry(), commit|abort) -> txid_entry().
set_status(Entry, Decision) -> setelement(5, Entry, Decision).

