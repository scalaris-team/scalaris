% @copyright 2012-2015 Zuse Institute Berlin,

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
-export([new/3]).
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

-export_type([txid/0]).

-spec read(txid(), comm:erl_local_pid()) -> ok.
read(Key, ReplyTo) ->
    %% decide which db is responsible, ie. if the key is from
    %% the first quarter of the ring, use lease_db1, if from 2nd
    %% quarter -> use lease_db2, ...
    DB = rbrcseq:get_db_for_id(tx_id, Key),
    %% perform qread
    rbrcseq:qread(DB, ReplyTo, Key, ?MODULE).

-spec new(txid(), [client_key()], comm:erl_local_pid()) -> ok.
new(Key, InvolvedKeys, ReplyTo) ->
    DB = rbrcseq:get_db_for_id(tx_id, Key),
    Value = new_entry(Key, InvolvedKeys, comm:make_global(ReplyTo)),
    RBRCseqPid = comm:make_global(pid_groups:find_a(DB)),
    rbrcseq:qwrite_fast(DB, ReplyTo, Key, ?MODULE,
                        fun txid_on_cseq:is_valid_new/3, Value,
                        pr:smallest_round(RBRCseqPid), prbr_bottom).

-spec decide(txid(), commit | abort, comm:erl_local_pid(),
             pr:pr(), any()) -> ok.
decide(Key, Decision, ReplyTo, Round, OldVal) ->
    DB = rbrcseq:get_db_for_id(tx_id, Key),
    rbrcseq:qwrite_fast(DB, ReplyTo, Key, ?MODULE,
                        fun txid_on_cseq:is_valid_decide/3, Decision,
                        Round, OldVal).

-spec delete(txid(), comm:erl_local_pid()) -> ok.
delete(Key, ReplyTo) ->
    DB = rbrcseq:get_db_for_id(tx_id, Key),
    rbrcseq:qwrite(DB, ReplyTo, Key, ?MODULE,
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
                      prbr:write_filter(), New::prbr_bottom %% =:= txid_entry()
                                                ) ->
                               {boolean(), null}.
is_valid_delete(prbr_bottom, _WriteFilter, prbr_bottom) ->
    {false, null}; %% not necessary to delete deleted entry
%%is_valid_delete(ExistingEntry, _WriteFilter, ExistingEntry) ->
%%    %% deleting same entry is ok (write through may happened)
%%    {false, null};
is_valid_delete(ExistingEntry, _WriteFilter, prbr_bottom) ->
    case status(ExistingEntry) of
        open -> {false, null};
        _ -> {true, null} %% comit | abort
    end.

%% write filters
%% WF(old_dbdata(), UpdateInfo, value()) -> {dbdata(), val_passed_to_caller()}.
-spec wf_decide(txid_entry(), null, commit | abort) -> {txid_entry(), none}.
wf_decide(Old, null, Decision) ->
    {set_status(Old, Decision), none}.

%% abstract data type: txid_entry
-spec new_entry(?RT:key(), [client_key()], comm:mypid()) -> txid_entry().
new_entry(TxId, Keys, Client) ->
    {TxId, Client, Keys, os:timestamp(), open}.

-spec status(txid_entry()) -> open | commit | abort.
status(Entry) -> element(5, Entry).

-spec set_status(txid_entry(), commit|abort) -> txid_entry().
set_status(Entry, Decision) -> setelement(5, Entry, Decision).

