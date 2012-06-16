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

%% @author Thorsten Schuett <schuett@zib.de>
%% @author Florian Schintke <schintke@zib.de>

%% @doc The rbr module implements a round based register using paxos,
%% distributed in the DHT. The redundant storage (acceptors) are
%% defined by the key based addressing of the DHT.  Several paxos
%% instances can be run on the same key in sequence. A callback is
%% used to check whether a proposed next instance is a valid successor
%% for the current state.

%% @end
%% @version $Id$
-module(rbr).
-author('schuett@zib.de').
-author('schintke@zib.de').
-vsn('$Id ').

-export([qread/4, qwrite/3, qwrite_fast/3]).
-export([on/2]).
-export([new_state/0]).

-include("scalaris.hrl").
-include("client_types.hrl").

-ifdef(with_export_type_support).
-export_type([state/0]).
-endif.

-type state() :: tuple().

-type database() ::
        kv_db
      | lease_db.

-type lookup_type() ::
        consistent
      | consistent_or_cquorum.

%@doc Trigger a quorum read for the given key and the given database
-spec qread(comm:erl_local_pid(), ?RT:key(), database(), lookup_type()) -> ok.
qread(ReplyAsPid, Key, DB, LookupType) ->
    comm:send_local(self(), {rbr, qread, ReplyAsPid, Key, DB, LookupType}).

-spec on(comm:message(), state()| {data_node:consistency(), state()}) -> state().
on({rbr, qread, ReplyAsPid, Key, DB, LookupType}, TableName) ->
    RKeys = ?RT:get_replica_keys_with_index(Key),

    ReqId = uid:get_global_uid(),
    RBRPid = comm:reply_as(comm:this(), 3, {rbr, qread_reply, ReqId, '_'}),
    ColState =
        case LookupType of
            consistent  ->
                _ = [ lookup:consistent(
                    RKey, {acceptor, {DB, Nth}, get_key, Key, Nth, DB, RBRPid})
                  || {RKey, Nth} <- RKeys ],
                {_Counter = 0, _Vers = -2, _Val = '_'};
            consistent_or_cquorum ->
                _ = [ lookup:best_effort_consistent(
                    RKey, {acceptor, DB, get_key, Key, Nth, DB, RBRPid})
                  || {RKey, Nth} <- RKeys ],
                _ColState = []
        end,

    pdb:set({{rbr, ReqId}, LookupType, ColState, ReplyAsPid}, TableName),

    TableName;

%% collect majority of replies
%% replies may be consistent or not consistent
on({rbr, qread_reply, ReqId, {acceptor, get_key_reply, InVers, InVal}},
   {Consistency, TableName}) ->
    case pdb:get({rbr, ReqId}, TableName) of
        undefined -> ok; %% drop outdated messages
        {_, LookupType, ColState, ReplyAsPid} ->
            NewColState = add_to_colstate(ColState, InVers, InVal, Consistency),

            case have_majority(NewColState) of
                false ->
                    NewReqState =
                        {{rbr, ReqId}, LookupType, NewColState, ReplyAsPid},
                    pdb:set(NewReqState, TableName);
                true ->
                    pdb:delete({rbr, ReqId}, TableName),
                    case decision(NewColState) of
                        {done, NewVers, NewVal} ->
                            comm:send(ReplyAsPid, {qread_reply, NewVers, NewVal});
                        failed ->
                            comm:send(ReplyAsPid, {qread_reply, please_retry})
                    end
          end
    end,
    TableName.

have_majority({Counter, _, _}) ->
    Counter =:= quorum:majority_for_accept(config:read(replication_factor));
have_majority(L) ->
    length(L)
        =:= quorum:majority_for_accept(config:read(replication_factor)).

add_to_colstate({Counter, Vers, Val}, InVers, InVal, _Consistency) ->
    case Vers < InVers of
        true  -> {Counter + 1, InVers, InVal};
        false -> {Counter + 1, Vers, Val}
    end;
add_to_colstate(L, InVers, InVal, Consistency) ->
    [ {InVers, InVal, Consistency} | L].

decision({_Counter, Vers, Val}) -> {done, Vers, Val};

decision(L) ->
    OnlyConsistent = lists:all(fun(X) -> element(3, X) == consistent end, L),
    case OnlyConsistent of
        true ->
            MaxVers =
                lists:foldl(fun(X, Acc) -> util:max(element(3, X), Acc) end, -1, L),
            Matches = lists:filter(fun(X) -> element(3, X) =:= MaxVers end, L),
            {Vers, Val, _} = hd(Matches),
            {done, Vers, Val};
        false ->
            get_cquorum(L)
    end.

get_cquorum(L) ->
    MaxVers =
        lists:foldl(fun(X, Acc) -> util:max(element(3, X), Acc) end, -1, L),
    Matches = lists:filter(fun(X) -> element(3, X) =:= MaxVers end, L),
    Maj = quorum:majority_for_accept(config:read(replication_factor)),
    case length(Matches) =:= Maj of
        true ->
            {Vers, Val, _} = hd(Matches),
            {done, Vers, Val};
        false ->
            failed
    end.

%% lookup: consistent / consistent_or_cquorum / cquorum
%% existance: must, must_not, may


-spec qwrite(comm:erl_local_pid(), ?RT:key(), client_value()) -> ok.
qwrite(_ReplyPid, _Key, _Val) ->
    ok.

-spec qwrite_fast(comm:erl_local_pid(), ?RT:key(), client_value()) -> ok.
qwrite_fast(_ReplyPid, _Key, _Val) ->

%    only for Lease renewal;
%    only for tm ? but there may be several concurrent tms working on same key.

% 2nd paxos for commit/abort: tm can use fast paxos, because locks are
% used for concurrency control

    ok.

-spec new_state() -> state().
new_state() ->
    {pdb:new(?MODULE, [set, private])}.
