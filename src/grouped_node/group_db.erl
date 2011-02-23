%  @copyright 2007-2011 Zuse Institute Berlin

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
%% @version $Id$
-module(group_db).
-author('schuett@zib.de').
-vsn('$Id$').

-include("scalaris.hrl").

-export([new_empty/0, new_replica/0,
         get_version/2, get_size/1,
         write/4, read/2,
         get_chunk/3,
         % repair
         repair/7, start_repair_job/2, repair_timeout/3,
         % cleanup after split
         prune_out_of_range_entries/2, delete_chunk/3]).

-type(mode_type() :: is_current | is_filling).

-type(repair_job() :: {UUID::any(), Interval::any(), Members::any(), Version::non_neg_integer()} | none).
-type(state() :: {Mode::mode_type(), DB::?DB:db(), RepairState::repair_job()}).

-ifdef(with_export_type_support).
-export_type([state/0]).
-endif.

-spec new_empty() -> {is_current, ?DB:db(), none}.
new_empty() ->
    % @todo
    {is_current, ?DB:new(), none}.

-spec new_replica() -> {is_filling, ?DB:db(), none}.
new_replica() ->
    % @todo
    {is_filling, ?DB:new(), none}.

-spec write(state(), ?RT:key(), any(), non_neg_integer()) ->
    state().
write({Mode, DB, Job}, Key, Value, Version) ->
    % @todo only if newer
    {Mode, ?DB:write(DB, Key, Value, Version), Job}.

-spec read(state(), ?RT:key()) -> {value, any()} | is_not_current.
read({is_current, DB, none}, HashedKey) ->
    {value, ?DB:read(DB, HashedKey)};
read({is_filling, _DB, _}, _Hashed_Key) ->
    is_not_current.

-spec get_version(state(), ?RT:key()) -> non_neg_integer() | unknown | not_ready.
get_version({is_current, DB, none}, Key) ->
    case ?DB:get_entry2(DB, Key) of
        {true, Entry} ->
            db_entry:get_version(Entry);
        {false, _} ->
            unknown
    end;
get_version({is_filling, DB, _}, Key) ->
    case ?DB:get_entry2(DB, Key) of
        {true, Entry} ->
            db_entry:get_version(Entry);
        {false, _} ->
            not_ready
    end.

-spec get_size(state()) -> non_neg_integer().
get_size({_, DB, _}) ->
    ?DB:get_load(DB).

-spec get_chunk(state(), Interval::intervals:interval(), ChunkSize::non_neg_integer()) ->
    is_not_current | {intervals:interval(), ?DB:db_as_list()}.
get_chunk({is_filling, _DB, _}, _Interval, _ChunkSize) ->
    is_not_current;
get_chunk({is_current, DB, none}, Interval, ChunkSize) ->
    % @todo
    ?DB:get_chunk(DB, Interval, ChunkSize).
    %is_not_current.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% repair code
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% @doc received chunk from other group member -> repair local db
-spec repair(Error::ok | is_not_current, GivenInterval::intervals:interval(),
             RemainingInterval::intervals:interval(),
             Chunk::?DB:db_as_list(), Sender::comm:mypid(), UUID::util:global_uid(),
             State::group_state:state())->
    group_state:state().
repair(is_not_current, _GivenInterval, RemainingInterval, [], _Sender, UUID, State) ->
    % partner is not current -> retrigger
    case group_state:get_db(State) of
        {is_current, _DB, none} -> % we are already done
            State;
        {_Mode, _DB, {UUID, _Interval, Members, Version} = _Job} ->
            io:format("repair: is_not_current -> retriggering~n", []),
            trigger_repair(Members, RemainingInterval, Version, UUID),
            State
    end;
repair(ok, _GivenInterval, RemainingInterval, Chunk, _Sender, UUID, State) ->
    case group_state:get_db(State) of
        {is_current, _, none} -> % repair has already finished
            State;
        {_Mode, DB, {UUID, _RemainingInterval, Members, Version} = _Job} ->
            % apply chunk
            NewDB = apply_chunk(DB, Chunk),
            % do we have to continue?
            io:format("rest interval ~p~n", [RemainingInterval]),
            case intervals:is_empty(RemainingInterval) of
                true -> % done
                    io:format("done~n", []),
                    DB2 = {is_current, NewDB, none},
                    group_state:set_db(State, DB2);
                false -> % continue
                    trigger_repair(Members, RemainingInterval, Version, UUID),
                    DB2 = {is_filling, NewDB, {UUID, RemainingInterval, Members, Version}},
                    group_state:set_db(State, DB2)
            end
    end.

% @doc start to repair the database
-spec start_repair_job(DB::{is_filling, ?DB:db(), none}, repair_job()) ->
    state().
start_repair_job({Mode, DB, none}, {UUID, Interval, Members, Version} = Job) ->
    trigger_repair(Members, Interval, Version, UUID),
    {Mode, DB, Job}.

% @doc timeout message from last chunk request, maybe we didn't get a
% chunk in time
-spec repair_timeout(State::group_state:state(), util:global_uid(),
                     Interval::intervals:interval()) ->
    group_state:state().
repair_timeout(State, UUID, Interval) ->
    case group_state:get_db(State) of
        {_Mode, _DB, none} ->
            State;
        {_Mode, _DB, {UUID, NextInterval, Members, Version} = _Job} ->
            case Interval == NextInterval of
                false -> State;
                true -> trigger_repair(Members, Interval, Version, UUID), State
            end
    end.

-spec trigger_repair(list(), intervals:interval(), any(), util:global_uid()) ->
    ok.
trigger_repair(_Members, nil, _Version, _UUID) ->
    nil = nil2;
trigger_repair(Members, Interval, Version, UUID) ->
    Msg = {db_repair_request, Interval, config:read(group_repair_chunk_size),
           Version, UUID, comm:this()},
    Candidate = util:randomelem(Members),
    comm:send(Candidate, Msg), % hd(Members) would be me
    TimeoutMsg = {group_repair, timeout, UUID, Interval},
    comm:send_local_after(config:read(group_repair_timeout), self(), TimeoutMsg),
    ok.

-spec apply_chunk(?DB:db(), ?DB:db_as_list()) -> ?DB:db().
apply_chunk(DB, []) ->
    DB;
apply_chunk(DB, [Entry | Rest]) ->
    Key = db_entry:get_key(Entry),
    NewVersion = db_entry:get_version(Entry),
    DB2 = case ?DB:get_entry2(DB, Key) of
              {true, OldEntry} ->
                  OldVersion = db_entry:get_version(OldEntry),
                  case OldVersion < NewVersion of
                      true -> ?DB:set_entry(DB, Entry);
                      false  -> DB
                  end;
              {false, _} -> ?DB:set_entry(DB, Entry)
          end,
    apply_chunk(DB2, Rest).

-spec prune_out_of_range_entries(state(), intervals:interval()) ->
    state().
prune_out_of_range_entries(State, Range) ->
    ChunkSize = config:read(group_delete_chunk_size),
    Complement = intervals:minus(intervals:all(), Range),
    comm:send_local(self(), {db_delete_chunked, Complement, ChunkSize}),
    State.

-spec delete_chunk(state(), intervals:interval(), pos_integer()) ->
    state().
delete_chunk({is_current, DB, none} = State, Range, ChunkSize) ->
    {Next, DB2} = ?DB:delete_chunk(DB, Range, ChunkSize),
    case intervals:is_empty(Next) of
        true ->
            {is_current, DB2, none};
        false ->
            comm:send_local(self(), {db_delete_chunked, Next, ChunkSize}),
            {is_current, DB2, none}
    end.
