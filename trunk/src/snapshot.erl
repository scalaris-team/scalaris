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

%% @author Stefan Keidel <keidel@informatik.hu-berlin.de>
%% @doc Generic snapshot-related functions (utils)
%% @version $Id: snapshot.erl 2850 2012-03-12 09:10:51Z stefankeidel85@gmail.com $
-module(snapshot).
-author('keidel@informatik.hu-berlin.de').
-vsn('$Id: snapshot.erl 2850 2012-03-12 09:10:51Z stefankeidel85@gmail.com $').
-include("scalaris.hrl").

-export([snapshot_is_done/1,on_do_snapshot/3,on_local_snapshot_is_done/1]).

-spec snapshot_is_done(dht_node_state:state()) -> boolean().
snapshot_is_done(DHTNodeState) ->
    SnapState = dht_node_state:get(DHTNodeState, snapshot_state),
    DB = dht_node_state:get(DHTNodeState,db),
    case {snapshot_state:is_in_progress(SnapState),?DB:snapshot_is_running(DB),?DB:snapshot_is_lockfree(DB)} of
        {true,true,0} -> true;
        _ -> false
    end.
    
-spec on_do_snapshot(non_neg_integer(),any(),dht_node_state:state()) -> dht_node_state:state().
on_do_snapshot(SnapNumber, Leader, DHTNodeState) ->
    SnapState = dht_node_state:get(DHTNodeState, snapshot_state),
    case snapshot_state:is_in_progress(SnapState) of
        true ->
            case snapshot_state:get_number(SnapState) < SnapNumber of
                true ->
                    %TODO: send error message to leader!
                    NewState = delete_and_init_snapshot(SnapNumber,Leader,DHTNodeState);
                false -> % the incoming snapshot is current or old
                    case snapshot_state:get_number(SnapState) =:= SnapNumber of
                        true ->
                            % additional msg for current snapshot -> add leader to dht node state for later messaging
                            NewSnapState = snapshot_state:add_leader(SnapState, Leader),
                            NewState = dht_node_state:set_snapshot_state(DHTNodeState, NewSnapState);
                        false ->
                            % old snapshot -> ignore (or error msg?)
                            NewState = DHTNodeState
                    end
            end;
        false ->
            % no snapshot is progress -> init new
            NewState = delete_and_init_snapshot(SnapNumber,Leader,DHTNodeState)
    end,
    % check if snapshot is already done (i.e. there were no active transactions when the snapshot arrived)
    case snapshot:snapshot_is_done(NewState) of
        true ->
            comm:send(self(), {local_snapshot_is_done});
        false ->
            ok
    end,
    % return
    NewState.

-spec on_local_snapshot_is_done(dht_node_state:state()) -> dht_node_state:state().
on_local_snapshot_is_done(DHTNodeState) ->
    Db = dht_node_state:get(DHTNodeState,db),
    SnapState = dht_node_state:get(DHTNodeState,snapshot_state),
    
    % collect local state and send it
    _Data = ?DB:join_snapshot_data(Db),
    _Leaders = snapshot_state:get_leaders(SnapState),
    %lists:foreach(msg(Data), Leaders), TODO: send stuff

    % cleanup
    NewDB = ?DB:delete_snapshot(dht_node_state:get(DHTNodeState,db)),
    NewSnapState = snapshot_state:stop_progress(SnapState),
    NewState = dht_node_state:set_snapshot_state(DHTNodeState, NewSnapState),

    dht_node_state:set_db(NewState, NewDB).

-spec delete_and_init_snapshot(non_neg_integer(),any(),dht_node_state:state()) -> dht_node_state:state().
delete_and_init_snapshot(SnapNumber,Leader,DHTNodeState) ->
    NewDB = ?DB:init_snapshot(?DB:delete_snapshot(dht_node_state:get(DHTNodeState,db))),
    TmpSnapState = snapshot_state:new(SnapNumber, true, [Leader]),
    TmpState = dht_node_state:set_db(DHTNodeState, NewDB),
    dht_node_state:set_snapshot_state(TmpState, TmpSnapState).

