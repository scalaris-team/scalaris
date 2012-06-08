%  @copyright 2012 Zuse Institute Berlin
%  @end
%
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
%%-------------------------------------------------------------------
%% File    message_tags.hrl
%% @author Nico Kruber <kruber@zib.de>
%% @doc    Defines integers for some message tags on the hot path to reduce
%%         overhead (atoms are send as strings!).
%% @end
%%-------------------------------------------------------------------
%% @version $Id$

%% TAKE EXTRA CARE THAT ALL VALUES ARE UNIQUE! %%

%% lookup
-define(lookup_aux_atom, lookup_aux).
-define(lookup_aux, 01).

-define(lookup_fin_atom, lookup_fin).
-define(lookup_fin, 02).

%% dht_node
-define(get_key_with_id_reply_atom, get_key_with_id_reply).
-define(get_key_with_id_reply, 21).

%% paxos
-define(proposer_accept_atom, proposer_accept).
-define(proposer_accept, 41).

-define(acceptor_accept_atom, acceptor_accept).
-define(acceptor_accept, 42).

%% transactions
-define(register_TP_atom, register_TP).
-define(register_TP, 61).

-define(tx_tm_rtm_init_RTM_atom, tx_tm_rtm_init_RTM).
-define(tx_tm_rtm_init_RTM, 62).

-define(tp_do_commit_abort_atom, tp_do_commit_abort).
-define(tp_do_commit_abort, 63).

-define(tx_tm_rtm_delete_atom, tx_tm_rtm_delete).
-define(tx_tm_rtm_delete, 64).

-define(tp_committed_atom, tp_committed).
-define(tp_committed, 65).
