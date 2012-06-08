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

-ifdef(DEBUG).
-define(int_or_atom(Int, Atom), Atom).
-else.
-define(int_or_atom(Int, Atom), Int).
-endif.

%% lookup
-define(lookup_aux_atom, lookup_aux).
-define(lookup_aux, ?int_or_atom(01, ?lookup_aux_atom)).

-define(lookup_fin_atom, lookup_fin).
-define(lookup_fin, ?int_or_atom(02, ?lookup_fin_atom)).

%% dht_node
-define(get_key_with_id_reply_atom, get_key_with_id_reply).
-define(get_key_with_id_reply, ?int_or_atom(21, ?get_key_with_id_reply_atom)).

%% paxos
-define(proposer_accept_atom, proposer_accept).
-define(proposer_accept, ?int_or_atom(41, ?proposer_accept_atom)).

-define(acceptor_accept_atom, acceptor_accept).
-define(acceptor_accept, ?int_or_atom(42, ?acceptor_accept_atom)).

%% transactions
-define(register_TP_atom, register_TP).
-define(register_TP, ?int_or_atom(61, ?register_TP_atom)).

-define(tx_tm_rtm_init_RTM_atom, tx_tm_rtm_init_RTM).
-define(tx_tm_rtm_init_RTM, ?int_or_atom(62, ?tx_tm_rtm_init_RTM_atom)).

-define(tp_do_commit_abort_atom, tp_do_commit_abort).
-define(tp_do_commit_abort, ?int_or_atom(63, ?tp_do_commit_abort_atom)).

-define(tx_tm_rtm_delete_atom, tx_tm_rtm_delete).
-define(tx_tm_rtm_delete, ?int_or_atom(64, ?tx_tm_rtm_delete_atom)).

-define(tp_committed_atom, tp_committed).
-define(tp_committed, ?int_or_atom(65, ?tp_committed_atom)).

-define(tx_state_atom, tx_state).
-define(tx_state, ?int_or_atom(66, ?tx_state_atom)).

-define(tx_id_atom, tx_id).
-define(tx_id, ?int_or_atom(67, ?tx_id_atom)).

-define(tx_item_id_atom, tx_item_id).
-define(tx_item_id, ?int_or_atom(68, ?tx_item_id_atom)).

-define(tx_item_state_atom, tx_item_state).
-define(tx_item_state, ?int_or_atom(69, ?tx_item_state_atom)).

-define(commit_client_id_atom, commit_client_id).
-define(commit_client_id, ?int_or_atom(70, ?commit_client_id_atom)).

-define(undecided_atom, undecided).
-define(undecided, ?int_or_atom(71, ?undecided_atom)).

-define(prepared_atom, prepared).
-define(prepared, ?int_or_atom(72, ?prepared_atom)).

-define(commit_atom, commit).
-define(commit, ?int_or_atom(73, ?commit_atom)).

-define(abort_atom, abort).
-define(abort, ?int_or_atom(74, ?abort_atom)).
