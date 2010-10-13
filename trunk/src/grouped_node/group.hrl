-type(proposal() :: {group_node_join, Pid::comm:mypid(), Acceptor::comm:mypid(),
                      Learner::comm:mypid()}
      | {group_node_remove, Pid::comm:mypid()}
      | {read , Key::?RT:key(), Pid::comm:mypid(), Value::any(),
         Proposer::comm:mypid()}
      | {write, Key::?RT:key(), Pid::comm:mypid(), Value::any(),
         Proposer::comm:mypid()}
      | {group_split, Proposer::comm:mypid(), SplitKey::?RT:key(),
         LeftGroup::list(comm:mypid()), RightGroup::list(comm:mypid())}).

-type(group_id() :: non_neg_integer()).

-type(paxos_id() :: {group_id(), pos_integer()}).

-type(mode_type() :: joining | joined).

-type(state() :: {Mode::mode_type(),
                  NodeState::group_local_state:local_state(),
                  GroupState::group_state:group_state(),
                  TriggerState::trigger:state()}).

-type(joined_state() :: {joined,
                         NodeState::dht_node_state:state(),
                         GroupState::group_state:group_state(),
                         TriggerState::trigger:state()}).


-type(group_node() :: {GroupId::group_id(),
                       Version::non_neg_integer(),
                       Members::list(comm:mypid())}).

-type(decision_hint() :: my_proposal_won | had_no_proposal).

