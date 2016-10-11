%  @copyright 2015-2016 Zuse Institute Berlin

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

%% @author Nico Kruber <kruber@zib.de>
%% @doc    Record definitions and type defs for the replica repair evaluation
%%         modules.

-include("scalaris.hrl").
-include("record_helpers.hrl").

-type ring_type() :: uniform | random.
-type fail_distribution() :: random | uniform | {binomial, P::float()}.
-type data_distribution() :: random | uniform | {binomial, P::float()}.
-record(scenario,
        {
         ring_type                  :: ring_type(),
         data_distribution          :: data_distribution() | undefined, % undefined only temporary!
         data_failure_type          :: db_generator:failure_type() | undefined, % undefined only temporary!
         fail_distribution          :: fail_distribution() | undefined, % undefined only temporary!
         data_type                  :: db_generator:db_type(),
         trigger_prob       = 100   :: 0..100
        }).
-type scenario() :: #scenario{}.

-type step_param() :: node_count | data_count | fprob | rounds |
                      recon_fail_rate | expected_delta |
                      merkle_bucket | merkle_branch | merkle_num_trees |
                      art_corr_factor | art_leaf_fpr | art_inner_fpr.
-type step_size() :: pos_integer() | float().
-type step_inc() :: step_size() | {power, Base::pos_integer()}.
-type fail_rate() :: float() | pos_integer().

-record(ring_config, {
                      node_count        = ?required(rc_config, node_count) :: integer(),
                      data_count        = ?required(rc_config, data_count) :: integer(),
                      data_failure_rate = ?required(rc_config, fprob)      :: 0..100,       % probability of data failures
                      fquadrants        = all                              :: all | [rt_beh:segment()],
                      round             = 1                                :: integer()
                     }).
-type ring_config() :: #ring_config{}.

-record(rc_config, {
                    recon_method    = ?required(rc_config, recon_method) :: rr_recon:method(),
                    recon_fail_rate = 0.1                                :: fail_rate(),
                    expected_delta  = 100                                :: number() | as_fprob,
                    merkle_bucket   = 25                                 :: pos_integer(), %shared with art
                    merkle_branch   = 4                                  :: pos_integer(), %shared with art
                    merkle_num_trees= 1                                  :: pos_integer(),
                    art_corr_factor = 2                                  :: non_neg_integer(),
                    art_leaf_fpr    = 0.1                                :: float(),
                    art_inner_fpr   = 0.01                               :: float()
                   }).
-type rc_config() :: #rc_config{}.

-type ring_setup() :: {scenario(), ring_config(), rc_config()}.
