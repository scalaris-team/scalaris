% @copyright 2012-2015 Zuse Institute Berlin

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

%% @author Maik Lange <lakedaimon300@googlemail.com>
%% @doc    Administrative helper functions for replica repair evaluation
%%         through the provided methods.
%% @version $Id:  $
-module(rr_eval_admin).

-include("scalaris.hrl").
-include("record_helpers.hrl").

% for external scripts
-export([% trivial
         trivial/2, trivial/6,
         trivial_ddists_fdists/2, trivial_ddists_fdists/6,
         trivial_scale/2, trivial_scale/5,
         % shash
         shash/2, shash/6,
         shash_ddists_fdists/2, shash_ddists_fdists/6,
         shash_scale/2, shash_scale/5,
         % bloom
         bloom/2, bloom/6,
         bloom_ddists_fdists/2, bloom_ddists_fdists/6,
         bloom_scale/2, bloom_scale/5,
         % merkle
         merkle/4, merkle/8,
         merkle_ddists_fdists/4, merkle_ddists_fdists/8,
         merkle_scale/4, merkle_scale/7,
         % art
         art/3, art/5, 
         art_scale/3, art_scale/5,
         system/1, system/2]).

% for debugging:
-export([ring_build/0, get_node_interval/1,
         get_db/1, get_db_status/1, print_db_status/1]).

% for other modules:
-export([get_bandwidth/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TYPES
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-export_type([scenario/0, ring_config/0, rc_config/0, ring_setup/0,
              ring_type/0, data_distribution/0, fail_distribution/0]).

-type ring_type() :: uniform | random.

-type init_mp() :: {Missing::non_neg_integer(),
                    Outdated::non_neg_integer()}.

-type fail_distribution() :: random | uniform | {binomial, P::float()}.
-type data_distribution() :: random | uniform | {binomial, P::float()}.
-record(scenario,
        {
         ring_type                  :: ring_type(),
         data_distribution          :: data_distribution(),
         data_failure_type          :: db_generator:failure_type(),
         fail_distribution          :: fail_distribution(),
         data_type                  :: db_generator:db_type(),
         trigger_prob       = 100   :: 0..100
        }).
-type scenario() :: #scenario{}.

-type step_param() :: node_count | data_count | fprob | rounds |
                      recon_p1e | merkle_bucket | merkle_branch | art_corr_factor |
                      art_leaf_fpr | art_inner_fpr.
-type step_size() :: pos_integer() | float().

-record(ring_config, {
                      node_count         = ?required(rc_config, node_count)   :: integer(),
                      data_count         = ?required(rc_config, data_count)   :: integer(),
                      data_failure_prob  = ?required(rc_config, fprob)        :: 0..100,        % probability of data failure
                      fquadrants         = all                                :: all | [1..4],
                      round              = 1                                  :: integer()
                     }).
-type ring_config() :: #ring_config{}.

-record(rc_config, {
                    recon_method    = ?required(rc_config, recon_method) :: rr_recon:method(),
                    recon_p1e       = 0.1                                :: float(),
                    merkle_bucket   = 25                                 :: pos_integer(), %shared with art
                    merkle_branch   = 4                                  :: pos_integer(), %shared with art
                    art_corr_factor = 2                                  :: non_neg_integer(),
                    art_leaf_fpr    = 0.1                                :: float(),
                    art_inner_fpr   = 0.01                               :: float(),
                    align_to_bytes  = true                               :: boolean()
                   }).
-type rc_config() :: #rc_config{}.

-type ring_setup() :: {scenario(), ring_config(), rc_config()}.

-type eval_option() :: {eval_dir, string()} |
                       {filename, string()} |
                       {eval_time, {Hour::integer(), Min::integer(), Sec::integer()}} |
                       {start_ep_id, integer()} |
                       {ep_file, file:io_device()} |
                       {mp_file, file:io_device()}.
-type eval_options() :: [eval_option()].

-type sync_result() :: {[rr_eval_point:eval_point()],       %Sync EvalPoints 
                        [rr_eval_point:measure_point()],    %Sync MeasurePoints
                        NextEPId::non_neg_integer()}.       %Next unique EvalPointId

-dialyzer([{no_match, [get_param_value/2, set_params/3,
                       init_rc_conf/3, init_ring_conf/3]},
           {no_return, [system/1, system/2]}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(DBSizeKey, rr_eval_admin_dbsize).    %Process Dictionary Key for generated db size
-define(REP_FACTOR, 4).
-define(EVAL_DIR, "../../eval"). %execution path is ~/scalaris/bin/
-define(TAB, 9).

-define(BINOM, 0.2).       %binomial(N, p) - this is p
-define(EVAL_REPEATS, 10).  %every measurement is average over X-runs

-define(EVAL_DDISTS, [random, {binomial, ?BINOM}]).
-define(EVAL_FTYPES, [update, regen]).
-define(EVAL_FDISTS, [random, {binomial, ?BINOM}]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec ring_build() -> ok.
ring_build() ->
    RingType = uniform,
    NodeCount = 4,
    DBType = random,
    DBSize = 10000,
    FType = update,
    FProb = 50,
    DBDist = uniform,
    
    {RingT, _} = util:tc(fun() -> make_ring(RingType, NodeCount) end),
    {FillT, _} = util:tc(fun() -> fill_ring(DBType, DBSize,
                                            [{ftype, FType}, {fprob, FProb}, {distribution, DBDist}])
                         end),
    io:format("RingTime: ~.4fs~nFillTime=~.4fs~n", [RingT / 1000000, FillT / 1000000]),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec default_dir_and_name(Alg::atom()) -> {Dir::string(), FileName::string()}.
default_dir_and_name(Alg) ->
    {{YY, MM, DD}, {Hour, Min, Sec}} = erlang:localtime(),
    SubDir = io_lib:format("~p-~p-~p_~p-~p-~p_~p", [YY, MM, DD, Hour, Min, Sec, Alg]),
    {filename:join([?EVAL_DIR, SubDir]), "results.dat"}.

-spec gen_setup(DDists::[data_distribution()],
                FTypes::[db_generator:failure_type()],
                FDists::[fail_distribution()],
                scenario(), ring_config(), [rc_config()]) -> [ring_setup()].
gen_setup(DDists, FTypes, FDists, Scen, Ring, RCList) ->
    [{Scen#scenario{fail_distribution = FDist,
                    data_distribution = DDist,
                    data_failure_type = FType}, Ring, RC}
     || DDist <- DDists, FDist <- FDists, RC <- RCList, FType <- FTypes].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% TRIVIAL EVAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec trivial(N::pos_integer(), P1E::float()) -> ok.
trivial(N, P1E) ->
    {Dir, FileName} = default_dir_and_name(trivial),
    trivial(Dir, FileName, N, P1E, false, 2).

-spec trivial(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
              AlignToBytes::boolean(), StepSize::step_size() | power) -> ok.
trivial(Dir, FileName, N, P1E, AlignToBytes, StepSize) ->
    trivial(Dir, FileName, N, P1E, [random], [random], AlignToBytes, StepSize).

-spec trivial_ddists_fdists(N::pos_integer(), P1E::float()) -> ok.
trivial_ddists_fdists(N, P1E) ->
    {Dir, FileName} = default_dir_and_name(trivial),
    trivial_ddists_fdists(Dir, FileName, N, P1E, false, 2).

-spec trivial_ddists_fdists(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
                            AlignToBytes::boolean(),
                            StepSize::step_size() | power) -> ok.
trivial_ddists_fdists(Dir, FileName, N, P1E, AlignToBytes, StepSize) ->
    trivial(Dir, FileName, N, P1E, ?EVAL_DDISTS, ?EVAL_FDISTS, AlignToBytes, StepSize).

-spec trivial(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
              DDists::[data_distribution()], FDists::[fail_distribution()],
              AlignToBytes::boolean(), StepSize::step_size() | power) -> ok.
trivial(Dir, FileName, N, P1E, DDists, FDists, AlignToBytes, StepSize) ->
    Scenario = #scenario{ ring_type = uniform,
                          data_type = random },
    PairRing = #ring_config{ data_count = N,
                             node_count = 4,
                             fquadrants = [1,3],
                             data_failure_prob = 3,
                             round = 1 },
    Options = [{eval_dir, Dir}, {filename, FileName}],
    
    Trivial = #rc_config{ recon_method = trivial, recon_p1e = P1E,
                          align_to_bytes = AlignToBytes },
    
    eval(pair,
         gen_setup(DDists, ?EVAL_FTYPES, FDists, Scenario, PairRing, [Trivial]),
         fprob, 5, StepSize, 0, Options),
    ok.

-spec trivial_scale(N::pos_integer(), P1E::float()) -> ok.
trivial_scale(N, P1E) ->
    {Dir, FileName} = default_dir_and_name(trivial),
    trivial_scale(Dir, FileName, N, P1E, false).

-spec trivial_scale(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
                    AlignToBytes::boolean()) -> ok.
trivial_scale(Dir, FileName, N, P1E, AlignToBytes) ->
    Scenario = #scenario{ ring_type = uniform,
                          data_type = random },
    PairRing = #ring_config{ data_count = N,
                             node_count = 4,
                             fquadrants = [1,3],
                             data_failure_prob = 3,
                             round = 1 },
    Options = [{eval_dir, Dir}, {filename, FileName}],
    
    Trivial = #rc_config{ recon_method = trivial, recon_p1e = P1E,
                        align_to_bytes = AlignToBytes },

    eval(pair,
         gen_setup([random], ?EVAL_FTYPES, [random],
                   Scenario, PairRing, [Trivial]),
         data_count, 5, power, N, Options),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% SHASH EVAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec shash(N::pos_integer(), P1E::float()) -> ok.
shash(N, P1E) ->
    {Dir, FileName} = default_dir_and_name(shash),
    shash(Dir, FileName, N, P1E, false, 2).

-spec shash(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
            AlignToBytes::boolean(), StepSize::step_size() | power) -> ok.
shash(Dir, FileName, N, P1E, AlignToBytes, StepSize) ->
    shash(Dir, FileName, N, P1E, [random], [random], AlignToBytes, StepSize).

-spec shash_ddists_fdists(N::pos_integer(), P1E::float()) -> ok.
shash_ddists_fdists(N, P1E) ->
    {Dir, FileName} = default_dir_and_name(shash),
    shash_ddists_fdists(Dir, FileName, N, P1E, false, 2).

-spec shash_ddists_fdists(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
                          AlignToBytes::boolean(),
                          StepSize::step_size() | power) -> ok.
shash_ddists_fdists(Dir, FileName, N, P1E, AlignToBytes, StepSize) ->
    shash(Dir, FileName, N, P1E, ?EVAL_DDISTS, ?EVAL_FDISTS, AlignToBytes, StepSize).

-spec shash(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
            DDists::[data_distribution()], FDists::[fail_distribution()],
            AlignToBytes::boolean(), StepSize::step_size() | power) -> ok.
shash(Dir, FileName, N, P1E, DDists, FDists, AlignToBytes, StepSize) ->
    Scenario = #scenario{ ring_type = uniform,
                          data_type = random },
    PairRing = #ring_config{ data_count = N,
                             node_count = 4,
                             fquadrants = [1,3],
                             data_failure_prob = 3,
                             round = 1 },
    Options = [{eval_dir, Dir}, {filename, FileName}],
    
    SHash = #rc_config{ recon_method = shash, recon_p1e = P1E,
                        align_to_bytes = AlignToBytes },
    
    eval(pair,
         gen_setup(DDists, ?EVAL_FTYPES, FDists, Scenario, PairRing, [SHash]),
         fprob, 5, StepSize, 0, Options),
    ok.

-spec shash_scale(N::pos_integer(), P1E::float()) -> ok.
shash_scale(N, P1E) ->
    {Dir, FileName} = default_dir_and_name(shash),
    shash_scale(Dir, FileName, N, P1E, false).

-spec shash_scale(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
                  AlignToBytes::boolean()) -> ok.
shash_scale(Dir, FileName, N, P1E, AlignToBytes) ->
    Scenario = #scenario{ ring_type = uniform,
                          data_type = random },
    PairRing = #ring_config{ data_count = N,
                             node_count = 4,
                             fquadrants = [1,3],
                             data_failure_prob = 3,
                             round = 1 },
    Options = [{eval_dir, Dir}, {filename, FileName}],
    
    SHash = #rc_config{ recon_method = shash, recon_p1e = P1E,
                        align_to_bytes = AlignToBytes },

    eval(pair,
         gen_setup([random], ?EVAL_FTYPES, [random],
                   Scenario, PairRing, [SHash]),
         data_count, 5, power, N, Options),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% BLOOM EVAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec bloom(N::pos_integer(), P1E::float()) -> ok.
bloom(N, P1E) ->
    {Dir, FileName} = default_dir_and_name(bloom),
    bloom(Dir, FileName, N, P1E, false, 2).

-spec bloom(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
            AlignToBytes::boolean(), StepSize::step_size() | power) -> ok.
bloom(Dir, FileName, N, P1E, AlignToBytes, StepSize) ->
    bloom(Dir, FileName, N, P1E, [random], [random], AlignToBytes, StepSize).

-spec bloom_ddists_fdists(N::pos_integer(), P1E::float()) -> ok.
bloom_ddists_fdists(N, P1E) ->
    {Dir, FileName} = default_dir_and_name(bloom),
    bloom_ddists_fdists(Dir, FileName, N, P1E, false, 2).

-spec bloom_ddists_fdists(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
                          AlignToBytes::boolean(), StepSize::step_size() | power) -> ok.
bloom_ddists_fdists(Dir, FileName, N, P1E, AlignToBytes, StepSize) ->
    bloom(Dir, FileName, N, P1E, ?EVAL_DDISTS, ?EVAL_FDISTS, AlignToBytes, StepSize).

-spec bloom(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
            DDists::[data_distribution()], FDists::[fail_distribution()],
            AlignToBytes::boolean(), StepSize::step_size() | power) -> ok.
bloom(Dir, FileName, N, P1E, DDists, FDists, AlignToBytes, StepSize) ->
    Scenario = #scenario{ ring_type = uniform,
                          data_type = random },
    PairRing = #ring_config{ data_count = N,
                             node_count = 4,
                             fquadrants = [1,3],
                             data_failure_prob = 3,
                             round = 1 },
    Options = [{eval_dir, Dir}, {filename, FileName}],
    
    Bloom = #rc_config{ recon_method = bloom, recon_p1e = P1E,
                        align_to_bytes = AlignToBytes },
    
    eval(pair,
         gen_setup(DDists, ?EVAL_FTYPES, FDists, Scenario, PairRing, [Bloom]),
         fprob, 5, StepSize, 0, Options),
    ok.

-spec bloom_scale(N::pos_integer(), P1E::float()) -> ok.
bloom_scale(N, P1E) ->
    {Dir, FileName} = default_dir_and_name(bloom),
    bloom_scale(Dir, FileName, N, P1E, false).

-spec bloom_scale(DestDir::string(), FileName::string(), N::pos_integer(), P1E::float(),
                  AlignToBytes::boolean()) -> ok.
bloom_scale(Dir, FileName, N, P1E, AlignToBytes) ->
    Scenario = #scenario{ ring_type = uniform,
                          data_type = random },
    PairRing = #ring_config{ data_count = N,
                             node_count = 4,
                             fquadrants = [1,3],
                             data_failure_prob = 3,
                             round = 1 },
    Options = [{eval_dir, Dir}, {filename, FileName}],
    
    Bloom = #rc_config{ recon_method = bloom, recon_p1e = P1E,
                        align_to_bytes = AlignToBytes },

    eval(pair,
         gen_setup([random], ?EVAL_FTYPES, [random],
                   Scenario, PairRing, [Bloom]),
         data_count, 5, power, N, Options),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% MERKLE EVAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec merkle(N::pos_integer(), MBranch::pos_integer(), MBucket::pos_integer(), P1E::float()) -> ok.
merkle(N, MBranch, MBucket, P1E) ->
    {Dir, FileName} = default_dir_and_name(merkle),
    merkle(Dir, FileName, N, MBranch, MBucket, P1E, false, 2).

-spec merkle(DestDir::string(), FileName::string(), N::pos_integer(), MBranch::pos_integer(),
             MBucket::pos_integer(), P1E::float(), AlignToBytes::boolean(),
             StepSize::step_size() | power) -> ok.
merkle(Dir, FileName, N, MBranch, MBucket, P1E, AlignToBytes, StepSize) ->
    merkle(Dir, FileName, N, MBranch, MBucket, P1E, [random], [random],
           AlignToBytes, StepSize).

-spec merkle_ddists_fdists(N::pos_integer(), MBranch::pos_integer(), MBucket::pos_integer(),
                           P1E::float()) -> ok.
merkle_ddists_fdists(N, MBranch, MBucket, P1E) ->
    {Dir, FileName} = default_dir_and_name(merkle),
    merkle_ddists_fdists(Dir, FileName, N, MBranch, MBucket, P1E, false, 2).

-spec merkle_ddists_fdists(DestDir::string(), FileName::string(),
                           N::pos_integer(), MBranch::pos_integer(), MBucket::pos_integer(),
                           P1E::float(), AlignToBytes::boolean(),
                           StepSize::step_size() | power) -> ok.
merkle_ddists_fdists(Dir, FileName, N, MBranch, MBucket, P1E, AlignToBytes, StepSize) ->
    merkle(Dir, FileName, N, MBranch, MBucket, P1E, ?EVAL_DDISTS, ?EVAL_FDISTS,
           AlignToBytes, StepSize).

-spec merkle(DestDir::string(), FileName::string(), N::pos_integer(), MBranch::pos_integer(),
             MBucket::pos_integer(), P1E::float(),
             DDists::[data_distribution()], FDists::[fail_distribution()],
             AlignToBytes::boolean(), StepSize::step_size() | power) -> ok.
merkle(Dir, FileName, N, MBranch, MBucket, P1E, DDists, FDists, AlignToBytes, StepSize) ->
    Scenario = #scenario{ ring_type = uniform,
                          data_type = random },
    PairRing = #ring_config{ data_count = N,
                             node_count = 4,
                             fquadrants = [1,3],
                             data_failure_prob = 3,
                             round = 1 },
    Options = [{eval_dir, Dir}, {filename, FileName}],
    
    Merkle = #rc_config{ recon_method = merkle_tree, recon_p1e = P1E,
                         merkle_branch = MBranch, merkle_bucket = MBucket,
                         align_to_bytes = AlignToBytes },
    
    eval(pair,
         gen_setup(DDists, ?EVAL_FTYPES, FDists, Scenario, PairRing, [Merkle]),
         fprob, 5, StepSize, 0, Options),
    ok.

-spec merkle_scale(N::pos_integer(), MBranch::pos_integer(), MBucket::pos_integer(),
                   P1E::float()) -> ok.
merkle_scale(N, MBranch, MBucket, P1E) ->
    {Dir, FileName} = default_dir_and_name(merkle),
    merkle_scale(Dir, FileName, N, MBranch, MBucket, P1E, false).

-spec merkle_scale(DestDir::string(), FileName::string(), N::pos_integer(), MBranch::pos_integer(),
                   MBucket::pos_integer(), P1E::float(), AlignToBytes::boolean())
        -> ok.
merkle_scale(Dir, FileName, N, MBranch, MBucket, P1E, AlignToBytes) ->
    Scenario = #scenario{ ring_type = uniform,
                          data_type = random },
    PairRing = #ring_config{ data_count = N,
                             node_count = 4,
                             fquadrants = [1,3],
                             data_failure_prob = 3,
                             round = 1 },
    Options = [{eval_dir, Dir}, {filename, FileName}],
    
    Merkle = #rc_config{ recon_method = merkle_tree, recon_p1e = P1E,
                         merkle_branch = MBranch, merkle_bucket = MBucket,
                         align_to_bytes = AlignToBytes },

    eval(pair,
         gen_setup([random], ?EVAL_FTYPES, [random],
                   Scenario, PairRing, [Merkle]),
         data_count, 5, power, N, Options),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ART EVAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec art(MBranch::pos_integer(), MBucket::pos_integer(),
          ACorrFactor::non_neg_integer()) -> ok.
art(MBranch, MBucket, ACorrFactor) ->
    {Dir, FileName} = default_dir_and_name(art),
    art(Dir, FileName, MBranch, MBucket, ACorrFactor).

-spec art(DestDir::string(), FileName::string(), MBranch::pos_integer(),
          MBucket::pos_integer(), ACorrFactor::non_neg_integer()) -> ok.
art(Dir, FileName, MBranch, MBucket, ACorrFactor) ->
    Scenario = #scenario{ ring_type = uniform,
                          data_type = random },
    PairRing = #ring_config{ data_count = 10000,
                             node_count = 4,
                             fquadrants = [1,3],
                             data_failure_prob = 3,
                             round = 1 },
    Options = [{eval_dir, Dir}, {filename, FileName}],
    
    Art = #rc_config{recon_method = art, 
                     merkle_bucket = MBucket, merkle_branch = MBranch,
                     art_corr_factor = ACorrFactor,
                     art_inner_fpr = 0.01, art_leaf_fpr = 0.01},
    
    eval(pair,
         gen_setup([random], ?EVAL_FTYPES, [random], Scenario, PairRing, [Art]),
         fprob, 5, 2, 0, Options),
    ok.

-spec art_scale(MBranch::pos_integer(), MBucket::pos_integer(),
                ACorrFactor::non_neg_integer()) -> ok.
art_scale(MBranch, MBucket, ACorrFactor) ->
    {Dir, FileName} = default_dir_and_name(art),
    art_scale(Dir, FileName, MBranch, MBucket, ACorrFactor).

-spec art_scale(DestDir::string(), FileName::string(), MBranch::pos_integer(),
                MBucket::pos_integer(), ACorrFactor::non_neg_integer()) -> ok.
art_scale(Dir, FileName, MBranch, MBucket, ACorrFactor) ->
    Scenario = #scenario{ ring_type = uniform,
                          data_type = random },
    PairRing = #ring_config{ data_count = 10000,
                             node_count = 4,
                             fquadrants = [1,3],
                             data_failure_prob = 3,
                             round = 1 },
    Options = [{eval_dir, Dir}, {filename, FileName}],
    
    Art = #rc_config{recon_method = art,
                     merkle_bucket = MBucket, merkle_branch = MBranch,
                     art_corr_factor = ACorrFactor, art_inner_fpr = 0.01, art_leaf_fpr = 0.01},  

    eval(pair,
         gen_setup([random], ?EVAL_FTYPES, [random], Scenario, PairRing, [Art]),
         data_count, 10, power, 1000, Options),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% SYSTEM EVAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec system(atom()) -> ok.
system(EvalName) ->
    {{YY, MM, DD}, {Hour, Min, Sec}} = erlang:localtime(),
    Dir = io_lib:format("~s/~p-~p-~p_~p-~p-~p_System", [?EVAL_DIR, YY, MM, DD, Hour, Min, Sec]),
    system(Dir, EvalName).

-spec system(DestDir::string(), atom()) -> ok.
system(Dir, EvalName) ->
    Nodes = 16,
    Scenario = #scenario{ ring_type = uniform,
                          data_type = random },
    Ring = #ring_config{ data_count = 10000,
                         node_count = Nodes,
                         fquadrants = all,
                         data_failure_prob = 4*3,
                         round = 1 },
    
    Bloom0 = #rc_config{ recon_method = bloom, recon_p1e = 0.01 },
    %Bloom = #rc_config{ recon_method = bloom, recon_p1e = 0.1 },
    %Merkle1 = #rc_config{ recon_method = merkle_tree, recon_p1e = 0.01, merkle_branch = 4, merkle_bucket = 4 },
    %Merkle2 = #rc_config{ recon_method = merkle_tree, recon_p1e = 0.01, merkle_branch = 4, merkle_bucket = 1 },
    %Art1 = #rc_config{ recon_method = art, art_corr_factor = 3, merkle_bucket = 4, merkle_branch = 16, art_inner_fpr = 0.001, art_leaf_fpr = 0.01 },
    %Art2 = #rc_config{ recon_method = art, art_corr_factor = 3, merkle_bucket = 2, merkle_branch = 32, art_inner_fpr = 0.01, art_leaf_fpr = 0.2 },

    case EvalName of
        a0 -> eval(sys,
                   gen_setup([uniform], ?EVAL_FTYPES, [random],
                             Scenario, Ring, [Bloom0]),
                   rounds, 2*Nodes, 2*Nodes, 0, [{eval_dir, Dir}, {file_suffix, atom_to_list(EvalName)}, {start_ep_id, 1}])
    end,
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% EVAL
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec eval(Mode::pair | sys,
           [ring_setup()],
           StepP::step_param(),
           StepCount::pos_integer(),
           StepSize::step_size() | power,
           StepInit::step_size(),
           Options::eval_options()) -> ok.
eval(Mode, Setups, StepParam, StepCount, StepSize, Init, _Options) ->
    {{YY, MM, DD}, {Hour, Min, Sec}} = erlang:localtime(),
    Options = [{eval_time, {Hour, Min, Sec}} | _Options],
    
    StartEPId = proplists:get_value(start_ep_id, Options, 0),
    Dir = proplists:get_value(eval_dir, Options, ?EVAL_DIR),
    FileName = proplists:get_value(filename, Options, io_lib:format("~p.dat", [StepParam])),
    
    EPFile = rr_eval_export:create_file([{filename, FileName},
                                      {dir, Dir},
                                      {comment, [io_lib:format("CREATED ~p-~p-~p~c~p:~p:~p", [YY, MM, DD, ?TAB, Hour, Min, Sec])]},
                                      {column_names, rr_eval_point:column_names()}]),
    MPFile = rr_eval_export:create_file([{filename, string:join(["MP_", FileName], "")},
                                      {dir, filename:join([Dir, "raw"])},
                                      {comment, [io_lib:format("CREATED ~p-~p-~p~c~p:~p:~p", [YY, MM, DD, ?TAB, Hour, Min, Sec])]},
                                      {column_names, rr_eval_point:mp_column_names()}]),
    NOptions = [{ep_file, EPFile}, {mp_file, MPFile} | Options],
    
    StartT = erlang:now(),
    
    lists:foldl(
      fun({Scenario, _RingP, _ReconP}, EPId) ->
              ReconP = init_rc_conf(_ReconP, StepParam, Init),
              RingP = init_ring_conf(_RingP, StepParam, Init),

              SetupText = eval_setup_comment(Scenario, RingP, StepParam, StepSize, ?EVAL_REPEATS),
              ReconText = rc_conf_comment(ReconP),
              rr_eval_export:append_ds(EPFile, {[{comment, lists:append(SetupText, [ReconText])}], []}),
              rr_eval_export:append_ds(MPFile, {[{comment, lists:append(SetupText, [ReconText])}], []}),
              
              RingSetup = {Scenario, RingP, ReconP},
              % NOTE - Mode=Sys only supports StepParam=rounds
              %        Mode=Pair does not support StepParam=rounds
              {_EP, _MP, NextEPId} = case Mode of
                                         sys -> system_sync(RingSetup, NOptions, StepCount, EPId);
                                         pair -> pair_sync(RingSetup, NOptions, StepParam, StepSize, StepCount, {[], [], EPId})
                                     end,
              
              rr_eval_export:close_ds(EPFile),
              rr_eval_export:close_ds(MPFile),
              NextEPId
      end, StartEPId, Setups),
    
    TimeDiff = erlang:round(timer:now_diff(erlang:now(), StartT) / (1000*1000)),
    {_, {NH, NM, NS}} = erlang:localtime(),
    io:format("~n~cFinished at ~p:~p:~p~c in ~c~p:~p:~p~n~n~n",
              [?TAB, NH, NM, NS, ?TAB, ?TAB,
               TimeDiff div (60*60),
               (TimeDiff div 60) rem 60,
               TimeDiff rem 60]),
    ok.

-spec get_node_interval(NodePid::comm:mypid()) -> intervals:interval().
get_node_interval(NodePid) ->
    comm:send(NodePid, {get_state, comm:this(), my_range}),
    receive
        ?SCALARIS_RECV({get_state_response, MyI}, MyI)
    end.

-spec get_db(NodePid::comm:mypid()) -> {Size::non_neg_integer(), db_dht:db_as_list()}.
get_db(NodePid) ->
    MyI = get_node_interval(NodePid),
    comm:send(NodePid, {get_chunk, self(), MyI, all}),
    D = receive
            ?SCALARIS_RECV({get_chunk_response, {_, DB}}, DB)
        end,
%rr_recon:map_key_to_quadrant(db_entry:get_key(X), 1),
    {length(D), [{db_entry:get_key(X), db_entry:get_version(X), db_entry:get_value(X) }
                 || X <- D]}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% SYNC RUN
%% repeat ?EVAL_Repeats-Times and rrepair sync in a given setup
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Starts one sync between 2 nodes.
-spec pair_sync(ring_setup(),
                 eval_options(),
                 step_param(),
                 IncSize::step_size() | power,
                 Steps::pos_integer(),
                 Acc::sync_result()) -> sync_result().
pair_sync(_RingSetup, _Options, _IncParam, _IncSize, -1, Acc) ->
    Acc;
pair_sync(Setup = {Scen, RingP, ReconP}, Options, IncParam, IncSize, StepCount, {AccEP, AccMP, EPId}) ->
    
    RunRound = fun(Nodes) ->
                       % the nodelist may not be sorted by quadrants!
                       % -> sort the nodes by their RT keys first:
                       [{_AKey, _A}, {BKey, _B}, {_CKey, _C}, {_DKey, D}] =
                           lists:keysort(1, [{pid_to_rtkey(N), N} || N <- Nodes]),
                       %B->D
%%                        comm:send(B, {request_sync, DKey}, [{group_member, rrepair}]),
%%                        wait_sync_end([B])
                       %D->B
                       comm:send(D, {request_sync, BKey}, [{group_member, rrepair}]),
                       wait_sync_end([D])
               end,
    
    EPFile = proplists:get_value(ep_file, Options, null),
    MPFile = proplists:get_value(mp_file, Options, null),
    StepValue = case IncSize of
                    power -> erlang:round(get_param_value({RingP, ReconP}, IncParam) * math:pow(4, StepCount));
                    _ -> (IncSize * StepCount) + get_param_value({RingP, ReconP}, IncParam)
                end,
    {StepRing, StepRC} = set_params({RingP, ReconP}, IncParam, StepValue),
    io:format(">EVALUATE STEPS LEFT: ~p (Repeats per Step: ~p)~cStepValue=~p~n~c~s~n",
              [StepCount, ?EVAL_REPEATS, ?TAB, StepValue, ?TAB, rc_conf_comment(StepRC)]),
    _ = [io:format("~c~s~n", [?TAB, X]) || X <- eval_setup_comment(Scen, StepRing, IncParam, StepValue, ?EVAL_REPEATS)],
    io:format("-----------------------------------------------------------------------~n"),
    StartT = erlang:now(),
    
    TraceName = rr_eval_trace,
    %Trace Export Parameter
%    EvalDir = proplists:get_value(eval_dir, Options, "../"),
%    {Hour, Min, Sec} = proplists:get_value(eval_time, Options, {0, 0, 0}),
    
    MPList = util:for_to_ex(1, ?EVAL_REPEATS,
                            fun(I) ->
                                    ActI = ?EVAL_REPEATS - I + 1,
                                    io:format("~p ", [ActI]),
                                    {_DBSize, _Load, Missing, Outdated} =
                                        build_dht({Scen, StepRing, StepRC}),
                                    InitMP = {Missing, Outdated},
                                    NodeList = get_node_list(),
                                    ?ASSERT2(length(NodeList) =:= StepRing#ring_config.node_count,
                                             "NODES NOT COMPLETE, ABORT"),
                                    
                                    %start sync
                                    trace_mpath:start(TraceName, [{map_fun, fun bw_map_fun/3},
                                                                  {filter_fun, fun bw_filter_fun/1}]),
                                    RunRound(NodeList),
                                    trace_mpath:stop(),
                                    Trace = trace_mpath:get_trace(TraceName),
                                    trace_mpath:cleanup(TraceName),
                                    
                                    MP = get_measure_point(EPId, ActI, 1, InitMP, Trace, NodeList),
                                    
                                    log:pal("Regenerated: ~B/~B, Updated: ~B/~B",
                                            [element(5, MP), element(4, MP),
                                             element(7, MP), element(6, MP)]),
                                    
                                    %Trace export
                                    %rr_eval_export:write_raw(Trace, [{filename, io_lib:format("~p-~p-~p_TRACE_ID~p_I~p", [Hour, Min, Sec, EPId, ActI])},
                                    %                              {subdir, io_lib:format("~s/Trace", [EvalDir])}]),
                                    
                                    reset(),
                                    MP
                            end),
    
    TimeDiff = erlang:round(timer:now_diff(erlang:now(), StartT) / (1000*1000)),
    io:format("~n~c~c~cSTEP TIME=~ph ~pm ~ps~n~n", [?TAB, ?TAB, ?TAB,
                                                      TimeDiff div (60*60),
                                                      (TimeDiff div 60) rem 60,
                                                      TimeDiff rem 60]),
    
    EP = rr_eval_point:generate_ep(EPId, {Scen, StepRing, StepRC}, MPList),
    
    % WRITE LINE
    EPFile =/= null andalso
        rr_eval_export:append_ds(EPFile, {[], [tuple_to_list(EP)]}),
    MPFile =/= null andalso
        rr_eval_export:append_ds(MPFile, {[], [tuple_to_list(Row) || Row <- MPList]}),
    
    pair_sync(Setup, Options, IncParam, IncSize, StepCount - 1, {[EP | AccEP], lists:append(MPList, AccMP), EPId + 1}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Builds an Scalaris Ring and runs [Rounds]-Times syncs.
%%      The nodes will clock-wise be selected to sync with an related node.
%%      eg. in a system with 6 nodes and 6 rounds every node will initiate one sync with another node.
%%      in a system with 6 nodes and 7 rounds all nodes start one sync exept node 0 with will start two. 
-spec system_sync(ring_setup(),
                   eval_options(),
                   Rounds::pos_integer(),
                   EPId::non_neg_integer()) -> sync_result().
system_sync({Scen, RingP, ReconP}, Options, Rounds, EPId) ->
    EPFile = proplists:get_value(ep_file, Options, null),
    MPFile = proplists:get_value(mp_file, Options, null),

    io:format(">EVALUATE IN ROUNDS Repeats: ~p - Rounds=~p - ~s~n", [?EVAL_REPEATS, Rounds, rc_conf_comment(ReconP)]),
    _ = [io:format("~c~s~n", [?TAB, X]) || X <- eval_setup_comment(Scen, RingP, rounds, Rounds, ?EVAL_REPEATS)],
    io:format("-----------------------------------------------------------------------~n"),
    StartT = erlang:now(),
    TraceName = rr_eval_trace,
    
    Results = util:for_to_ex(1, ?EVAL_REPEATS,
                         fun(I) ->
                                 ActI = ?EVAL_REPEATS - I + 1,
                                 io:format("~n~p r", [ActI]),
                                 {_DBSize, Load, _Missing, Outdated} =
                                     build_dht({Scen, RingP, ReconP}),
                                 InitMO = {Load, Outdated},
                                 NodeList = get_node_list(),
                                 ?ASSERT2(length(NodeList) =:= RingP#ring_config.node_count,
                                          "NODES NOT COMPLETE, ABORT"),
                                 FirstMP = get_mp_round(0, ActI, 0, InitMO, [], NodeList),

                                 %start sync
                                 {_, _, MPL} = lists:foldl(
                                                 fun(Round, {[ActNode|RNodes], LastMO, AccMP}) ->
                                                         io:format("~p-", [Round]),
                                                         trace_mpath:start(TraceName, [{map_fun, fun bw_map_fun/3}, {filter_fun, fun bw_filter_fun/1}]),
                                                         start_round([ActNode]),
                                                         trace_mpath:stop(),
                                                         Trace = trace_mpath:get_trace(TraceName),
                                                         trace_mpath:cleanup(TraceName),
                                                         
                                                         MP = get_mp_round(EPId + Round, ActI, Round, LastMO, Trace, NodeList),
                                                         {lists:append(RNodes, [ActNode]), {element(4, MP), element(6, MP)}, [MP|AccMP]}
                                                 end,
                                                 {NodeList, InitMO, []},
                                                 lists:seq(1, Rounds)),
                                 reset(),
                                 {MPL, FirstMP}
                         end),
    MPList = lists:flatten([element(1, X) || X <- Results]),
    NullMP = element(2, hd(Results)),
    
    TimeDiff = erlang:round(timer:now_diff(erlang:now(), StartT) / (1000*1000)),
    io:format("~n~c~c~cSTEP TIME=~p:~p:~p~n~n", [?TAB, ?TAB, ?TAB,
                                                 TimeDiff div (60*60),
                                                 (TimeDiff div 60) rem 60,
                                                 TimeDiff rem 60]),
    
    EPList0 = rr_eval_point:generate_ep_rounds(EPId, {Scen, RingP, ReconP}, MPList, Rounds),
    EPList = lists:append(EPList0, [rr_eval_point:generate_ep(0, {Scen, RingP, ReconP}, [NullMP])]),
    
    % WRITE LINE
    EPFile =/= null andalso
        rr_eval_export:append_ds(EPFile, {[], [tuple_to_list(EPRow) || EPRow <- EPList]}),
    MPFile =/= null andalso
        rr_eval_export:append_ds(MPFile, {[], [tuple_to_list(Row) || Row <- MPList]}),
    
    {EPList, MPList, EPId + Rounds}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% API Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec build_dht(ring_setup()) -> db_generator:db_status().
build_dht({#scenario{ ring_type = RingType,
                      data_type = DBType,
                      data_distribution = _DBDist,
                      data_failure_type = FType,
                      fail_distribution = _FDist,
                      trigger_prob = TProb
                     },
           #ring_config{ data_count = DBSize,
                         data_failure_prob = FProb,
                         node_count = NodeCount,
                         fquadrants = FDest },
           RCParams}) ->
    _ = set_config(RCParams, TProb),
    make_ring(RingType, NodeCount),
    
    DBDist = case _DBDist of
                 uniform -> uniform;
                 random -> random;
                 {binomial, P1} -> {non_uniform, random_bias:binomial(DBSize - 1, P1)}
             end,
    FDist = case _FDist of
                uniform -> uniform;
                random -> random;
                {binomial, _P2} when FProb == 0 -> uniform;
                {binomial, P2} when FProb > 0 ->
                    DBErrors = erlang:round(DBSize * (FProb / 100)),
                    {non_uniform, random_bias:binomial(DBErrors - 1, P2)}
            end,
    
    fill_ring(DBType, DBSize, [{ftype, FType},
                               {fprob, FProb},
                               {distribution, DBDist},
                               {fdest, FDest},
                               {fdistribution, FDist}]).

-spec make_ring(ring_type(), pos_integer()) -> ok.
make_ring(Type, Size) ->
    _ = case Type of
            random ->
                _ = admin:add_node([{first}]),
                admin:add_nodes(Size -1);
            uniform ->
                RemIds = ?RT:get_split_keys(?MINUS_INFINITY, ?PLUS_INFINITY, Size),
                _ = admin:add_node([{first}, {{dht_node, id}, ?MINUS_INFINITY}]),
                [admin:add_node_at_id(Id) || Id <- RemIds]
        end,
    unittest_helper:check_ring_size_fully_joined(Size),
    unittest_helper:wait_for_stable_ring(),
    ok.

% @doc  DBSize=Number of Data Entities in DB (without replicas)
-spec fill_ring(db_generator:db_type(), pos_integer(), [db_generator:db_parameter()]) -> db_generator:db_status().
fill_ring(Type, DBSize, Params) ->
    DbStatus = {Entries, _Existing, Missing, Outdated} =
                   db_generator:fill_ring(Type, DBSize, Params),
    {fprob, FProb} = lists:keyfind(fprob, 1, Params),
    ?ASSERT2(Entries =:= DBSize * ?REP_FACTOR, {incorrect_db_size, Entries, DBSize}),
    ?ASSERT2((Missing + Outdated) =:= erlang:round((FProb / 100) * DBSize),
             {incorrect_fprob, Missing + Outdated, FProb * DBSize}),
    erlang:put(?DBSizeKey, element(1, DbStatus)),
    ?DBG_ASSERT2(DbStatus =:= (DbStatus2 = get_db_status2(get_node_list())),
                 {different_db_status, DbStatus, DbStatus2}),
    % more details for debugging:
%%     Data = lists:keysort(2, unittest_helper:get_ring_data(full)),
%%     _ = [begin
%%              OldX = lists:foldl(fun(E, Acc) ->
%%                                         case db_entry:get_value(E) of
%%                                             old -> Acc + 1;
%%                                             _   -> Acc
%%                                         end
%%                                 end, 0, DBEntries),
%%              ExistingX = length(DBEntries),
%%              log:pal("ID: ~.2p Old/Exist/Target: ~B/~B/~B",
%%                      [R, OldX, ExistingX, DBSize])
%%          end || {_Node, {_LBr, _L, R, _RBr}, DBEntries, {pred, _Pred},
%%                  {succ, _Succ}, ok} <- Data],
    DbStatus.

-spec reset() -> ok | failed.
reset() ->
    Nodes = api_vm:get_nodes(),
    {_, NotFound} = api_vm:kill_nodes_by_name(Nodes),
    NotFound =/= [] andalso
        erlang:error(some_nodes_not_found),
    case api_vm:number_of_nodes() of
        0 -> ok;
        _ -> failed
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start_round([comm:mypid()]) -> ok.
start_round(Nodes) ->
    lists:foreach(fun(N) ->
                          comm:send(N, {rr_trigger}, [{group_member, rrepair}]),
                          wait_sync_end(Nodes)
                  end, Nodes),
    ok.

-spec wait_sync_end(Nodes::[comm:mypid()]) -> ok.
wait_sync_end(Nodes) ->
    Req = {get_state, comm:this(), [open_sessions, open_recon, open_resolve]},
    util:wait_for(fun() -> wait_for_sync_round_end2(Req, Nodes) end, 200),
    timer:sleep(100).

-spec wait_for_sync_round_end2(Req::comm:message(), Nodes::[comm:mypid()]) -> boolean().
wait_for_sync_round_end2(_Req, []) -> true;
wait_for_sync_round_end2(Req, [Node | Nodes]) ->
    comm:send(Node, Req, [{group_member, rrepair}]),
    KeyResult =
        receive
            ?SCALARIS_RECV(
            {get_state_response, [Sessions, ORC, ORS]}, % ->
            begin
                if (ORC =:= 0 andalso ORS =:= 0 andalso
                        Sessions =:= []) ->
                       true;
                   true ->
%%                        log:pal("Node: ~.2p~nOS : ~.2p~nORC: ~p, ORS: ~p~n",
%%                                [Node, Sessions, ORC, ORS]),
                       false
                end
            end)
        end,
    if KeyResult -> wait_for_sync_round_end2(Req, Nodes);
       true -> false
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec set_config(rc_config(), 0..100) -> ok | {error, term()}.
set_config(#rc_config{ recon_method = Method,
                       recon_p1e = P1E,
                       art_corr_factor = ArtCorrF,
                       art_inner_fpr = ArtInnerFpr,
                       art_leaf_fpr = ArtLeafFpr,
                       merkle_branch = MerkleBranch,
                       merkle_bucket = MerkleBucket,
                       align_to_bytes = AlignToBytes }, TriggerProb) ->
    config:write(rrepair_enabled, true),
    config:write(rrepair_after_crash, false), % disable (just in case)
    config:write(rr_trigger_interval, 0), % disabled (we trigger manually!)
    config:write(rr_session_ttl, 10*60*1000),     % 2 days
    config:write(rr_gc_interval, 24*60*60*1000),  % 1 day
    
    config:write(rr_trigger_probability, TriggerProb),
    config:write(rr_align_to_bytes, AlignToBytes),
    config:write(rr_recon_method, Method),
    config:write(rr_recon_p1e, P1E),
    config:write(rr_art_inner_fpr, ArtInnerFpr),
    config:write(rr_art_leaf_fpr, ArtLeafFpr),
    config:write(rr_art_correction_factor, ArtCorrF),
    config:write(rr_merkle_branch_factor, MerkleBranch),
    config:write(rr_merkle_bucket_size, MerkleBucket),
    RM = config:read(rr_recon_method),
    case RM =:= Method of
        true -> ok;
        _ -> {error, set_failed}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_param_value({ring_config(), rc_config()}, Param::step_param()) -> any().
get_param_value({Ring, Recon}, Param) ->
    case Param of
        node_count -> Ring#ring_config.node_count;
        data_count -> Ring#ring_config.data_count;
        fprob -> Ring#ring_config.data_failure_prob;
        rounds -> Ring#ring_config.round;
        art_corr_factor -> Recon#rc_config.art_corr_factor;
        art_inner_fpr -> Recon#rc_config.art_inner_fpr;
        art_leaf_fpr -> Recon#rc_config.art_leaf_fpr;
        recon_p1e -> Recon#rc_config.recon_p1e;
        merkle_branch -> Recon#rc_config.merkle_branch;
        merkle_bucket -> Recon#rc_config.merkle_bucket
    end.

-spec set_params({ring_config(), rc_config()}, Param::step_param(), Value::any()) -> {ring_config(), rc_config()}.
set_params({RC, RCC}, Param, Value) ->
    NRC = case Param of
              node_count -> RC#ring_config{ node_count = Value };
              data_count -> RC#ring_config{ data_count = Value };
              fprob -> RC#ring_config{ data_failure_prob = Value };
              rounds -> RC#ring_config{ round = Value };
              _ -> RC
          end,
    NRCC = case Param of
               art_corr_factor -> RCC#rc_config{ art_corr_factor = Value };
               art_inner_fpr -> RCC#rc_config{ art_inner_fpr = Value };
               art_leaf_fpr -> RCC#rc_config{ art_leaf_fpr = Value };
               recon_p1e -> RCC#rc_config{ recon_p1e = Value };
               merkle_branch -> RCC#rc_config{ merkle_branch = Value };
               merkle_bucket -> RCC#rc_config{ merkle_bucket = Value };
               _ -> RCC
           end,
    {NRC, NRCC}.

-spec init_rc_conf(rc_config(), step_param(), any()) -> rc_config().
init_rc_conf(RC, StepP, Init) ->
    case StepP of
        art_corr_factor -> RC#rc_config{ art_corr_factor = Init };
        art_inner_fpr -> RC#rc_config{ art_inner_fpr = Init };
        art_leaf_fpr -> RC#rc_config{ art_leaf_fpr = Init };
        recon_p1e -> RC#rc_config{ recon_p1e = Init };
        merkle_branch -> RC#rc_config{ merkle_branch = Init };
        merkle_bucket -> RC#rc_config{ merkle_bucket = Init };
        _ -> RC
    end.

init_ring_conf(RC, StepP, Init) ->
    case StepP of
        node_count -> RC#ring_config{ node_count = Init };
        data_count -> RC#ring_config{ data_count = Init };
        fprob -> RC#ring_config{ data_failure_prob = Init };
        rounds -> RC#ring_config{ round = Init };
        _ -> RC
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec print_db_status(db_generator:db_status()) -> ok.
print_db_status({Count, Inserted, Missing, Outdated}) ->
    io:format("Items=~p~nExisting=~p~nMissing=~p~nOutdated=~p~n",
              [Count, Inserted, Missing, Outdated]),
    ok.

-spec eval_setup_comment(scenario(), ring_config(), step_param(), StepSize::any(), Runs::pos_integer()) -> [string()].
eval_setup_comment(#scenario{ ring_type = RType,
                              data_distribution = Dist,
                              data_failure_type = FType,
                              fail_distribution = FDist,
                              data_type = DType,
                              trigger_prob = TProb},
                   #ring_config{ node_count = Nodes,
                                 data_count = DBSize,
                                 fquadrants = FQuadrants,
                                 data_failure_prob = FProb,
                                 round = Rounds}, StepParam, StepSize, Runs) ->
    [io_lib:format("Scenario: Ring=~p~cDataDist=~p~cFailType=~p~cFailDist=~p~cDataType=~p~cTriggerProb=~p",
                   [RType, ?TAB, Dist, ?TAB, FType, ?TAB, FDist, ?TAB, DType, ?TAB, TProb]),
     io_lib:format("Ring_Config: Nodes=~p~cDBSize=~p~cFProb=~p~cFQuadrants=~p~cRounds=~p",
                   [Nodes, ?TAB, DBSize, ?TAB, FProb, ?TAB, FQuadrants, ?TAB, Rounds]),
     io_lib:format("Walk: ~p~cStepSize=~p~cRunsPerPoint=~p",
                   [StepParam, ?TAB, StepSize, ?TAB, Runs])].

-spec rc_conf_comment(rc_config()) -> string().
rc_conf_comment(#rc_config{ recon_method = trivial, recon_p1e = P1E }) ->
    io_lib:format("Trivial: P1E=~p", [P1E]);
rc_conf_comment(#rc_config{ recon_method = shash, recon_p1e = P1E }) ->
    io_lib:format("SHash: P1E=~p", [P1E]);
rc_conf_comment(#rc_config{ recon_method = bloom, recon_p1e = P1E }) ->
    io_lib:format("Bloom: P1E=~p", [P1E]);
rc_conf_comment(#rc_config{ recon_method = merkle_tree,
                            recon_p1e = P1E,
                            merkle_branch = Branch,
                            merkle_bucket = Bucket }) ->
    io_lib:format("Merkle: P1E=~p~cBranchSize=~p~cBucketSize=~p",
                  [P1E, ?TAB, Branch, ?TAB, Bucket]);
rc_conf_comment(#rc_config{ recon_method = art,
                            art_corr_factor = Corr,
                            art_inner_fpr = InnerFpr,
                            art_leaf_fpr = LeafFpr }) ->
    io_lib:format("Art: CorrectionFactor=~p~cInnerFpr=~p~cLeafFpr=~p",
                  [Corr, ?TAB, InnerFpr, ?TAB, LeafFpr]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ACCURANCY MEASUREMENT
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_db_status(NodeList::[comm:mypid()]) -> db_generator:db_status().
get_db_status(NodeList) ->
    {DBSize, Stored, Missing, _Outdated} = Res = get_db_status2(NodeList),
    DBSize - Stored < 0 andalso
        log:pal("DBSIZE - STORED < 0 ->>>>>>>>>>>>>>>ERROR ~p-~p=~p",
                [DBSize, Stored, Missing]),
    Res.

-spec get_db_status2(NodeList::[comm:mypid()]) -> db_generator:db_status().
get_db_status2(NodeList) ->
    DBSize = erlang:get(?DBSizeKey),

    DB =
        lists:sort(
          lists:append(
            util:par_map(
              fun(Node) ->
                      comm:send(
                        Node,
                        {get_chunk, self(), intervals:all(),
                         fun(Item) -> not db_entry:is_empty(Item) end,
                         fun(Item) ->
                                 {lists:min(?RT:get_replica_keys(db_entry:get_key(Item))),
                                  db_entry:get_version(Item)}
                         end,
                         all}),
                      receive
                          {get_chunk_response, {_, DBList}} -> DBList
                      end
              end, NodeList))),
    {Stored, Outdated} = get_stored_and_outdated(DB, undefined, 0, 0),
    {DBSize, Stored, DBSize - Stored, Outdated}.

-spec get_stored_and_outdated([{?RT:key(), db_dht:version()}],
                              LastItem::{?RT:key(), [db_dht:version()]} | undefined,
                              Stored, Outdated) -> {Stored, Outdated}
    when is_subtype(Stored, non_neg_integer()),
         is_subtype(Outdated, non_neg_integer()).
get_stored_and_outdated([{Key, Version} | DB], undefined, Stored, Outdated) ->
    get_stored_and_outdated(DB, {Key, [Version]}, Stored + 1, Outdated);
get_stored_and_outdated([{Key, Version} | DB], {Key, Versions}, Stored, Outdated) ->
    get_stored_and_outdated(DB, {Key, [Version | Versions]}, Stored + 1, Outdated);
get_stored_and_outdated(DB0, {_LastKey, LastVersions}, Stored, Outdated) ->
    ?ASSERT(length(LastVersions) =< 4),
    NewestVersion = lists:max(LastVersions),
    Outdated1 = lists:foldl(fun(V, AccOutdated) ->
                                    if V < NewestVersion -> AccOutdated + 1;
                                       true -> AccOutdated
                                    end
                            end, Outdated, LastVersions),
    case DB0 of
        [{Key, Version} | DB] ->
            % different keys
            ?ASSERT(Key =/= _LastKey),
            get_stored_and_outdated(DB, {Key, [Version]}, Stored + 1, Outdated1);
        [] -> {Stored, Outdated1}
    end;
get_stored_and_outdated([], undefined, Stored, Outdated) ->
    {Stored, Outdated}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% BANDWIDTH MEASUREMENT WITH MPATH TRACE
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% only trace send messages with first atom recon_map or resolve_map
% messages with these atoms will be generated by bw_map_fun.
% Trace command executes filter fun after map fun while tracing.
-spec bw_filter_fun(trace_mpath:trace_event()) -> boolean().
bw_filter_fun(Event) ->
    case Event of
        {log_send, _Time, _TraceId, Source, Dest, Msg, LorG} ->
            case erlang:element(1, Msg) of
                rr_map -> true;
                recon_map -> true;
                recon_map2 -> true;
                resolve_map -> true;
                unmapped ->
                    Source1 = trace_mpath:normalize_pidinfo(Source),
                    Dest1 = trace_mpath:normalize_pidinfo(Dest),
                    case is_global_msg(Source1, Dest1, LorG) of
                        true ->
                            RealMsg = element(2, Msg),
                            log:log(warn, "Un-mapped global message: ~p -- ~.2p",
                                    [util:extint2atom(element(1, RealMsg)), RealMsg]),
                            ok;
                        unknown ->
                            % this is most likely from our own process
                            % requesting the number of open sessions!
                            %RealMsg = element(2, Msg),
                            %log:log(warn, "Un-mapped unknown message: ~p -- ~.2p",
                            %        [util:extint2atom(element(1, RealMsg)), RealMsg]),
                            ok;
                        false ->
                            ok
                    end,
                    false
            end;
        _ -> false
    end.

%% @doc Checks whether a message with the given source and destination as well
%%      as the use of comm:send or comm:send_local is a message to another
%%      Scalaris node or not.
-spec is_global_msg(Source::trace_mpath:pidinfo(), Dest::trace_mpath:pidinfo(),
                    Local_or_Global::global | local) -> boolean() | unknown.
is_global_msg(_Source, _Dest, local) -> false;

is_global_msg({_SPid, {Group, _SName}}, {_DPid, {Group, _DName}}, global) -> false;
is_global_msg({_SPid, {_SGroup, _SName}}, {_DPid, {_DGroup, _DName}}, global) -> true;

is_global_msg({_SPid, {_SGroup, _SName}}, {_DPid, non_local_pid_name_unknown}, global) -> false;
is_global_msg({_SPid, no_pid_name},     {_DPid, non_local_pid_name_unknown}, global) -> false;
is_global_msg({_SPid, non_local_pid_name_unknown}, {_DPid, {_DGroup, _DName}}, global) -> false;
is_global_msg({_SPid, non_local_pid_name_unknown}, {_DPid, no_pid_name}, global) -> false;
is_global_msg(_Source, _Dest, global) -> unknown.

%% @doc Calculates the message size if needed.
-spec msg_size(Msg::comm:message(), true) -> pos_integer();
              (Msg::comm:message(), false) -> 0.
msg_size(Msg, true) ->
    erlang:byte_size(erlang:term_to_binary(Msg, [{minor_version, 1}, {compressed, 2}]));
msg_size(_Msg, false) ->
    0.

%% @doc Reduces the traced message size by mapping recorded messages to a
%%      shorter representation including its tag, size and number of KVV
%%      triples (if a resolve message). Non-mapped messages will be wrapped in a
%%      {unmapped, Msg} and remain intact but can be filtered out with
%%      bw_filter_fun/1.
%% Recon:   start_recon (inside continue_recon) will be mapped,
%%          ?check_nodes and ?check_nodes_response (merkle_tree) will also be mapped
%% Resolve: ?key_upd and ?interval_upd will be mapped
%%          key_upd_send and interval_upd_send are node-internal messages and will be ommited
-spec bw_map_fun(comm:message(), Source::pid() | comm:mypid(),
                 Dest::pid() | comm:mypid())
        -> {unmapped, Msg::comm:message()} |
           {rr_map, ExtSize::pos_integer()} |
           {recon_map | recon_map2, ExtSize::pos_integer()} |
           {resolve_map, ExtSize::pos_integer(), KVVLen::non_neg_integer()}.
bw_map_fun(MsgI, Source, Dest) ->
    bw_map_fun(MsgI, Source, Dest, true).

-spec bw_map_fun(comm:message(), Source::pid() | comm:mypid(),
                 Dest::pid() | comm:mypid(), CalcMsgSize::boolean())
        -> {unmapped, Msg::comm:message()} |
           {rr_map, ExtSize::pos_integer()} |
           {recon_map | recon_map2, ExtSize::pos_integer()} |
           {resolve_map, ExtSize::pos_integer(), KVVLen::non_neg_integer()}.
bw_map_fun({?key_upd, KVV, _ReqKeys} = Msg, _Source, _Dest, CalcMsgSize) ->
    {resolve_map, msg_size(Msg, CalcMsgSize), length(KVV)};
bw_map_fun({?interval_upd, _I, KVV} = Msg, _Source, _Dest, CalcMsgSize) ->
    {resolve_map, msg_size(Msg, CalcMsgSize), length(KVV)};

bw_map_fun({X, MsgI, _Options} = Msg, Source, Dest, CalcMsgSize)
  when X =:= request_resolve orelse X =:= continue_resolve ->
    bw_map_fun2(MsgI, Source, Dest, Msg, CalcMsgSize);
bw_map_fun({X, _SID, MsgI, _Options} = Msg, Source, Dest, CalcMsgSize)
  when X =:= request_resolve orelse X =:= continue_resolve ->
    bw_map_fun2(MsgI, Source, Dest, Msg, CalcMsgSize);

bw_map_fun({request_sync, _DestKey} = Msg, _Source, _Dest, CalcMsgSize) ->
    {rr_map, msg_size(Msg, CalcMsgSize)};
bw_map_fun({request_sync, _Method, _DestKey} = Msg, _Source, _Dest, CalcMsgSize) ->
    {rr_map, msg_size(Msg, CalcMsgSize)};
bw_map_fun({request_sync, _Method, _DestKey, _Principal} = Msg, _Source, _Dest, CalcMsgSize) ->
    {rr_map, msg_size(Msg, CalcMsgSize)};
bw_map_fun({start_recon, _SenderPid, _SId, {create_struct, _Method, _SenderI}} = Msg,
           _Source, _Dest, CalcMsgSize) ->
    {rr_map, msg_size(Msg, CalcMsgSize)};
bw_map_fun({continue_recon, _SenderPid, _SId, {create_struct, _Method, _SenderI}} = Msg,
           _Source, _Dest, CalcMsgSize) ->
    {rr_map, msg_size(Msg, CalcMsgSize)};

bw_map_fun({shutdown, sync_finished_remote} = Msg, _Source, _Dest, CalcMsgSize) ->
    {recon_map, msg_size(Msg, CalcMsgSize)};
bw_map_fun({start_recon, _SenderPid, _SId, {start_recon, _Method, _Struct}} = Msg,
           _Source, _Dest, CalcMsgSize) ->
    {recon_map, msg_size(Msg, CalcMsgSize)};
bw_map_fun({continue_recon, _SenderPid, _SId, {start_recon, _Method, _Struct}} = Msg,
           _Source, _Dest, CalcMsgSize) ->
    {recon_map, msg_size(Msg, CalcMsgSize)};
bw_map_fun(Msg, _Source, _Dest, CalcMsgSize)
  when element(1, Msg) =:= resolve_req ->
    {recon_map2, msg_size(Msg, CalcMsgSize)};
bw_map_fun(Msg, _Source, _Dest, CalcMsgSize)
  when element(1, Msg) =:= ?check_nodes;
       element(1, Msg) =:= ?check_nodes_response ->
    % for debugging:
%%     log:log(warn, "mapped global message: ~p (~p) -- ~.2p",
%%             [util:extint2atom(element(1, Msg)), msg_size(Msg, true), Msg]),
    {recon_map, msg_size(Msg, CalcMsgSize)};

%% encapsulated messages:
% look 2 levels deep as an optimisation:
bw_map_fun({?lookup_aux, _, _, {?send_to_group_member, rrepair, MsgI}} = Msg,
           Source, Dest, CalcMsgSize) ->
    bw_map_fun2(MsgI, Source, Dest, Msg, CalcMsgSize);
bw_map_fun({?lookup_fin, _, _, {?send_to_group_member, rrepair, MsgI}} = Msg,
           Source, Dest, CalcMsgSize) ->
    bw_map_fun2(MsgI, Source, Dest, Msg, CalcMsgSize);
% general cases for encapsulated messages:
bw_map_fun({?lookup_aux, _, _, MsgI} = Msg, Source, Dest, CalcMsgSize) ->
    bw_map_fun2(MsgI, Source, Dest, Msg, CalcMsgSize);
bw_map_fun({?lookup_fin, _, _, MsgI} = Msg, Source, Dest, CalcMsgSize) ->
    bw_map_fun2(MsgI, Source, Dest, Msg, CalcMsgSize);
bw_map_fun({?send_to_group_member, rrepair, MsgI} = Msg, Source, Dest, CalcMsgSize) ->
    bw_map_fun2(MsgI, Source, Dest, Msg, CalcMsgSize);

bw_map_fun(Msg, _Source, _Dest, _CalcMsgSize) ->
    {unmapped, Msg}.

-spec bw_map_fun2(MsgI::comm:message(), Source::pid() | comm:mypid(),
                  Dest::pid() | comm:mypid(), FullMsg::comm:message(),
                  CalcMsgSize::boolean())
        -> {unmapped, Msg::comm:message()} |
           {rr_map, ExtSize::pos_integer()} |
           {recon_map | recon_map2, ExtSize::pos_integer()} |
           {resolve_map, ExtSize::pos_integer(), KVVLen::non_neg_integer()}.
bw_map_fun2(MsgI, Source, Dest, FullMsg, CalcMsgSize) ->
    case bw_map_fun(MsgI, Source, Dest, false) of
        {resolve_map, _MsgSize, KVVLen} ->
            {resolve_map, msg_size(FullMsg, CalcMsgSize), KVVLen};
        {MsgTag, MsgSize} when is_integer(MsgSize) ->
            {MsgTag, msg_size(FullMsg, CalcMsgSize)};
        X -> X
    end.

-spec get_bandwidth(trace_mpath:trace())
        -> {Recon_size        :: non_neg_integer(),   % number of recon-msg bytes
            Recon_msg_count   :: non_neg_integer(),   % number of recon messages
            Recon2_size       :: non_neg_integer(),   % number of recon2-msg bytes
            Recon2_msg_count  :: non_neg_integer(),   % number of recon2 messages
            Resolve_size      :: non_neg_integer(),   % number of transmitted resolve bytes
            Resolve_msg_count :: non_neg_integer(),   % number of resolve messages
            Resolve_kvv_count :: non_neg_integer()}.  % number of kvv-triple send
get_bandwidth(Trace) ->
    lists:foldl(fun(X, {RCAccBytes, RCAccMsgs,
                        RC2AccBytes, RC2AccMsgs,
                        RSAccBytes, RSAccMsgs, RSAccKVV} = Acc) ->
                        case element(6, X) of
                            {recon_map, Byte} ->
                                {RCAccBytes + Byte, RCAccMsgs + 1,
                                 RC2AccBytes, RC2AccMsgs,
                                 RSAccBytes, RSAccMsgs, RSAccKVV};
                            {recon_map2, Byte} ->
                                {RCAccBytes, RCAccMsgs,
                                 RC2AccBytes + Byte, RC2AccMsgs + 1,
                                 RSAccBytes, RSAccMsgs, RSAccKVV};
                            {resolve_map, Byte, KVVCount} ->
                                {RCAccBytes, RCAccMsgs,
                                 RC2AccBytes, RC2AccMsgs,
                                 RSAccBytes + Byte, RSAccMsgs + 1, RSAccKVV + KVVCount};
                            _ -> Acc
                        end
                end,
                {0, 0, 0, 0, 0, 0, 0},
                Trace).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Eval Measurment Point Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec get_measure_point(rr_eval_point:point_id(), Iteration::non_neg_integer(),
                        Round::non_neg_integer(), init_mp(),
                        Trace::trace_mpath:trace(), NodeList::[comm:mypid()])
        -> rr_eval_point:measure_point().
get_measure_point(Id, Iter, Round, {Miss, Outd}, Trace, NodeList) ->
    {_, _, M, O} = get_db_status(NodeList),
    {RC_S, RC_Msg, RC2_S, RC2_Msg, RS_S, RS_Msg, RS_KVV} = get_bandwidth(Trace),
    {Id, Iter, Round,
     Miss, Miss - M,
     Outd, Outd - O,
     RC_S, RC_Msg, RC2_S, RC2_Msg, RS_S, RS_Msg, RS_KVV}.

-spec get_mp_round(rr_eval_point:point_id(), Iteration::non_neg_integer(),
                   Round::non_neg_integer(), init_mp(),
                   Trace::trace_mpath:trace(), NodeList::[comm:mypid()])
        -> rr_eval_point:measure_point().
get_mp_round(Id, Iter, Round, {Miss, Outd}, Trace, NodeList) ->
    {_DBSize, ActLoad, _Missing, ActOut} = get_db_status2(NodeList),
    {RC_S, RC_Msg, RC2_S, RC2_Msg, RS_S, RS_Msg, RS_KVV} = get_bandwidth(Trace),
    {Id, Iter, Round,
     ActLoad, ActLoad - Miss,
     ActOut, Outd - ActOut,
     RC_S, RC_Msg, RC2_S, RC2_Msg, RS_S, RS_Msg, RS_KVV}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Local Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec pid_to_rtkey(comm:mypid()) -> ?RT:key().
pid_to_rtkey(Pid) ->
    comm:send(Pid, {get_state, comm:this(), node_id}),
    receive
        ?SCALARIS_RECV({get_state_response, Key}, Key)
    end.

-spec get_node_list() -> [comm:mypid()].
get_node_list() ->
    mgmt_server:node_list(),
    receive
        ?SCALARIS_RECV({get_list_response, List}, List)
    end.
