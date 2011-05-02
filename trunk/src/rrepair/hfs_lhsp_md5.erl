% @copyright 2010-2011 Zuse Institute Berlin

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

%% @author Maik Lange <malange@informatik.hu-berlin.de>
%% @doc    Less hashing, same performance hash function set container
%%
%%         Implementation of a hash function set proposed in 
%%         2006 by A. Kirsch, M. Mitzenmacher - 
%%         "Less Hashing, Same Perofrmance: Building a Better Bloom Filter 
%%         Build k Hash functions of the form g_i(x) = h_1(X) + i * h_2(X)
%%  
%%         Used MD5 Hash-Function like in 
%%         2000 - L.Fan, P. Cao., JA, ZB : "Summary Cache: A Scalable Wide-Area Web Cache Sharing Protocol" (Counting Bloom Filters Paper)
%% @end
%% @version $Id$

-module(hfs_lhsp_md5).

% types
-behaviour(hfs_beh).

-type hf_number() :: integer().
-type hfs_t() :: {hfs_lhsp_md5, hf_number()}.

% include
-include("hfs_beh.hrl").

% API functions

% @doc returns a new lhsp hfs
new_(_, HFCount) -> 
	new_(HFCount).
new_(HFCount) -> 
	{hfs_lhsp_md5, HFCount}.

% @doc Applies Val to all hash functions in container HC
apply_val_({hfs_lhsp_md5, K}, Val) ->
	if
		is_integer(Val) -> MD5 = erlang:md5(integer_to_list(Val));
		is_float(Val) -> MD5 = erlang:md5(float_to_list(Val));
		true -> MD5 = erlang:md5(Val)
	end,
	<<HF1:64, HF2:64>> = MD5,
	[ HF1 + I * HF2 || I <- lists:seq(0, K-1, 1) ].

% @doc Returns number of hash functions in the container
hfs_size_(Hfs) -> 
	{hfs_lhsp_md5, K} = Hfs,
	K.

