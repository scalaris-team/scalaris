%  @copyright 2010-2016 Zuse Institute Berlin

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
%% @doc    test generator type definitions
%% @version $Id$
-include("scalaris.hrl").
-include("record_helpers.hrl").

-type(test_any() ::
   % | none()
   % | no_return()
   % | pid()
   % | port()
   % | ref()
       []
     | atom()
   % | binary()
     | float()
   % | Fun
     | integer()
     | 42
     | -1..1
     | list()
     | list(test_any())
   % | improper_list(test_any(), test_any()
   % | maybe_improper_list(test_any(), test_any())
     | tuple()
     | {}
     | {test_any(), test_any(), test_any()}
   % | Union
   % | Userdefined
           ).


-type(builtin_type() ::
      array
    | bitstring
    | dict
    | gb_set
    | gb_tree
    | identifier
    | iodata
    | maybe_improper_list
    | map
    | module
    | set
    | timeout).

-type(type_name() ::
      {'fun', Module :: module(), FunName :: atom(), FunArity :: byte()}
      | {type, Module :: module(), TypeName :: atom(), Arity::arity()}
      | {record, Module :: module(), TypeName :: atom()}).

-type(var_list() :: list(atom())).

-type(record_field_type() ::
      {typed_record_field, atom(), type_spec()}
    | {untyped_record_field, atom()}
    | {field_type, Name::atom(), Type::type_spec()}
     ).

-type(type_spec() ::
      {'fun', type_spec(), type_spec()}
    | {'union_fun', [{'fun', type_spec(), type_spec()}]}
    | {product, [type_spec()]}
    | {tuple, [type_spec()]}
    | {tuple, {typedef, tester, test_any}}
    | {list, type_spec()}
    | {nonempty_list, type_spec()}
    | {range, {integer, integer()}, {integer, integer()}}
    | {union, [type_spec()]}
    | {record, atom(), atom()}
    | nonempty_string
    | integer
    | pos_integer
    | neg_integer
    | non_neg_integer
    | bool
    | {binary, list()}
    | iolist
    | node
    | pid
    | port
    | reference
    | none
    | no_return
    | atom
    | float
    | nil
    | {atom, atom()}
    | {integer, integer()}
    | {typedef, module(), atom(), [type_spec()]}
    | {var, atom()}
    | {var_type, list(atom()), type_spec()}
    | {builtin_type, builtin_type()}
    | {builtin_type, array_array, ValueType::type_spec()}
    | {builtin_type, dict_dict, KeyType::type_spec(), ValueType::type_spec()}
    | {builtin_type, queue_queue, ValueType::type_spec()}
    | {builtin_type, gb_sets_set, ValueType::type_spec()}
    | {builtin_type, gb_trees_tree, KeyType::type_spec(), ValueType::type_spec()}
    | {builtin_type, gb_trees_iter, KeyType::type_spec(), ValueType::type_spec()}
    | {builtin_type, sets_set, ValueType::type_spec()}
    | {record, [record_field_type()]} % TODO: is this still used?
    | {record, module(), Name::atom()}
    | {record, module(), Name::atom(), FieldTypes::[type_spec()]}
    | record_field_type()
     ).

-type(test_fun_type() :: {'fun', type_spec(), type_spec()}).
