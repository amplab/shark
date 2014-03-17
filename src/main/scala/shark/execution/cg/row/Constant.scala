/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution.cg.row

object Constant {
  val CG_EXPR_NAME_INPUT_SOI = "__soi__"
  val CG_EXPR_NAME_INPUT = "__input__"
  val CG_EXPR_NAME_OUTPUT = "__output__"
    
  val CODE_IS_VALID = "code_is_valid"
  val CODE_VALUE_REPL = "code_value_repl"
  val CODE_INVALIDATE = "code_invalidate"
  val CODE_VALIDATE = "code_validate"
  val EXPR_VARIABLE_NAME = "expr_variable_name"
  val EXPR_VARIABLE_TYPE = "expr_varialbe_type"
  val EXPR_NULL_INDICATOR_NAME = "expr_null_indicator"
  val EXPR_NULL_INDICATOR_DEFAULT_VALUE = "expr_null_indicator_default_val"
  val EXPR_DEFAULT_VALUE = "expr_default_val"
}
