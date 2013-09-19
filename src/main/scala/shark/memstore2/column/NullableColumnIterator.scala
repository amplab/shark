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

package shark.memstore2.column

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Reads a nullable column. Expects the byte buffer to contain as first element
 * the null count, followed by the null indices, and finally the non nulls.
 * Reading of non nulls is delegated by setting the buffer position to the first
 * non null.
 */
class NullableColumnIterator(buffer: ByteBuffer) extends ColumnIterator {
  private var _d: ByteBuffer = _
  private var _nullCount: Int = _
  private var _nulls = 0

  private var _isNull = false
  private var _currentNullIndex: Int = _
  private var _pos = 0

  private var _delegate: ColumnIterator = _

  override def init() {
    _d = buffer.duplicate()
    _d.order(ByteOrder.nativeOrder())
    _nullCount = _d.getInt()
    _currentNullIndex = if (_nullCount > 0) _d.getInt() else Integer.MAX_VALUE
    _pos = 0

    // Move the buffer position to the non-null region.
    buffer.position(buffer.position() + 4 + _nullCount * 4)
    _delegate = ColumnIterator.newNonNullIterator(buffer)
  }

  override def next() {
    if (_pos == _currentNullIndex) {
      _nulls += 1
      if (_nulls < _nullCount) {
        _currentNullIndex = _d.getInt()
      }
      _isNull = true
    } else {
      _isNull = false
      _delegate.next()
    }
    _pos += 1
  }

  override def hasNext: Boolean = (_nulls < _nullCount) || _delegate.hasNext

  def current: Object = if (_isNull) null else _delegate.current
}
