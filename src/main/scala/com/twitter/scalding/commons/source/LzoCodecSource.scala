/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.twitter.scalding.commons.source

import com.twitter.chill.MeatLocker
import com.twitter.scalding._
import com.twitter.bijection.Bijection

/**
 * Source used to write some type T into an LZO-compressed SequenceFile using a
 * codec on T for serialization.
 */

object LzoCodecSource {
  def apply[T](paths: String*)(implicit passedBijection: Bijection[T, Array[Byte]]) =
    new LzoCodec[T] {
      val hdfsPaths = paths
      val boxed = MeatLocker(passedBijection)
      override def bijection = boxed.get
    }
}
