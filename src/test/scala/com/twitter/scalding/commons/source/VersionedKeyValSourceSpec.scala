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

import com.twitter.scalding._
import org.specs._

import TDsl._
import Dsl._

case class DummySource extends VersionedKeyValSource[Int, Int]("input", None, None)

class DummyJob(args : Args) extends Job(args) {
  DummySource().map{ x=>x }.write(Tsv("output"))
}

class VersionedKeyValSourceSpec extends Specification {
  "A VersionedKeyValSourceSpec" should {
    "not experience ClassCastException: java.lang.Integer cannot be cast to [B" in {
      val job = new Job(Args("")) { }
      val inputData = List(1 -> 1, 2 -> 2, 3 -> 3)

      val expectedOutput = inputData

      val jobTest = JobTest("com.twitter.scalding.commons.source.DummyJob")
      .source(DummySource(), inputData)
      .sink[(Int, List[Int])](Tsv("output")) { buf =>
        buf.toList must_== expectedOutput
      }
      .run
      .finish
    }
  }
}
