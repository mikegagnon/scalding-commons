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

import com.google.protobuf.Message
import com.twitter.elephantbird.cascading2.scheme._
import com.twitter.elephantbird.util.{ ThriftUtils, TypeRef }
import com.twitter.scalding._

import cascading.tuple.Fields
import java.io.Serializable
import org.apache.thrift.TBase

// Retrieve implicits
import Dsl._

/**
 * Base class for logs that are stored daily
 */

abstract class DailyPrefixSuffixSource(prefixTemplate: String, suffixTemplate: String, dateRange: DateRange)
  extends TimePathedSource(prefixTemplate + TimePathedSource.YEAR_MONTH_DAY + suffixTemplate + "/*", dateRange, DateOps.UTC)

abstract class DailyPrefixSuffixMostRecentSource(prefixTemplate: String, suffixTemplate: String, dateRange: DateRange)
  extends MostRecentGoodSource(prefixTemplate + TimePathedSource.YEAR_MONTH_DAY + suffixTemplate + "/*", dateRange, DateOps.UTC)

abstract class DailySuffixSource(prefixTemplate: String, dateRange: DateRange)
  extends TimePathedSource(prefixTemplate + TimePathedSource.YEAR_MONTH_DAY + "/*", dateRange, DateOps.UTC)

abstract class DailySuffixMostRecentSource(prefixTemplate: String, dateRange: DateRange)
  extends MostRecentGoodSource(prefixTemplate + TimePathedSource.YEAR_MONTH_DAY + "/*", dateRange, DateOps.UTC)

abstract class DailySuffixLzoProtobuf[T <: Message: Manifest](prefix: String, dateRange: DateRange)
  extends DailySuffixSource(prefix, dateRange) with LzoProtobuf[T] {
  def column = manifest[T].erasure
}

abstract class DailySuffixLzoThrift[T <: TBase[_, _]: Manifest](prefix: String, dateRange: DateRange)
  extends DailySuffixSource(prefix, dateRange) with LzoThrift[T] {
  def column = manifest[T].erasure
}

abstract class TimePathedLongThriftSequenceFile[V <: TBase[_, _]: Manifest](f: Fields, prefix: String, dateFormat: String, dateRange: DateRange)
  extends TimePathedSource(prefix + dateFormat + "/*", dateRange, DateOps.UTC)
  with WritableSequenceScheme
  with Serializable
  with Mappable[(Long, V)]
  with LongThriftTransformer[V] {
  override val fields = f
  override val mt = implicitly[Manifest[V]]
  override val converter = implicitly[TupleConverter[(Long, V)]]
}

abstract class MostRecentGoodLongThriftSequenceFile[V <: TBase[_, _]: Manifest](f: Fields, pattern: String, dateRange: DateRange)
  extends MostRecentGoodSource(pattern, dateRange, DateOps.UTC)
  with WritableSequenceScheme
  with Serializable
  with Mappable[(Long, V)]
  with LongThriftTransformer[V] {
  override val fields = f
  override val mt = implicitly[Manifest[V]]
  override val converter = implicitly[TupleConverter[(Long, V)]]
}

abstract class DailySuffixLongThriftSequenceFile[V <: TBase[_, _]: Manifest](f: Fields, prefix: String, dateRange: DateRange)
  extends TimePathedLongThriftSequenceFile[V](f, prefix, TimePathedSource.YEAR_MONTH_DAY, dateRange)

case class DailySuffixTsv(prefix: String, fs: Fields = Fields.ALL)(override implicit val dateRange: DateRange)
  extends DailySuffixSource(prefix, dateRange) with DelimitedScheme {
  override val fields = fs
}

case class DailySuffixCsv(prefix: String, fs: Fields = Fields.ALL)(override implicit val dateRange: DateRange)
  extends DailySuffixSource(prefix, dateRange) with DelimitedScheme {
  override val fields = fs
  override val separator = ","
}

case class DailySuffixMostRecentCsv(prefix: String, fs: Fields = Fields.ALL)(override implicit val dateRange: DateRange)
  extends DailySuffixMostRecentSource(prefix, dateRange) with DelimitedScheme {
  override val fields = fs
  override val separator = ","
}

case class DailySuffixLzoTsv(prefix: String, fs: Fields = Fields.ALL)(override implicit val dateRange: DateRange)
  extends DailySuffixSource(prefix, dateRange) with LzoTsv {
  override val fields = fs
}
