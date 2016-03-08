/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.killrweather

import org.joda.time.DateTime

// TODO document the Event API
object BusinessEvent {
  /** Base marker trait. */
  @SerialVersionUID(1L)
  sealed trait BusinessEvent extends Serializable

  sealed trait LifeCycleEvent extends BusinessEvent
  case object OutputStreamInitialized extends LifeCycleEvent
  case object NodeInitialized extends LifeCycleEvent
  case object Start extends LifeCycleEvent
  case object DataFeedStarted extends LifeCycleEvent
  case object Shutdown extends LifeCycleEvent
  case object TaskCompleted extends LifeCycleEvent

  sealed trait Task extends Serializable
  case object QueryTask extends Task
  case object GracefulShutdown extends LifeCycleEvent
}
