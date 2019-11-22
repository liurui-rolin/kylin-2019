/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.engine.spark;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.MRBatchCubingEngine2;
import org.apache.kylin.job.execution.DefaultChainedExecutable;

/**
 */
public class SparkBatchCubingEngine2 extends MRBatchCubingEngine2 {
    @Override
    public DefaultChainedExecutable createBatchCubingJob(CubeSegment newSegment, String submitter) {
        //********** 创建构建器，调用build方法 **********//
        //********** 吐槽一下：此处这继承总觉的很奇怪，spark引擎还继承mr，是不是抽象的不好，另外注释太少，方法和类上几乎没有注释 **********//
        return new SparkBatchCubingJobBuilder2(newSegment, submitter).build();
    }

}
