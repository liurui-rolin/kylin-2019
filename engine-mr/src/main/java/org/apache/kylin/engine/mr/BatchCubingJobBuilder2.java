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

package org.apache.kylin.engine.mr;

import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.IMRInput.IMRBatchCubingInputSide;
import org.apache.kylin.engine.mr.IMROutput2.IMRBatchCubingOutputSide2;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.MapReduceExecutable;
import org.apache.kylin.engine.mr.steps.BaseCuboidJob;
import org.apache.kylin.engine.mr.steps.CubingExecutableUtil;
import org.apache.kylin.engine.mr.steps.InMemCuboidJob;
import org.apache.kylin.engine.mr.steps.NDCuboidJob;
import org.apache.kylin.engine.mr.steps.SaveStatisticsStep;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchCubingJobBuilder2 extends JobBuilderSupport {
    private static final Logger logger = LoggerFactory.getLogger(BatchCubingJobBuilder2.class);

    private final IMRBatchCubingInputSide inputSide;
    private final IMRBatchCubingOutputSide2 outputSide;

    public BatchCubingJobBuilder2(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
        this.inputSide = MRUtil.getBatchCubingInputSide(seg);
        this.outputSide = MRUtil.getBatchCubingOutputSide2(seg);
    }

    /**
     * 此处调用构建方法，是父类的build方法
     *
     * 步骤：
     * #1 Step Name: Create Intermediate Flat Hive Table  生成扁平的hive表，星型模型会做关联处理，此处容易出现全表扫描问题
     * #2 Step Name: Redistribute Flat Hive Table 重分区
     * #3 Step Name: Extract Fact Table Distinct Columns  统计维度的基数
     * #4 Step Name: Build UHC Dictionary 基于HyperLogLog算法构建超高基数维度字典，
     * #5 Step Name: Build Dimension Dictionary 构建维度字典
     * #6 Step Name: Save Cuboid Statistics 保存cubeid的统计信息
     * #7 Step Name: Create HTable 创建hbase 表
     * #8 Step Name: Build Cube with Spark 构建cube立方体
     * #9 Step Name: Convert Cuboid Data to HFile 装换成hfile
     * #10 Step Name: Load HFile to HBase Table 加载hfile到hive表中
     * #11 Step Name: Update Cube Info 更新meta信息
     * #12 Step Name: Hive Cleanup 清理中间表
     *
     * @return
     */
    public CubingJob build() {
        logger.info("MR_V2 new job to BUILD segment " + seg);

        final CubingJob result = CubingJob.createBuildJob(seg, submitter, config);
        final String jobId = result.getId();
        final String cuboidRootPath = getCuboidRootPath(jobId);

        // Phase 1: Create Flat Table & Materialize Hive View in Lookup Tables
        //**********  **********//
        inputSide.addStepPhase1_CreateFlatTable(result);

        // Phase 2: Build Dictionary
        //********** 构建字典 **********//
        result.addTask(createFactDistinctColumnsStepWithStats(jobId));
        result.addTask(createBuildDictionaryStep(jobId));
        result.addTask(createSaveStatisticsStep(jobId));
        outputSide.addStepPhase2_BuildDictionary(result);

        // Phase 3: Build Cube
        //********** 此处是两种build方法，第一种是层级构建，会启动多伦任务 第二种是基于内存的构建，启动一轮任务，会消耗更多内存 **********//
        //********** return getOptional("kylin.cube.algorithm", "auto"); **********//
        addLayerCubingSteps(result, jobId, cuboidRootPath); // layer cubing, only selected algorithm will execute
        addInMemCubingSteps(result, jobId, cuboidRootPath); // inmem cubing, only selected algorithm will execute
        //********** 转换cube成hfile，并加载到hive表的任务  **********//
        outputSide.addStepPhase3_BuildCube(result);

        // Phase 4: Update Metadata & Cleanup
        //********** 第11，12步 **********//
        result.addTask(createUpdateCubeInfoAfterBuildStep(jobId));
        inputSide.addStepPhase4_Cleanup(result);
        outputSide.addStepPhase4_Cleanup(result);

        return result;
    }

    protected void addLayerCubingSteps(final CubingJob result, final String jobId, final String cuboidRootPath) {
        final int maxLevel = seg.getCubeDesc().getBuildLevel();
        // base cuboid step
        result.addTask(createBaseCuboidStep(getCuboidOutputPathsByLevel(cuboidRootPath, 0), jobId));
        // n dim cuboid steps
        for (int i = 1; i <= maxLevel; i++) {
            result.addTask(createNDimensionCuboidStep(getCuboidOutputPathsByLevel(cuboidRootPath, i-1), getCuboidOutputPathsByLevel(cuboidRootPath, i), i, jobId));
        }
    }

    private SaveStatisticsStep createSaveStatisticsStep(String jobId) {
        SaveStatisticsStep result = new SaveStatisticsStep();
        result.setName(ExecutableConstants.STEP_NAME_SAVE_STATISTICS);
        CubingExecutableUtil.setCubeName(seg.getRealization().getName(), result.getParams());
        CubingExecutableUtil.setSegmentId(seg.getUuid(), result.getParams());
        CubingExecutableUtil.setStatisticsPath(getStatisticsPath(jobId), result.getParams());
        CubingExecutableUtil.setCubingJobId(jobId, result.getParams());
        return result;
    }

    protected void addInMemCubingSteps(final CubingJob result, String jobId, String cuboidRootPath) {
        // base cuboid job
        MapReduceExecutable cubeStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd, JobEngineConfig.IN_MEM_JOB_CONF_SUFFIX);

        cubeStep.setName(ExecutableConstants.STEP_NAME_BUILD_IN_MEM_CUBE);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cuboidRootPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Cube_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        cubeStep.setMapReduceParams(cmd.toString());
        cubeStep.setMapReduceJobClass(getInMemCuboidJob());
        result.addTask(cubeStep);
    }

    protected Class<? extends AbstractHadoopJob> getInMemCuboidJob() {
        return InMemCuboidJob.class;
    }

    private MapReduceExecutable createBaseCuboidStep(String cuboidOutputPath, String jobId) {
        // base cuboid job
        MapReduceExecutable baseCuboidStep = new MapReduceExecutable();

        StringBuilder cmd = new StringBuilder();
        appendMapReduceParameters(cmd);

        baseCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_BASE_CUBOID);

        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, "FLAT_TABLE"); // marks flat table input
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, cuboidOutputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_Base_Cuboid_Builder_" + seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "0");
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        baseCuboidStep.setMapReduceParams(cmd.toString());
        baseCuboidStep.setMapReduceJobClass(getBaseCuboidJob());
//        baseCuboidStep.setCounterSaveAs(CubingJob.SOURCE_RECORD_COUNT + "," + CubingJob.SOURCE_SIZE_BYTES);
        return baseCuboidStep;
    }

    protected Class<? extends AbstractHadoopJob> getBaseCuboidJob() {
        return BaseCuboidJob.class;
    }

    private MapReduceExecutable createNDimensionCuboidStep(String parentPath, String outputPath, int level, String jobId) {
        // ND cuboid job
        MapReduceExecutable ndCuboidStep = new MapReduceExecutable();

        ndCuboidStep.setName(ExecutableConstants.STEP_NAME_BUILD_N_D_CUBOID + " : level " + level);
        StringBuilder cmd = new StringBuilder();

        appendMapReduceParameters(cmd);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBE_NAME, seg.getRealization().getName());
        appendExecCmdParameters(cmd, BatchConstants.ARG_SEGMENT_ID, seg.getUuid());
        appendExecCmdParameters(cmd, BatchConstants.ARG_INPUT, parentPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_OUTPUT, outputPath);
        appendExecCmdParameters(cmd, BatchConstants.ARG_JOB_NAME, "Kylin_ND-Cuboid_Builder_" + seg.getRealization().getName() + "_Step");
        appendExecCmdParameters(cmd, BatchConstants.ARG_LEVEL, "" + level);
        appendExecCmdParameters(cmd, BatchConstants.ARG_CUBING_JOB_ID, jobId);

        ndCuboidStep.setMapReduceParams(cmd.toString());
        ndCuboidStep.setMapReduceJobClass(getNDCuboidJob());
        return ndCuboidStep;
    }

    protected Class<? extends AbstractHadoopJob> getNDCuboidJob() {
        return NDCuboidJob.class;
    }
}
