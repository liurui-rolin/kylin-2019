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
package org.apache.kylin.common.util;

import org.apache.commons.lang.StringUtils;

/**
 */
public final class SparkEntry {

    /**
     * 提交spark任务的方法
     * 此处直接main方法反射类，构建服务
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.out.println("SparkEntry args:" + StringUtils.join(args, " "));
        if (!(args.length >= 2)) {
            throw new IllegalArgumentException(String.valueOf("-className is required"));
        }
        if (!(args[0].equals("-className"))) {
            throw new IllegalArgumentException(String.valueOf("-className is required"));
        }
        final String className = args[1];
        //********** 反射对象 **********//
        final Object o = Class.<AbstractApplication> forName(className).newInstance();
        if (!(o instanceof AbstractApplication)) {
            throw new IllegalArgumentException(String.valueOf(className + " is not a subClass of AbstractApplication"));
        }
        String[] appArgs = new String[args.length - 2];
        for (int i = 2; i < args.length; i++) {
            appArgs[i - 2] = args[i];
        }

        //********** 强转 & 执行 SparkCubingByLayer是AbstractApplication的实现类 **********//
        AbstractApplication abstractApplication = (AbstractApplication) o;
        abstractApplication.execute(appArgs);
    }
}
