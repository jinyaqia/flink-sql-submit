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

package com.jinyaqia.flink.sql.cli;

import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Command line options to configure the SQL client. Arguments that have not been specified
 * by the user are null.
 */
@Data
@Accessors(chain = true)
public class CliOptions {
    @Parameter(names = "-sql", description = "url encoded sql", order = 0)
    private String sqlEncode;
    @Parameter(names = "-name", description = "job name", required = true, order = 1)
    private String jobName;
    @Parameter(names = "-f", description = "sql file name",  order = 2)
    private String sqlFile;
    @Parameter(names = "-explain", description = "explain sql", order = 3)
    private boolean explain=false;
    @Parameter(names = {"-h","--help"}, order = 4)
    private boolean help=false;


}
