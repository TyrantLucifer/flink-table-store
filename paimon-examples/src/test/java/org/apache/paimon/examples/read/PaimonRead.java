/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.examples.read;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;

public class PaimonRead {
    public static void main(String[] args) {
        Map<String, String> optionMap = new HashMap<>();
        optionMap.put(WAREHOUSE.key(), "/tmp/table_store");
        Options options = Options.fromMap(optionMap);
        Configuration hadoopConf = new Configuration();
        CatalogContext catalogContext = CatalogContext.create(options, hadoopConf);
        try (Catalog catalog = CatalogFactory.createCatalog(catalogContext)) {
            Table table = catalog.getTable(Identifier.create("default", "st_test"));
            ReadBuilder readBuilder = table.newReadBuilder();
            TableScan tableScan = readBuilder.newScan();
            TableScan.Plan plan = tableScan.plan();
            List<Split> splits = plan.splits();
            long l = splits.get(0).rowCount();
        } catch (Exception e) {
            throw new RuntimeException("Get table failed", e);
        }
    }
}
