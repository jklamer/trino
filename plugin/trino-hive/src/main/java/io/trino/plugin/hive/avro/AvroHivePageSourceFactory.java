/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hive.avro;

import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static io.trino.plugin.hive.HiveSessionProperties.isAvroNativeReaderEnabled;

public class AvroHivePageSourceFactory
        implements HivePageSourceFactory
{
    private final TrinoFileSystem fileSystem;

    @Inject
    public AvroHivePageSourceFactory(TrinoFileSystem fileSystem)
    {
        this.fileSystem = fileSystem;
    }

    @Override
    public Optional<ReaderPageSource> createPageSource(Configuration configuration,
            ConnectorSession session,
            Path path,
            long start, long length, long estimatedFileSize, Properties schema, List<HiveColumnHandle> columns, TupleDomain<HiveColumnHandle> effectivePredicate, Optional<AcidInfo> acidInfo, OptionalInt bucketNumber, boolean originalFile, AcidTransaction transaction)
    {
        if (!isAvroNativeReaderEnabled(session)) {
            return Optional.empty();
        }
        throw new NotImplementedException();
    }
}
