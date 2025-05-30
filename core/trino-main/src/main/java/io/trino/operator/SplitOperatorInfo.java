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
package io.trino.operator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Mergeable;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class SplitOperatorInfo
        implements OperatorInfo, Mergeable<SplitOperatorInfo>
{
    private final Map<String, String> splitInfo;

    @JsonCreator
    public SplitOperatorInfo(@JsonProperty("splitInfo") Map<String, String> splitInfo)
    {
        this.splitInfo = splitInfo;
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }

    @JsonProperty
    public Map<String, String> getSplitInfo()
    {
        return splitInfo;
    }

    @Override
    public SplitOperatorInfo mergeWith(SplitOperatorInfo other)
    {
        return mergeWith(List.of(other));
    }

    @Override
    public SplitOperatorInfo mergeWith(List<SplitOperatorInfo> others)
    {
        return new SplitOperatorInfo(
                splitInfo.entrySet().stream().collect(toImmutableMap(Map.Entry::getKey, e -> e.getValue() + " (" + others.size() + " more)")));
    }
}
