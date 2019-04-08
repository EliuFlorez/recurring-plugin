/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.devmaster.elasticsearch.script;

import org.devmaster.elasticsearch.index.mapper.Recurring;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.text.ParseException;
import java.util.Map;

public class NotHasExpiredSearchScript extends AbstractRecurringSearchScript {

    private static final String PARAM_FIELD = "field";

    public NotHasExpiredSearchScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        super(params, lookup, leafContext);
    }

    @Override
    public double runAsDouble() {
    	Recurring recurring = getRecurring(getParamValueFor(PARAM_FIELD));
        try {
            return recurring != null && recurring.notHasExpired() ? 1.0d : 0.0d;
        } catch (ParseException e) {
        	// ParseException null
        }
        return 0.0d;
    }
}
