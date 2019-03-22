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

package org.devmaster.elasticsearch.plugin;

import org.elasticsearch.index.mapper.Mapper;
import org.devmaster.elasticsearch.index.mapper.RecurringFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.script.ScriptContext;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class RecurringPlugin extends Plugin implements MapperPlugin, ScriptPlugin  {
    
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<String, Mapper.TypeParser>();
        mappers.put(RecurringFieldMapper.CONTENT_TYPE, new RecurringFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }
    
    @Override 
    public List<ScriptContext<?>> getContexts() {
        List<ScriptContext<?>> contexts = new ArrayList<ScriptContext<?>>();
        return contexts;
    }
    
    /*
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<String, Mapper.TypeParser>();
        mappers.put(RecurringFieldMapper.CONTENT_TYPE, new RecurringFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }
    
    @Override 
    public List<ScriptContext<?>> getContexts() {
        List<ScriptContext<?>> contexts = new ArrayList<ScriptContext<?>>();
        return contexts;
    }
    */
    /*
    @Override
    public void onModule(ScriptModule module) {
        
        module.registerScript(NextOccurrenceSearchScript.SCRIPT_NAME, NextOccurrenceSearchScript.Factory.class);
        module.registerScript(HasOccurrencesAtSearchScript.SCRIPT_NAME, HasOccurrencesAtSearchScript.Factory.class);
        module.registerScript(OccurBetweenSearchScript.SCRIPT_NAME, OccurBetweenSearchScript.Factory.class);
        module.registerScript(NotHasExpiredSearchScript.SCRIPT_NAME, NotHasExpiredSearchScript.Factory.class);
        module.registerScript(OccurrencesBetweenSearchScript.SCRIPT_NAME, OccurrencesBetweenSearchScript.Factory.class);
        module.registerScript(HasAnyOccurrenceBetweenSearchScript.SCRIPT_NAME, HasAnyOccurrenceBetweenSearchScript.Factory.class);  
    }
    */
    /*
    public void onModule(IndicesModule module) {
        module.registerMapper(RecurringFieldMapper.CONTENT_TYPE, new RecurringFieldMapper.TypeParser());
    }
    */
    /*
    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(RecurringFieldMapper.CONTENT_TYPE, new RecurringFieldMapper.TypeParser());
    }
    */
    /*
    @Override
    public List<ScriptContext<?>> getContexts() {
        return Arrays.asList(new NextOccurrenceSearchScript.Factory(),
                new HasOccurrencesAtSearchScript.Factory(),
                new OccurBetweenSearchScript.Factory(),
                new NotHasExpiredSearchScript.Factory(),
                new OccurrencesBetweenSearchScript.Factory(),
                new HasAnyOccurrenceBetweenSearchScript.Factory());
    }
    */
}
