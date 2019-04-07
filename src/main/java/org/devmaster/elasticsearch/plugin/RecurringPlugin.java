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

import org.devmaster.elasticsearch.index.mapper.RecurringFieldMapper;
import org.devmaster.elasticsearch.script.HasAnyOccurrenceBetweenSearchScript;
import org.devmaster.elasticsearch.script.HasOccurrencesAtSearchScript;
import org.devmaster.elasticsearch.script.NextOccurrenceSearchScript;
import org.devmaster.elasticsearch.script.NotHasExpiredSearchScript;
import org.devmaster.elasticsearch.script.OccurBetweenSearchScript;
import org.devmaster.elasticsearch.script.OccurrencesBetweenSearchScript;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.ScriptPlugin;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class RecurringPlugin extends Plugin implements MapperPlugin, ScriptPlugin {

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(RecurringFieldMapper.CONTENT_TYPE, new RecurringFieldMapper.TypeParser());
    }
    
    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new RecurringEngine();
    }

    private static class RecurringEngine implements ScriptEngine {

    	@Override
		public String getType() {
			return "native";
		}

        @Override
        public <T> T compile(String scriptName, String scriptSource, ScriptContext<T> context, Map<String, String> params) {
        	if (context.equals(SearchScript.CONTEXT) == false && context.equals(FilterScript.CONTEXT) == false) {
                throw new IllegalArgumentException(getType() + " scripts cannot be used for context [" + context.name + "]");
            }
            // we use the script "source" as the script identifier
        	if ("hasAnyOccurrenceBetween".equals(scriptSource)) {
            	if (context.equals(SearchScript.CONTEXT) == true) {
            		SearchScript.Factory factory = hasAnyOccurrenceBetweenSearch::new;
                    return context.factoryClazz.cast(factory);
            	} else if (context.equals(FilterScript.CONTEXT) == true) {
            		FilterScript.Factory factory = (p, lookup) -> hasAnyOccurrenceBetweenFilter(p, lookup);
            		return context.factoryClazz.cast(factory);
            	}
            }
            if ("hasOccurrencesAt".equals(scriptSource)) {
            	if (context.equals(SearchScript.CONTEXT) == true) {
            		SearchScript.Factory factory = HasOccurrencesAtSearch::new;
                    return context.factoryClazz.cast(factory);
            	} else if (context.equals(FilterScript.CONTEXT) == true) {
            		FilterScript.Factory factory = (p, lookup) -> HasOccurrencesAtFilter(p, lookup);
                    return context.factoryClazz.cast(factory);
            	}
            }
            if ("nextOccurrence".equals(scriptSource)) {
            	if (context.equals(SearchScript.CONTEXT) == true) {
            		SearchScript.Factory factory = nextOccurrenceSearch::new;
                    return context.factoryClazz.cast(factory);
            	} else if (context.equals(FilterScript.CONTEXT) == true) {
            		FilterScript.Factory factory = (p, lookup) -> nextOccurrenceFilter(p, lookup);
                    return context.factoryClazz.cast(factory);
            	}
            }
            if ("notHasExpired".equals(scriptSource)) {
            	if (context.equals(SearchScript.CONTEXT) == true) {
            		SearchScript.Factory factory = notHasExpiredSearch::new;
                    return context.factoryClazz.cast(factory);
            	} else if (context.equals(FilterScript.CONTEXT) == true) {
            		FilterScript.Factory factory = (p, lookup) -> notHasExpiredFilter(p, lookup);
                    return context.factoryClazz.cast(factory);
            	}
            }
            if ("occurBetween".equals(scriptSource)) {
            	if (context.equals(SearchScript.CONTEXT) == true) {
            		SearchScript.Factory factory = occurBetweenSearch::new;
                    return context.factoryClazz.cast(factory);
            	} else if (context.equals(FilterScript.CONTEXT) == true) {
            		FilterScript.Factory factory = (p, lookup) -> occurBetweenFilter(p, lookup);
                    return context.factoryClazz.cast(factory);
            	}
            }
            if ("occurrencesBetween".equals(scriptSource)) {
            	if (context.equals(SearchScript.CONTEXT) == true) {
            		SearchScript.Factory factory = occurrencesBetweenSearch::new;
                    return context.factoryClazz.cast(factory);
            	} else if (context.equals(FilterScript.CONTEXT) == true) {
            		FilterScript.Factory factory = (p, lookup) -> occurrencesBetweenFilter(p, lookup);
                    return context.factoryClazz.cast(factory);
            	}
            }
            throw new IllegalArgumentException("Unknown script name " + scriptSource);
        }

        @Override
        public void close() {
            // optionally close resources
        }
        
        // Factory: hasAnyOccurrenceBetween
        private static class hasAnyOccurrenceBetweenSearch implements SearchScript.LeafFactory {
        	
            private final Map<String, Object> params;
            private final SearchLookup lookup;
            
            private hasAnyOccurrenceBetweenSearch(Map<String, Object> params, SearchLookup lookup) {
                if (params.containsKey("field") == false) {
                    throw new IllegalArgumentException("Missing parameter [field]");
                }
                if (params.containsKey("start") == false) {
                    throw new IllegalArgumentException("Missing parameter [start]");
                }
                if (params.containsKey("end") == false) {
                    throw new IllegalArgumentException("Missing parameter [end]");
                }
                this.params = params;
                this.lookup = lookup;
            }
            
            @Override
            public boolean needs_score() {
                return false;  // Return true if the script needs the score
            }

            @Override
            public SearchScript newInstance(LeafReaderContext context) throws IOException {
            	return new HasAnyOccurrenceBetweenSearchScript(params, lookup, context);
            }
        }
        // --------------
        private FilterScript.LeafFactory hasAnyOccurrenceBetweenFilter(Map<String, Object> params, SearchLookup lookup) {
        	SearchScript.LeafFactory searchLeafFactory = new hasAnyOccurrenceBetweenSearch(params, lookup);
        	return ctx -> {
                SearchScript search = searchLeafFactory.newInstance(ctx);
                return new FilterScript(params, lookup, ctx) {
                    @Override
                    public boolean execute() {
                        return search.runAsDouble() != 0.0;
                    }
                    
                    @Override
                    public void setDocument(int docid) {
                    	search.setDocument(docid);
                    }
                };
            };
        }
        // FactoryEnd: hasAnyOccurrenceBetween
        
        // Factory: HasOccurrencesAt
    	private static class HasOccurrencesAtSearch implements SearchScript.LeafFactory {
        	
            private final Map<String, Object> params;
            private final SearchLookup lookup;
            
            private HasOccurrencesAtSearch(Map<String, Object> params, SearchLookup lookup) {
                if (params.containsKey("field") == false) {
                    throw new IllegalArgumentException("Missing parameter [field]");
                }
                if (params.containsKey("date") == false) {
                    throw new IllegalArgumentException("Missing parameter [date]");
                }
                this.params = params;
                this.lookup = lookup;
            }

            @Override
            public boolean needs_score() {
                return false;  // Return true if the script needs the score
            }

            @Override
            public SearchScript newInstance(LeafReaderContext context) throws IOException {
                return new HasOccurrencesAtSearchScript(params, lookup, context);
            }
        }
    	// --------------
    	private FilterScript.LeafFactory HasOccurrencesAtFilter(Map<String, Object> params, SearchLookup lookup) {
        	SearchScript.LeafFactory searchLeafFactory = new HasOccurrencesAtSearch(params, lookup);
        	return ctx -> {
                SearchScript search = searchLeafFactory.newInstance(ctx);
                return new FilterScript(params, lookup, ctx) {
                    @Override
                    public boolean execute() {
                        return search.runAsDouble() != 0.0;
                    }
                    
                    @Override
                    public void setDocument(int docid) {
                    	search.setDocument(docid);
                    }
                };
            };
        }
    	// FactoryEnd: HasOccurrencesAt
    	
    	// Factory: nextOccurrence
    	private static class nextOccurrenceSearch implements SearchScript.LeafFactory {
        	
            private final Map<String, Object> params;
            private final SearchLookup lookup;
            
            private nextOccurrenceSearch(Map<String, Object> params, SearchLookup lookup) {
                if (params.containsKey("field") == false) {
                    throw new IllegalArgumentException("Missing parameter [field]");
                }
                this.params = params;
                this.lookup = lookup;
            }
            
            @Override
            public boolean needs_score() {
                return false;  // Return true if the script needs the score
            }

            @Override
            public SearchScript newInstance(LeafReaderContext context) throws IOException {
                return new NextOccurrenceSearchScript(params, lookup, context);
            }
        }
    	// --------------
    	private FilterScript.LeafFactory nextOccurrenceFilter(Map<String, Object> params, SearchLookup lookup) {
        	SearchScript.LeafFactory searchLeafFactory = new nextOccurrenceSearch(params, lookup);
        	return ctx -> {
                SearchScript search = searchLeafFactory.newInstance(ctx);
                return new FilterScript(params, lookup, ctx) {
                    @Override
                    public boolean execute() {
                        return search.runAsDouble() != 0.0;
                    }
                    
                    @Override
                    public void setDocument(int docid) {
                    	search.setDocument(docid);
                    }
                };
            };
        }
    	// FactoryEnd: nextOccurrence
		
    	// Factory: notHasExpired
		private static class notHasExpiredSearch implements SearchScript.LeafFactory {
			
		    private final Map<String, Object> params;
		    private final SearchLookup lookup;
		    
		    private notHasExpiredSearch(Map<String, Object> params, SearchLookup lookup) {
		        if (params.containsKey("field") == false) {
		            throw new IllegalArgumentException("Missing parameter [field]");
		        }
		        this.params = params;
		        this.lookup = lookup;
		    }
		
		    @Override
		    public boolean needs_score() {
		        return false;  // Return true if the script needs the score
		    }
		
		    @Override
		    public SearchScript newInstance(LeafReaderContext context) throws IOException {
		        return new NotHasExpiredSearchScript(params, lookup, context);
		    }
		}
		// -------------
		private FilterScript.LeafFactory notHasExpiredFilter(Map<String, Object> params, SearchLookup lookup) {
        	SearchScript.LeafFactory searchLeafFactory = new notHasExpiredSearch(params, lookup);
        	return ctx -> {
                SearchScript search = searchLeafFactory.newInstance(ctx);
                return new FilterScript(params, lookup, ctx) {
                    @Override
                    public boolean execute() {
                        return search.runAsDouble() != 0.0;
                    }
                    
                    @Override
                    public void setDocument(int docid) {
                    	search.setDocument(docid);
                    }
                };
            };
        }
		// FactoryEnd: notHasExpired
		
		// Factory: occurBetween
		private static class occurBetweenSearch implements SearchScript.LeafFactory {
        	
            private final Map<String, Object> params;
            private final SearchLookup lookup;
            
            private occurBetweenSearch(Map<String, Object> params, SearchLookup lookup) {
                if (params.containsKey("field") == false) {
                    throw new IllegalArgumentException("Missing parameter [field]");
                }
                if (params.containsKey("start") == false) {
                    throw new IllegalArgumentException("Missing parameter [start]");
                }
                if (params.containsKey("end") == false) {
                    throw new IllegalArgumentException("Missing parameter [end]");
                }
                this.params = params;
                this.lookup = lookup;
            }

            @Override
            public boolean needs_score() {
                return false;  // Return true if the script needs the score
            }

            @Override
            public SearchScript newInstance(LeafReaderContext context) throws IOException {
                return new OccurBetweenSearchScript(params, lookup, context);
            }
        }
		// -----------
		private FilterScript.LeafFactory occurBetweenFilter(Map<String, Object> params, SearchLookup lookup) {
        	SearchScript.LeafFactory searchLeafFactory = new occurBetweenSearch(params, lookup);
        	return ctx -> {
                SearchScript search = searchLeafFactory.newInstance(ctx);
                return new FilterScript(params, lookup, ctx) {
                    @Override
                    public boolean execute() {
                        return search.runAsDouble() != 0.0;
                    }
                    
                    @Override
                    public void setDocument(int docid) {
                    	search.setDocument(docid);
                    }
                };
            };
        }
		// FactoryEnd: occurBetween

		// Factory: occurrencesBetween
		private static class occurrencesBetweenSearch implements SearchScript.LeafFactory {
			
		    private final Map<String, Object> params;
		    private final SearchLookup lookup;
		    
		    private occurrencesBetweenSearch(Map<String, Object> params, SearchLookup lookup) {
		        if (params.containsKey("field") == false) {
		            throw new IllegalArgumentException("Missing parameter [field]");
		        }
		        this.params = params;
		        this.lookup = lookup;
		    }
		
		    @Override
		    public boolean needs_score() {
		        return false;  // Return true if the script needs the score
		    }
		
		    @Override
		    public SearchScript newInstance(LeafReaderContext context) throws IOException {
		        return new OccurrencesBetweenSearchScript(params, lookup, context);
		    }
		}
		// ------------------
		private FilterScript.LeafFactory occurrencesBetweenFilter(Map<String, Object> params, SearchLookup lookup) {
        	SearchScript.LeafFactory searchLeafFactory = new occurrencesBetweenSearch(params, lookup);
        	return ctx -> {
                SearchScript search = searchLeafFactory.newInstance(ctx);
                return new FilterScript(params, lookup, ctx) {
                    @Override
                    public boolean execute() {
                        return search.runAsDouble() != 0.0;
                    }
                    
                    @Override
                    public void setDocument(int docid) {
                    	search.setDocument(docid);
                    }
                };
            };
        }
		// FactoryEnd: occurrencesBetween
        
    }
    
}
