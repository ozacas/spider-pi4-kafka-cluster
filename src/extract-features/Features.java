/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package au.com.acassin.js.feature.extract;

import com.google.gson.Gson;
import java.util.HashMap;

/**
 * Responsible for collating statistics on function calls within the target code
 * and exporting to JSON
 * 
 * @author acas
 */
public class Features {
    private final HashMap<String, Integer> statements_by_count = new HashMap<>(10 * 1000);
    private final HashMap<String, Integer> calls_by_count = new HashMap<>(10 * 1000);
    private final String id;
    
    public Features(final String unique_id) {
        this.id = unique_id;
    }
    
    public void bump_statement(final String name) {
        if (statements_by_count.containsKey(name)) {
        } else {
            statements_by_count.put(name, 0);
        }
        statements_by_count.put(name, statements_by_count.get(name) + 1);
    }
    
    public void bump_call(final String name) {
        if (calls_by_count.containsKey(name)) {
        } else {
            calls_by_count.put(name, 0);
        }
        calls_by_count.put(name, calls_by_count.get(name) + 1);
    }
    
    public String asJSON() {
        return new Gson().toJson(this);
    }
}
