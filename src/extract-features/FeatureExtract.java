/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package au.com.acassin.js.feature.extract;

import java.io.File;
import java.io.FileReader;
import org.mozilla.javascript.CompilerEnvirons;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Parser;
import org.mozilla.javascript.ast.AstNode;
import org.mozilla.javascript.ast.AstRoot;
import org.mozilla.javascript.ast.FunctionCall;
import org.mozilla.javascript.ast.FunctionNode;
import org.mozilla.javascript.ast.Name;
import org.mozilla.javascript.ast.NodeVisitor;
import org.mozilla.javascript.ast.PropertyGet;

/**
 * Responsible for extracting features to identify skimmers using whatever static
 * analysis methods are required. Supports Javascript scripts only eg. jquery-3.2.1.min.js as input
 * 
 * Mozilla rhino is used to identify features in the Javascript.
 * 
 * @author acas
 */
public class FeatureExtract {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: feature-extract <javascript file> <unique_object_id>");
            System.exit(1);
        }
        try {
            System.err.println("Processing file: "+args[0]);
            File scriptFile = new File(args[0]);
            FileReader fr = new FileReader(scriptFile);
            CompilerEnvirons compilerEnvirons = new CompilerEnvirons();
            compilerEnvirons.setRecordingComments(false);
            compilerEnvirons.setRecordingLocalJsDocComments(false);
            compilerEnvirons.setStrictMode(false);
            AstNode astNode = new Parser(compilerEnvirons).parse(fr, null, 1);
            AstRoot astRoot = astNode.getAstRoot();
            final Features features = new Features(args[1]);
            astRoot.visit(new NodeVisitor() {
                public String visit_target(AstNode target) {
                    // maybe FunctionNode or Name or PropertyGet node so we figure it all out here...
                    if (target instanceof Name) {
                        return ((Name)target).getIdentifier();
                    } else if (target instanceof PropertyGet) {
                        return visit_target(((PropertyGet)target).getProperty()); // Name instance
                    } else if (target instanceof FunctionNode) {
                        return visit_target(((FunctionNode)target).getFunctionName()); // also Name instance
                    }
                    
                    return "N/A";
                }
                
                @Override
                public boolean visit(AstNode n) {
                    String name = n.shortName();
                    features.bump_statement(name);
                    if ("FunctionCall".equals(name)) {
                       final String target = visit_target(((FunctionCall)n).getTarget());
                       features.bump_call(target);
                    }
                    return true;
                }
            });
            System.out.println(features.asJSON());
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
            System.exit(1);
        }
    }
    
}
