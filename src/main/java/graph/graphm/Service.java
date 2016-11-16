/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package graph.graphm;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import org.neo4j.driver.v1.*;
import org.neo4j.helpers.collection.IteratorUtil;

/**
 *
 * @author Qih
 */
public class Service {    
    private final Driver driver;
    private final Session session;
    

    public Service() {
        driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic( "neo4j", "qazplm22" ) );        
        session = driver.session();        
    }   

    
    /*
    Retorna a quantidade de nós de um determinado tipo
    */
    public int getNodeCount(String type){
        int count = 0;
        String Query = "start n=Node(*) MATCH (n:"+type+") return count(n) as contagem";
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            count = record.get("contagem").asInt();
        }
        return count;
    }
    
 
    public Double getClosenessCentrality(int nodeID, int minHops, int maxHops, String relationship, String nodeLabel){
        Double CC, n, allPathsCount;
        n = (double) this.getNodeCount(nodeLabel); 
       CC= 0.0;
        allPathsCount=0.0;
        String Query="MATCH (n), (x), path=shortestpath((n)-[r:"+relationship+"*"+minHops+".."+maxHops+"]-(x))" +
                    " WHERE id(n) = "+nodeID+" " +
                    " RETURN n, x, path, Sum(length(path)) AS passos";
        System.out.println(Query);
        StatementResult result = session.run(Query);
        if ( result.hasNext() )
        {
            Record record = result.next();
            allPathsCount = allPathsCount + record.get("passos").asDouble();
        }
        CC = allPathsCount / (n*(n-1));
        return CC;
    }            
    
    /*
    Retorna a propriedade de um nó a partir do seu ID
    */
    public String getNodeProperty(int id, String property){
        String nome=null;
        String Query="MATCH (a)" +
                "WHERE ID(a)= "+id+" "+
                    "RETURN a."+property+" AS prop";
        //System.out.println(Query);
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            nome = record.get("prop").asString();
        }
        return nome;
    }
    
    /*
    Retorna a quantidade de arestas de um determinado tipo
    */
    public int getEdgeCount(String type){
        int count=0;
        String Query="match ()-[r:"+type+"]-()" +
                "return count(r)as contagem";
        //System.out.println(Query);
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            count = record.get("contagem").asInt();
        }
        return count;        
    }
    
    /*
    Todos os caminhos mínimos entre dois nós
    */
    public int getShortestPathCount(int node1, int node2){
        int count=0;
        String Query="MATCH (a), (b), path = allShortestPaths((a)-[*]-(b))" +
                "WHERE ID(a)= "+node1+" AND  ID(b)="+node2+" "+
                "RETURN count(path) AS contagem";
        //System.out.println(Query);
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            count = record.get("contagem").asInt();
        }
        return count;
    }
    /*
    Todos os caminhos mínimos entre dois nós que passam por um nó específico
    */
    public int getShortestPathCountBy(int source, int destination, int intermediaryNode){
        int count=0;        
        String Query="MATCH (a), (b), path = allShortestPaths((a)-[*]-(b)) " +
                    "WHERE ANY (n in nodes(path) where (ID(n)="+intermediaryNode+")) " +
                    "AND ID(a)= "+source+" "+
                    "AND ID(b)="+destination+" "+
                     "RETURN count(path) as contagem";
        //System.out.println(Query);
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            count = record.get("contagem").asInt();
        }
        return count;
    }
    /*
    Retorna uma lista com os id's dos nós do tipo.
    */
    public ArrayList<Integer> getAllNodesByType(String type){
        String Query="MATCH (n:"+type+") return ID(n) as id";
        StatementResult result = session.run(Query);
        ArrayList<Integer> arrId= new ArrayList<Integer>();
        while ( result.hasNext() )
        {
            Record record = result.next();
            arrId.add(Integer.parseInt(record.get("id").toString()));            
        }
        return arrId;
    }
    
    /*
    Retorna uma lista com os id's dos relacionamentos do tipo.
    */
    public ArrayList<Integer> getAllRelationshipsByType(String type){
        String Query="MATCH ()-[r:"+type+"]-() return DISTINCT ID(r) as id";
        StatementResult result = session.run(Query);
        ArrayList<Integer> arrId= new ArrayList<Integer>();
        while ( result.hasNext() )
        {
            Record record = result.next();
            arrId.add(Integer.parseInt(record.get("id").toString()));            
        }
        return arrId;
    }
    
    /*
    Retorna o ID de uma nó a partir da string e da sua label.
    */
    public Integer getNodeID(String s, String label){
        String Query="MATCH (n {"+label+": '"+s+"'}) return ID(n) as id, count(n) as contagem";
        StatementResult result = session.run(Query);
        if ( result.hasNext() )
        {
            Record record = result.next();
            if(Integer.parseInt(record.get("contagem").toString())!=1){
                System.out.println("Método NodeService.getNodeID com mais de um resultado na query");                
            }
            return (Integer.parseInt(record.get("id").toString()));            
        }        
        System.out.println("Método NodeService.getNodeID sem dados no resultado da query");
        return -1;
    }
    
    /*
    Retorna a Betweenness de um nó.
    */
    public Double getBetweennessCentrality(int intermediaryNodeID, String nodeType){
        //Variaves
        Double BC =0.0;
        Integer source=0;
        Integer target=1;
        Integer sourceID, targetID, shortestSTbyIntermediary, shortestST;
        Double top=0.0;
        Double down=0.0;
        
        //Pega o conjunto de nós e remove o nó que se quer calcular a centralidade
        ArrayList<Integer> nodes = this.getAllNodesByType(nodeType);
        nodes.remove(nodes.indexOf(intermediaryNodeID));
        //Checa se o DataSet tem tamanho ok
        if(nodes.size()<2){
            System.out.println("Conjunto muito pequeno, favor avaliar o dataset");
            return BC;
        }
        
        
        Double[][] matriz = new Double[nodes.size()][nodes.size()];
        for (int i=0; i< nodes.size();i++){
            for (int j=0; j< nodes.size();j++){
                top=top + (double) this.getShortestPathCountBy(nodes.get(i), nodes.get(j), intermediaryNodeID);
                down=down + (double) this.getShortestPathCount(nodes.get(i), nodes.get(j));                            
                matriz[i][j] = (double) this.getShortestPathCount(nodes.get(i), nodes.get(j));                
            }
        }
        BC = BC + (top/down);
        /* prints de BC
        System.out.println("BC: "+BC);
        System.out.println("No: "+this.getNodeName(intermediaryNodeID));
        */
        
        return BC;
    }
    
    /*
    Retorna o grau de um nó
    */
    public Integer getDegree(int id){
        String Query="MATCH (a)" +
                    " WITH a, size((a)-[]-()) as degree" +
                    " WHERE ID(a) = "+id+"" +
                    " return a, degree";
        StatementResult result = session.run(Query);
        if ( result.hasNext() )
        {
            Record record = result.next();            
            return (Integer.parseInt(record.get("degree").toString()));            
        }        
        System.out.println("Método NodeService.getNodeID sem dados no resultado da query");
        return -1;
    }
    
    /*
    Retorna o ID dos vizinhos do nó
    
    */
    public ArrayList<Integer> getNeighbors(int id){
        ArrayList<Integer> neighbors = new ArrayList<Integer>();
        String Query="MATCH (a)-[]-(b)" +
                    " WITH a,b" +
                    " WHERE ID(a) = "+id+" " +
                    " return ID(b) as chave";
        //System.out.println(Query);
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            neighbors.add(record.get("chave").asInt());
        }
        return neighbors;
    }
    
    /*
    Retorna a quantidade de relações entre dois nós
    */    
    public int getRelationsBetweenNodes(int a, int b){
        int rel = 0;
         String Query="MATCH (a)-[r]-(b)" +
                    " WITH a,b,r" +
                    " WHERE ID(a) = "+a+" " +
                    " AND  ID(b) = "+b+
                    " return count(r) as contagem";
        //System.out.println(Query);
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            rel = record.get("contagem").asInt();
        }
        return rel;
    }
    
    /*
    Retorna a quantidade de relações entre um conjunto de nós
    */
    public int getRelationsBetweenNodes(ArrayList<Integer> arr){
        int rel = 0;
        int i=0;
        int j=i+1;
        
        while(i<arr.size()){
            while(j< arr.size()){                
                rel = rel + this.getRelationsBetweenNodes(arr.get(i), arr.get(j));
                j++;
            }
            i++;
            j=i+1;
            
        }        
        return rel;
    }
    
    /*
    Retorna o coeficiente de clusterização de um nó.
    */
    public Double getClusteringCoefficient(int id){
        Double CC=0.0;
        int degree=0;
        int neighborRel=0;
        
        degree = this.getDegree(id);
        neighborRel = this.getRelationsBetweenNodes(this.getNeighbors(id));
        CC = (2 * neighborRel) / (double)( degree *(degree-1));                
        return CC;
    }
    
    /*
    Retorna um array com ID's de nós de um determinado tipo.
    */
    public ArrayList<Integer> getNodesByType(String nodeType){
        ArrayList<Integer> arr = new ArrayList<Integer>();
        String Query="MATCH (node:"+nodeType+")" +
                    " return ID(node) as chave";
        //System.out.println(Query);
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            arr.add(record.get("chave").asInt());
        }
        return arr;
    }
    /*
    Retorna o PageRank de um conjunto de nós
    */
    public Map<Integer,Double> getNodesPageRank(ArrayList<Integer> nodes, String nodeType){
        Map<Integer,Double> map = new HashMap<Integer,Double>();
        
        String Query="MATCH (node:"+nodeType+")" +
        " WITH collect(node) AS nodes" +
        " CALL apoc.algo.pageRank(nodes) YIELD node, score" +
        " RETURN ID(node) as chave, score as score" +
        " ORDER BY score DESC";
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            map.put(record.get("chave").asInt(), record.get("score").asDouble());            
        }
        return map;
    }
    
    /*
    Atribui OU atualiza uma propriedade com um valor a um determinado Nó
    */
    public void setNodeProperty(int nodeID, String property, String value){
        String Query = "MATCH (n)" +
                        " WHERE ID(n) = "+nodeID+
                        " SET n."+property+" = "+value+" ";
        //System.out.println(Query);
        session.run(Query);
    }
    
    /*
    Atribui OU atualiza uma propriedade com um valor a uma determinada aresta
    */
    public void setEdgeLabel(int nodeID, String property, String value){
        String Query = "MATCH ()-[r]-()" +
                        " WHERE ID(r) = "+nodeID+
                        " SET r."+property+" = "+value+" ";
        //System.out.println(Query);
        session.run(Query);
    }    
    
    /*
    Utiliza biblioteca APOC para calcular via dijkstra o menor caminho entre 2 nos
    */
    public double getWeightedShortestPath(int nodeID1, int nodeID2, String relationship, String weightProperty){
        double pathWeight = 0.0;
        String Query = "match (startNode), (endNode) "
                + "where ID(startNode) = "+nodeID1+"  and id(endNode)= "+nodeID2+" "
                + "call apoc.algo.dijkstra(startNode, endNode, '"+relationship+"', '"+weightProperty+"') YIELD path, weight "
                + "return path as path ,weight as weight";
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            pathWeight = record.get("weight").asDouble();
        }
        
        return pathWeight;
        
    }
    
      /*
    Em um grafo com pesos retorna um par com contagem de caminhos entre dois nós passsando por um terceiro e tamanho do caminho. 
    PS: o valor retornado está em double!
    */
    public ArrayList<Double> getWeightedShortestPathBy(int nodeID1, int nodeID2, int intermediaryNodeID, String relationshipName, 
            int maxHops, String propertyName){
        ArrayList<Double> arr = new ArrayList<Double>();
        String Query = "START  startNode=node("+nodeID1+"), endNode=node("+nodeID2+")"
                + " MATCH  p=(startNode)-[:"+relationshipName+"*.."+maxHops+"]-(endNode)"
                + " WHERE ANY(n in nodes(p) where (ID(n)="+intermediaryNodeID+"))"
                + " RETURN count(p) AS shortestPath, "
                + " reduce("+propertyName+"=0, r in relationships(p) | "+propertyName+"+r."+propertyName+") AS totalWeight"
                + " ORDER BY totalWeight ASC"
                + " LIMIT 1";
        System.out.println(Query);
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            arr.add(record.get("shortestPath").asDouble());
            arr.add(record.get("totalWeight").asDouble());
        }
        return arr;
    }
    
    /*
    Verifica se um nó está conectado a todos os nós de um determinado tipo.
    */
    public boolean checkConnectedToAll(int nodeID, int maxHops, String nodeType){
        String Query = "START  startNode=node("+nodeID+") " +
            "MATCH (startNode)-[:CONNECTED*.."+maxHops+"]-(n:"+nodeType+") " +
            "WITH startNode, COLLECT(DISTINCT n) AS others " +
            "WITH COLLECT(others) AS coll " +
            "RETURN FILTER(x IN coll[0] WHERE ALL(y IN coll[1..] WHERE x IN y)) AS res;";
        //System.out.println(Query);
        StatementResult result = session.run(Query);
        List<Object> results = new ArrayList<Object>();
        while ( result.hasNext() )
        {
            Record record = result.next();            
            results = record.get("res").asList();
            //Lista com os ids dos nós que se conectam no nó desejado de alguma forma
            //System.out.println(results.get(0).toString().replaceAll("[^\\d.]", ""));
            
            //Se a quantidade de nós retornados for diferente da contagem de nós deste tipo, então está desconectado de alguma forma
            if(results.size() == this.getNodeCount(nodeType)){
                return true;
            }
        }
        return false;
    }
    
    /*
    Identifica a quantidade de nós de um determinado tipo que não estão conectados ao nó alvo.
    */
    public int getCountUnreachableNodes(int nodeID, int maxHops, String nodeType){
        int nodeCount = this.getNodeCount(nodeType);
        String Query = "START  startNode=node("+nodeID+") " +
            "MATCH (startNode)-[:CONNECTED*.."+maxHops+"]-(n:"+nodeType+") " +
            "WITH startNode, COLLECT(DISTINCT n) AS others " +
            "WITH COLLECT(others) AS coll " +
            "RETURN FILTER(x IN coll[0] WHERE ALL(y IN coll[1..] WHERE x IN y)) AS res;";
        //System.out.println(Query);
        StatementResult result = session.run(Query);
        List<Object> results = new ArrayList<Object>();
        while ( result.hasNext() )
        {
            Record record = result.next();            
            results = record.get("res").asList();
            //Lista com os ids dos nós que se conectam no nó desejado de alguma forma
            //System.out.println(results.get(0).toString().replaceAll("[^\\d.]", ""));
            
            //Se a quantidade de nós retornados for diferente da contagem de nós deste tipo, então está desconectado de alguma forma
            if(results.size() == nodeCount){
                return 0;
            }
        }
        return (nodeCount - results.size());
    }
    
    /*
    Executa uma query recebida no formato de string.
    */
    public void executeQuery (String query){
        StatementResult result = session.run(query);
    }
    
    /*
    Retorna a soma dos pesos das relações de um nó por tipo de relacao
    */
    public double getWeightedDegree(int nodeID, String relationship){
        double degree=0.0;
        String Query = "match p=(n)-[r:"+relationship+"]-() "
                + "WHERE ID(n) = "+nodeID
                + " return  sum (toFloat(r.weight)) as grau, count(r)";
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            degree = record.get("grau").asDouble();
        }
        return degree;
    }    
    
    /*
    Calcula um fator de redução exponencial a partir do tempo, utilizando uma taxa
    (1-e)ª onde e= porcentagem de declinio e ª = tempo
    */
    public double getWeightWithDecay(double originalWeight, double decay, int timePassed){
        return originalWeight*Math.pow((1+decay),timePassed);
    }
    
    
    /*
    Novo fator de redução
    */
    public double getWeightWithDecay2(double originalWeight, double decay, int timePassed){
        return originalWeight;
    }
    
    /*
    Pega um conjunto de relacoes entre dois nós e unifica em uma única relação.
    */
    
    public void sintetizaRelações(int nodeID1, int nodeID2, String relationship,
            String weight, String time, int timeNow, double decay){
       
        //Query para pegar as relações entre os dois nós
        String Query = " Match (a), (b), p=(a)-[r:"+relationship+"]-(b)" +
                       " WHERE ID(a) = "+nodeID1+" AND ID(b) = "+nodeID2+
                       " Return r."+weight+" as pesos, r."+time+" as tempo";
        StatementResult result = session.run(Query);

        //Armazena os pesos e anos de cada relação
        List<Double> weights = new ArrayList<Double>();
        List<Integer> times = new ArrayList<Integer>();
        while ( result.hasNext() )
        {
            Record record = result.next();            
            weights.add(Double.parseDouble(record.get("pesos").asString()));
            times.add(Integer.parseInt(record.get("tempo").asString()));
            
        }
        Double newWeight =0.0;
        Double sumWeight =0.0;
        
        //No for calcula a soma dos pesos e a nova soma com o fator redutor por tempo.
        int i;        
        for(i=0; i<times.size(); i++){
            newWeight = newWeight + getWeightWithDecay(weights.get(i), decay, timeNow-times.get(i));
            sumWeight = sumWeight + weights.get(i);
        }
       
        //Novo peso
        int sumTime=0;
        Double bonus;
        Double bonusWeight;
        for (int x : times){
            sumTime += x;
        }
        /*
        O bonus representa: 
        dividendo: (soma das diferencas entre o ano da relação e o ano atual) 
        divisor: somatorio do intervalo entre o ano 
        
        Esse valor tende a ser maior quanto mais recente for a interrupção de comunicação.                
        O intervalo de bonus é [0,1]
        */
        bonus = ((double)(times.size()*(double)timeNow)-(double)sumTime) / (double)this.getSomatorio(timeNow-this.getMenor(times));

        // Aqui pega a outra parte do bonus e multiplica pela quantidade de relações
        bonus = (1-bonus)*times.size();
        
        /*
        O peso unificado realiza a soma dos pesos das relações, divide pela quantidade de relações
        Depois divide esse valor pelo bonus calculado.         
        */
        bonusWeight = (sumWeight/i)/bonus;
        
        
        if(newWeight != 0){
            this.setNewRelationshipBetweenNodes(nodeID1, nodeID2, "decay"+relationship, "decayweight", newWeight, "count", i,
                    "decayWeightDiv", newWeight/i, "sumWeights", sumWeight, "sumWeightDiv", sumWeight/i, "BonusWeight", bonusWeight);
        }
        
        System.out.println("count: "+i+"\n newWeight: "+newWeight+"\n decayWeightDiv: "+newWeight/i + "\n sumWeights: "+sumWeight+" \n sumWeightDiv: "+sumWeight/i+ "\n novaFreq: "+bonusWeight);
    }
    
    /*
    Calcula somatorio para auxiliar a dar peso nas relacoes unificadas
    */
    public int getSomatorio(int valor){
        int somatorio=0;
        for(int i=1; i<valor+1;i++){
            somatorio = somatorio + i;
        }
        return somatorio;
    }
    
    /*
    Pega menor do array
    */
    public int getMenor(List<Integer> arr){
        int menor = arr.get(0);
        for (int i=0; i<arr.size();i++){
            if(arr.get(i)< menor){
                menor = arr.get(i);
            }
        }
        return menor;
    }
    
    /*
    Pega maior do array
    */
    public int getMaior(List<Integer> arr){
        int maior = arr.get(0);
        for (int i=0; i<arr.size();i++){
            if(arr.get(i)> maior){
                maior = arr.get(i);
            }
        }
        return maior;
    }
    
    /*
    Cria uma nova relação entre dois nós.
    A relação possui uma propriedade que armazena um double.
    */
    public void setNewRelationshipBetweenNodes(int nodeID1, int nodeID2, 
            String relationshipName, String propertyName, Double Value, String propertyName2, int Value2, String propertyName3, double Value3,
            String propertyName4, double Value4, String propertyName5, double Value5, String propertyName6, double Value6){
        String Query = "Match (a), (b) "
                + "WHERE ID(a) = "+nodeID1+" AND ID(b) = "+nodeID2+" "
                + "CREATE (a)-[:"+relationshipName+" {"+propertyName+": "+Value+", "
                + ""+propertyName2+": "+Value2+", "+propertyName3+": "+Value3+","
                + " "+propertyName4+": "+Value4+", "+propertyName5+": "+Value5+""
                + ", "+propertyName6+": "+Value6+"}]->(b)";        
        StatementResult result = session.run(Query);
        
        
    }
    
    /*
    Closeness Centrality em relacoes com peso    
    */
    
    public double getWeightedClosenessCentrality(int nodeID, String relationship, String weightProperty){
        List<Object> label= new ArrayList<Object>();
        String Query= "MATCH (r) WHERE ID(r) = "+nodeID+" RETURN  labels(r) as label";
        StatementResult result = session.run(Query);
        while ( result.hasNext() )
        {
            Record record = result.next();
            label = record.get("label").asList();
        }
        
        ArrayList<Integer> nodeIDS =this.getAllNodesByType(label.get(0).toString());
        nodeIDS.remove(Integer.valueOf(nodeID));
        double totalShortestPaths = 0.0;
        for (int i=0;i<nodeIDS.size();i++){
            totalShortestPaths = totalShortestPaths + this.getWeightedShortestPath(nodeID, nodeIDS.get(i), relationship, weightProperty);
        }
        return totalShortestPaths / (double) (nodeIDS.size()* (nodeIDS.size()-1));
    }
    
    
}

