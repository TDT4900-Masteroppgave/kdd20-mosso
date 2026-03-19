package mosso;

import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import jdk.jshell.spi.ExecutionControl;
import mosso.SummaryGraphModule;
import mosso.AdjacencyList;
import mosso.util.*;

import static java.lang.Double.min;

public class SupernodeHelper extends SummaryGraphModule {
    protected ObjectArrayList<IntHashSet> invV = new ObjectArrayList<>();
    private IntArrayList deg = new IntArrayList();

    private IntArrayList emptyNodeStack = new IntArrayList();

    // Helper variables for counting
    private boolean useEdgeCounter = true;
    private AdjacencyList E = new AdjacencyList();
    private IntArrayList EdgeCnt = new IntArrayList();

    public SupernodeHelper(boolean directed) {
        super(directed);
        if(directed){
            try {
                throw new ExecutionControl.NotImplementedException("Directed version is NOT_IMPLEMENTED");
            } catch (ExecutionControl.NotImplementedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    protected int getSize(int Sv){
        return invV.get(Sv).size();
    }

    protected int getDegree(int v){
        return deg.getInt(v);
    }


    protected int getNumberOfSupernodes(){
        return n - emptyNodeStack.size();
    }


    protected int newSupernode(){
        return emptyNodeStack.topInt();
    }


    protected IntSet getMembers(int Sv){
        return invV.get(Sv);
    }

    protected IntArrayList getNeighbors(int v){
        // Based on algorithms in SWeG
        IntOpenHashSet nbrs = new IntOpenHashSet();
        int Sv = V.getInt(v);
        for(int nbr: P.getNeighbors(Sv)){
            nbrs.addAll(invV.get(nbr));
        }
        for(int nbr: Cm.getNeighbors(v)){
            nbrs.remove(nbr);
        }
        nbrs.remove(v);
        IntArrayList Nv = new IntArrayList(nbrs);
        for(int nbr: Cp.getNeighbors(v)){
            Nv.add(nbr);
        }
        return Nv;
    }

    // protected IntArrayList get2HopNeighbors(int v, int b) {
    //     IntArrayList nbrs = getNeighbors(v);
    //     IntArrayList nbrs2Hop = new IntArrayList();

    //     final int num_to_sample = Integer.min(b, nbrs.size());

    //     for (int one_hop : nbrs) {
    //         if (one_hop > v) { // Avoiding duplicate node pairs, as {u, v} = {v, u}
    //             nbrs2Hop.add(one_hop);
    //         }
    //     }

    //     IntArrayList subset = new IntArrayList(num_to_sample);
    //     int count = 0;

    //     for (int i = 0; i < num_to_sample; i++) {
    //         subset.add(nbrs.getInt(i));
    //     }

    //     for (int w : subset) {
    //         IntArrayList nbrsW = getNeighbors(w);
    //         for (int nbr : nbrsW) {
    //             if (!nbrs2Hop.contains(nbr)) {
    //                 nbrs2Hop.add(nbr);
    //             }
    //         }
    //     }

    //     return nbrs2Hop;
    // }

    
    protected IntArrayList get2HopNeighbors(int v, int b) {
        final IntArrayList oneHop = getNeighbors(v);        // N(u)
        final int n = oneHop.size();
        if (n == 0) return new IntArrayList(0);

        // === 1) Put N(u) into a set (dedup base), and exclude v if present ===
        final IntOpenHashSet pool = new IntOpenHashSet(Math.max(16, n * 2));
        for (int i = 0; i < n; i++) {
            final int uNbr = oneHop.getInt(i);
            if (uNbr != v) pool.add(uNbr);
        }

        // === 2) Sample b distinct neighbors S ⊂ N(u) uniformly (without replacement) ===
        final int s = Math.min(b, n);
        if (s > 0) {
            // Reservoir sample indices [0..n-1]
            final int[] pickIdx = new int[s];
            for (int i = 0; i < s; i++) pickIdx[i] = i;
            for (int i = s; i < n; i++) {
                final int j = randInt(0, i);   // inclusive [0,i]
                if (j < s) pickIdx[j] = i;
            }

            // === 3) Expand via neighbors of each sampled w and add to pool ===
            for (int i = 0; i < s; i++) {
                final int w = oneHop.getInt(pickIdx[i]);   // w ∈ S
                final IntArrayList nbrsW = getNeighbors(w);
                for (int k = 0, m = nbrsW.size(); k < m; k++) {
                    final int x = nbrsW.getInt(k);
                    if (x != v) pool.add(x); // avoid self
                }
            }
        }

        // === 4) Emit as IntArrayList ===
        return new IntArrayList(pool);
    }


    protected int getNeighborCost(int v){
        return getDegree(v) + P.getAdjList().get(V.getInt(v)).size() + 2 * Cm.getAdjList().get(v).size();
    }

    protected int getSupernodeDegree(int Sv){
        return E.getAdjList().get(Sv).size();
    }

    protected IntArrayList getRandomNeighbors(int v, int k) {
        int Sv = V.getInt(v);
        IntArrayList randomNeighbors = new IntArrayList(k);
        Int2IntHashMap Cp_v = Cp.getAdjList().get(v);
        Int2IntHashMap Cm_v = Cm.getAdjList().get(v);
        Int2IntHashMap P_v = P.getAdjList().get(Sv);

        int psz = Cp.getAdjList().get(v).size();
        int now = -1, nowsz = 0;

        for (int i = 0; i < k; i++) {
            int idx = randInt(0, deg.getInt(v) - 1);
            if (idx < psz) {
                randomNeighbors.add(Cp_v.getRandomElement());
            } else {
                while (true) {
                    int candidate = P_v.getRandomElement(), candisz = P_v.get(candidate);
                    // MCMC for sampling neighbors
                    if (now == -1 || randDouble() < min(1.0, candisz / (double) nowsz)) {
                        now = candidate;
                        nowsz = candisz;
                    }
                    int nbd = invV.get(now).getRandomElement();
                    if (!Cm_v.containsKey(nbd) && (nbd != v)) {
                        randomNeighbors.add(nbd);
                        break;
                    }
                }
            }
        }
        return randomNeighbors;
    }

    protected int getEdgeCount(int Su, int Sv){
        return E.getEdgeCount(Su, Sv);
    }

    protected Int2IntOpenHashMap getEdgeCountAll(int Su){
        return E.getAdjList().get(Su).clone();
    }

    protected void updateEdgeCount(int Su, int Sv, int delta){
        E.updateEdge(Su, Sv, delta);
        EdgeCnt.set(Su, EdgeCnt.getInt(Su) + delta);
    }

    protected int getTotalEdgeCount(int Sv){
        return EdgeCnt.getInt(Sv);
    }

    protected IntSet getSupernodeNeighbors(int Sv){
        if(useEdgeCounter) return E.getNeighbors(Sv);
        else return null;
    }

    protected Int2IntMap.FastEntrySet getSupernodeNeighborsAndWeights(int Sv){
        if(useEdgeCounter) return E.getNeighborsAndWeights(Sv);
        else return null;
    }

    protected Int2IntOpenHashMap getRawNeighbors(int Sv){
        if(useEdgeCounter) return E.getAdjList().get(Sv);
        else return null;
    }

    protected void moveNode(int v, int R, int S){
        if(R > -1) invV.get(R).remove(v);
        if(S > -1) invV.get(S).add(v);
        V.set(v, S);
        
        // Do not change ordering! (it is crucial)
        if(S > -1 && getSize(S) == 1) emptyNodeStack.popInt();
        if(R > -1 && getSize(R) == 0) emptyNodeStack.push(R);
    }

    @Override
    public void addVertex(int idx) {
        super.addVertex(idx);
        if(useEdgeCounter){
            E.expand();
            EdgeCnt.add(0);
        }
        deg.add(0);
        int[] newSingleton = {n-1};
        invV.add(new IntHashSet(newSingleton));
    }

    @Override
    public void processEdge(final int src, final int dst, final boolean add) {
        //System.out.println(src + " -> " + dst + " : " + add);
        final int SRC = V.getInt(src), DST = V.getInt(dst);
        if (useEdgeCounter) {
            if (add) {
                E.updateEdge(SRC, DST, 1);
                EdgeCnt.set(SRC, EdgeCnt.getInt(SRC) + 1);
                if (!directed) {
                    E.updateEdge(DST, SRC, 1);
                    EdgeCnt.set(DST, EdgeCnt.getInt(DST) + 1);
                }
            } else {
                E.updateEdge(SRC, DST, -1);
                EdgeCnt.set(SRC, EdgeCnt.getInt(SRC) - 1);
                if (!directed) {
                    E.updateEdge(DST, SRC, -1);
                    EdgeCnt.set(DST, EdgeCnt.getInt(DST) - 1);
                }
            }
        }
        deg.set(src, deg.getInt(src) + (add ? 1 : -1));
        deg.set(dst, deg.getInt(dst) + (add ? 1 : -1));
    }

    @Override
    public void processBatch(){
    }

    protected long printForDebug(){
        return 0;
    }
}
