package io.pixelsdb.pixels.trino.vector;

import io.trino.spi.function.AccumulatorState;

import java.util.PriorityQueue;

public interface ExactNNSState
        extends AccumulatorState
{
    //todo I think each state having a priority queue should work
    //todo also I think I need to implement a state factory

    // each state maintains a priority queue, in the input function we initialize the priority queue;
    // in the combine function, we merge the priority queue from the other state to state
    // in the output function we turn the priority queue into a json

    PriorityQueue<double[]> getVecs();

    void setVecs(PriorityQueue<double[]> vecs);
}
