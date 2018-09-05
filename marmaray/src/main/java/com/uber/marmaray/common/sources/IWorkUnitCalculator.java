/*
 * Copyright (c) 2018 Uber Technologies, Inc.
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions
 * of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */
package com.uber.marmaray.common.sources;

import com.uber.marmaray.common.metadata.AbstractValue;
import com.uber.marmaray.common.metadata.IMetadataManager;
import com.uber.marmaray.common.metrics.IChargebackCalculator;
import com.uber.marmaray.common.metrics.IMetricable;
import com.uber.marmaray.common.sources.IWorkUnitCalculator.IWorkUnitCalculatorResult;
import java.util.List;

/**
 * It uses previous {@link IRunState} information to compute WorkUnits for next run. It will also compute
 * {@link IRunState} for the next run.
 *
 * @param <T> WorkUnit
 * @param <S> {@link IRunState} which holds run state information for the run.
 */
public interface IWorkUnitCalculator<T, S extends IRunState<S>,
    K extends IWorkUnitCalculatorResult<T, S>, V extends AbstractValue> extends IMetricable {

    /**
     * Initializes previous run state using the state stored in {@link IMetadataManager}
     * @param metadataManager
     */
    void initPreviousRunState(IMetadataManager<V> metadataManager);

    /**
     * Saves next run state in {@link IMetadataManager}
     * @param metadataManager
     * @param nextRunState
     */
    void saveNextRunState(IMetadataManager<V> metadataManager, S nextRunState);

    /**
     * It computes {@link IWorkUnitCalculatorResult} which holds information about work units for current run
     * and also about the run state for next run if the current run succeeds. Avoid saving next run state until
     * current job has succeeded.
     */
    K computeWorkUnits();

    /**
     * Compute the cost of the execution of this work unit.
     */
    void setChargebackCalculator(IChargebackCalculator calculator);

    /**
     * It is returned from {@link #computeWorkUnits()} when computation is successful. Constructor for it should be
     * kept private to ensure that it is only created from within {@link IWorkUnitCalculator}.
     *
     * @param <T> WorkUnit
     * @param <S> {@link IRunState} which holds run state information for the run.
     */
    interface IWorkUnitCalculatorResult<T, S> {

        /**
         * Returns new set of work units for the run.
         */
        List<T> getWorkUnits();

        /**
         * Returns true if there are work units available to read. It should throw
         * {@link com.uber.marmaray.common.exceptions.JobRuntimeException} if it is called before workunits are
         * computed for the current job run.
         */
        boolean hasWorkUnits();

        /**
         * Returns {@link IRunState} for next run.
         */
        S getNextRunState();
    }
}
