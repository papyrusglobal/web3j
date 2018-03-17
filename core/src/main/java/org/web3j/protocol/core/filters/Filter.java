package org.web3j.protocol.core.filters;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.web3j.protocol.Web3j;

import org.web3j.protocol.core.Request;
import org.web3j.protocol.core.Response;
import org.web3j.protocol.core.methods.response.EthFilter;
import org.web3j.protocol.core.methods.response.EthLog;
import org.web3j.protocol.core.methods.response.EthUninstallFilter;


/**
 * Class for creating managed filter requests with callbacks.
 */
public abstract class Filter<T> {

    private static final Logger log = LoggerFactory.getLogger(Filter.class);

    final Web3j web3j;
    final Callback<T> callback;

    private volatile EthFilter ethFilter;
    private volatile BigInteger filterId;
    private boolean filterFailed = false;

    private ScheduledFuture<?> schedule;

    public Filter(Web3j web3j, Callback<T> callback) {
        this.web3j = web3j;
        this.callback = callback;
    }

    public void run(ScheduledExecutorService scheduledExecutorService, long blockTime) {
        try {
            initFilter();

            /*
            We want the filter to be resilient against client issues. On numerous occasions
            users have reported socket timeout exceptions when connected over HTTP to Geth and
            Parity clients. For examples, refer to
            https://github.com/web3j/web3j/issues/144 and
            https://github.com/ethereum/go-ethereum/issues/15243.

            Hence we consume errors and log them as errors, allowing our polling for changes to
            resume. The downside of this approach is that users will not be notified of
            downstream connection issues. But given the intermittent nature of the connection
            issues, this seems like a reasonable compromise.

            The alternative approach would be to have another thread that blocks waiting on
            schedule.get(), catching any Exceptions thrown, and passing them back up to the
            caller. However, the user would then be required to recreate subscriptions manually
            which isn't ideal given the aforementioned issues.
            */
            schedule = scheduledExecutorService.scheduleAtFixedRate(
                    () -> {
                        try {
                            if (filterFailed) {
                                prepareForRestart();
                                initFilter();
                            }
                            this.pollFilter(ethFilter);
                        } catch (Throwable e) {
                            // All exceptions must be caught, otherwise our job terminates without
                            // any notification
                            log.error("Error sending request", e);
                        }
                    },
                    0, blockTime, TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            throwException(e);
        }
    }

    private void initFilter() throws IOException {
        ethFilter = sendRequest();
        if (ethFilter.hasError()) {
            throwException(ethFilter.getError());
        }
        filterId = ethFilter.getFilterId();
        getInitialFilterLogs();
    }

    private void getInitialFilterLogs() {
        try {
            Optional<Request<?, EthLog>> maybeRequest = this.getFilterLogs(filterId);
            EthLog ethLog = null;
            if (maybeRequest.isPresent()) {
                ethLog = maybeRequest.get().send();
            } else {
                ethLog = new EthLog();
                ethLog.setResult(Collections.emptyList());
            }
            process(ethLog.getLogs());

        } catch (IOException e) {
            throwException(e);
        }
    }

    private void pollFilter(EthFilter ethFilter) {
        EthLog ethLog = null;
        try {
            ethLog = web3j.ethGetFilterChanges(filterId).send();
        } catch (IOException e) {
            throwException(e);
        }
        if (ethLog.hasError()) {
            if (ethLog.getError().getCode() == -32000) {
                log.warn("Filter {} is probably dead on geth side", filterId);
                filterFailed = true;
            }
            throwException(ethLog.getError());
        } else {
            filterFailed = false;
            process(ethLog.getLogs());
        }
    }

    abstract EthFilter sendRequest() throws IOException;

    abstract void process(List<EthLog.LogResult> logResults);

    protected void prepareForRestart() {
        //Default implementation doing nothing
    }

    public void cancel() {
        schedule.cancel(false);

        try {
            EthUninstallFilter ethUninstallFilter = web3j.ethUninstallFilter(filterId).send();
            if (ethUninstallFilter.hasError()) {
                throwException(ethUninstallFilter.getError());
            }

            if (!ethUninstallFilter.isUninstalled()) {
                throw new FilterException("Filter with id '" + filterId + "' failed to uninstall");
            }
        } catch (IOException e) {
            throwException(e);
        }
    }

    /**
     * Retrieves historic filters for the filter with the given id.
     * Getting historic logs is not supported by all filters.
     * If not the method should return an empty EthLog object
     *
     * @param filterId Id of the filter for which the historic log should be retrieved
     * @return Historic logs, or an empty optional if the filter cannot retrieve historic logs
     */
    protected abstract Optional<Request<?, EthLog>> getFilterLogs(BigInteger filterId);

    void throwException(Response.Error error) {
        throw new FilterException("Invalid request: "
                + (error == null ? "Unknown Error" : error.getMessage()));
    }

    void throwException(Throwable cause) {
        throw new FilterException("Error sending request", cause);
    }
}

