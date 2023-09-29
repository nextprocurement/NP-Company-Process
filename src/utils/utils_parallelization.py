import contextlib
from typing import Dict, List, Union

import joblib
import pandas as pd
from joblib import Parallel, delayed
from tqdm import tqdm


def parallelize_function(
    func,
    data: Union[pd.Series, List],
    workers=-1,
    prefer="processes",
    output: str = "series",
    *args,
    **kwargs,
):
    results = Parallel(
        n_jobs=workers,
        prefer=prefer,
        verbose=0,
        # backend="threading", # loky, multiprocessing, threading
        # return_as="generator", # list
        # timeout=None,
        # pre_dispatch="2 * n_jobs",
        batch_size="auto",
        # max_nbytes="1M",
        # mmap_mode="r",
        # require=None,
    )(delayed(func)(x, *args, **kwargs) for x in data)
    results = list(results)
    if output == "series" and isinstance(data, pd.Series):
        return pd.Series(results, index=data.index)
    return results


@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument"""

    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()


def parallelize_function_with_progress_bar(
    func,
    data: Union[pd.Series, List],
    batch_size: int,
    desc: str = "",
    workers=-1,
    prefer="processes",
    output: str = "series",
    *args,
    **kwargs,
):
    """
    Parallelizes function execution with a progress bar and returns results.

    Parameters:
    -----------
    func:
        Function to be parallelized
    data:
        Data to be processed
    batch_size:
        Size of each batch of data
    desc:
        Description for the progress bar
    workers:
        Number of worker processes
    prefer:
        Preferred backend for parallelization
    output:
        Type of output ('series' or 'list')
    args, kwargs:
        Additional arguments for the function

    Returns:
    --------
    Processed results as a Pandas Series or List
    """

    num_batches = len(data) // batch_size + (len(data) % batch_size != 0)

    with tqdm_joblib(tqdm(desc=desc, total=num_batches)) as progress_bar:
        results = Parallel(n_jobs=workers, prefer=prefer, verbose=0, batch_size="auto")(
            delayed(func)(data[i * batch_size : (i + 1) * batch_size], *args, **kwargs)
            for i in range(num_batches)
        )
    results = list(results)
    if output == "series" and isinstance(data, pd.Series):
        return pd.Series(results, index=data.index)
    return results
