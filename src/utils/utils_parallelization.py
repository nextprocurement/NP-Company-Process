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
    """
    Parallelizes function execution

    Parameters:
    -----------
    func:
        Function to be parallelized
    data:
        Data to be processed
    workers:
        Number of worker processes
    prefer:
        Preferred backend for parallelization ("processes" or "threads")
    output:
        Type of output ("series" or "list")
    args, kwargs:
        Additional arguments for the function

    Returns:
    --------
    Processed results as a Pandas Series or List
    """
    results = Parallel(n_jobs=workers, prefer=prefer, verbose=0)(
        delayed(func)(x, *args, **kwargs) for x in data
    )
    results = list(results)
    if output == "series" and isinstance(data, pd.Series):
        return pd.Series(results, index=data.index)
    return results


@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument"""

    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._tqdm_object = tqdm_object

        def __call__(self, *args, **kwargs):
            # Check if the callback has `batch_size` attribute
            if hasattr(self, "batch_size"):
                self._tqdm_object.update(n=self.batch_size)
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
    workers=-1,
    prefer="processes",
    output: str = "series",
    desc: str = "",
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
    workers:
        Number of worker processes
    prefer:
        Preferred backend for parallelization
    output:
        Type of output ("series" or "list")
    desc:
        Description for the progress bar
    args, kwargs:
        Additional arguments for the function

    Returns:
    --------
    Processed results as a Pandas Series or List
    """

    with tqdm_joblib(tqdm(desc=desc, total=len(data), leave=False)) as progress_bar:
        results = parallelize_function(
            func,
            data,
            workers=workers,
            prefer=prefer,
            *args,
            **kwargs,
        )
    if output == "series" and isinstance(data, pd.Series):
        return pd.Series(results, index=data.index)
    return results
