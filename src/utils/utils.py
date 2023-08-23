from collections import Counter

import pandas as pd


def fill_na(cell, fill=[]):
    """
    Fill elements in pd.DataFrame with `fill`.
    """
    if hasattr(cell, "__iter__"):
        if isinstance(cell, str):
            if cell == "nan":
                return fill
            return cell
        nas = []
        for el in cell:
            if el == "nan" or pd.isna(el):
                nas.append(True)
            else:
                nas.append(False)
        if all(nas):
            return fill
        return cell
    if pd.isna(cell):
        return fill
    return cell


def fill_to_length(iterable, l):
    """
    Fill an iterable to a certain length `l`.
    """
    l_iter = len(iterable)
    iterable = iterable + [None] * (l - l_iter)
    return iterable


def suggest_value(elements):
    """
    Select elements based on appearance.
    If same number of appearances, choose the longest.
    If shorter elements are not included in the 'main' one, return all.
    """
    cnt = Counter(elements)
    cnt.pop(None, None)
    cnt = cnt.most_common()
    if cnt:
        max_cnt = cnt[0][1]
        els = sorted([k for k, v in cnt if v == max_cnt], key=lambda x: (-len(x), x))
        # return els[0]
        base = els.pop(0)
        return [base]
        # if all(
        #     [all(t in base for t in regex.sub(r"\W", " ", el).split()) for el in els]
        # ):
        #     return [base]
        # return [base] + els
    else:
        # return None
        return [None]
