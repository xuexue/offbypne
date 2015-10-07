# Do not run with python 3.x
# To run THIS_FILE.py, for some directory of $YOUR_CHOICE do the following:
# git clone git@github.com:gregr/python-misc.git "$YOUR_CHOICE"/gregr_misc
# PYTHONPATH="$YOUR_CHOICE" python -i THIS_FILE.py
from gregr_misc import data, seq
from gregr_misc.logging import config as config_logging
import logging

config_logging(use_utc=False)

train_t = 'train%s.csv'
test_t = 'test%s.csv'

#initial_idx = ''
initial_idx = '0'
train = train_t % initial_idx
test = test_t % initial_idx

limit = 5000


def pairings(xs):
    return [(lhs, rhs) for lhs, rhs in seq.cross(xs, xs) if lhs < rhs]


def show_low(frame, threshold, detailed=False):
    data.show_low_uniques(frame, threshold, detailed)
    print '^^ count', threshold
    print


def show_eq(frame, count, detailed=False):
    data.show_eq_uniques(frame, count, detailed)
    print '^^ count', count
    print


def chi2(file_name, col0, col1):
    results = data.chi_squared_with(file_name, [([col0], [col1])], limit=limit)
    return [(result, data.chi_squared_prob_correlation(*result))
            for result in results]


def chi2_target(file_name, names):
    return [(name, chi2(file_name, name, 'target')) for name in names]


def show_chi2(file_name, names):
    for name, result in chi2_target(file_name, names):
        print name, result


def apply_transforms(src, tgt, transforms):
    data.apply_transforms(train_t % src, train_t % tgt, *transforms)
    data.apply_transforms(test_t % src, test_t % tgt, *transforms)


def remove_constants(frame):
    matches = data.frame_uniques_lteq(frame, 1)
    for name, _ in matches:
        del frame[name]

dirty_constants = ['VAR_0008', 'VAR_0009', 'VAR_0010', 'VAR_0011', 'VAR_0012',
                   'VAR_0018', 'VAR_0019', 'VAR_0020', 'VAR_0021', 'VAR_0022',
                   'VAR_0023', 'VAR_0024', 'VAR_0025', 'VAR_0026', 'VAR_0027',
                   'VAR_0028', 'VAR_0029', 'VAR_0030', 'VAR_0031', 'VAR_0032',
                   'VAR_0038', 'VAR_0039', 'VAR_0040', 'VAR_0041', 'VAR_0042',
                   'VAR_0043', 'VAR_0044', 'VAR_0196', 'VAR_0197', 'VAR_0199',
                   'VAR_0202', 'VAR_0203', 'VAR_0215', 'VAR_0216', 'VAR_0221',
                   'VAR_0222', 'VAR_0223', 'VAR_0229', 'VAR_0239', 'VAR_0188',
                   'VAR_0189', 'VAR_0190', 'VAR_0246', 'VAR_0394', 'VAR_0438',
                   'VAR_0446', 'VAR_0527', 'VAR_0528', 'VAR_0530']
strange_constants = ['VAR_0466']
insignificants = []  # 'VAR_0214', 'VAR_0106', 'VAR_0526', 'VAR_0529']
redundants = ['VAR_0089',  # VAR_0238
              'VAR_0227',  # VAR_0228
              'VAR_0208',  # VAR_0210
              'VAR_0210',  # VAR_0211
              'VAR_0916',  # VAR_1036
              'VAR_0051',  # VAR_0201
              'VAR_0006',  # VAR_0013
              'VAR_0506',  # VAR_0512
              'VAR_0180',  # VAR_0181
              'VAR_0181',  # VAR_0182
              'VAR_0526',  # VAR_0529
              'VAR_0260',  # VAR_0357
              'VAR_0670',  # VAR_0672
              ]


def remove_names(frame, names):
    for name in names:
        del frame[name]


def clean0():
    isrc = ''
    itgt = '0'
    train_frame = data.summarize(train_t % isrc, limit=None)
    del train_frame['ID']
    # remove constant/irrelevant train columns
    #   won't matter if non-constant in test
    remove_constants(train_frame)
    for names in [dirty_constants, strange_constants, insignificants,
                  redundants]:
        remove_names(train_frame, names)
    # produce cleaned data files; should remove at least these columns:
    # ['ID', 'VAR_0207', 'VAR_0213', 'VAR_0840', 'VAR_0847', 'VAR_1428']
    apply_transforms(isrc, itgt, train_frame.transforms())
    import sys
    sys.exit()

coverage = None
ratio = 0.01

cats_obvious = ['VAR_0001', 'VAR_0005', 'VAR_0073', 'VAR_0075', 'VAR_0156',
                'VAR_0157', 'VAR_0158', 'VAR_0159', 'VAR_0166', 'VAR_0167',
                'VAR_0168', 'VAR_0169', 'VAR_0176', 'VAR_0177', 'VAR_0178',
                'VAR_0179', 'VAR_0204', 'VAR_0214', 'VAR_0217', 'VAR_0226',
                'VAR_0230', 'VAR_0232', 'VAR_0236', 'VAR_0237', 'VAR_1934']

num_misses = ['VAR_0200', 'VAR_0404', 'VAR_0493']


def show_proportions(frame, names=cats_obvious, threshold=0.95):
    for name in cats_obvious:
        col = frame.get(name)
        if col is None:
            continue
        summ = col.summary
        total = float(sum(summ.cats.itervalues()))
        for key, count in summ.cats.iteritems():
            pp = count / total
            if pp >= threshold:
                print name, key, pp, len(summ.cats)


def show_uniques(frame):
    ranked = sorted((len(col.summary.cats), name)
                    for name, col in zip(frame.names, frame.cols))
    for count, name in ranked:
        print name, count


def intervalize(frame):
    cats_assumed = set(cats_obvious) | set(num_misses)
    intervals = []
    logging.info('enabling intervals')
    for name, col in zip(frame.names, frame.cols):
        if name not in cats_assumed:
            summ = col.summary
            summ.enable_interval()
            intervals.append((len(summ.nums), len(summ.cats), name))
    logging.info('finished enabling intervals')
    data.show_anomalies(frame, ratio=ratio, coverage=coverage)
    intervals.sort()
    return intervals


def show_intervals(intervals, frame, outfname):
    logging.info('writing intervals to %s:', outfname)
    f = open(outfname, 'w')
    for nums, cats, name in intervals:
        f.write(' '.join(map(str, (name, nums, cats))) + '\n')
    f.flush()
    logging.info('finished writing intervals to %s:', outfname)


def show_duplicates(frame, file_name):
    for pair in data.duplicates(frame, file_name):
        print pair

#clean0()
limit = None
train_frame = data.summarize(train, limit=limit)
#intervals = intervalize(train_frame)
#show_intervals(intervals, train_frame, 'train-intervals.txt')
#show_eq(train_frame, 3, True)
#show_uniques(train_frame)

#test_frame = data.summarize(test, limit=limit)
#intervals = intervalize(test_frame)
#show_intervals(intervals, test_frame, 'test-intervals.txt')
#show_eq(test_frame, 3, True)

#union_frame = train_frame.union(test_frame)
#intersect_frame = train_frame.intersection(test_frame)


def target_correlations(frame, file_name, max_arity=1, target='target',
                        limit=None):
    chi2_names = []
    pearson_names = []
    for k, names in sorted(data.cardinality_sorted_cols(frame).iteritems()):
        if k < 11:  # TODO: include anything with fewer than 11 nums instead?
            chi2_names.extend(names)
        else:
            pearson_names.extend(names)
    chi2_entries = [([name], [target]) for name in chi2_names]
    pearson_entries = [(name, target) for name in pearson_names]
    logging.info('computing chi-squared correlations with %s for %s vars'
                 + '; arity=1', target, len(chi2_entries))
    chi2s = data.chi_squared_prob_correlation_with(
        file_name, chi2_entries, limit=limit)
    chi2s_sorted_sig = sorted(
        (1.0 - sig, corr, entry)
        for (sig, corr), entry in zip(chi2s, chi2_entries))
    chi2s_sorted_cor = sorted(
        (corr, 1.0 - sig, entry)
        for (sig, corr), entry in zip(chi2s, chi2_entries))
    chi2s_sorted_sig.reverse()
    chi2s_sorted_cor.reverse()
    chi2s_sig = [(entry, (1.0 - isig, corr))
                 for isig, corr, entry in chi2s_sorted_sig]
    chi2s_cor = [(entry, (1.0 - isig, corr))
                 for corr, isig, entry in chi2s_sorted_cor]
    logging.info('computing pearson correlations with %s for %s vars',
                 target, len(pearson_entries))
    pearsons = data.pearson_correlation_with(
        file_name, pearson_entries, limit=limit)
    pearsons_sorted = sorted((abs(corr), corr, entry)
                             for corr, entry in zip(pearsons, pearson_entries))
    pearsons_sorted.reverse()
    pearsons = [(entry, corr) for acorr, corr, entry in pearsons_sorted]
    return chi2s_sig, chi2s_cor, pearsons

chi2s_sig, chi2s_cor, pearsons = target_correlations(
    train_frame, train, limit=20000)
print 'chi2s sorted by significance'
for entry, result in chi2s_sig[:20]:
    print entry, result
print
print 'chi2s sorted by correlation'
for entry, result in chi2s_cor[:20]:
    print entry, result
print
print 'pearsons'
for entry, result in pearsons[:20]:
    print entry, result


# old stuff

#make_pred = data.col_match_pred({
    #'VAR_0008': ''})
    #'VAR_0008': '', 'VAR_0009': '', 'VAR_0010': '', 'VAR_0011': '',
    #'VAR_0012': ''
    #})
#train_rows = data.filter_rows(train, make_pred, max_results=1)

#('VAR_0252', 'VAR_0281') (0.001, 0.9027356227507606)
#('VAR_0099', 'VAR_0139') (0.001, 0.9257274018899889)
#('VAR_0115', 'VAR_0139') (0.001, 0.9257274018899889)
#('VAR_1012', 'VAR_1590') (0.001, 0.9280407547581209)
#('VAR_0292', 'VAR_0312') (0.001, 0.9778208066984203)
#('VAR_0122', 'VAR_0518') (0.001, 0.9882040370898529)
#('VAR_0122', 'VAR_0513') (0.001, 0.9882072684679126)
#('VAR_0345', 'VAR_0346') (0.001, 0.9960574943704534)
#('VAR_0090', 'VAR_0091') (0.001, 0.9987483373020494)
#('VAR_0463', 'VAR_0523') (0.001, 0.9999999999999999)
#('VAR_0463', 'VAR_0524') (0.001, 0.9999999999999999)
#('VAR_0099', 'VAR_0115') (0.001, 1.0)
#('VAR_0291', 'VAR_0522') (0.001, 1.0)