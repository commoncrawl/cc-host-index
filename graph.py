import sys
import io

import duckdb
import pyarrow.csv as csv
import matplotlib.pyplot as plt
import surt

import duck_utils
import graph_utils
import utils


host_sql = '''
SELECT
  {cols}
FROM host_index
WHERE surt_host_name = '{surt_host_name}'{and_tld}
ORDER BY crawl ASC
'''

subdomain_sql = '''
SELECT
  {cols}
FROM host_index
WHERE surt_host_name LIKE '{surt_host_name}'{and_tld}
GROUP BY crawl
ORDER BY crawl ASC
'''

many_host_sql = '''
SELECT
  {cols}
FROM host_index
WHERE contains(ARRAY [{surt_list}], surt_host_name){and_tld}
GROUP BY crawl
ORDER BY crawl ASC
'''


host_columns = {
    # list order does matter for the graphs
    'rank': ('crawl', 'fetch_200', 'fetch_200_lote', 'prank10', 'hcrank10'),
    'fetch': ('crawl', 'fetch_200', 'fetch_gone', 'fetch_redirPerm', 'fetch_redirTemp', 'fetch_notModified', 'fetch_3xx', 'fetch_4xx', 'fetch_5xx', 'fetch_other'),
    'pagesize': ('crawl', 'warc_record_length_av', 'warc_record_length_median'),
    'nutch': ('crawl', 'nutch_numRecords', 'nutch_fetched', 'nutch_unfetched', 'nutch_gone', 'nutch_redirTemp', 'nutch_redirPerm', 'nutch_notModified'),
    'nutch_pct': ('crawl', 'nutch_fetched_pct', 'nutch_unfetched_pct', 'nutch_gone_pct', 'nutch_redirTemp_pct', 'nutch_redirPerm_pct', 'nutch_notModified_pct'),
    'nutch_all': ('crawl', 'nutch_numRecords', 'nutch_fetched', 'nutch_unfetched', 'nutch_gone', 'nutch_redirTemp', 'nutch_redirPerm', 'nutch_notModified',
                  'nutch_fetched_pct', 'nutch_unfetched_pct', 'nutch_gone_pct', 'nutch_redirTemp_pct', 'nutch_redirPerm_pct', 'nutch_notModified_pct'),
    'robots': ('crawl', 'robots_200', 'robots_gone', 'robots_redirPerm', 'robots_redirTemp', 'robots_notModified', 'robots_3xx', 'robots_4xx', 'robots_5xx', 'robots_other'),
}
domain_columns = {
    'sum': ('crawl', 'fetch_200', 'fetch_200_lote'),
}
many_host_columns = {
    'sum': ('crawl', 'fetch_200'),
    'sum_lote': ('crawl', 'fetch_200', 'fetch_200_lote'),
    'sum_nutch': ('crawl', 'fetch_200', 'fetch_200_lote', 'nutch_numRecords', 'nutch_unfetched', 'nutch_gone', 'nutch_redirTemp', 'nutch_redirPerm', 'nutch_notModified'),
}


def left_right(cols):
    # different normalizations
    cols = tuple(col for col in cols if col != 'crawl')
    rank10 = tuple(col for col in cols if col in {'hcrank10', 'prank10'})
    rank = tuple(col for col in cols if col in {'hcrank', 'prank'})
    pct = tuple(col for col in cols if col.endswith('_pct'))
    other = set(cols)
    other = other.difference(rank10).difference(rank).difference(pct)
    other = tuple(other)

    # at most two
    # put other on the left, if present
    count = [bool(rank10), bool(rank), bool(pct), bool(other)].count(True)
    if count > 2:
        raise ValueError('too many scales: '+repr(cols))

    if other:
        return other, rank10 or rank or pct  # 2nd can be None

    if rank:
        print('foo', rank, rank10, pct)
        return rank, rank10 or pct

    if pct:
        return pct, rank10

    return rank10, tuple()


def surt_host_name_to_title(surt_host_name):
    parts = list(reversed(surt_host_name.split(',')))
    if parts[0] == '':
        parts[0] = '*'
    return '.'.join(parts)


def get_values(host_index, surt_host_name, col_names, verbose=0):
    if not isinstance(surt_host_name, str):
        # if not a string, it's a list of strings
        surt_list = ','.join(f"'{s}'" for s in surt_host_name)

        tlds = set([s.split(',', 1)[0] for s in surt_host_name])
        if len(tlds) == 1:
            tld = next(iter(tlds))
            and_tld = f" AND url_host_tld = '{tld}'"
        else:
            and_tld = ''

        cols = ', '.join(f'CAST(SUM({col}) AS INT64) AS sum_{col}' for col in col_names if col != 'crawl')
        cols = 'crawl, '+cols
        sql = many_host_sql.format(cols=cols, surt_list=surt_list, and_tld=and_tld)
        if verbose:
            print(sql)
        return duckdb.sql(sql).arrow()

    tld = surt_host_name.split(',', 1)[0]
    and_tld = f" AND url_host_tld = '{tld}'"

    if surt_host_name.endswith(','):
        cols = ', '.join(f'CAST(SUM({col}) AS INT64) AS sum_{col}' for col in col_names if col != 'crawl')
        cols = 'crawl, '+cols
        sql = subdomain_sql.format(cols=cols, surt_host_name=surt_host_name+'%', and_tld=and_tld)
    else:
        cols = ', '.join(col_names)
        sql = host_sql.format(cols=cols, surt_host_name=surt_host_name, and_tld=and_tld)

    if verbose:
        print(sql)
    return duckdb.sql(sql).arrow()


def host_csv(table, fname):
    with open(fname, 'wb') as fd:
        csv.write_csv(table, fd)


def plot_values(table, col_names, title):
    df = table.to_pandas()
    cols = list(df.columns)
    left, right = left_right(cols)

    lines = []
    for name in col_names:
        if name == 'crawl':
            continue
        side = 'l' if name in left else 'r'
        # x, y, side, marker, label
        lines.append(['crawl', name, side, None, name])
    return do_plot(df, lines, title)


def do_plot(df, lines, title):
    fig, ax1 = plt.subplots()
    ax2 = None
    saw_right = False
    our_lines = []

    for i, line in enumerate(lines):
        x, y, side, marker, label = line
        yvalues = df[y].astype(float)
        if yvalues.sum() == 0.0:
            # declutter plots by suppressing all-zero lines and their legends
            continue
        xvalues = df[x].astype(str)
        xvalues = [x.replace('CC-MAIN-', '') for x in xvalues]
        ls = None
        #color, ls = graph_utils.get_color_ls(i)
        color, marker = graph_utils.get_color_marker(i)
        if side == 'l':
            our_line, = ax1.plot(xvalues, yvalues, marker=marker, label=label, color=color, ls=ls)
        else:
            if not ax2:
                ax2 = ax1.twinx()
                ###ax2.set_ylim(top=10.0)  # the legend tends to get clobbered if you do this
            our_line, = ax2.plot(xvalues, yvalues, marker=marker, label=label, color=color, ls=ls)
            saw_right = True
        our_lines.append(our_line)
    plt.xlabel('crawl')
    ax1.set_ylim(bottom=0)
    if saw_right:
        # use hcrank's color?
        ax2.set_ylabel('rank')  # color=color # XXX might be _pct
        ax2.set_ylim(bottom=0)
    plt.title(title)

    # more complicated because of the twinx
    labels = [line.get_label() for line in our_lines]
    ax1.legend(our_lines, labels, fontsize='x-small')  # default is medium
    for label in ax1.get_xticklabels():
        label.set_rotation(90)

    plt.setp(plt.gcf(), figwidth=5, figheight=5)  # "inches"
    plt.tight_layout()  # avoid clipping of the x axis labels
    buffer = io.BytesIO()
    plt.savefig(buffer, format='png', dpi=200)  # it's 1000 x 1000
    # <img style="width:500px;" src="..."> for retina
    plt.close()
    return buffer


def get_plots(host_index, surt_host_name, title, config, verbose=0):
    plots = {}
    tables = {}
    for key, cols in config.items():
        table = get_values(host_index, surt_host_name, cols, verbose=verbose)
        tables[key] = table

        # this preserves the original order, which is a good thing
        cols = table.column_names

        buff = plot_values(table, cols, title)
        plot = buff.getvalue()
        plots[key] = plot
    return tables, plots


def output_stuff(title, tables, plots,
                 do_csv=False, do_png=False, do_html=False, verbose=0, html_template='domain.html'):
    for key in tables:
        out = title + '_' + key
        if do_csv:
            host_csv(tables[key], out+'.csv')
        if do_png:
            with open (out+'.png', 'wb') as fd:
                fd.write(plots[key])
        if do_html:
            plots[key] = graph_utils.png_to_embed(plots[key])
    if do_html:
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        env = Environment(
            loader=FileSystemLoader('./templates'),
            autoescape=select_autoescape(['html']),
        )
        template = env.get_template(html_template)
        page = template.render(title=title, plots=plots)
        with open(title + '.html', 'w') as f:
            f.write(page)


def do_work(surt_host_name, host_index, title, verbose=0):
    if not isinstance(surt_host_name, str):
        config = many_host_columns
        check_sums = True
    elif surt_host_name.endswith(','):
        config = domain_columns
        check_sums = True
    else:
        config = host_columns
        check_sums = False

    if check_sums:
        for k, cols in config.items():
            if any([c.endswith('_pct') for c in cols]):
                raise ValueError('cannot sum _pct')
            if any([c in {'hcrank', 'hcrank10', 'crank', 'crank10'} for c in cols]):
                raise ValueError('cannot sum ranks')

    tables, plots = get_plots(host_index, surt_host_name, title, config, verbose=verbose)
    output_stuff(title, tables, plots, do_csv=True, do_png=True, do_html=True, verbose=verbose)


def main():
    verbose = 1
    duck_utils.init_duckdb_httpfs(verbose=verbose)
    grep = None
    #grep = 'CC-MAIN-2022'
    host_index = duck_utils.open_host_index(grep=grep, verbose=verbose)
    if len(sys.argv) > 2 and sys.argv[1] == '-f':
        assert len(sys.argv) == 3
        surts = []
        title =  sys.argv[2]
        with open(sys.argv[2], encoding='utf8') as fd:
            for thing in fd:
                surt_host_name = utils.thing_to_surt_host_name(thing.rstrip(), verbose=verbose)
                if surt_host_name:
                    surts.append(surt_host_name)
        if verbose:
            print(f'making a plot for {len(surts)} hosts')
        do_work(surts, host_index, title, verbose=verbose)
        return
    for thing in sys.argv[1:]:
        surt_host_name = utils.thing_to_surt_host_name(thing)
        if not surt_host_name:
            continue
        title = surt_host_name_to_title(surt_host_name)
        do_work(surt_host_name, host_index, title, verbose=verbose)


if __name__ == '__main__':
    main()
