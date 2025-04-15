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
WHERE surt_host_name = '{surt_host_name}'
ORDER BY crawl ASC
'''

sub_host_sql = '''
SELECT
  {cols}
FROM host_index
WHERE surt_host_name LIKE '{surt_host_name}'
GROUP BY crawl
ORDER BY crawl ASC
'''

host_columns = {
    # list order does matter for the graphs
    'rank': ['crawl', 'fetch_200', 'fetch_200_lote', 'prank10', 'hcrank10'],
    'fetch': ['crawl', 'fetch_200', 'fetch_gone', 'fetch_redirPerm', 'fetch_redirTemp', 'fetch_notModified', 'fetch_3xx', 'fetch_4xx', 'fetch_5xx', 'fetch_other'],
    'pagesize': ['crawl', 'warc_record_length_av', 'warc_record_length_median'],
    'nutch': ['crawl', 'nutch_numRecords', 'nutch_fetched', 'nutch_unfetched', 'nutch_gone', 'nutch_redirTemp', 'nutch_redirPerm', 'nutch_notModified'],
    'robots': ['crawl', 'robots_200', 'robots_gone', 'robots_redirPerm', 'robots_redirTemp', 'robots_notModified', 'robots_3xx', 'robots_4xx', 'robots_5xx', 'robots_other'],
}
domain_columns = {
    'sum': ['crawl', 'fetch_200', 'fetch_200_lote'],
}

def surt_host_name_to_title(surt_host_name):
    parts = list(reversed(surt_host_name.split(',')))
    if parts[0] == '':
        parts[0] = '*'
    return '.'.join(parts)


def get_surt_host_name_values(host_index, surt_host_name, col_names):
    if surt_host_name.endswith(','):
        cols = ', '.join(f'CAST(SUM({col}) AS INT64) AS sum_{col}' for col in col_names)
        cols = 'crawl, '+cols
        sql = sub_host_sql.format(cols=cols, surt_host_name=surt_host_name+'%')
        print(sql)
    else:
        cols = ', '.join(col_names)
        sql = host_sql.format(cols=cols, surt_host_name=surt_host_name)
        print(sql)
    return duckdb.sql(sql).arrow()


def host_csv(table, fname):
    with open(fname, 'wb') as fd:
        csv.write_csv(table, fd)


def host_plot_values(table, col_names, title):
    df = table.to_pandas()

    # right plots
    # if hcrank or prank present, and there are other lines, hcrank and prank should be on the right y axis
    count = int('hcrank10' in df) + int('prank10' in df)
    rank_right = bool(count and len(df.columns) > count)

    lines = []
    for name in col_names:
        if name == 'crawl':
            continue
        if rank_right and name in {'hcrank10', 'prank10'}:
            side = 'r'
        else:
            side = 'l'
        lines.append(['crawl', name, side, 'o', name])
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
        color, ls = graph_utils.get_color(i)
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
        ax2.set_ylabel('rank')  # color=color
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


def get_host(host_index, surt_host_name, title):
    plots = {}
    tables = {}
    for key, cols in host_columns.items():
        table = get_surt_host_name_values(host_index, surt_host_name, cols)
        tables[key] = table
        buff = host_plot_values(table, cols, title)
        plot = buff.getvalue()
        plots[key] = plot
    return tables, plots


def plot_host(host_index, surt_host_name, title,
              do_csv=False, do_png=False, do_html=False, html_template = 'host.html'):
    tables, plots = get_host(host_index, surt_host_name, title)
    for key in tables:
        if do_csv:
            out = surt_host_name + '_' + key
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
        with open(surt_host_name + '.html', 'w') as f:
            f.write(page)


def get_domain(host_index, surt_host_name, title):
    plots = {}
    tables = {}
    for key, cols in domain_columns.items():
        table = get_surt_host_name_values(host_index, surt_host_name, cols)
        tables[key] = table
        # because this is a domain (trailing , in surt_hostname) the column names have changed
        cols = table.column_names
        buff = host_plot_values(table, cols, title)
        plot = buff.getvalue()
        plots[key] = plot
    return tables, plots


def plot_domain(host_index, surt_host_name, title,
                do_csv=False, do_png=False, do_html=False, html_template = 'domain.html'):
    tables, plots = get_domain(host_index, surt_host_name, title)
    for key in tables:
        if do_csv:
            out = surt_host_name + '_' + key
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
        with open(surt_host_name + '.html', 'w') as f:
            f.write(page)


def make_plot(surt_host_name, host_index):
    title = surt_host_name_to_title(surt_host_name)
    if surt_host_name.endswith(','):
        plot_domain(host_index, surt_host_name, title,
                    do_csv=True, do_png=True, do_html=True)
        return

    plot_host(host_index, surt_host_name, title,
              do_csv=True, do_png=True, do_html=True)


def main():
    verbose = 1
    duck_utils.init_duckdb_httpfs(verbose=verbose)
    grep = None
    #grep = 'CC-MAIN-2022'
    host_index = duck_utils.open_host_index(grep=grep, verbose=verbose)
    for thing in sys.argv[1:]:
        surt_host_name = utils.thing_to_surt_host_name(thing)
        if not surt_host_name:
            continue
        make_plot(surt_host_name, host_index)


if __name__ == '__main__':
    main()
