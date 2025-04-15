import base64


color_list = [  # IBM palette for color-blindness
    '#648FFF',  # blue
    '#785EF0',  # purple
    '#DC267F',  # magenta
    '#FE6100',  # orange
    '#FFB000',  # dark yellow
]

# https://matplotlib.org/stable/gallery/lines_bars_and_markers/linestyles.html to add more
ls_list = ['solid', 'dashed', 'dashdot', 'dotted']
marker_list = ['o', 'v', '^', 's', 'd']

combinations = len(color_list) * len(ls_list)


def get_color(i):
    assert i < combinations
    return color_list[i % 5], ls_list[i // 5]


def png_to_embed(png):
    png_b64 = base64.b64encode(png).decode('utf8')
    return '<img src="data:image/jpeg;base64,'+png_b64+'">'
