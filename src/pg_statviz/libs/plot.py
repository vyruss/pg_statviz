"""
pg_statviz - stats visualization and time series analysis
"""

__author__ = "Jimmy Angelakos"
__copyright__ = "Copyright (c) 2025 Jimmy Angelakos"
__license__ = "PostgreSQL License"

import importlib.resources
import matplotlib.pyplot as plt
import matplotlib.font_manager as fnt


MAX_POINTS = 100


def setup():
    for f in ["NotoSans-Regular.ttf", "NotoSans-SemiBold.ttf"]:
        f = importlib.resources.files("pg_statviz.libs").joinpath(f)
        fnt.fontManager.addfont(f)
    plt.rcParams['font.family'] = 'Noto Sans'
    plt.rcParams['font.size'] = 12
    base_image_path = importlib.resources.files("pg_statviz.libs")\
        .joinpath("pg_statviz.png")
    im = plt.imread(str(base_image_path))
    height = im.shape[0]
    fig = plt.figure(figsize=(19.2, 10.8))
    fig.figimage(im, 5, (fig.bbox.ymax - height - 6), zorder=3)
    plt.grid(visible=True)
    plt.ticklabel_format(axis='y', style='plain')
    plt.gcf().autofmt_xdate()
    return plt, fig


def setupdouble():
    plt = setup()[0]
    fig, (splt1, splt2) = plt.subplots(2, figsize=(19.2, 10.8))
    base_image_path = importlib.resources.files("pg_statviz.libs")\
        .joinpath("pg_statviz.png")
    im = plt.imread(str(base_image_path))
    height = im.shape[0]
    fig.figimage(im, 5, (fig.bbox.ymax - height - 6), zorder=3)
    for s in [splt1, splt2]:
        s.grid(visible=True)
        s.ticklabel_format(axis='y', style='plain')
    return plt, fig, splt1, splt2
